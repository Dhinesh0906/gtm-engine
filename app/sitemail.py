"""Find a live email address on a company's own website.

NO LLM. Pulling an address out of HTML is a regular expression, not a reasoning
task - so this costs a couple of HTTP requests and a few milliseconds of CPU,
not a model call. That makes it cheap enough to run inline while sending.

Crawled exactly once per company, ever. The SiteCrawl table records the
outcome, including "we looked and found nothing", so a fruitless site is never
revisited.

Why bother: listing data (Overture/OSM) carries addresses scraped years ago.
The business's own website is live right now, so an address on it is far more
likely to be real. This turns a dead lead into a fresh one instead of
discarding it.
"""

from __future__ import annotations

import logging
import re
import socket
import threading
from urllib.parse import urljoin, urlparse

from sqlmodel import Session, select

from app.config import settings
from app.db import Company, ContactPoint, Person, SiteCrawl, engine, now
from app.verify import BAD_LOCALPARTS, FREEMAIL, check_address

logger = logging.getLogger(__name__)

try:
    import httpx
except ImportError:  # pragma: no cover
    httpx = None  # type: ignore

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_MAILTO_RE = re.compile(r'mailto:([^"\'>\s?]+)', re.IGNORECASE)

# Pages most likely to carry a contact address, in the order worth trying.
# Homepage first (it often has the address in the header or footer), then the
# pages a business actually puts one on: contact, support, customer service.
# Only the first CRAWL_MAX_PAGES of these are ever fetched, so adding a path
# costs nothing on sites that answer early.
#
# ORDERED BY DISTINCT VALUE, NOT BY SPELLING.
#
# The budget is small, so the first few entries must be different KINDS of
# page. Listing /contact, /contact-us, /contactus, /contact.html back to back
# spent the whole budget on one idea and never reached /support or
# /customer-service at all.
_CONTACT_PATHS = (
    "",                    # homepage - header/footer address
    "/contact",
    "/contact-us",
    "/support",
    "/customer-service",
    "/about",
    # Rarer spellings and fallbacks, reached only when the ones above 404.
    "/contactus", "/contact.html", "/contact-us.html",
    "/customer-support", "/customers", "/help",
    "/get-in-touch", "/reach-us", "/enquiries", "/about-us",
    "/impressum",          # legally required contact page on EU/DE sites
)

# A contact address we are happy to stop on: on the company's own domain and
# an address a business publishes deliberately. Anything weaker (a freemail
# address, or a personal name) is kept but we keep looking for a better one,
# because the first address on a homepage is often a web designer's.
_PREFERRED_LOCALS = ("info", "hello", "contact", "enquiries", "enquiry",
                     "reception", "bookings", "booking", "admin", "office",
                     "mail", "support", "reservations", "sales")

# Junk that shows up in scraped HTML and is never a real business contact.
_JUNK_DOMAINS = ("sentry.io", "wixpress.com", "example.com", "godaddy.com",
                 "squarespace.com", "shopify.com", "cloudflare.com",
                 "w3.org", "schema.org", "googleapis.com", "gstatic.com")
_JUNK_EXT = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js")

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


def _clean(addr: str) -> str:
    a = addr.strip().strip(".,;:()<>\"'").lower()
    a = a.split("?")[0]
    return a


def extract_emails(html: str, site_domain: str = "") -> list[str]:
    """Pull plausible contact addresses out of a page, best first."""
    found: list[str] = []
    for raw in _MAILTO_RE.findall(html) + _EMAIL_RE.findall(html):
        a = _clean(raw)
        if not a or a in found:
            continue
        if any(a.endswith(e) for e in _JUNK_EXT):
            continue
        local, _, dom = a.partition("@")
        if not dom or "." not in dom:
            continue
        if any(j in dom for j in _JUNK_DOMAINS):
            continue
        if local in BAD_LOCALPARTS:
            continue
        if len(a) > 100:
            continue
        found.append(a)

    # Keep ONLY addresses that plausibly belong to this business:
    #   * on the site's own domain (or a subdomain of it), or
    #   * a free provider, which is normal for a small local business.
    # Anything on an unrelated third-party domain is template boilerplate,
    # an agency's address, or spam. Such addresses are excluded to avoid
    # contacting someone unrelated to the business.
    root = _root_domain(site_domain)
    kept: list[str] = []
    for a in found:
        dom = a.partition("@")[2]
        if root and (dom == root or dom.endswith("." + root)):
            kept.append(a)
        elif dom in FREEMAIL:
            kept.append(a)
    found = kept

    def score(a: str) -> tuple:
        local, _, dom = a.partition("@")
        on_site = bool(root) and (dom == root or dom.endswith("." + root))
        preferred = local in ("info", "hello", "contact", "enquiries",
                              "enquiry", "reception", "bookings", "admin",
                              "office", "mail")
        return (not on_site, dom in FREEMAIL, not preferred, len(a))

    return sorted(set(found), key=score)


def _root_domain(domain: str) -> str:
    """Strip a leading www. so example.com and www.example.com compare equal."""
    d = (domain or "").strip().lower()
    return d[4:] if d.startswith("www.") else d


_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_DESC_RE = re.compile(
    r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
    re.IGNORECASE | re.DOTALL,
)
_OG_DESC_RE = re.compile(
    r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
    re.IGNORECASE | re.DOTALL,
)
_TAG_RE = re.compile(r"<[^>]+>")


def extract_company_info(html: str) -> dict:
    """Pull a usable description out of the page. NO LLM.

    The <title> and meta description are written by the business to describe
    itself - which is exactly what the email writer needs for a specific
    opening line. A model adds nothing a regex cannot get here.
    """
    def clean(s: str) -> str:
        s = _TAG_RE.sub(" ", s or "")
        s = re.sub(r"\s+", " ", s).strip()
        return s[:400]

    title = clean(_TITLE_RE.search(html).group(1)) if _TITLE_RE.search(html) else ""
    desc = ""
    for rx in (_DESC_RE, _OG_DESC_RE):
        m = rx.search(html)
        if m:
            desc = clean(m.group(1))
            if desc:
                break
    return {"title": title, "description": desc}


_DNS_CACHE: dict[str, bool] = {}
_DNS_LOCK = threading.Lock()

# One HTTP client per THREAD, reused across every request that thread makes.
# httpx.get() builds a fresh client each call, so a 14-request crawl paid for
# 14 TCP handshakes and 14 TLS negotiations against the same server.
_LOCAL = threading.local()


def _client() -> "httpx.Client":
    c = getattr(_LOCAL, "client", None)
    if c is None:
        kw = dict(
            timeout=httpx.Timeout(connect=settings.crawl_connect_timeout,
                                  read=settings.crawl_read_timeout,
                                  write=5.0, pool=5.0),
            follow_redirects=True,
            headers={"User-Agent": _UA},
            limits=httpx.Limits(max_keepalive_connections=4, max_connections=8),
            verify=False,       # expired certs are common and not our problem
        )
        try:
            c = httpx.Client(**kw)
        except ValueError:
            # A proxy variable httpx cannot parse - socks5h:// without the
            # socks extra installed, typically from a VPN client. That raised
            # on client construction and killed EVERY crawl thread. Ignore the
            # environment and crawl directly rather than crawl nothing.
            logger.warning("ignoring unusable proxy settings from the "
                           "environment; crawling direct")
            c = httpx.Client(trust_env=False, **kw)
        _LOCAL.client = c
    return c


def domain_resolves(domain: str) -> bool:
    """Does this hostname exist at all? Cached per process.

    THE SINGLE BIGGEST WIN IN THE CRAWLER.

    A dead domain used to cost 14 HTTP attempts - 7 paths over https, then 7
    more over http - each waiting the full timeout. That is up to two minutes
    of doing nothing, per company, and most uncrawled domains are dead. One DNS
    lookup answers it in milliseconds.
    """
    with _DNS_LOCK:
        hit = _DNS_CACHE.get(domain)
    if hit is not None:
        return hit
    try:
        socket.getaddrinfo(domain, None)
        ok = True
    except Exception:  # noqa: BLE001
        ok = False
    with _DNS_LOCK:
        _DNS_CACHE[domain] = ok
    return ok


def _rank(addr: str, site_domain: str) -> tuple:
    """Sort key: lower is better. Same idea as extract_emails' scorer."""
    local, _, dom = addr.partition("@")
    root = _root_domain(site_domain)
    on_own = bool(root) and (dom == root or dom.endswith("." + root))
    return (not on_own, dom in FREEMAIL, local not in _PREFERRED_LOCALS, len(addr))


def best_email(emails: list[str], site_domain: str) -> str:
    """The best address found ACROSS every page, not the first one seen.

    extract_emails() sorts within a single page, but the crawler accumulates
    across pages - so a weak address on the homepage (dave@, or a freemail
    one) stayed at position 0 and was used even after /contact yielded a
    proper bookings@ address. Re-rank the union before choosing.
    """
    return sorted(set(emails), key=lambda a: _rank(a, site_domain))[0] if emails else ""


def _is_good(addr: str, site_domain: str) -> bool:
    """Worth stopping the crawl for: the business's own published address."""
    if not addr:
        return False
    local, _, dom = addr.partition("@")
    root = _root_domain(site_domain)
    on_own_domain = bool(root) and (dom == root or dom.endswith("." + root))
    return on_own_domain and local in _PREFERRED_LOCALS


def _fetch(url: str, timeout: float | None = None) -> str:
    if httpx is None:
        return ""
    try:
        r = _client().get(url)
        ctype = r.headers.get("content-type", "")
        if r.status_code == 200 and "text/html" in ctype:
            return r.text[:400_000]
    except Exception:  # noqa: BLE001
        pass
    return ""


def crawl_company_email(company_id: int, db: Session) -> dict:
    """Fetch a company's site and return the best contact address found.

    Returns {"status", "email", "found"}. Records the attempt in SiteCrawl so
    this company is never crawled again, whatever the outcome.
    """
    existing = db.exec(
        select(SiteCrawl).where(SiteCrawl.company_id == company_id)
    ).first()
    if existing:
        return {"status": "cached", "email": existing.best_email,
                "found": existing.emails_found}

    company = db.get(Company, company_id)
    domain = (company.domain or "").strip().lower() if company else ""
    domain = domain.replace("https://", "").replace("http://", "").strip("/")

    if not domain or "." not in domain:
        db.add(SiteCrawl(company_id=company_id, domain="", status="no_domain"))
        db.commit()
        return {"status": "no_domain", "email": "", "found": 0}

    # DNS FIRST. A hostname that does not resolve cannot be fetched, so make
    # one cheap lookup instead of 14 doomed requests that each wait out a
    # timeout. Most uncrawled domains in listing data are dead, so this is
    # where nearly all the wasted time was going.
    if not domain_resolves(domain):
        db.add(SiteCrawl(company_id=company_id, domain=domain, status="unreachable"))
        db.commit()
        return {"status": "unreachable", "email": "", "found": 0}

    emails: list[str] = []
    info: dict = {}
    reached = False
    tried = 0                       # REQUESTS made, not pages parsed
    budget = settings.crawl_max_pages

    for scheme in ("https://", "http://"):
        for path in _CONTACT_PATHS:
            # Count ATTEMPTS. Counting only successful fetches let a site that
            # 404s everything burn all 17 paths on both schemes - 34 requests
            # for nothing, which is the slow case we are trying to remove.
            if tried >= budget:
                break
            tried += 1
            html = _fetch(urljoin(f"{scheme}{domain}", path))
            if not html:
                continue
            reached = True
            if not info.get("description") and not info.get("title"):
                info = extract_company_info(html)   # homepage describes it best
            emails.extend(e for e in extract_emails(html, domain) if e not in emails)

            # STOP ON A GOOD ADDRESS, NOT MERELY ON ANY ADDRESS.
            #
            # The old condition was `if emails and path`, so an address on the
            # homepage never stopped the loop and it fetched every remaining
            # page for nothing. Stopping on the FIRST address of any kind is
            # the opposite mistake: the first address on a homepage is often
            # the web designer's or a generic freemail one, and the real
            # contact address is on /contact.
            #
            # So: stop immediately on a proper business address, otherwise keep
            # looking within the page budget and keep the weak one as a
            # fallback.
            if _is_good(best_email(emails, domain), domain):
                break
        # Only fall back to http:// if https:// never reached the server at
        # all. If it answered but held no address, plain http is the same
        # server and will not hold one either - that was seven more requests
        # per site for nothing.
        if reached or tried >= budget:
            break

    # Best across ALL pages visited, not the first one seen.
    best = best_email(emails, domain)
    status = "ok" if best else ("no_email" if reached else "unreachable")

    # Store what the business says about itself. This is free (regex over the
    # title and meta description) and gives the writer a specific opening line
    # instead of a generic one about the trade.
    if company and (info.get("description") or info.get("title")):
        blurb = info.get("description") or info.get("title")
        if blurb and len(blurb) > len(company.description or ""):
            company.description = blurb[:400]
        if info.get("title") and not company.web_text:
            company.web_text = f"{info.get('title','')}\n{info.get('description','')}"[:2000]
        db.add(company)

    db.add(SiteCrawl(company_id=company_id, domain=domain, status=status,
                     emails_found=len(emails), best_email=best))
    db.commit()

    if best:
        logger.info("crawl: %s -> %s", domain, best)
    return {"status": status, "email": best, "found": len(emails),
            "description": info.get("description", "")}


def attach_email(company_id: int, address: str, db: Session) -> int | None:
    """Store a crawled address as a ContactPoint. Returns its id."""
    addr = _clean(address)
    ok, _why = check_address(addr)
    if not ok:
        return None

    existing = db.exec(
        select(ContactPoint).where(ContactPoint.value == addr)
    ).first()
    if existing:
        return existing.id

    person = db.exec(select(Person).where(Person.company_id == company_id)).first()
    if not person:
        person = Person(company_id=company_id, source="site")
        db.add(person)
        db.flush()

    cp = ContactPoint(
        person_id=person.id, type="email", value=addr,
        verified=0, verify_status="unknown",
        confidence=85,          # from the company's own site: better than a listing
        source="site",
    )
    db.add(cp)
    db.commit()
    db.refresh(cp)
    return cp.id


def already_crawled(company_id: int, db: Session) -> bool:
    return db.exec(
        select(SiteCrawl.id).where(SiteCrawl.company_id == company_id)
    ).first() is not None
