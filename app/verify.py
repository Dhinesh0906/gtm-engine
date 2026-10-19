"""Cheap deliverability checks, run at send time.

Verifying 1.5M addresses up front is wasted work: you will only ever mail a
fraction of them. This checks a domain the moment it is about to be used,
caches the answer, and writes the verdict back to contact_points so the same
domain is never looked up twice.

Only ONE thing is checked, because only one thing is knowable for free: can the
domain receive mail at all? A domain with no MX and no A record bounces 100% of
the time. Whether a specific MAILBOX exists cannot be determined without an
SMTP probe (unreliable, and harmful to sender reputation) or a paid API.
"""

from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)

try:
    import dns.resolver  # type: ignore

    _RESOLVER = dns.resolver.Resolver()
    _RESOLVER.timeout = 3.0
    _RESOLVER.lifetime = 3.0
    _DNS_AVAILABLE = True
except Exception:  # noqa: BLE001
    _DNS_AVAILABLE = False
    _RESOLVER = None

# Free providers always accept mail; never waste a lookup or mark them invalid.
FREEMAIL = {
    "gmail.com", "googlemail.com", "yahoo.com", "yahoo.co.uk", "hotmail.com",
    "hotmail.co.uk", "outlook.com", "outlook.co.uk", "live.com", "live.co.uk",
    "icloud.com", "aol.com", "btinternet.com", "sky.com", "virginmedia.com",
    "me.com", "mac.com", "protonmail.com", "proton.me",
}

# Mailboxes nobody reads, or that exist for legal/abuse purposes only.
BAD_LOCALPARTS = {
    "noreply", "no-reply", "donotreply", "do-not-reply", "postmaster", "abuse",
    "webmaster", "mailer-daemon", "unsubscribe", "bounce", "bounces", "spam",
    "privacy", "legal", "dpo", "press", "careers", "jobs", "recruitment",
}

_cache: dict[str, bool] = {}
_lock = threading.Lock()
_stats = {"lookups": 0, "cache_hits": 0, "dead": 0, "seconds": 0.0}


def is_blocked_domain(domain: str) -> bool:
    """Domains we refuse to email at all, from BLOCKED_DOMAINS.

    Separate from suppression: this is a policy decision about a whole
    provider, not a per-recipient opt-out.
    """
    try:
        from app.config import settings

        return (domain or "").strip().lower() in settings.blocked_domain_list
    except Exception:  # noqa: BLE001
        return False


def domain_accepts_mail(domain: str) -> bool:
    """True if the domain can receive mail. Cached for the process lifetime."""
    d = (domain or "").strip().lower()
    if not d:
        return False
    if d in FREEMAIL:
        return True
    with _lock:
        if d in _cache:
            _stats["cache_hits"] += 1
            return _cache[d]
    if not _DNS_AVAILABLE:
        return True  # cannot check; do not block sending

    started = time.time()
    ok = False
    try:
        ok = len(_RESOLVER.resolve(d, "MX")) > 0
    except Exception:  # NXDOMAIN / NoAnswer / timeout
        # RFC 5321: with no MX, an A record is an implicit mail destination.
        try:
            _RESOLVER.resolve(d, "A")
            ok = True
        except Exception:
            ok = False

    with _lock:
        _cache[d] = ok
        _stats["lookups"] += 1
        _stats["seconds"] += time.time() - started
        if not ok:
            _stats["dead"] += 1
    return ok


def check_address(value: str) -> tuple[bool, str]:
    """Return (deliverable, reason).

    On PASS the reason describes what was actually verified, so the caller can
    log it. "MX ok" is not a promise the mailbox exists - only that the domain
    can receive mail at all. A live domain with a dead mailbox still bounces,
    and that is the majority of bounces once dead domains are filtered out.
    """
    addr = (value or "").strip().lower()
    if "@" not in addr or addr.count("@") != 1:
        return False, "malformed address"
    local, _, domain = addr.partition("@")
    if not local or "." not in domain:
        return False, "malformed address"
    if local in BAD_LOCALPARTS:
        return False, f"unmonitored mailbox ({local}@)"
    if is_blocked_domain(domain):
        return False, f"blocked domain ({domain})"
    if domain in FREEMAIL:
        return True, f"freemail domain ({domain}) - MX assumed, mailbox UNVERIFIED"
    if not domain_accepts_mail(domain):
        return False, f"domain cannot receive mail ({domain})"
    return True, f"MX ok for {domain} - mailbox itself UNVERIFIED"


# ---------------------------------------------------------------------------
# SMTP probe - free mailbox verification
# ---------------------------------------------------------------------------
#
# Ask the recipient's own mail server whether the mailbox exists, using the
# handshake only. We issue MAIL FROM and RCPT TO and then QUIT - no DATA, so no
# message is ever sent and nothing appears in their inbox.
#
# This closes the gap MX cannot: 130dentalcentre.com has perfectly good MX and
# a dead info@ mailbox. Only the server itself knows that.
#
# Three honest caveats:
#  1. CATCH-ALL servers accept every address, so a 250 proves nothing. We
#     detect those by probing a random address first and return "unknown".
#  2. Some servers greylist, tarpit or refuse probes outright -> "unknown".
#  3. Some ISPs block outbound port 25. If so this degrades to "unknown" and
#     costs nothing but a timeout.
#
# Run from THIS machine, never from the sending IP: probing does not touch
# SES's reputation because SES is not involved.

_catchall_cache: dict[str, bool | None] = {}
_mx_cache: dict[str, list[str]] = {}


def _mx_hosts(domain: str) -> list[str]:
    if domain in _mx_cache:
        return _mx_cache[domain]
    hosts: list[str] = []
    if _DNS_AVAILABLE:
        try:
            answers = _RESOLVER.resolve(domain, "MX")
            hosts = [str(r.exchange).rstrip(".")
                     for r in sorted(answers, key=lambda a: a.preference)]
        except Exception:  # noqa: BLE001
            try:
                _RESOLVER.resolve(domain, "A")
                hosts = [domain]
            except Exception:  # noqa: BLE001
                hosts = []
    _mx_cache[domain] = hosts
    return hosts


def _rcpt_probe(host: str, addr: str, mail_from: str, timeout: float = 8.0):
    """Return (code, message) from RCPT TO, or (None, reason) on failure."""
    import smtplib

    try:
        srv = smtplib.SMTP(timeout=timeout)
        srv.connect(host, 25)
        srv.helo(mail_from.split("@", 1)[1])
        srv.mail(mail_from)
        code, msg = srv.rcpt(addr)
        try:
            srv.quit()
        except Exception:  # noqa: BLE001
            pass
        return code, (msg.decode(errors="replace") if isinstance(msg, bytes) else str(msg))
    except Exception as exc:  # noqa: BLE001
        return None, f"{type(exc).__name__}: {str(exc)[:80]}"


def smtp_probe(address: str, mail_from: str = "", timeout: float = 8.0) -> tuple[str, str]:
    """Return (verdict, detail) where verdict is valid | invalid | unknown.

    'unknown' means the server would not tell us - catch-all, greylisting,
    blocked port, or a timeout. Treat unknown as sendable; treat invalid as a
    guaranteed bounce.
    """
    addr = (address or "").strip().lower()
    if "@" not in addr:
        return "invalid", "malformed"
    domain = addr.partition("@")[2]
    if domain in FREEMAIL:
        # Gmail/Yahoo deliberately return 250 for everything at RCPT time.
        return "unknown", "freemail - provider does not disclose mailbox state"

    hosts = _mx_hosts(domain)
    if not hosts:
        return "invalid", "no MX or A record"

    mail_from = mail_from or f"postmaster@{domain}"
    host = hosts[0]

    # Is this a catch-all? Ask about an address that cannot exist.
    if domain not in _catchall_cache:
        import uuid as _uuid

        probe = f"zz-{_uuid.uuid4().hex[:12]}@{domain}"
        code, _msg = _rcpt_probe(host, probe, mail_from, timeout)
        # 250 for a random address = accepts everything = tells us nothing.
        _catchall_cache[domain] = (code is not None and 200 <= code < 300)
    if _catchall_cache.get(domain):
        return "unknown", f"{domain} is catch-all - accepts any address"

    code, msg = _rcpt_probe(host, addr, mail_from, timeout)
    if code is None:
        return "unknown", f"probe failed ({msg})"
    if 200 <= code < 300:
        return "valid", f"{host} accepted RCPT ({code})"
    if code in (450, 451, 452, 421):
        return "unknown", f"temporary refusal ({code}) - greylisting"
    if 500 <= code < 600:
        return "invalid", f"{host} rejected: {code} {msg[:80]}"
    return "unknown", f"unexpected {code}"


# ---------------------------------------------------------------------------
# Confidence scoring - a local version of what paid validators return
# ---------------------------------------------------------------------------
#
# Commercial validators (AWS included) publish roughly this signal set:
#   valid syntax · valid DNS · mailbox exists · role address · disposable ·
#   random input  ->  overall confidence high/medium/low
#
# Five of those six are computable locally. The sixth - "mailbox exists" behind
# a catch-all - is not computable by ANYONE from the protocol; AWS answers it
# from bounce history accumulated across every SES customer, which is a data
# advantage rather than a cleverer algorithm. Roughly 30-40% of B2B domains are
# catch-all, and on those every validator is guessing.
#
# We do have one signal AWS does not: whether the address was scraped off the
# company's own live website today. For a small local business that is stronger
# evidence than anything a protocol check can give.

_DISPOSABLE = {
    "mailinator.com", "guerrillamail.com", "10minutemail.com", "tempmail.com",
    "temp-mail.org", "throwawaymail.com", "yopmail.com", "trashmail.com",
    "getnada.com", "sharklasers.com", "maildrop.cc", "dispostable.com",
    "fakeinbox.com", "mailnesia.com", "mytemp.email", "emailondeck.com",
    "spamgourmet.com", "mohmal.com", "burnermail.io", "temp-mail.io",
}

_VOWELS = set("aeiouy")


def looks_random(local: str) -> bool:
    """Heuristic for machine-generated local parts like 'x7f2k9qz'.

    Real mailboxes are pronounceable or structured (info, j.smith, reception).
    Random strings have few vowels, mixed digits and no separators.
    """
    s = (local or "").lower()
    if len(s) < 8:
        return False
    if any(ch in s for ch in "._-+"):
        return False           # structured, e.g. j.smith
    letters = [c for c in s if c.isalpha()]
    if not letters:
        return True
    vowel_ratio = sum(1 for c in letters if c in _VOWELS) / len(letters)
    digit_ratio = sum(1 for c in s if c.isdigit()) / len(s)
    # Very few vowels, or a long digit-heavy blob.
    return vowel_ratio < 0.18 or (digit_ratio > 0.45 and len(s) >= 10)


def score_address(value: str, source: str = "", probe: bool = True,
                  mail_from: str = "") -> dict:
    """Return a validator-style verdict for one address.

    {"confidence": "high"|"medium"|"low", "signals": {...}, "reasons": [...]}

    low     -> do not send, it will bounce or is worthless
    medium  -> send, but it is unverifiable (catch-all or freemail)
    high    -> the mail server confirmed the mailbox, or the company publishes
               this address on its own live site
    """
    addr = (value or "").strip().lower()
    sig: dict = {}
    reasons: list[str] = []

    # syntax
    ok_syntax = ("@" in addr and addr.count("@") == 1
                 and "." in addr.partition("@")[2]
                 and " " not in addr and len(addr) <= 254)
    sig["valid_syntax"] = ok_syntax
    if not ok_syntax:
        return {"confidence": "low", "signals": sig, "reasons": ["malformed"]}

    local, _, domain = addr.partition("@")

    sig["is_role_address"] = local in BAD_LOCALPARTS
    if sig["is_role_address"]:
        reasons.append(f"unmonitored role mailbox ({local}@)")

    sig["is_blocked"] = is_blocked_domain(domain)
    if sig["is_blocked"]:
        # Hard stop. Never worth a probe or an LLM call.
        return {"confidence": "low", "signals": sig,
                "reasons": [f"{domain} is on the blocked list"]}

    sig["is_disposable"] = domain in _DISPOSABLE
    if sig["is_disposable"]:
        reasons.append("disposable domain")

    sig["is_random_input"] = looks_random(local)
    if sig["is_random_input"]:
        reasons.append("local part looks machine-generated")

    sig["is_freemail"] = domain in FREEMAIL
    sig["from_company_website"] = (source or "").lower() == "site"

    sig["valid_dns"] = domain in FREEMAIL or domain_accepts_mail(domain)
    if not sig["valid_dns"]:
        reasons.append(f"{domain} cannot receive mail")

    sig["mailbox_exists"] = "unknown"
    if probe and sig["valid_dns"] and not sig["is_disposable"]:
        # Prefer a self-hosted Reacher if one is configured - it handles many
        # more provider quirks than the probe below. Falls back automatically
        # when it is not running, so this is never a hard dependency.
        verdict = detail = None
        try:
            from app import reacher as _reacher

            if _reacher.is_configured():
                res = _reacher.check(addr)
                if res:
                    verdict, detail = res["verdict"], res["detail"]
                    sig["backend"] = "reacher"
                    sig["is_catch_all"] = res["is_catch_all"]
                    sig["has_full_inbox"] = res["has_full_inbox"]
                    if res["is_role"]:
                        sig["is_role_address"] = True
                    if res["is_disposable"]:
                        sig["is_disposable"] = True
        except Exception:  # noqa: BLE001
            verdict = None

        if verdict is None:
            verdict, detail = smtp_probe(addr, mail_from)
            sig["backend"] = "builtin"

        sig["mailbox_exists"] = verdict
        sig["probe_detail"] = detail
        if verdict == "invalid":
            reasons.append(f"server rejected the mailbox: {str(detail)[:60]}")

    # ---- combine -------------------------------------------------------
    if (not sig["valid_dns"] or sig["is_disposable"]
            or sig["is_role_address"] or sig["is_random_input"]
            or sig["mailbox_exists"] == "invalid"):
        confidence = "low"
    elif sig["mailbox_exists"] == "valid" or sig["from_company_website"]:
        # Either the server confirmed it, or the business publishes it today.
        confidence = "high"
    else:
        # Catch-all or freemail: unverifiable by anyone. Worth sending, but
        # this is the bucket where bounces still come from.
        confidence = "medium"
        reasons.append("unverifiable (catch-all or freemail) - sending on faith")

    return {"confidence": confidence, "signals": sig, "reasons": reasons}


def stats() -> dict:
    with _lock:
        s = dict(_stats)
    s["cached_domains"] = len(_cache)
    s["dns_available"] = _DNS_AVAILABLE
    if s["lookups"]:
        s["avg_ms"] = round(1000 * s["seconds"] / s["lookups"], 1)
    return s
