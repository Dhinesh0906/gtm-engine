"""Local semantic search and lookalike module. Phase 5.

Embeds companies with fastembed (ONNX, no PyTorch) and stores vectors in a separate
usearch index file keyed by company_id. The frozen SQLite schema is not modified.

# TODO(phase5-scale): product-quantized index + UMAP/cluster Segments Explorer (Phase 11 dashboard).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

import numpy as np
from sqlalchemy import func
from sqlmodel import select
from usearch.index import Index

from app.config import settings
from app.db import Company, Session, engine, init_db

logger = logging.getLogger(__name__)

EMBED_BATCH_SIZE = 32
DIM = 384  # bge-small-en and bge-small-en-v1.5

_embedder_instance: Any | None = None


def _embedder() -> Any:
    """Lazy singleton for the fastembed TextEmbedding model."""
    global _embedder_instance
    if _embedder_instance is None:
        from fastembed import TextEmbedding

        _embedder_instance = TextEmbedding(settings.embed_model)
    return _embedder_instance


def embed_texts(texts: list[str]) -> np.ndarray:
    """Return L2-normalized float32 vectors for a list of texts."""
    if not texts:
        return np.zeros((0, DIM), dtype=np.float32)
    model = _embedder()
    vectors = list(model.embed(texts))
    arr = np.stack(vectors).astype(np.float32)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return arr / norms


def company_embed_text(company: Company) -> str:
    """Return the text representation used for embedding a company."""
    parts: list[str] = []
    if company.name:
        parts.append(company.name.strip())

    # Prefer enriched signal if available.
    if company.enriched_at:
        if company.description:
            parts.append(company.description.strip())
        if company.keywords:
            parts.append(company.keywords.strip())
    elif company.web_text:
        snippet = company.web_text.strip()
        if len(snippet) > 400:
            snippet = snippet[:400]
        parts.append(snippet)

    if company.country:
        parts.append(company.country.strip())

    text = " ".join(p for p in parts if p)
    return text if text else f"company {company.id}"


def is_targetable(company: Company) -> bool:
    """Return True if the company has enough descriptive signal to be vector-indexed.

    Default INDEX_MIN_SIGNAL="description" requires a non-empty enriched description.
    Parked/junk Common Crawl domains get an empty description from Phase 4 enrichment,
    so they are excluded. Companies with web_text but not yet enriched must be enriched
    before they can enter the index. Name-only companies are raw inventory found via
    structured search and are promoted into the index lazily once enriched.
    """
    signal = settings.index_min_signal.lower()
    if signal == "all":
        return True
    if signal == "enriched":
        return bool(company.enriched_at)
    if signal == "text":
        return bool((company.description or "").strip() or (company.web_text or "").strip())
    # Default "description": real enriched description.
    return bool((company.description or "").strip())


def _targetable_count() -> int:
    """Return the number of targetable companies in the DB."""
    # Count in SQL (streaming every row through Python is fatal at millions of
    # companies). Mirror each INDEX_MIN_SIGNAL mode of is_targetable() exactly.
    signal = (settings.index_min_signal or "description").strip().lower()
    with Session(engine) as db:
        q = select(func.count()).select_from(Company)
        if signal == "all":
            pass
        elif signal == "enriched":
            q = q.where(Company.enriched_at.is_not(None))
        elif signal == "text":
            q = q.where((Company.description != "") | (Company.web_text != ""))
        else:  # "description"
            q = q.where(Company.description != "")
        return db.exec(q).one()


def _enriched_count() -> int:
    """Return the number of enriched companies in the DB."""
    with Session(engine) as db:
        # COUNT in SQL — loading every enriched row into Python just to len() it
        # allocates millions of ORM objects at scale.
        return db.exec(
            select(func.count()).select_from(Company).where(Company.enriched_at.is_not(None))
        ).one()


def _index_path() -> Path:
    path = Path(settings.vector_index_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _metadata_path() -> Path:
    path = Path(settings.vector_metadata_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _load_metadata() -> dict[int, dict[str, Any]]:
    path = _metadata_path()
    if not path.exists():
        return {}
    meta: dict[int, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            company_id = record.get("company_id")
            if isinstance(company_id, int):
                meta[company_id] = record
    return meta


def _save_metadata(meta: dict[int, dict[str, Any]]) -> None:
    path = _metadata_path()
    with path.open("w", encoding="utf-8") as fh:
        for record in meta.values():
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _load_index() -> Index | None:
    path = _index_path()
    if not path.exists():
        return None
    try:
        return Index.restore(str(path))
    except Exception:  # noqa: BLE001
        logger.warning("Failed to restore vector index from %s", path)
        return None


def _create_index() -> Index:
    return Index(ndim=DIM, metric="cos")


def build_index(limit: int | None = None, rebuild: bool = False) -> dict[str, int]:
    """Build or update the local usearch index from companies.

    Returns {"indexed": n, "skipped": n}.
    """
    init_db()
    index = _create_index() if rebuild else (_load_index() or _create_index())
    metadata = {} if rebuild else _load_metadata()
    existing_ids = set(metadata.keys()) if not rebuild else set()

    batch_ids: list[int] = []
    batch_texts: list[str] = []
    indexed = 0
    skipped = 0

    with Session(engine) as db:
        query = select(Company).order_by(Company.id)
        for company in db.exec(query):
            if limit is not None and (indexed + skipped) >= limit:
                break
            if company.id in existing_ids:
                skipped += 1
                continue
            if not is_targetable(company):
                skipped += 1
                continue

            text = company_embed_text(company)
            batch_ids.append(company.id)
            batch_texts.append(text)

            metadata[company.id] = {
                "company_id": company.id,
                "name": company.name or "",
                "domain": company.domain or "",
                "country": company.country or "",
            }

            if len(batch_ids) >= EMBED_BATCH_SIZE:
                vectors = embed_texts(batch_texts)
                index.add(batch_ids, vectors)
                indexed += len(batch_ids)
                batch_ids = []
                batch_texts = []

        if batch_ids:
            vectors = embed_texts(batch_texts)
            index.add(batch_ids, vectors)
            indexed += len(batch_ids)

    index.save(str(_index_path()))
    _save_metadata(metadata)
    return {"indexed": indexed, "skipped": skipped}


def _load_index_and_meta() -> tuple[Index | None, dict[int, dict[str, Any]]]:
    index = _load_index()
    if index is None:
        return None, {}
    meta = _load_metadata()
    return index, meta


def _company_rows(company_ids: list[int]) -> dict[int, Company]:
    """Batch-load companies by id and return a lookup dict."""
    if not company_ids:
        return {}
    with Session(engine) as db:
        rows = db.exec(select(Company).where(Company.id.in_(company_ids))).all()  # type: ignore[attr-defined]
        return {c.id: c for c in rows}


def search(
    query: str,
    k: int = 20,
    country: str | None = None,
    min_b2b: float | None = None,
    min_saas: float | None = None,
    min_tech: float | None = None,
) -> list[dict[str, Any]]:
    """Semantic search over companies, with optional structured filters."""
    index, _meta = _load_index_and_meta()
    if index is None or len(index) == 0:
        return []

    query_vec = embed_texts([query])
    matches = index.search(query_vec[0], k * 4)
    candidate_ids = [int(key) for key in matches.keys]
    companies = _company_rows(candidate_ids)

    results: list[dict[str, Any]] = []
    for key, distance in zip(matches.keys, matches.distances):
        company_id = int(key)
        company = companies.get(company_id)
        if company is None:
            continue
        if country and (company.country or "").upper() != country.upper():
            continue
        if min_b2b is not None and (company.b2b_score or 0.0) < min_b2b:
            continue
        if min_saas is not None and (company.saas_score or 0.0) < min_saas:
            continue
        if min_tech is not None and (company.tech_score or 0.0) < min_tech:
            continue
        results.append(
            {
                "company_id": company_id,
                "name": company.name or "",
                "domain": company.domain or "",
                "country": company.country or "",
                "score": round(float(1.0 - distance), 4),
            }
        )
        if len(results) >= k:
            break

    return results


def lookalike(company_id: int, k: int = 20) -> list[dict[str, Any]]:
    """Find companies similar to the given company_id."""
    index, _meta = _load_index_and_meta()
    if index is None or len(index) == 0:
        return []

    try:
        vec = index.get(company_id)
    except Exception:  # noqa: BLE001
        vec = None

    if vec is None or vec.shape[0] != DIM:
        # Fallback: re-embed the company.
        with Session(engine) as db:
            company = db.get(Company, company_id)
        if company is None:
            return []
        vec = embed_texts([company_embed_text(company)])[0]

    matches = index.search(vec, k * 4)
    candidate_ids = [int(key) for key in matches.keys if int(key) != company_id]
    companies = _company_rows(candidate_ids)

    results: list[dict[str, Any]] = []
    for key, distance in zip(matches.keys, matches.distances):
        cid = int(key)
        if cid == company_id:
            continue
        company = companies.get(cid)
        if company is None:
            continue
        results.append(
            {
                "company_id": cid,
                "name": company.name or "",
                "domain": company.domain or "",
                "country": company.country or "",
                "score": round(float(1.0 - distance), 4),
            }
        )
        if len(results) >= k:
            break

    return results


def report() -> dict[str, int]:
    """Return index coverage numbers."""
    init_db()
    with Session(engine) as db:
        total = db.exec(select(func.count()).select_from(Company)).one()
    enriched = _enriched_count()
    targetable = _targetable_count()
    index, _ = _load_index_and_meta()
    indexed = len(index) if index else 0
    return {"total": total, "enriched": enriched, "targetable": targetable, "indexed": indexed}


def _print_report(report_data: dict[str, int]) -> None:
    print(f"Total companies:     {report_data['total']}")
    print(f"Enriched companies:  {report_data['enriched']}")
    print(f"Targetable companies: {report_data['targetable']}")
    print(f"Indexed companies:   {report_data['indexed']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GTM Engine Phase 5 semantic search")
    subparsers = parser.add_subparsers(dest="command")

    build_parser = subparsers.add_parser("build", help="Build/update the vector index")
    build_parser.add_argument("--rebuild", action="store_true", help="Rebuild from scratch")
    build_parser.add_argument("--limit", type=int, default=None, help="Max companies to index")

    query_parser = subparsers.add_parser("query", help="Search companies by natural language")
    query_parser.add_argument("text", help="Query text")
    query_parser.add_argument("--k", type=int, default=10, help="Number of results")
    query_parser.add_argument("--country", type=str, default=None, help="Filter by country code")
    query_parser.add_argument("--min-b2b", type=float, default=None, help="Minimum b2b_score")
    query_parser.add_argument("--min-saas", type=float, default=None, help="Minimum saas_score")
    query_parser.add_argument("--min-tech", type=float, default=None, help="Minimum tech_score")

    lookalike_parser = subparsers.add_parser("lookalike", help="Find companies similar to a company")
    lookalike_parser.add_argument("company_id", type=int, help="Company ID")
    lookalike_parser.add_argument("--k", type=int, default=10, help="Number of results")

    subparsers.add_parser("report", help="Print index coverage report")

    args = parser.parse_args(argv)

    if args.command == "build":
        counts = build_index(limit=args.limit, rebuild=args.rebuild)
        print(f"Indexed: {counts['indexed']}  Skipped: {counts['skipped']}")
        report_data = report()
        _print_report(report_data)
        print(
            f"PHASE5 OK: {report_data['indexed']}/{report_data['targetable']} "
            f"targetable companies indexed (of {report_data['total']} total)"
        )
        return 0

    if args.command == "query":
        results = search(
            args.text,
            k=args.k,
            country=args.country,
            min_b2b=args.min_b2b,
            min_saas=args.min_saas,
            min_tech=args.min_tech,
        )
        for r in results:
            print(
                f"[{r['score']:.3f}] {r['name']!r} ({r['country']}) {r['domain']} #{r['company_id']}"
            )
        return 0 if results else 1

    if args.command == "lookalike":
        results = lookalike(args.company_id, k=args.k)
        for r in results:
            print(
                f"[{r['score']:.3f}] {r['name']!r} ({r['country']}) {r['domain']} #{r['company_id']}"
            )
        return 0 if results else 1

    if args.command == "report":
        report_data = report()
        _print_report(report_data)
        print(
            f"PHASE5 OK: {report_data['indexed']}/{report_data['targetable']} "
            f"targetable companies indexed (of {report_data['total']} total)"
        )
        return 0 if report_data["indexed"] > 0 or report_data["total"] == 0 else 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
