# Public code map

This portfolio snapshot contains 35 of the private application's 58 Python modules under `src/app/`, plus selected tests and UI templates. It shows the structure and substantial implementation without publishing the campaign writer, production prompts, live sending and reply handlers, credentials, or customer data.

| Area | Files | What they demonstrate |
| --- | --- | --- |
| Data model | `src/app/db.py`, `schema.sql` | SQLModel entities, SQLite pragmas, indexes, and the schema reference |
| Collection | `src/app/collect/`, `src/app/harvest.py` | Public-source adapters, normalization, staging, and resumable harvesting |
| Data preparation | `src/app/resolve.py`, `src/app/discover.py`, `src/app/webtext.py`, `src/app/enrich.py`, `src/app/listings.py` | Deduplication, domain discovery, website extraction, enrichment, and contact promotion |
| Contacts and search | `src/app/people.py`, `src/app/sitemail.py`, `src/app/verify.py`, `src/app/search.py`, `src/app/search_index.py`, `src/app/vertical_map.py` | Contact discovery, quality checks, indexed search, and classification |
| Product brief | `src/app/agent/brief.py` | Per-campaign facts and guardrails used by the private AI writer |
| Operator interface | `src/app/dashboard/queries.py`, `src/app/dashboard/templates/`, `src/app/dashboard/static/` | Dashboard read models and interface templates |
| Data viewer | `src/app/viewer/` | Read-only browser for the shared company pool |
| Tests | `tests/` | Selected offline checks for resolution, website text, enrichment, and search |

The retained files still reference omitted private modules. They are shared for code review and are not a standalone, runnable version of Raliess. The full system and its test suite remain private.
