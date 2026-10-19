# Public code map

This portfolio snapshot contains 35 of the private application's 58 Python modules, plus selected tests and UI templates. It shows the structure and substantial implementation without publishing the campaign writer, production prompts, live sending and reply handlers, credentials, or customer data.

| Area | Files | What they demonstrate |
| --- | --- | --- |
| Data model | `app/db.py`, `schema.sql` | SQLModel entities, SQLite pragmas, indexes, and the schema reference |
| Collection | `app/collect/`, `app/harvest.py` | Public-source adapters, normalization, staging, and resumable harvesting |
| Data preparation | `app/resolve.py`, `app/discover.py`, `app/webtext.py`, `app/enrich.py`, `app/listings.py` | Deduplication, domain discovery, website extraction, enrichment, and contact promotion |
| Contacts and search | `app/people.py`, `app/sitemail.py`, `app/verify.py`, `app/search.py`, `app/search_index.py`, `app/vertical_map.py` | Contact discovery, quality checks, indexed search, and classification |
| Product brief | `app/agent/brief.py` | Per-campaign facts and guardrails used by the private AI writer |
| Operator interface | `app/dashboard/queries.py`, `app/dashboard/templates/`, `app/dashboard/static/` | Dashboard read models and interface templates |
| Data viewer | `app/viewer/` | Read-only browser for the shared company pool |
| Tests | `tests/` | Selected offline checks for resolution, website text, enrichment, and search |

The retained files still reference omitted private modules. They are shared for code review and are not a standalone, runnable version of Raliess. The full system and its test suite remain private.
