# Selected source samples

This directory contains 29 of the private application's 58 Python source files, selected to show the data platform and read-only UI layer. Files were screened for credentials, personal records, and local paths before publication.

| Area | Included modules | What they demonstrate |
| --- | --- | --- |
| Data model | `app/db.py` | SQLModel entities, SQLite pragmas, and indexes |
| Collection | `app/collect/` and `app/harvest.py` | Public-source adapters, normalization, staging, and resumable harvesting |
| Data preparation | `app/resolve.py`, `app/discover.py`, `app/webtext.py`, `app/enrich.py`, `app/listings.py` | Deduplication, domain discovery, website extraction, enrichment, and contact promotion |
| Search and quality | `app/search.py`, `app/search_index.py`, `app/people.py`, `app/verify.py`, `app/vertical_map.py` | Search, contact discovery, validation, and vertical classification |
| Read-only UI | `app/dashboard/queries.py`, `app/viewer/` | Dashboard queries and a separate data viewer |
| Interface | `app/channels/base.py` | Abstract outbound channel contract |

The sample retains imports from private modules and is provided for code review, not as a standalone application. Campaign orchestration, production prompts, sender and reply implementations, credentials, databases, contacts, and message content are not included.
