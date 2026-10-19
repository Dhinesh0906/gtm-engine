# Selected source samples

These files are a screened subset of the private Raliess application. They show source ingestion, staging, website text extraction, address quality checks, SQLite search indexing, and the channel interface.

| File | Demonstrates |
| --- | --- |
| `app/collect/staging.py` | Normalization, validation, resumable JSONL staging, and source outcomes |
| `app/collect/edgar.py` | SEC EDGAR source adapter |
| `app/collect/gleif.py` | GLEIF source adapter |
| `app/collect/canada_corporations.py` | Canadian corporation source adapter |
| `app/webtext.py` | Website text extraction, caching, and retry behavior |
| `app/search_index.py` | SQLite FTS index lifecycle |
| `app/verify.py` | Contact address quality checks |
| `app/channels/base.py` | Channel interface |

The samples retain imports from private modules and are provided for code review. They are not a standalone application. No production configuration, credentials, databases, contacts, or messages are included.
