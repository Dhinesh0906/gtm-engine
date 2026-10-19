# Architecture

Raliess has two work loops sharing one transactional database.

## Shared data loop

1. Source adapters read public company and place datasets.
2. A staging writer normalizes records, records provenance, and checkpoints each source.
3. DuckDB resolves duplicate entities in disk-backed passes.
4. SQLite stores companies, people, contact points, and source history.
5. Website extraction and enrichment add searchable descriptions and scores.
6. Text and vector indexes make the pool useful to every product campaign.

The published `src/app/collect/`, `src/app/resolve.py`, `src/app/webtext.py`, and `src/app/search.py` files show this loop. Source outcomes distinguish a clean skip from a failed import, and repeat runs update known records rather than duplicating them.

## Campaign loop

1. An operator supplies a product website or brief.
2. The private campaign agent profiles the offer and proposes ideal-customer segments.
3. Search and filters find candidate companies from the shared pool.
4. Contact discovery and verification rank reachable addresses.
5. Research and writing produce reviewable outreach drafts.
6. Delivery checks gate cold sends through Amazon SES.
7. Inbound mail and SES events update conversations and analytics.

The campaign writer, production prompts, sender, reply handler, and operator actions are intentionally omitted from the public snapshot. Their responsibilities and data flow are documented in the root README and [AI workflow](AI_WORKFLOW.md).

## Data boundaries

The full installation keeps public source data, prospect records, message content, credentials, and model configuration on the operator's machine or configured storage. This repository contains code samples and fictional examples only. `schema.sql` describes the main data model without publishing records.

## Concurrency and recovery

SQLite WAL mode permits readers while workers write. Long-running source jobs use checkpoints. Bulk matching is moved to DuckDB so the entire pool need not fit in Python memory. The operator dashboard and read-only data viewer are separate interfaces to the stored data.
