# Deployment topology

The full system runs locally with a Python environment, a SQLite database, DuckDB scratch space, source-data storage, and a model endpoint. The operator starts the model service and the FastAPI dashboard, then triggers bounded outreach batches from the UI. Source harvesting and website crawling can run as separate long-lived jobs.

Amazon SES handles outbound SMTP delivery. S3 stores inbound messages, while SQS delivers email events for bounce and delivery tracking. Credentials, verified sending domains, storage buckets, model files, service scripts, and live environment settings are private and are not part of this portfolio edition.

This folder documents the deployment boundary; it is not a deployable package. The [architecture guide](../docs/ARCHITECTURE.md) and [root README](../README.md) explain the application flow.
