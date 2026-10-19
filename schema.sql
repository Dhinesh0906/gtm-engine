-- A company. Global and deduped by domain. Shared across all products.
CREATE TABLE companies (
  id            INTEGER PRIMARY KEY,
  domain        TEXT UNIQUE,               -- normalized, no www, may be NULL if unknown
  name          TEXT NOT NULL DEFAULT '',
  country       TEXT NOT NULL DEFAULT '',
  industry_nace TEXT NOT NULL DEFAULT '',  -- filled in enrichment
  description   TEXT NOT NULL DEFAULT '',   -- AI-written, what they do
  keywords      TEXT NOT NULL DEFAULT '',   -- newline separated
  web_text      TEXT NOT NULL DEFAULT '',   -- cleaned homepage text (Phase 3)
  size_estimate INTEGER,                    -- employee count estimate, nullable
  size_range    TEXT NOT NULL DEFAULT '',
  b2b_score     REAL,                       -- 0..1, nullable until enriched
  saas_score    REAL,
  startup_score REAL,
  tech_score    REAL,
  source        TEXT NOT NULL DEFAULT '',   -- primary source tag
  enriched_at   TEXT,                       -- ISO datetime, NULL until enriched
  created_at    TEXT NOT NULL
);

-- Where each company's data came from. One row per source that mentioned it.
CREATE TABLE company_sources (
  id          INTEGER PRIMARY KEY,
  company_id  INTEGER NOT NULL,
  source      TEXT NOT NULL,               -- common_crawl | gleif | edgar | osm | companies_house | places
  external_id TEXT NOT NULL DEFAULT '',
  raw_json    TEXT NOT NULL DEFAULT ''
);

-- A person at a company. Global and deduped.
CREATE TABLE people (
  id          INTEGER PRIMARY KEY,
  company_id  INTEGER,                      -- nullable; may know person before company row
  first_name  TEXT NOT NULL DEFAULT '',
  last_name   TEXT NOT NULL DEFAULT '',
  title       TEXT NOT NULL DEFAULT '',
  source      TEXT NOT NULL DEFAULT '',
  created_at  TEXT NOT NULL
);

-- A way to reach a person on a channel. Polymorphic. This is why there is no email column.
CREATE TABLE contact_points (
  id            INTEGER PRIMARY KEY,
  person_id     INTEGER NOT NULL,
  type          TEXT NOT NULL,              -- email | reddit | x | linkedin | phone | discord
  value         TEXT NOT NULL,             -- the address/handle/number
  verified      INTEGER NOT NULL DEFAULT 0, -- 0/1
  verify_status TEXT NOT NULL DEFAULT 'unknown', -- valid | invalid | catch_all | unknown
  confidence    INTEGER NOT NULL DEFAULT 0, -- 0..100
  source        TEXT NOT NULL DEFAULT '',
  created_at    TEXT NOT NULL
);

-- One product you are promoting.
CREATE TABLE campaigns (
  id          INTEGER PRIMARY KEY,
  domain      TEXT NOT NULL,
  name        TEXT NOT NULL DEFAULT '',
  summary     TEXT NOT NULL DEFAULT '',
  value_props TEXT NOT NULL DEFAULT '',     -- newline separated
  proof_points TEXT NOT NULL DEFAULT '',    -- newline separated
  status      TEXT NOT NULL DEFAULT 'new',  -- new|profiled|segmented|running|paused
  created_at  TEXT NOT NULL
);

-- A customer hypothesis within a campaign.
CREATE TABLE segments (
  id             INTEGER PRIMARY KEY,
  campaign_id    INTEGER NOT NULL,
  name           TEXT NOT NULL,
  description    TEXT NOT NULL DEFAULT '',
  fit_score      INTEGER NOT NULL DEFAULT 0,   -- 0..100
  angle          TEXT NOT NULL DEFAULT '',
  titles         TEXT NOT NULL DEFAULT '',     -- newline separated
  search_queries TEXT NOT NULL DEFAULT '',     -- newline separated
  status         TEXT NOT NULL DEFAULT 'testing', -- testing|working|scaling|paused
  created_at     TEXT NOT NULL
);

-- Links a person to a campaign+segment as a target. Enforces multi-product targeting.
CREATE TABLE assignments (
  id               INTEGER PRIMARY KEY,
  campaign_id      INTEGER NOT NULL,
  segment_id       INTEGER,
  person_id        INTEGER NOT NULL,
  contact_point_id INTEGER NOT NULL,           -- which channel address we will use
  status           TEXT NOT NULL DEFAULT 'new', -- new|queued|sent|replied|bounced|suppressed
  created_at       TEXT NOT NULL
);

-- Every message in or out, any channel.
CREATE TABLE messages (
  id           INTEGER PRIMARY KEY,
  assignment_id INTEGER NOT NULL,
  campaign_id  INTEGER NOT NULL,
  channel      TEXT NOT NULL DEFAULT 'email',  -- email|reddit|x|linkedin|...
  step         INTEGER NOT NULL DEFAULT 1,     -- 1=opener, 2+=follow-up, 99=human reply
  direction    TEXT NOT NULL DEFAULT 'out',    -- out|in
  subject      TEXT NOT NULL DEFAULT '',
  body         TEXT NOT NULL DEFAULT '',
  status       TEXT NOT NULL DEFAULT 'draft',  -- draft|queued|sent|failed|received
  intent       TEXT NOT NULL DEFAULT '',       -- for inbound classification
  provider_id  TEXT NOT NULL DEFAULT '',
  scheduled_at TEXT,
  sent_at      TEXT,
  created_at   TEXT NOT NULL
);

-- Never contact again. Global, per channel.
CREATE TABLE suppressions (
  id         INTEGER PRIMARY KEY,
  channel    TEXT NOT NULL DEFAULT 'email',
  value      TEXT NOT NULL,                    -- the contact value (email addr etc)
  reason     TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);

-- Powers the warmup ramp and the daily cap. One row per channel per day.
CREATE TABLE send_log (
  id      INTEGER PRIMARY KEY,
  channel TEXT NOT NULL DEFAULT 'email',
  day     TEXT NOT NULL,                        -- YYYY-MM-DD
  count   INTEGER NOT NULL DEFAULT 0
);

-- Enforces the GLOBAL frequency cap: one person, across ALL products and channels.
CREATE TABLE contact_history (
  id           INTEGER PRIMARY KEY,
  person_id    INTEGER NOT NULL,
  campaign_id  INTEGER NOT NULL,
  channel      TEXT NOT NULL DEFAULT 'email',
  contacted_at TEXT NOT NULL                    -- ISO datetime
);

-- Per-message persona + routing metadata. One row per outbound message.
CREATE TABLE message_meta (
  id              INTEGER PRIMARY KEY,
  message_id      INTEGER NOT NULL UNIQUE,
  rfc_message_id  TEXT NOT NULL DEFAULT '',
  in_reply_to     TEXT NOT NULL DEFAULT '',
  references_ids  TEXT NOT NULL DEFAULT '',
  s3_key          TEXT NOT NULL DEFAULT '',
  from_addr       TEXT NOT NULL DEFAULT '',
  to_addr         TEXT NOT NULL DEFAULT '',
  persona_id      TEXT NOT NULL DEFAULT '',
  created_at      TEXT NOT NULL
);

-- SES email events (delivery, bounce, complaint, open, click, etc.).
-- Populated in Phases 8 (send events) and 9 (inbound/reply matching).
CREATE TABLE email_events (
  id              INTEGER PRIMARY KEY,
  ses_message_id  TEXT NOT NULL DEFAULT '',
  message_id      INTEGER,
  event_type      TEXT NOT NULL DEFAULT '',
  occurred_at     TEXT NOT NULL DEFAULT '',
  raw_json        TEXT NOT NULL DEFAULT '',
  created_at      TEXT NOT NULL
);
