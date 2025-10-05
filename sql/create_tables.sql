-- sql/create_tables.sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.raw_web_events (
  event_id     TEXT PRIMARY KEY,
  user_id      TEXT,
  event_ts     TIMESTAMP,
  event_type   TEXT,
  utm_source   TEXT,
  utm_medium   TEXT,
  utm_campaign TEXT,
  referrer     TEXT,
  page_url     TEXT,
  revenue      NUMERIC
);

CREATE TABLE IF NOT EXISTS raw.ad_spend (
  date         DATE,
  utm_source   TEXT,
  utm_medium   TEXT,
  utm_campaign TEXT,
  cost         NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_events_user_ts ON raw.raw_web_events(user_id, event_ts);
CREATE INDEX IF NOT EXISTS idx_events_utm      ON raw.raw_web_events(utm_source, utm_medium, utm_campaign);
CREATE INDEX IF NOT EXISTS idx_spend_date_utm  ON raw.ad_spend(date, utm_source, utm_medium, utm_campaign);
