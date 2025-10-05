-- sql/build_models.sql
-- Builds sessionization, conversions, touchpoints, attribution models, and ROI mart.

SET search_path TO public;

-- Ensure mart schema exists
CREATE SCHEMA IF NOT EXISTS mart;

-- 1) Sessionization (30-minute timeout or campaign change)
DROP TABLE IF EXISTS mart.sessions;
CREATE TABLE mart.sessions AS
WITH base AS (
  SELECT
    e.*,
    LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) AS prev_ts,
    LAG(utm_campaign) OVER (PARTITION BY user_id ORDER BY event_ts) AS prev_campaign
  FROM raw.raw_web_events e
),
flags AS (
  SELECT
    user_id,
    event_id,
    event_ts,
    utm_source,
    utm_medium,
    utm_campaign,
    referrer,
    page_url,
    revenue,
    CASE
      WHEN prev_ts IS NULL THEN 1
      WHEN event_ts - prev_ts > INTERVAL '30 minutes' THEN 1
      WHEN utm_campaign IS DISTINCT FROM prev_campaign THEN 1
      ELSE 0
    END AS is_session_start
  FROM base
),
sessionized AS (
  SELECT
    user_id,
    event_id,
    event_ts,
    utm_source,
    utm_medium,
    utm_campaign,
    referrer,
    page_url,
    revenue,
    SUM(is_session_start) OVER (PARTITION BY user_id ORDER BY event_ts
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_num
  FROM flags
),
with_id AS (
  SELECT
    user_id,
    event_id,
    event_ts,
    utm_source,
    utm_medium,
    utm_campaign,
    referrer,
    page_url,
    revenue,
    CONCAT(user_id, '_', session_num) AS session_id
  FROM sessionized
)
SELECT
  s.session_id,
  s.user_id,
  MIN(s.event_ts) AS session_start,
  MAX(s.event_ts) AS session_end,
  -- entry utm values = from the first event in this session
  (ARRAY_AGG(utm_source ORDER BY event_ts ASC))[1]  AS entry_source,
  (ARRAY_AGG(utm_medium ORDER BY event_ts ASC))[1]  AS entry_medium,
  (ARRAY_AGG(utm_campaign ORDER BY event_ts ASC))[1] AS entry_campaign
FROM with_id s
GROUP BY s.session_id, s.user_id;

CREATE INDEX IF NOT EXISTS idx_sessions_user ON mart.sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_time ON mart.sessions(session_start, session_end);

-- 2) Conversions (signup/purchase)
DROP TABLE IF EXISTS mart.conversions;
CREATE TABLE mart.conversions AS
WITH conv AS (
  SELECT
    e.event_id,
    e.user_id,
    e.event_ts,
    e.event_type,
    e.revenue,
    -- find session_id for each event by joining where event_ts within session bounds
    s.session_id
  FROM raw.raw_web_events e
  LEFT JOIN mart.sessions s
    ON s.user_id = e.user_id
   AND e.event_ts BETWEEN s.session_start AND s.session_end
  WHERE e.event_type IN ('signup', 'purchase')
)
SELECT
  event_id AS conversion_id,
  user_id,
  session_id,
  event_ts,
  CASE WHEN event_type='signup' THEN 'signup'
       WHEN event_type='purchase' THEN 'purchase'
       ELSE 'other' END AS conversion_type,
  revenue
FROM conv;

CREATE INDEX IF NOT EXISTS idx_conversions_user ON mart.conversions(user_id);
CREATE INDEX IF NOT EXISTS idx_conversions_session ON mart.conversions(session_id);

-- 3) Touchpoints per conversion (ordered)
DROP TABLE IF EXISTS mart.touchpoints;
CREATE TABLE mart.touchpoints AS
WITH eligible AS (
  SELECT
    c.conversion_id,
    c.user_id,
    c.session_id AS conv_session_id,
    c.event_ts   AS conversion_ts,
    se.session_id,
    se.entry_source AS utm_source,
    se.entry_medium AS utm_medium,
    se.entry_campaign AS utm_campaign,
    se.session_start
  FROM mart.conversions c
  JOIN mart.sessions se
    ON se.user_id = c.user_id
   AND se.session_start <= c.event_ts  -- touch must occur before conversion
)
SELECT
  conversion_id,
  user_id,
  session_id,
  conversion_ts,
  utm_source,
  utm_medium,
  utm_campaign,
  ROW_NUMBER() OVER (PARTITION BY conversion_id ORDER BY session_start) AS touch_order,
  COUNT(*) OVER (PARTITION BY conversion_id) AS total_touches
FROM eligible
ORDER BY conversion_id, touch_order;

CREATE INDEX IF NOT EXISTS idx_touchpoints_conv ON mart.touchpoints(conversion_id);

-- 4) Attribution Models

-- 4a) First-Touch (100% to earliest touch)
DROP TABLE IF EXISTS mart.attribution_first_touch;
CREATE TABLE mart.attribution_first_touch AS
WITH ft AS (
  SELECT
    t.*,
    FIRST_VALUE(utm_source)  OVER (PARTITION BY conversion_id ORDER BY touch_order) AS ft_source,
    FIRST_VALUE(utm_medium)  OVER (PARTITION BY conversion_id ORDER BY touch_order) AS ft_medium,
    FIRST_VALUE(utm_campaign)OVER (PARTITION BY conversion_id ORDER BY touch_order) AS ft_campaign
  FROM mart.touchpoints t
)
SELECT
  conversion_id,
  'first_touch'::text AS model,
  ft_source  AS utm_source,
  ft_medium  AS utm_medium,
  ft_campaign AS utm_campaign,
  1.0::numeric AS weight,
  1.0::numeric AS credit_conversions,
  COALESCE((SELECT revenue FROM mart.conversions c WHERE c.conversion_id = ft.conversion_id), 0)::numeric AS credit_revenue
FROM ft
GROUP BY conversion_id, ft_source, ft_medium, ft_campaign;

CREATE INDEX IF NOT EXISTS idx_attr_ft ON mart.attribution_first_touch(utm_source, utm_medium, utm_campaign);

-- 4b) Last-Touch (100% to latest touch)
DROP TABLE IF EXISTS mart.attribution_last_touch;
CREATE TABLE mart.attribution_last_touch AS
WITH lt AS (
  SELECT
    t.*,
    LAST_VALUE(utm_source)  OVER (PARTITION BY conversion_id ORDER BY touch_order
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lt_source,
    LAST_VALUE(utm_medium)  OVER (PARTITION BY conversion_id ORDER BY touch_order
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lt_medium,
    LAST_VALUE(utm_campaign)OVER (PARTITION BY conversion_id ORDER BY touch_order
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lt_campaign
  FROM mart.touchpoints t
)
SELECT
  conversion_id,
  'last_touch'::text AS model,
  lt_source  AS utm_source,
  lt_medium  AS utm_medium,
  lt_campaign AS utm_campaign,
  1.0::numeric AS weight,
  1.0::numeric AS credit_conversions,
  COALESCE((SELECT revenue FROM mart.conversions c WHERE c.conversion_id = lt.conversion_id), 0)::numeric AS credit_revenue
FROM lt
GROUP BY conversion_id, lt_source, lt_medium, lt_campaign;

CREATE INDEX IF NOT EXISTS idx_attr_lt ON mart.attribution_last_touch(utm_source, utm_medium, utm_campaign);

-- 4c) Position-Based (U-Shape 40/20/40)
DROP TABLE IF EXISTS mart.attribution_position;
CREATE TABLE mart.attribution_position AS
WITH weights AS (
  SELECT
    conversion_id,
    user_id,
    session_id,
    conversion_ts,
    utm_source,
    utm_medium,
    utm_campaign,
    touch_order,
    total_touches,
    CASE
      WHEN total_touches = 1 THEN 1.0
      WHEN total_touches = 2 THEN 0.5
      WHEN touch_order = 1 THEN 0.4
      WHEN touch_order = total_touches THEN 0.4
      ELSE 0.2::numeric / NULLIF((total_touches - 2), 0)
    END AS weight
  FROM mart.touchpoints
),
revenue_map AS (
  SELECT conversion_id, COALESCE(revenue,0)::numeric AS revenue
  FROM mart.conversions
)
SELECT
  w.conversion_id,
  'position_based'::text AS model,
  w.utm_source,
  w.utm_medium,
  w.utm_campaign,
  w.weight,
  w.weight AS credit_conversions,
  (w.weight * rm.revenue) AS credit_revenue
FROM weights w
LEFT JOIN revenue_map rm
  ON rm.conversion_id = w.conversion_id;

CREATE INDEX IF NOT EXISTS idx_attr_pos ON mart.attribution_position(utm_source, utm_medium, utm_campaign);

-- 5) ROI by date x campaign (you can choose a primary model; here we use position-based)
DROP TABLE IF EXISTS mart.channel_roi;
CREATE TABLE mart.channel_roi AS
WITH convs AS (
  SELECT
    c.conversion_id,
    c.event_ts::date AS date,
    c.conversion_type,
    COALESCE(c.revenue,0)::numeric AS revenue
  FROM mart.conversions c
),
attr AS (
  SELECT
    a.conversion_id,
    a.utm_source, a.utm_medium, a.utm_campaign,
    a.weight, a.credit_conversions, a.credit_revenue
  FROM mart.attribution_position a
),
joined AS (
  SELECT
    convs.date,
    attr.utm_source, attr.utm_medium, attr.utm_campaign,
    SUM(attr.credit_conversions) AS conversions,
    SUM(CASE WHEN convs.conversion_type='purchase' THEN attr.weight ELSE 0 END) AS customers,
    SUM(attr.credit_revenue) AS revenue
  FROM convs
  JOIN attr ON attr.conversion_id = convs.conversion_id
  GROUP BY convs.date, attr.utm_source, attr.utm_medium, attr.utm_campaign
),
with_spend AS (
  SELECT
    j.date,
    j.utm_source, j.utm_medium, j.utm_campaign,
    j.conversions,
    j.customers,
    j.revenue,
    COALESCE(s.cost,0)::numeric AS cost
  FROM joined j
  LEFT JOIN raw.ad_spend s
    ON s.date = j.date
   AND s.utm_source = j.utm_source
   AND s.utm_medium = j.utm_medium
   AND s.utm_campaign = j.utm_campaign
)
SELECT
  date,
  utm_source, utm_medium, utm_campaign,
  conversions,
  customers,
  revenue,
  cost,
  CASE WHEN conversions > 0 THEN ROUND(cost::numeric / conversions, 4) ELSE NULL END AS cpa,
  CASE WHEN customers  > 0 THEN ROUND(cost::numeric / customers, 4)  ELSE NULL END AS cac,
  CASE WHEN cost > 0 THEN ROUND(revenue::numeric / cost, 4) ELSE NULL END AS roas
FROM with_spend;

CREATE INDEX IF NOT EXISTS idx_roi_date_utm ON mart.channel_roi(date, utm_source, utm_medium, utm_campaign);
