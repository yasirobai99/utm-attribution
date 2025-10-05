-- sql/powerbi_views.sql
-- Flatted views for easy import into Power BI

-- Combined attribution view (stack the three models)
DROP VIEW IF EXISTS mart.v_attribution;
CREATE VIEW mart.v_attribution AS
SELECT
  conversion_id,
  model,
  -- derive date from conversion join
  (SELECT event_ts::date FROM mart.conversions c WHERE c.conversion_id = a.conversion_id) AS date,
  utm_source, utm_medium, utm_campaign,
  weight,
  credit_conversions,
  credit_revenue
FROM (
  SELECT * FROM mart.attribution_first_touch
  UNION ALL
  SELECT * FROM mart.attribution_last_touch
  UNION ALL
  SELECT * FROM mart.attribution_position
) a;

-- ROI view
DROP VIEW IF EXISTS mart.v_roi;
CREATE VIEW mart.v_roi AS
SELECT
  date,
  utm_source, utm_medium, utm_campaign,
  conversions, customers, revenue, cost, cpa, cac, roas
FROM mart.channel_roi;
