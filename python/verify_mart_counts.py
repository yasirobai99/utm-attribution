#!/usr/bin/env python3
import yaml, psycopg2

CFG_PATH = "config/db_config.yml"
queries = [
    "SELECT COUNT(*) FROM mart.sessions;",
    "SELECT COUNT(*) FROM mart.conversions;",
    "SELECT COUNT(*) FROM mart.touchpoints;",
    "SELECT COUNT(*) FROM mart.attribution_first_touch;",
    "SELECT COUNT(*) FROM mart.attribution_last_touch;",
    "SELECT COUNT(*) FROM mart.attribution_position;",
    "SELECT COUNT(*) FROM mart.channel_roi;",
    "SELECT COUNT(*) FROM mart.v_attribution;",
    "SELECT COUNT(*) FROM mart.v_roi;"
]

cfg = yaml.safe_load(open(CFG_PATH))
conn = psycopg2.connect(
    host=cfg["host"], port=cfg.get("port", 5432),
    dbname=cfg["dbname"], user=cfg["user"], password=cfg["password"]
)
cur = conn.cursor()
for q in queries:
    cur.execute(q)
    print(q, "=>", cur.fetchone()[0])
cur.close(); conn.close()
