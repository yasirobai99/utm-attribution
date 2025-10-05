#!/usr/bin/env python3
# python/load_postgres.py
import os, yaml, psycopg2
import pandas as pd

CFG_PATH = "config/db_config.yml"
SQL_CREATE = "sql/create_tables.sql"
EVENTS_CSV = "data/events.csv"
SPEND_CSV  = "data/ad_spend.csv"

def read_cfg():
    with open(CFG_PATH, "r") as f:
        return yaml.safe_load(f)

def exec_sql_file(conn, path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def copy_from_df(conn, df: pd.DataFrame, table: str):
    from io import StringIO
    buf = StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cols = ",".join(df.columns)
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {table};")
        cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH CSV", buf)
    conn.commit()

def main():
    cfg = read_cfg()
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"]
    )
    try:
        print(f"Executing {SQL_CREATE} ...")
        exec_sql_file(conn, SQL_CREATE)

        # Load events
        events_cols = [
            "event_id","user_id","event_ts","event_type",
            "utm_source","utm_medium","utm_campaign",
            "referrer","page_url","revenue"
        ]
        df_events = pd.read_csv(EVENTS_CSV)
        df_events = df_events[events_cols]
        df_events["event_ts"] = pd.to_datetime(df_events["event_ts"], errors="coerce")
        print(f"Loading {len(df_events)} rows into raw.raw_web_events ...")
        copy_from_df(conn, df_events, "raw.raw_web_events")

        # Load ad_spend
        spend_cols = ["date","utm_source","utm_medium","utm_campaign","cost"]
        df_spend = pd.read_csv(SPEND_CSV)
        df_spend = df_spend[spend_cols]
        df_spend["date"] = pd.to_datetime(df_spend["date"], errors="coerce").dt.date
        print(f"Loading {len(df_spend)} rows into raw.ad_spend ...")
        copy_from_df(conn, df_spend, "raw.ad_spend")

        # Correctly unpack scalars from fetchone()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.raw_web_events;")
            ev_ct = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM raw.ad_spend;")
            sp_ct = cur.fetchone()[0]
        print(f"Loaded raw_web_events: {ev_ct:,}; ad_spend: {sp_ct:,}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
