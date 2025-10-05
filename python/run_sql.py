#!/usr/bin/env python3
import os, sys, yaml, psycopg2

CFG_PATH = "config/db_config.yml"

def read_cfg():
    with open(CFG_PATH, "r") as f:
        return yaml.safe_load(f)

def exec_sql_file(conn, path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def main():
    if len(sys.argv) < 2:
        print("Usage: python python/run_sql.py <sql_file1> [<sql_file2> ...]")
        sys.exit(1)

    cfg = read_cfg()
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"]
    )
    try:
        for sql_path in sys.argv[1:]:
            print(f"Executing {sql_path} ...")
            exec_sql_file(conn, sql_path)
        print("Done.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
