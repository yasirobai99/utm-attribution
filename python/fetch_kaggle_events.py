#!/usr/bin/env python3
# Tailored to events_source.csv headers shared:
# ['CustomerID','Age','Gender','Income','CampaignChannel','CampaignType',
#  'AdSpend','ClickThroughRate','ConversionRate','WebsiteVisits','PagesPerVisit',
#  'TimeOnSite','SocialShares','EmailOpens','EmailClicks','PreviousPurchases',
#  'LoyaltyPoints','AdvertisingPlatform','AdvertisingTool','Conversion']

import os, sys, hashlib
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

RAW_PATH = "data/raw_kaggle/events_source.csv"
OUT_PATH = "data/events.csv"

# Map channel fields to utm_source/medium
def map_source_medium(row):
    # Prefer CampaignChannel; fallback to AdvertisingPlatform
    src_raw = str(row.get("CampaignChannel") if pd.notna(row.get("CampaignChannel")) else row.get("AdvertisingPlatform")).strip().lower()
    typ_raw = str(row.get("CampaignType")).strip().lower() if pd.notna(row.get("CampaignType")) else ""

    # Normalize source
    if src_raw in ["facebook","instagram","meta","social","social_media"]:
        utm_source = "meta"
    elif src_raw in ["google","search","adwords","organic"]:
        utm_source = "google"
    elif src_raw in ["linkedin"]:
        utm_source = "linkedin"
    elif src_raw in ["email","newsletter"]:
        utm_source = "newsletter"
    elif src_raw in ["direct","(direct)","none","(none)","unknown","nan"]:
        utm_source = "direct"
    else:
        # default to channel name as-is
        utm_source = src_raw if src_raw and src_raw != "nan" else "direct"

    # Medium based on type/source
    if utm_source in ["google","meta","linkedin"]:
        utm_medium = "cpc"
    elif utm_source == "newsletter":
        utm_medium = "email"
    elif utm_source == "direct":
        utm_medium = "direct"
    else:
        # fallback: if type hints organic/social/email
        if "email" in typ_raw:
            utm_medium = "email"
        elif "organic" in typ_raw:
            utm_medium = "organic"
        elif "influencer" in typ_raw or "social" in typ_raw:
            utm_medium = "social"
        else:
            utm_medium = "other"
    return utm_source, utm_medium

def norm_campaign(c):
    if pd.isna(c): return "unknown_campaign"
    s = str(c).strip().lower()
    out = []
    for ch in s:
        if ch.isalnum(): out.append(ch)
        elif ch in [" ","-",".","/","|"]: out.append("_")
        else: out.append("_")
    norm = "".join(out)
    while "__" in norm: norm = norm.replace("__","_")
    return norm.strip("_") or "unknown_campaign"

def synthetic_timestamps(df, start_date="2021-01-01", step_minutes=10):
    # No timestamp in dataset -> create a reproducible timeline per user
    start = pd.to_datetime(start_date)
    # sort by CustomerID to keep stable order
    df = df.sort_values(["CustomerID"]).reset_index(drop=True)
    # Assign a sequence number per CustomerID to simulate multiple interactions
    df["_seq"] = df.groupby("CustomerID").cumcount()
    df["event_ts"] = start + pd.to_timedelta(df.index * step_minutes, unit="m")
    return df

def main():
    if not os.path.exists(RAW_PATH):
        print(f"[ERR] Missing file: {RAW_PATH}"); sys.exit(1)

    df = pd.read_csv(RAW_PATH)
    print(f"[INFO] Loaded {len(df)} rows from events_source.csv")
    expected_cols = {"CustomerID","CampaignChannel","CampaignType","EmailOpens","EmailClicks","AdvertisingPlatform","Conversion"}
    missing = expected_cols - set(df.columns)
    if missing:
        print(f"[WARN] Missing expected columns: {missing} (script still tries to proceed)")

    # Generate synthetic timestamps and ensure CustomerID as string
    df["CustomerID"] = df["CustomerID"].astype(str)
    df = synthetic_timestamps(df, start_date="2021-01-01", step_minutes=7)

    # Derive event_type:
    # purchase if Conversion==1, else signup if EmailOpens>0 or EmailClicks>0, else page_view
    def is_true(v):
        if pd.isna(v): return False
        s = str(v).strip().lower()
        if s in ["true","yes","y","1"]: return True
        try:
            return float(s) == 1.0
        except: return False

    def event_type_row(r):
        if is_true(r.get("Conversion", 0)): return "purchase"
        email_opens = pd.to_numeric(r.get("EmailOpens", 0), errors="coerce")
        email_clicks = pd.to_numeric(r.get("EmailClicks", 0), errors="coerce")
        if (email_opens and email_opens > 0) or (email_clicks and email_clicks > 0):
            return "signup"
        return "page_view"

    et = df.apply(event_type_row, axis=1)
    us_um = df.apply(map_source_medium, axis=1, result_type="expand")
    df["utm_source"] = us_um[0]
    df["utm_medium"] = us_um[1]
    df["utm_campaign"] = df["CampaignType"].apply(norm_campaign) if "CampaignType" in df.columns else "unknown_campaign"

    # revenue unknown in this dataset
    df["revenue"] = None
    df["referrer"] = None
    df["page_url"] = None

    # event_id deterministic
    def mk_id(u,t,c):
        return hashlib.md5(f"{u}|{t}|{c}".encode("utf-8")).hexdigest()

    out = pd.DataFrame({
        "event_id": [mk_id(u, str(t), c) for u,t,c in zip(df["CustomerID"], df["event_ts"], df["utm_campaign"])],
        "user_id": df["CustomerID"].astype(str),
        "event_ts": df["event_ts"],
        "event_type": et,
        "utm_source": df["utm_source"],
        "utm_medium": df["utm_medium"],
        "utm_campaign": df["utm_campaign"],
        "referrer": df["referrer"],
        "page_url": df["page_url"],
        "revenue": df["revenue"]
    })

    # Minimal drop: ensure user_id and event_ts present
    before = len(out)
    out = out.dropna(subset=["user_id","event_ts"])
    print(f"[INFO] Rows before dropna: {before}, after: {len(out)}")

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    out.to_csv(OUT_PATH, index=False)
    print(f"[OK] Wrote {OUT_PATH} with {len(out)} rows")

if __name__ == "__main__":
    main()
