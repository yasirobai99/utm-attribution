#!/usr/bin/env python3
# Tailored to spend_source.csv headers shared:
# ['Campaign_ID','Company','Campaign_Type','Target_Audience','Duration','Channel_Used',
#  'Conversion_Rate','Acquisition_Cost','ROI','Location','Language','Clicks',
#  'Impressions','Engagement_Score','Customer_Segment','Date']

import os, sys
import pandas as pd

RAW_PATH = "data/raw_kaggle/spend_source.csv"
OUT_PATH = "data/ad_spend.csv"

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

def map_source_medium(src_raw, camp_type):
    src = str(src_raw).strip().lower() if pd.notna(src_raw) else "direct"
    ctyp = str(camp_type).strip().lower() if pd.notna(camp_type) else ""
    # source normalization
    if src in ["facebook","instagram","meta","social","social_media"]:
        utm_source = "meta"
    elif src in ["google","search","adwords","organic"]:
        utm_source = "google"
    elif src in ["linkedin"]:
        utm_source = "linkedin"
    elif src in ["email","newsletter"]:
        utm_source = "newsletter"
    elif src in ["direct","(direct)","none","(none)","unknown","nan"]:
        utm_source = "direct"
    else:
        utm_source = src if src and src != "nan" else "direct"
    # medium
    if utm_source in ["google","meta","linkedin"]:
        utm_medium = "cpc"
    elif utm_source == "newsletter":
        utm_medium = "email"
    elif utm_source == "direct":
        utm_medium = "direct"
    else:
        # fallback by campaign type hints
        if "email" in ctyp:
            utm_medium = "email"
        elif "organic" in ctyp:
            utm_medium = "organic"
        elif "influencer" in ctyp or "social" in ctyp:
            utm_medium = "social"
        else:
            utm_medium = "other"
    return utm_source, utm_medium

def main():
    if not os.path.exists(RAW_PATH):
        print(f"[ERR] Missing file: {RAW_PATH}"); sys.exit(1)
    df = pd.read_csv(RAW_PATH)
    print(f"[INFO] Loaded {len(df)} rows from spend_source.csv")
    expected = {"Campaign_Type","Channel_Used","Acquisition_Cost","Date"}
    missing = expected - set(df.columns)
    if missing:
        print(f"[WARN] Missing expected columns: {missing} (script will still proceed if possible)")

    # Map to utm fields
    utm = df.apply(lambda r: map_source_medium(r.get("Channel_Used"), r.get("Campaign_Type")), axis=1, result_type="expand")
    df["utm_source"] = utm[0]
    df["utm_medium"] = utm[1]
    df["utm_campaign"] = df["Campaign_Type"].apply(norm_campaign) if "Campaign_Type" in df.columns else "unknown_campaign"

    # Cost proxy: Acquisition_Cost (no Spend column available)
    if "Acquisition_Cost" in df.columns:
        cost = pd.to_numeric(df["Acquisition_Cost"], errors="coerce")
    else:
        # If truly missing, fallback to 0 to avoid break, but log
        print("[WARN] No Acquisition_Cost found; defaulting cost=0")
        cost = pd.Series([0]*len(df))

    # Date
    date = pd.to_datetime(df["Date"], errors="coerce").dt.date if "Date" in df.columns else pd.NaT

    out = pd.DataFrame({
        "date": date,
        "utm_source": df["utm_source"],
        "utm_medium": df["utm_medium"],
        "utm_campaign": df["utm_campaign"],
        "cost": cost.fillna(0)
    })
    before = len(out)
    out = out.dropna(subset=["date"])
    print(f"[INFO] Rows before dropna: {before}, after: {len(out)}")

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    out.to_csv(OUT_PATH, index=False)
    print(f"[OK] Wrote {OUT_PATH} with {len(out)} rows")

if __name__ == "__main__":
    main()
