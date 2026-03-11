import json
import pandas as pd
import os
from pathlib import Path

BASE_DIR = Path(os.path.abspath(__file__)).parent.parent.parent
RAW_FILE = os.path.join(BASE_DIR, "data/raw/members/legislators.json")
OUT_DIR = os.path.join(BASE_DIR, "data/clean")
os.makedirs(OUT_DIR, exist_ok=True)

conversion = {}
fec_conversion = {}

with open(RAW_FILE, "r") as f:
    data = json.load(f)

rows = []

for person in data:
    bioguide = person["id"].get("bioguide")
    lis = person["id"].get("lis")
    fec = person["id"].get("fec")
    conversion[lis] = bioguide
    if fec:
        for fec_code in fec:
            fec_conversion[fec_code] = bioguide

    name = person["name"].get("official_full")

    start = person["terms"][0].get("start")
    info = person["terms"][-1]
    chamber = "house" if info["type"] == "rep" else "senate"

    rows.append({
        "bioguide_id": bioguide,
        "name": name,
        "state": info.get("state"),
        "party": info.get("party"),
        "rep_chamber": chamber,
        "start": start,
        "end": info.get("end")
    })

with open(OUT_DIR + "/lookup.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(conversion))

with open(OUT_DIR + "/fec_cand.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(fec_conversion))

df = pd.DataFrame(rows).dropna()
df.to_csv(f"{OUT_DIR}/people.csv", index=False)

print("Saved people.csv with", len(df), "rows")
