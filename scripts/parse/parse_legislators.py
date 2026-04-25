import json
import pandas as pd
import os
from pathlib import Path
from collections import Counter

# file paths
BASE_DIR = "./"
RAW_FILE = os.path.join(BASE_DIR, "data/raw/members/legislators.json")
COMM_FILE = os.path.join(BASE_DIR, "data/raw/members/committee_members.json")
COMM_LIST_FILE = os.path.join(BASE_DIR, "data/raw/members/committees.json")
OUT_DIR = os.path.join(BASE_DIR, "data/clean")
os.makedirs(OUT_DIR, exist_ok=True)

# get conversions from lis and fec to bioguide id for standardization
lis_conversion = {}
fec_conversion = {}

with open(RAW_FILE, "r") as f:
    data = json.load(f)

rows = []

for person in data:
    # get all their id versions
    bioguide = person["id"].get("bioguide")
    lis = person["id"].get("lis")
    fec = person["id"].get("fec")
    if lis:
        lis_conversion[lis] = bioguide
    if fec:
        for fec_code in fec:
            fec_conversion[fec_code] = bioguide

    # get info
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

with open(COMM_LIST_FILE, "r") as f:
    committees = json.load(f)
    committee_map = {}
    for committee in committees:
        thomas = committee.get("thomas_id")
        id = ("H" + committee.get("house_committee_id") if committee.get("house_committee_id") else committee.get("senate_committee_id"))
        committee_map[committee.get("thomas_id")] = {"committee_id": id + "00", "committee_name": committee.get("name")}
        for sub in committee.get("subcommittees", []):
            committee_map[thomas + sub.get("thomas_id")] = {"committee_id": id + sub.get("thomas_id"), "committee_name": sub.get("name")}

with open(COMM_FILE, "r") as f:
    committee_member_map = json.load(f)

committee_rows = []
for committee, members in committee_member_map.items():
    committee_id = committee_map[committee]
    for member in members:
        committee_rows.append({
            "committee_id": committee_id["committee_id"],
            "bioguide_id": member["bioguide"],
        })

member_counts = Counter([row['committee_id'] for row in committee_rows])

# update the committee_map data to include the count
committee_list = []
for thomas_id, info in committee_map.items():
    c_id = info['committee_id']
    info['member_count'] = member_counts.get(c_id, 0)
    committee_list.append(info)

# save conversion jsons
with open(OUT_DIR + "/lis_bio.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(lis_conversion))

with open(OUT_DIR + "/fec-cand_bio.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(fec_conversion))

# save people table
df = pd.DataFrame(rows).dropna()
df.to_csv(f"{OUT_DIR}/people.csv", index=False)

print("Saved people.csv with", len(df), "rows")

# save committee members table
df = pd.DataFrame(committee_rows).dropna()
df.to_csv(f"{OUT_DIR}/committees_members.csv", index=False)

print("Saved committees_members.csv with", len(df), "rows")

# save committee members table
df = pd.DataFrame(committee_list).dropna().sort_values("committee_id")
df.to_csv(f"{OUT_DIR}/committees.csv", index=False)

print("Saved committees.csv with", len(df), "rows")
