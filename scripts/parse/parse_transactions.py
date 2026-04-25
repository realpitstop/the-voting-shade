import json
import os
import re
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from rapidfuzz import fuzz, process
import numpy as np

# file paths
BASE_DIR = "./"
OUTPUT_DIR = os.path.join(BASE_DIR, "data/clean/")
INPUT_DIR = os.path.join(BASE_DIR, "data/raw/pacs/")
NAME_SIC_PATH = os.path.join(OUTPUT_DIR, "name_sic.json")
INDUSTRY_PATH = os.path.join(OUTPUT_DIR, "sic_meaning.json")

pacs = pd.read_csv(os.path.join(INPUT_DIR, "pacs.csv"))
transactions = pd.read_csv(os.path.join(INPUT_DIR, "transactions.csv"))

# fill empty orgs with committee names
pacs["CONNECTED_ORG_NM"] = pacs["CONNECTED_ORG_NM"].fillna(pacs["CMTE_NM"])

# get list of id's for every org
name_to_id = pacs.groupby('CONNECTED_ORG_NM')['CMTE_ID'].apply(list).to_dict()

# map name to sic code
with open(NAME_SIC_PATH, "r", encoding="utf-8") as f:
    SIC_MAP: dict = json.load(f)

# map sic code to it's industry meaning
with open(INDUSTRY_PATH, "r", encoding="utf-8") as f:
    INDUSTRY_MAP: dict = json.load(f)

# pre normalized names from SEC map
SEC_NAMES_RAW = pd.Series(list(SIC_MAP.keys()))

# suffixes that will be removed in the order of the list
suffixes = [
    " SEPARATE SEGREGATED FUND", " POLITICAL ACTION COMMITTEE", " CORP", " INC", " LLC", " PAC", " LTD", " PLC", " LLP", " CO", " GMBH",
    " SA", " BV", " LP", " PA", " PC", " CORPORATION", " INCORPORATED", " LIMITED LIABILITY COMPANY", " HOLDING COMPANY", " ENTERPRISES",
    " MANUFACTURING", " HOLDING CO", " OPERATING", " UNLIMITED", " SERVICES", " PHARMACY", " COMPANY", " LIMITED", " GROUP", " SERVICES",
    " SERVICE", " FUNDS", " HOTELS", " TRAVEL", " AGENCY CORP", " STORE", " SALES", " SVCS", " COMP", " PLLC", " INTL", " DMD", " MFG"
]

# words that will be removed anywhere in the order they appear
removes = [
    "ADVOCACY POLITICAL ACTION COMMITTEE", "POLITICAL ACTION COMMITTEE I", "POLITICAL ACTION COMMITTEE", "POLITICAL PARTICIPATION COMMITTEE", "POLITICAL ACTION COMM", "EMPLOYEES",
    "EMPLOYEE", "FEDERAL PAC", "MUTUAL LIFE INSURANCE", "MUTUAL INSURANCE", "INSURANCE", "ET AL", "COMMITTEE FOR GOOD GOVERNMENT"
]

# prefixes that will be removed in the order of the list
prefixes = ["THE "]

def normalize_text(text):
    # uppercase text
    text = text.upper()
    
    # remove any word that has PAC in it, things after slashes, things in parentheses, and replace characters
    text = (re.sub(r"\b\w*PAC\w*\s*", "", re.sub(r"/[^/]*", "", re.sub(r"\([^)]*\)", "", text)))
            .replace(". ", " ")
            .replace(".", "")
            .replace(",", "")
            .replace("&", "AND")
            .replace("'", "")
            .replace(" - ", " ")
            .replace("-", " ")
            .replace("AMERICAS", "AMERICA")).strip()
    
    for remove in removes:
        text = text.replace(remove, "")
    text = text.strip()

    for prefix in prefixes:
        if text.startswith(prefix):
            text = text.removeprefix(prefix).strip()
    text = text.strip()

    for suffix in suffixes:
        if text.endswith(suffix):
            text = text.removesuffix(suffix).strip()
    return " ".join(text.strip().split())

# normalize SEC names 
SEC_NAMES = SEC_NAMES_RAW.apply(normalize_text).tolist()

# add connected org nm to the transactions table
merged = transactions.merge(
    pacs[["CMTE_ID", "CONNECTED_ORG_NM"]],
    on="CMTE_ID",
    how="left"
)

# get total transaction amounts for each comapny
company_totals = merged.groupby("CONNECTED_ORG_NM")["TRANSACTION_AMT"].sum().reset_index()

company_totals["TRANSACTION_AMT"] = company_totals["TRANSACTION_AMT"].astype(float)

print(f"\nTotal unique companies: {len(company_totals)}")

# convert orgs to cmte ids
company_totals["CMTE_ID"] = company_totals["CONNECTED_ORG_NM"].apply(lambda x: name_to_id[x])
pacs_list = company_totals["CONNECTED_ORG_NM"].str.upper().fillna("").tolist()

# chunk of fuzzy matching size
CHUNK_SIZE = 4000
score_matrix_chunks = []

print("\nRunning fuzzy matching...\n")
best_match_names = []
for i in tqdm(range(0, len(pacs_list), CHUNK_SIZE), desc="Fuzzy Matching"):
    # normalize chunks of orgs
    chunk = list(pd.Series(pacs_list[i:i + CHUNK_SIZE]).apply(normalize_text))

    # fuzzy match
    chunk_results = process.cdist(
        chunk,
        SEC_NAMES,
        scorer=fuzz.WRatio,
        workers=-1,
        score_cutoff=0,
        dtype=np.uint8
    )

    # get best match for every org
    best_match_indices = np.argmax(chunk_results, axis=1)

    # add to best match list
    for idx in best_match_indices:
        best_match_names.append(SEC_NAMES_RAW[idx])

    # add results to the final chunk lists
    score_matrix_chunks.append(chunk_results)

# combine all the chunks
score_matrix = np.vstack(score_matrix_chunks)

# get the max score in each org
max_scores = score_matrix.max(axis=1)

# add to match df
match_df = pd.DataFrame({
    "CONNECTED_ORG_NM": pacs_list,
    "SEC_MATCH": best_match_names,
    "match_score": max_scores
})

# add the matches to the company data table
company_data = company_totals.merge(
    match_df,
    on="CONNECTED_ORG_NM",
    how="left"
)

# EVALUATE SUCCESS

total_corporate_money = company_data["TRANSACTION_AMT"].sum()

print(f"\nTotal corporate PAC money: ${total_corporate_money:,.0f}\n")

thresholds = [x for x in range(80, 101)]

print("Dollar Coverage by Match Threshold:\n")

for t in thresholds:
    covered_money = company_data.loc[
        company_data["match_score"] >= t,
        "TRANSACTION_AMT"
    ].sum()

    pct = covered_money / total_corporate_money

    print(
        f"Threshold {t}: "
        f"${covered_money:,.0f} "
        f"({pct:.2%} of total corporate PAC money)"
    )

# check some matches between 95 and 100 match score
fringe_sample = match_df.query(f"{95} <= match_score <= {100}")
fringe_sample = fringe_sample.sample(n=min(50, len(fringe_sample)))

print(f"\n--- FRINGE CHECK (Scores 95-100) ---")
fringe_sample["CONNECTED_ORG_NM_CLN"] = fringe_sample["CONNECTED_ORG_NM"].apply(str.upper).apply(normalize_text)
print(fringe_sample[["CONNECTED_ORG_NM", "CONNECTED_ORG_NM_CLN", "SEC_MATCH"]])

unmatched_companies = company_data[(company_data["match_score"] < 95)].copy()

# Sort by TRANSACTION_AMT in descending order
unmatched_sorted = unmatched_companies.sort_values(
    by="TRANSACTION_AMT",
    ascending=False
).reset_index(drop=True)

unmatched_sorted["CLEANED_NM"] = unmatched_sorted["CONNECTED_ORG_NM"].apply(str.upper).apply(normalize_text)
unmatched_sorted["SEC_MATCH"] = unmatched_sorted["SEC_MATCH"].apply(str.upper).apply(normalize_text)

print("\n--- UNMATCHED COMPANIES (Score < 99) Sorted by Donation ---")
# Adjust display settings for full visibility of names
pd.set_option('display.max_colwidth', 10000)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
print(len(unmatched_sorted))
unmatched_sorted["TRANSACTION_AMT"] /= total_corporate_money
print(unmatched_sorted[["CONNECTED_ORG_NM", "CLEANED_NM", "SEC_MATCH", "match_score", "TRANSACTION_AMT"]][:151].to_string(index=False))

final = company_totals.copy()
final["SEC_MATCH"] = company_data["SEC_MATCH"]

final["SIC"] = final["SEC_MATCH"].map(lambda x: SIC_MAP.get(x, "").zfill(4))

final["industry"] = final["SIC"].map(lambda x: INDUSTRY_MAP.get(x, ""))

final.loc[company_data['match_score'] < 0.95, 'industry'] = "Unknown"

final["industry"] = final["industry"].replace("", None)
final = final.dropna(subset=["industry"])

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
print(f"\n--- TOTAL % OF MONEY COVERED ---")
print(f"{final["TRANSACTION_AMT"].sum()/total_corporate_money:.2%}")

# explode for every cmte_id with all their pacs
final = final.explode("CMTE_ID")
final["CMTE_ID"] = final["CMTE_ID"].str[:]

# add committees, candidates, and transaction amounts
final = final[["CMTE_ID", "industry"]].merge(transactions[["CMTE_ID", "CAND_ID", "TRANSACTION_AMT", "YEAR", "bioguide_id"]], on="CMTE_ID")
final = final.dropna(subset="bioguide_id")

# rename column
final["money_recieved"] = final["TRANSACTION_AMT"]
final['cycle'] = final['YEAR'].apply(lambda x: int(x) + (int(x) % 2))

# group by industry and bioguide_id
final = final[["bioguide_id", "industry", "cycle", "money_recieved"]].groupby(["industry", "bioguide_id", "cycle"], as_index=False)["money_recieved"].sum()
final = final.sort_values(by='bioguide_id')

final.to_csv(os.path.join(OUTPUT_DIR, "donations.csv"), index=False)