import json
from pathlib import Path
import os
import pandas as pd

# file paths
BASE_DIR = "./"
DATA_PATH = os.path.join(BASE_DIR, "data/raw/pacs")
OUTPUT_DIR = os.path.join(BASE_DIR, "data/raw/pacs")
RAW_FILE = os.path.join(BASE_DIR, "data/clean/fec-cand_bio.json")

# get fec conversion file
with open(RAW_FILE, "r") as f:
    fec_to_bioguide = json.load(f)

# datatypes for transactions table
ptctypes = {
    'IMAGE_NUM': str,
    'ENTITY_TP': str,
    'ZIP_CODE': str,
    'EMPLOYER': str,
    'OCCUPATION': str,
    'TRAN_ID': str,
    'MEMO_CD': str,
    'MEMO_TEXT': str
}

# make pacs file (filter for corporate pacs, fix missing org names)
comm_master = pd.read_csv(os.path.join(DATA_PATH, "cm.csv"), dtype={'CMTE_ZIP': str})
comm_master = comm_master[((comm_master["CMTE_TP"] == "Q") | (comm_master["CMTE_TP"] == "N")) & (comm_master["ORG_TP"] == "C")].drop_duplicates(subset="CMTE_ID")
comm_master.loc[comm_master["CONNECTED_ORG_NM"].str.upper() == 'NONE', "CONNECTED_ORG_NM"] = comm_master['CMTE_NM']
comm_master.to_csv(os.path.join(OUTPUT_DIR, "pacs.csv"), index=False)

# make transactions file (groupby person & committee and get their total donations, replace cand_id with bioguide_id, filter for only committees present in pacs.csv)
ptc_master = pd.read_csv(os.path.join(DATA_PATH, "pas2.csv"), dtype=ptctypes)
ptc_master = ptc_master[['CMTE_ID', 'CAND_ID', 'TRANSACTION_AMT', 'ENTITY_TP']].groupby(['CMTE_ID', 'CAND_ID'])['TRANSACTION_AMT'].sum().reset_index()
ptc_master["bioguide_id"] = ptc_master["CAND_ID"].apply(lambda x: fec_to_bioguide.get(x, None))
ptc_master = ptc_master.dropna(subset="bioguide_id")
ptc_master = ptc_master.merge(comm_master[['CMTE_ID']], on='CMTE_ID')
ptc_master.to_csv(os.path.join(OUTPUT_DIR, "transactions.csv"), index=False)