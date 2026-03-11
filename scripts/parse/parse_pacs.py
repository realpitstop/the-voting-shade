import json
from pathlib import Path
import os
import pandas as pd

BASE_DIR = "./"
DATA_PATH = os.path.join(BASE_DIR, "data/raw/pacs")
OUTPUT_DIR = os.path.join(BASE_DIR, "data/raw/pacs")
RAW_FILE = os.path.join(BASE_DIR, "data/raw/members/legislators.json")
with open(RAW_FILE, "r") as f:
    data = json.load(f)

fec_to_bioguide = {
    fec_id: item['id']['bioguide']
    for item in data
    for fec_id in item['id'].get('fec', [])
}

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

def func(target_fec):
    return fec_to_bioguide.get(target_fec, None)

comm_master = pd.read_csv(os.path.join(DATA_PATH, "cm.csv"), dtype={'CMTE_ZIP': str})
ptc_master = pd.read_csv(os.path.join(DATA_PATH, "pas2.csv"), dtype=ptctypes)
ptc_master = ptc_master[['CMTE_ID', 'CAND_ID', 'TRANSACTION_AMT', 'ENTITY_TP']].groupby(['CMTE_ID', 'CAND_ID'])['TRANSACTION_AMT'].sum().reset_index()
comm_master = comm_master[((comm_master["CMTE_TP"] == "Q") | (comm_master["CMTE_TP"] == "N")) & (comm_master["ORG_TP"] == "C")].drop_duplicates(subset="CMTE_ID")
comm_master.loc[comm_master["CONNECTED_ORG_NM"].str.upper() == 'NONE', "CONNECTED_ORG_NM"] = comm_master['CMTE_NM']
ptc_master["bioguide_id"] = ptc_master["CAND_ID"].apply(func)
ptc_master = ptc_master.dropna(subset="bioguide_id")
ptc_master = ptc_master.merge(comm_master[['CMTE_ID']], on='CMTE_ID')

comm_master.to_csv(os.path.join(OUTPUT_DIR, "pacs.csv"), index=False)
ptc_master.to_csv(os.path.join(OUTPUT_DIR, "transactions.csv"), index=False)
print(comm_master)
print(ptc_master)