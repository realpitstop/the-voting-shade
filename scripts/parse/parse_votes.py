import json
import os
from concurrent.futures.thread import ThreadPoolExecutor
import pandas as pd
from tqdm import tqdm
from lxml import etree as ET

# file paths
BASE_DIR = "./"
RAW_DIR = os.path.join(BASE_DIR, "data/raw/govinfo/billstatus")
OUT_DIR = os.path.join(BASE_DIR, "data/clean")
os.makedirs(OUT_DIR, exist_ok=True)

# action codes to the stage of voting it is in
STAGE_MAPPING = {
    '8000': 'passed',
    '17000': 'passed',
    '19500': 'passed',
    '20500': 'passed',
    '21000': 'passed',
    '23000': 'passed',
    '28000': 'passed',
    '32000': 'passed',
    '34000': 'passed',
    '36000': 'passed',
    '72000': 'passed',
    '75000': 'passed',
    '77000': 'passed',
    '79000': 'passed',
    '94000': 'passed',
    '95000': 'passed',
    '97000': 'passed',

    '9000': 'failed',
    '18000': 'failed',
    '33000': 'failed',
    '35000': 'failed',
    '73000': 'failed',

    '14500': 'procedural',
    '71000': 'procedural',
    '76000': 'procedural',
    'H36110': 'procedural',
    'H36210': 'procedural',
    'H36610': 'procedural',
    'H36810': 'procedural',
    'H37100': 'procedural',
    'H37300': 'procedural',
    'H38410': 'procedural',
    'H42411': 'procedural',
    'H42510': 'procedural',
    'H43410': 'procedural',

    'H32111': 'amendment',
    'H32341': 'amendment',
    'H32351': 'amendment',
    'H34111': 'amendment',
    'H34520': 'amendment',
    'H41521': 'amendment',
    'H41541': 'amendment',
    'H41610': 'amendment',
    'H41931': 'amendment'
}

# get the lis to bioguide_id map, as some use LIS instead
with open(OUT_DIR + "/lookup.json", "r", encoding="utf-8") as f:
    LIS_MAP = json.load(f)

def process_bill(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        # get the stage of voting
        stage = file_path[:-4].split("_")[-1]
        stage = STAGE_MAPPING[stage]

        legis_num = file_path[:-4].split("_")[0].split("-")[1][3:].upper()
        legis_num = legis_num.replace(".", "").replace(" ", "")
        congress = int(file_path[:-4].split("_")[0].split("-")[1][:3].upper())

        local_votes = []
        # get all votes for senate
        for member in root.findall(".//member"):
            id = LIS_MAP[member.findtext('lis_member_id')]
            vote = member.findtext("vote_cast").strip() == "Yea"
            local_votes.append({
                "bill_number": legis_num,
                "stage": stage,
                "congress": congress,
                "bioguide_id": id,
                "voted": vote
            })
        
        # get all votes for house
        for legislator in root.findall(".//legislator"):
            id = legislator.get('name-id')
            vote = legislator.findtext("../vote").strip() == "Yea"
            local_votes.append({
                "bill_number": legis_num,
                "stage": stage,
                "congress": congress,
                "bioguide_id": id,
                "voted": vote
            })

        return local_votes

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

# all status files for votes
files = [os.path.join(RAW_DIR, f) for f in os.listdir(RAW_DIR) if f.endswith(".xml")]

votes_list = []

# process multiple at a time
with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_bill, f) for f in files]

    for future in tqdm(futures, desc="Processing Votes"):
        result = future.result()
        if result:
            votes_list.extend(result)

# read bills and make df for votes
bills = pd.read_csv(OUT_DIR + "/bills.csv")
votes = pd.DataFrame(votes_list).dropna()

# combine bills and votes to conver the bill number to the bill id
df1_indexed = votes.set_index(['congress', 'bill_number'])
df2_indexed = bills.set_index(['congress', 'bill_number'])

# add the bill id
votes = df1_indexed.join(df2_indexed['bill_id'], how='left', validate='many_to_one').reset_index().dropna()

votes['bill_id'] = votes['bill_id'].astype(int)
votes = votes[["bill_id", "stage", "bioguide_id", "voted"]]
votes.to_csv(f"{OUT_DIR}/votes.csv", index=False)

print(f"Done. Processed {len(votes)} votes.")

