import json
import os
import re
from datetime import datetime

from lxml import etree as ET
import pandas as pd
from tqdm import tqdm
from bulk_inference import PolicyClassifier
from concurrent.futures import ThreadPoolExecutor
import dateutil.parser as parser
from pathlib import Path

classifier = PolicyClassifier()

CONGRESS_MAP = {
    "one hundred thirteenth": 113,
    "one hundred fourteenth": 114,
    "one hundred fifteenth": 115,
    "one hundred sixteenth": 116,
    "one hundred seventeenth": 117,
    "one hundred eighteenth": 118,
    "one hundred nineteenth": 119
}

BASE_DIR = "./"
RAW_DIR = os.path.join(BASE_DIR, "data/raw/govinfo/bills")
OUT_DIR = os.path.join(BASE_DIR, "data/clean")

with open(OUT_DIR + "/lookup.json", "r", encoding="utf-8") as f:
    LIS_MAP = json.load(f)

def extract_text_recursive(elem):
    if elem is None:
        return ""
    return "".join(elem.itertext()).strip()

def quick_extract(text):
    text_lower = text.lower()

    if '1' in text_lower:
        for word in text_lower.split():
            if word[0].isdigit():
                return int(''.join(filter(str.isdigit, word)))

    for words, num in CONGRESS_MAP.items():
        if text_lower.startswith(words):
            return num

    return None
global count
count = 0
def process_bill(file_path, index):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        legis_num = str(root.findtext(".//legis-num")).replace(".", "").replace(" ", "").strip()
        chamber = "senate" if legis_num[0].upper() == "S" else "house" if legis_num[0].upper() == "H" else None
        congress = quick_extract(root.findtext(".//congress").strip())

        session = root.findtext(".//session").strip()
        if session == "At the First Session":
            session = 1
        elif session == "At the Second Session":
            session = 2
        else:
            session = int(session.strip()[0])
        title = root.findtext(".//short-title")
        title = title if title else root.findtext(".//official-title")

        body_el = root.find(".//legis-body")
        body_text = "".join(body_el.itertext()).strip() if body_el is not None else ""

        raw_date = root.find(".//action-date")
        if raw_date is None:
            return None
        date = raw_date.get('date')
        if date is None:
            date = parser.parse(re.sub(r'\s*\([^)]*\)', '', root.findtext(".//action-date"))).strftime("%Y%m%d")
        comm_el = root.find(".//committee-name")
        sponsor_el = root.find(".//sponsor")

        local_sponsors = []
        if sponsor_el is not None:
            id = sponsor_el.get('name-id')
            if len(id) == 4:
                id = LIS_MAP[id]
            local_sponsors.append({
                "bill_id": index,
                "bioguide_id": id,
                "role": "sponsor"
            })

        for cosponsor in root.findall(".//cosponsor"):
            id = cosponsor.get('name-id')
            if len(id) == 4:
                id = LIS_MAP[id]
            local_sponsors.append({
                "bill_id": index,
                "bioguide_id": id,
                "role": "cosponsor"
            })

        local_committees = []
        committee_id = "None"
        if comm_el is not None:
            committee_id = comm_el.get('committee-id')

        local_committees.append({
            "committee_id": committee_id,
            "committee": comm_el.text if comm_el is not None else "None"
        })
        bill_data = {
            "bill_id": index,
            "bill_number": legis_num,
            "congress": congress,
            "session": session,
            "title": title,
            "introduced_date": datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d"),
            "chamber_of_origin": chamber,
            "committee_id": committee_id,
            "raw_text": body_text
        }

        return bill_data, local_committees, local_sponsors

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

os.makedirs(OUT_DIR, exist_ok=True)

CHAMBER_MAP = {
    "IN THE HOUSE OF REPRESENTATIVES": "house",
    "IN THE SENATE OF THE UNITED STATES": "senate"
}
files = [os.path.join(RAW_DIR, f) for f in os.listdir(RAW_DIR) if f.endswith(".xml")]

bills_list, committees_list, sponsors_list, topics_list = [], [], [], []
inference_buffer = []
BATCH_SIZE = 64
SAVE_INTERVAL = 10000
temp_topics = []  # Temporary list for the 10k chunk
TOPICS_CSV = f"{OUT_DIR}/topics.csv"

with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_bill, f, i) for i, f in enumerate(files)]

    for future in tqdm(futures, desc="Processing Bills & Classifying"):
        result = future.result()
        if not result:
            count += 1
            continue

        b, c, s = result
        bills_list.append(b)
        committees_list.extend(c)
        sponsors_list.extend(s)

        if b.get("raw_text"):
            inference_buffer.append({"id": b["bill_id"], "text": b["raw_text"]})

        if len(inference_buffer) >= BATCH_SIZE:
            texts = [item["text"] for item in inference_buffer]
            ids = [item["id"] for item in inference_buffer]

            predictions = classifier.get_policy_codes_batch(texts)

            for bill_id, codes in zip(ids, predictions):
                for code in codes:
                    topic_id = code[0] if isinstance(code, list) else code
                    temp_topics.append({"bill_id": bill_id, "topic_id": topic_id})

            inference_buffer = []

        if len(temp_topics) >= SAVE_INTERVAL:
            pd.DataFrame(temp_topics).to_csv(TOPICS_CSV, mode='a', index=False, header=False)
            temp_topics = []

if inference_buffer:
    texts = [item["text"] for item in inference_buffer]
    ids = [item["id"] for item in inference_buffer]
    predictions = classifier.get_policy_codes_batch(texts)
    for bill_id, codes in zip(ids, predictions):
        for code in codes:
            topic_id = code[0] if isinstance(code, list) else code
            temp_topics.append({"bill_id": bill_id, "topic_id": topic_id})

if temp_topics:
    pd.DataFrame(temp_topics).to_csv(TOPICS_CSV, mode='a', index=False, header=False)

topics = pd.read_csv(TOPICS_CSV)

def major_title(code):
    return classifier.get_meaning(str(code)[:-2] + "00").split(" – ")[0]

def sub_title(code):
    return classifier.get_meaning(str(code)).split(" – ")[0]

topics["topic"] = topics["topic_id"].apply(major_title)
topics["subtopic"] = topics["topic_id"].apply(sub_title)

topics.to_csv(TOPICS_CSV)

bills = pd.DataFrame(bills_list).drop(columns=["raw_text"])
bills = bills.dropna().drop_duplicates(subset=["congress", "bill_number"])
bills.to_csv(f"{OUT_DIR}/bills.csv", index=False)
committees = pd.DataFrame(committees_list).dropna().drop_duplicates()
sponsors = pd.DataFrame(sponsors_list).dropna()
committees = committees[committees['committee_id'].isin(bills["committee_id"])]
sponsors = sponsors[sponsors['bill_id'].isin(bills["bill_id"])]
committees.to_csv(f"{OUT_DIR}/committees.csv", index=False)
sponsors.to_csv(f"{OUT_DIR}/sponsors.csv", index=False)