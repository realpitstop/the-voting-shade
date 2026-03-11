import json
import os
import requests
from pathlib import Path
from headers import headerUSER

BASE_DIR = "./"
MEM_DIR = os.path.join(BASE_DIR, "data/raw/members")
MEM_URL = "https://unitedstates.github.io/congress-legislators/legislators-current.json"
MEM_URL2 = "https://unitedstates.github.io/congress-legislators/legislators-historical.json"
CONVERSION = "https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml"
CONVERSION2 = "https://www.senate.gov/about/senator-lookup.xml"

headers = headerUSER

os.makedirs(MEM_DIR, exist_ok=True)
def download_member_xml(json_url):
    r = requests.get(json_url, headers=headers)
    r.raise_for_status()
    return r.text
def writeText(filename, text):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text)
if __name__ == "__main__":
    text = download_member_xml(MEM_URL)
    text2 = download_member_xml(MEM_URL2)
    writeText(MEM_DIR + "/legislators.json", json.dumps(json.loads(text) + json.loads(text2), indent=4))
    conv = download_member_xml(CONVERSION)
    writeText(MEM_DIR + "/lookup.xml", conv)
    conv2 = download_member_xml(CONVERSION2)
    writeText(MEM_DIR + "/lookup2.xml", conv2)