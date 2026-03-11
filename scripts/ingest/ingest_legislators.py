import json
import os
import requests
from pathlib import Path
from headers import headerUSER

# file paths
BASE_DIR = "./"
MEM_DIR = os.path.join(BASE_DIR, "data/raw/members")
os.makedirs(MEM_DIR, exist_ok=True)

# links
MEM_URL = "https://unitedstates.github.io/congress-legislators/legislators-current.json"
MEM_URL2 = "https://unitedstates.github.io/congress-legislators/legislators-historical.json"

# conversions for LIS
CONVERSION = "https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml"
CONVERSION2 = "https://www.senate.gov/about/senator-lookup.xml"

# header
headers = headerUSER

# get file text
def download_file(json_url):
    r = requests.get(json_url, headers=headers)
    r.raise_for_status()
    return r.text

# write text to file
def writeText(filename, text):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text)

if __name__ == "__main__":
    # legislators
    legislators_current = download_file(MEM_URL)
    legislators_past = download_file(MEM_URL2)
    writeText(MEM_DIR + "/legislators.json", json.dumps(json.loads(legislators_current) + json.loads(legislators_past), indent=4))
    
    # conversions for bioguide_id
    conv = download_file(CONVERSION)
    writeText(MEM_DIR + "/lookup.xml", conv)
    conv2 = download_file(CONVERSION2)
    writeText(MEM_DIR + "/lookup2.xml", conv2)