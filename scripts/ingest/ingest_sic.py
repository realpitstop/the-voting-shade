import gzip
import io
import os
import zipfile
import zlib
import json
import requests
from tqdm import tqdm
from pathlib import Path
from headers import headerSEC 

"""
{'cik': '0000004926', 'entityType': 'other', 'sic': '', 'sicDescription': '', 'ownerOrg': None, 
'insiderTransactionForOwnerExists': 0, 'insiderTransactionForIssuerExists': 0, 'name': 'AMERICAN ENTERPRISE DEVELOPMENT CORP', 
'tickers': [], 'exchanges': [], 'ein': None, 'lei': None, 'description': '', 'website': '', 'investorWebsite': '', 
'category': '', 'fiscalYearEnd': '0630', 'stateOfIncorporation': '', 'stateOfIncorporationDescription': '', 
'addresses': {'mailing': {'street1': '200 BERKELEY STREET', 'street2': 'BOSTON, MASSACHUSETTS 02116', 'city': None, 
'stateOrCountry': None, 'zipCode': None, 'stateOrCountryDescription': None, 'isForeignLocation': None, 
'foreignStateTerritory': None, 'country': None, 'countryCode': None}, 'business': {'street1': None, 'street2': None, 
'city': None, 'stateOrCountry': '', 'zipCode': None, 'stateOrCountryDescription': '', 'isForeignLocation': None, 
'foreignStateTerritory': None, 'country': None, 'countryCode': None}}, 'phone': None, 'flags': '', 'formerNames': [], 
'filings': {'recent': {'accessionNumber': [], 'filingDate': [], 'reportDate': [], 'acceptanceDateTime': [], 'act': [], 
'form': [], 'fileNumber': [], 'filmNumber': [], 'items': [], 'core_type': [], 'size': [], 'isXBRL': [], 'isInlineXBRL': [], 
'primaryDocument': [], 'primaryDocDescription': []}, 'files': []}}
"""

OUTPUT_PATH = os.path.join(Path(os.path.abspath(__file__)).parent.parent.parent, "data/clean/name_sic.json")

SUBMISSIONS_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
headers = headerSEC

mapping = {
    "Space Exploration Technologies Corp.": "3761",
    "Koch Industries": "2869",
    "UBS Americas Inc.": "6211",
    "PricewaterhouseCoopers LLC": "8721",
    "Koch Inc": "2869",
    "Invesco Holding Company": "6211",
    "Rocket Limited Partnership": "6162",
    "General Atomics Inc": "8731",
    "Beechcraft Corporation": "3721",
    "BHFS-E":"8111",
    "HDR Inc.": "8711",
    "Jackson Holdings LLC and Jackson National Life Insurance Company": "6311",
    "Capital Group Companies, Inc.": "6282",
    "ZENECA": "2834",
    "Principal Life Insurance Company": "6321",
    "The Vanguard Group": "6282",
    "LPL Financial LLC":"6200",
    "Health Care Service Corporation":"6324",
    "The Guardian Life Insurance Company of America":"6311",
    "Peraton Corp.":"7379",
    "Invesco Ltd.":"6211",
    "Cozen O'Connor":"8111",
    "McGuireWoods Federal PAC":"8111",
    "BASF Corporation":"2819",
    "American Electric Power Company, Inc.":"4911",
    "Enterprise Holdings, Inc.":"7514",
    "Sierra Nevada Company, LLC":"3728",
    "Novartis Corporation":"2834",
    "The Depository Trust & Clearing Corporation":"6221",
    "Serco Inc.":"8744",
    "United States Sugar Corporation":"0115",
    "CalPortland Company":"3241",
    "LG&E KU":"4931",
    "Airbus Americas, Inc.":"3721",
    "Blue Cross Blue Shield ASSOCIATION":"6324",
    "Barclays PLC":"6021",
    "Salt River Valley Water Users' Association":"4941",
    "Bayer US LLC":"2879",
    "Grant Thornton Advisors LLP":"8721",
    "Alticor Inc.":"5961",
    "Mastercard Incorporated":"6199",
    "Boehringer Ingelheim USA Corporation":"2836",
    "Herzog Corp.":"1611",
    "LSEG US Holdco Inc.":"6221",
    "State Street Bank and Trust":"6021",
    "Woolpert, Inc.":"8713",
    "UNITED STATES SUGAR CORPORATION EMPLOYEE STOCK OWNERSHIP PLAN":"2061",
    "BARCLAYS GROUP US": "6712",
    "SALT RIVER VALLEY WATER USERS ASSOCIATION POLITICAL INVOLVEMENT COMMITTEE":"4941",
    "Cargill Inc.": "2041",
    "Greenberg Traurig LLP": "811",
    "LIFEPOINT CORPORATE SERVICES GENERAL PARTNERSHIP AND FACILITIES WHICH ARE SUBSIDIARIES OF LIFEPOINT HEALTH GENERAL PARTNER": "8062",
    "TOYOTA MOTOR NORTH AMERICA, INC": "8880",
    "ORACLE America Inc.": "7372",
    "BAE SYSTEMS": "3812",
    "Torch Technologies, Inc.": "8731",
    "APEX Clean Energy": "4911",
    "Accelint Holdings": "7373",
    "Radiance Technologies": "3761",
    "COX ENTERPRISES INC ET AL": "4841",
    "FMR LLC FEDERAL": "6282",
    "TD BANK US": "6021",
    "GUIDEWELL MUTUAL HOLDING CORPORATION": "6321",
    "Zurich Holding Company of America": "6331",
    "LABORATORY CORPORATION OF AMERICA HOLDINGS": "8071",
    "Sanofi US": "2834",
    "SAMSUNG ELECTRONICS AMERICA":"3674",
    "TEAM AMERITECH CORP /DE/": "4813",
    "UNITED SPACE ALLIANCE": "3761",
    "GENERAL ELECTRIC CO (AFF) GELCO CORP": "7515"
}

def process_zip_continuously(content, encoding):
    if 'gzip' in encoding:
        content = gzip.decompress(content)
    elif 'deflate' in encoding:
        content = zlib.decompress(content)
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        for name in tqdm(z.namelist()):
            with z.open(name) as f:
                try:
                    company = json.load(f)
                except Exception as e:
                    print("Error:", e, name)
                nameNow = company.get("name")
                sic = company.get("sic", None)
                if sic is None or sic == "": continue
                for nameAll in [nameNow] + [item['name'] for item in company.get("formerNames", [])]:
                    mapping.setdefault(nameAll, sic)


def download_zip(zip_url):
    r = requests.get(zip_url, headers=headers)
    r.raise_for_status()
    process_zip_continuously(r.content, r.headers.get('Content-Encoding', '').lower())


download_zip(SUBMISSIONS_URL)
with open(OUTPUT_PATH, 'w') as f:
    json.dump(mapping, f, indent=4)
