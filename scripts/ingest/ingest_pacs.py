import io
import os
from zipfile import BadZipFile
from pathlib import Path
import requests
import zipfile
import pandas as pd
from tqdm import tqdm
from headers import headerKEEPALIVE

START = 2000
END = 2026

BASE_DIR = Path(os.path.abspath(__file__)).parent.parent.parent
MEM_DIR = os.path.join(BASE_DIR, "data/raw/pacs")
MASTER_BASE = "https://www.fec.gov/files/bulk-downloads/YEAR/TITLEYR.zip"
HEADER_URL = "https://www.fec.gov/files/bulk-downloads/data_dictionaries/TITLE_header_file.csv"

headers = headerKEEPALIVE

types = ["cn", "cm", "pas2"]

os.makedirs(MEM_DIR, exist_ok=True)
def download_csv(csv_url):
    r = requests.get(csv_url, headers=headers)
    r.raise_for_status()
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            return z.read(z.infolist()[0].filename)
    except BadZipFile:
        return r.content
def writeText(filename, text, headers):
    df = pd.read_csv(io.StringIO(text.decode('utf-8')), sep='|', header=None, names=pd.read_csv(io.StringIO(headers.decode('utf-8'))).columns.tolist())
    df.to_csv(filename, index=False)

if __name__ == "__main__":
    for t in tqdm(types):
        csv_headers = download_csv(HEADER_URL.replace("TITLE", t))
        text = b""
        for i in tqdm(range(START, END+1, 2)):
            text += download_csv(MASTER_BASE.replace("TITLE", str(t)).replace("YEAR", str(i)).replace("YR", str(i)[-2:]))
        writeText(os.path.join(MEM_DIR, t + ".csv"), text, csv_headers)
