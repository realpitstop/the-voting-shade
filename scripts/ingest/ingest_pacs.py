import io
import os
from zipfile import BadZipFile
from pathlib import Path
import requests
import zipfile
import pandas as pd
from tqdm import tqdm
from headers import headerKEEPALIVE

# FEC date range
START = 2000
END = 2026

# file paths
BASE_DIR = "./"
MEM_DIR = os.path.join(BASE_DIR, "data/raw/pacs")
os.makedirs(MEM_DIR, exist_ok=True)

# URLs
MASTER_BASE = "https://www.fec.gov/files/bulk-downloads/YEAR/TITLEYR.zip"
HEADER_URL = "https://www.fec.gov/files/bulk-downloads/data_dictionaries/TITLE_header_file.csv"

# header
headers = headerKEEPALIVE

# data titles
types = ["cn", "cm", "pas2"]

# get the csv file headers
def download_csv(csv_url):
    r = requests.get(csv_url, headers=headers)
    r.raise_for_status()
    return r.content

# get data zips
def download_zip(zip_url):
    r = requests.get(zip_url, headers=headers)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            return z.read(z.infolist()[0].filename)

# write csv to file
def writeText(filename, text, headers):
    df = pd.read_csv(io.StringIO(text.decode('utf-8')), sep='|', header=None, names=pd.read_csv(io.StringIO(headers.decode('utf-8'))).columns.tolist())
    df.to_csv(filename, index=False)

if __name__ == "__main__":
    # for every data type
    for t in tqdm(types):
        # get header
        csv_headers = download_csv(HEADER_URL.replace("TITLE", t))
        # add bytes to var
        text = b""
        for i in tqdm(range(START, END+1, 2)):
            # get data for every year
            text += download_zip(MASTER_BASE.replace("TITLE", str(t)).replace("YEAR", str(i)).replace("YR", str(i)[-2:]))
        # write to file
        writeText(os.path.join(MEM_DIR, t + ".csv"), text, csv_headers)
