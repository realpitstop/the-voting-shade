import requests
import os
from headers import headerUSER

URL = "https://minio.la.utexas.edu/compagendas/datasetfiles/US-Legislative-congressional_bills_19.3_3_3%20%281%29.csv"
BASE_DIR = "./"
OUTPUT_PATH = os.path.join(BASE_DIR, "data/raw/annotation/cap_class.csv")
header = headerUSER

response = requests.get(URL, headers=header)
response.raise_for_status()

with open(OUTPUT_PATH, 'wb') as f:
    f.write(response.content)

print(f"File downloaded and saved to {OUTPUT_PATH}")