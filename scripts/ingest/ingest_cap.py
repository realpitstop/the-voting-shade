import requests
import os
from headers import headerUSER

# Comparative Agendas Project labeled dataset
URL = "https://minio.la.utexas.edu/compagendas/datasetfiles/US-Legislative-congressional_bills_19.3_3_3%20%281%29.csv"

# File paths
BASE_DIR = "./"
os.makedirs(BASE_DIR, exist_ok=True)
OUTPUT_PATH = os.path.join(BASE_DIR, "data/raw/annotation/")

# header
header = headerUSER

# csv response
response = requests.get(URL, headers=header)
response.raise_for_status()

# write to the file
with open(OUTPUT_PATH, 'wb') as f:
    f.write(response.content)

print(f"File downloaded and saved to {OUTPUT_PATH + "cap_class.csv"}")