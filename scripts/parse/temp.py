from pathlib import Path
import os
import pandas

BASE_DIR = Path(os.path.abspath(__file__)).parent.parent.parent
RAW_DIR = os.path.join(BASE_DIR, "data/raw/govinfo/billstatus")
files = set(f.split("_")[-1].split(".")[0] for f in os.listdir(RAW_DIR) if f.endswith(".xml"))
print(files)

