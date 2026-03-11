import math
import os
import json
from lxml import etree
import pandas as pd
from tqdm import tqdm

XML_DIR = "../data/raw/govinfo/bills/"
CAP_CSV = "../data/raw/annotation/cap_class.csv"
OUTPUT = "../data/clean/output.jsonl"

parser = etree.XMLParser(recover=True)

def extract_text_recursive(elem):
    parts = []

    if elem.text:
        parts.append(elem.text)

    for child in elem:
        parts.append(extract_text_recursive(child))
        if child.tail:
            parts.append(child.tail)

    return " ".join(parts)

def parse_bill_xml(path):
    tree = etree.parse(path, parser=parser)
    root = tree.getroot()

    title = root.findtext(".//dc:title",
                          namespaces={"dc": "http://purl.org/dc/elements/1.1/"})
    official_title = root.findtext(".//official-title")

    body = root.find(".//legis-body")
    body_text = extract_text_recursive(body) if body is not None else ""

    legis_num = root.findtext(".//legis-num")
    if legis_num is None:
        return None
    congress = root.findtext(".//congress")
    bill_id = f"{congress.strip().split()[0][:-2]}-{legis_num.replace(' ', '')}".replace("H.R.", "HR-").replace("S.", "S-")
    full_text = f"TITLE: {title or official_title}\n\nBODY:\n{body_text}"

    return {
        "bill_id": bill_id,
        "text": " ".join(full_text.split())
    }


def load_cap_map(path):
    df = pd.read_csv(path)

    mapping = {}

    for _, row in df.iterrows():
        bill = row["bill_id"]

        sub = row["subtopic"]
        sub = str(int(sub)) if not math.isnan(sub) else "nan"
        code = None

        if sub != "9999" and sub != "0000" and sub != "nan":
            code = sub

        mapping[bill] = code
    return mapping

def main():
    cap_map = load_cap_map(CAP_CSV)
    ct = 0
    with open(OUTPUT, "w", encoding="utf8") as out:

        for fname in tqdm(os.listdir(XML_DIR)):
            if not fname.endswith(".xml"):
                continue

            if not (fname.startswith("BILLS-113") or fname.startswith("BILLS-114")):
                continue

            data = parse_bill_xml(os.path.join(XML_DIR, fname))
            if data is None:
                continue

            cap_code = cap_map.get(data["bill_id"], [])

            record = {
                "text": data["text"],
                "label": cap_code,
            }
            if record["label"]:
                out.write(json.dumps(record) + "\n")
    print(ct)

if __name__ == "__main__":
    main()
