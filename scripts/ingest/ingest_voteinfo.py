import gzip
import os
import time
import zlib

import requests
import zipfile
import io
from lxml import etree as ET
from headers import headersENC

# file paths
BASE_DIR = "./"
RAW_DIR = os.path.join(BASE_DIR, "data/raw/govinfo/billstatus/")
os.makedirs(RAW_DIR, exist_ok=True)

# bill types
BILL_TYPES = ['hr', 's']

def download_bulk_bills(start_congress, end_congress):
    # header
    headers = headersENC

    # in all the congresses
    for congress in range(start_congress, end_congress + 1):
        # for bill type
        for btype in BILL_TYPES:
            # generate zip file name
            zip_filename = f"BILLSTATUS-{congress}-{btype}.zip"
            url = f"https://www.govinfo.gov/bulkdata/BILLSTATUS/{congress}/{btype}/{zip_filename}"

            print(f"Checking {congress} type {btype}...")
            try:
                response = requests.get(url, headers=headers, timeout=30)
                # sleep to be courteous
                time.sleep(0.15)
            except Exception as e:
                print(f"  Connection error for {url}: {e}")
                continue

            if response.status_code == 200:
                content = response.content
                content_encoding = response.headers.get('Content-Encoding', '').lower()

                # decompress encoding
                if 'gzip' in content_encoding:
                    print(f"Decompressing gzip: {zip_filename}")
                    decompressed_data = gzip.decompress(content)
                elif 'deflate' in content_encoding:
                    print(f"Decompressing deflate: {zip_filename}")
                    decompressed_data = zlib.decompress(content)
                else:
                    print(f"Processing ZIP: {zip_filename}")
                    decompressed_data = content
                with zipfile.ZipFile(io.BytesIO(decompressed_data)) as z:
                    for file_info in z.infolist():
                        if not file_info.is_dir() and file_info.filename.endswith('.xml'):
                            xml_content = z.read(file_info.filename)
                            try:
                                root = ET.fromstring(xml_content)

                                bill_id = file_info.filename.removesuffix('.xml')

                                # get all the action codes
                                all_recorded_actions = root.xpath(
                                    ".//*[local-name()='item'][.//*[local-name()='rollNumber'] and .//*[local-name()='url']]"
                                )

                                if not all_recorded_actions:
                                    continue

                                targets = []

                                for action in all_recorded_actions:
                                    # Identify Chamber
                                    all_action_text = " ".join(action.xpath(".//text()")).lower()
                                    chamber = None
                                    if 'house' in all_action_text:
                                        chamber = "house"
                                    elif 'senate' in all_action_text:
                                        chamber = "senate"

                                    if not chamber:
                                        continue

                                    # Extract action code
                                    code_list = action.xpath("./*[local-name()='actionCode']/text()")
                                    stage_code = code_list[0].strip() if code_list else "UNKNOWN"

                                    # Add to targets (Fixed the generator bug from your snippet)
                                    targets.append((action, chamber, stage_code))

                                for action_item, chamber_name, stage_code in targets:
                                    # get votes
                                    roll_num_list = action_item.xpath(".//*[local-name()='rollNumber']/text()")
                                    vote_url_list = action_item.xpath(".//*[local-name()='url']/text()")

                                    if roll_num_list and vote_url_list:
                                        roll_num = roll_num_list[0].strip()
                                        vote_url = vote_url_list[0].strip()

                                        # where to save bill status
                                        save_name = f"{bill_id}_{chamber_name}_vote_{roll_num}_{stage_code}.xml"
                                        save_path = os.path.join(RAW_DIR, save_name)

                                        # dont remake the file
                                        if os.path.exists(save_path):
                                            continue

                                        print(f"  Downloading {chamber_name} vote {roll_num} for {bill_id}...")
                                        try:
                                            # get the votes
                                            vote_res = requests.get(vote_url, headers=headers, timeout=15)
                                            if vote_res.status_code == 200:
                                                with open(save_path, 'wb') as f:
                                                    f.write(vote_res.content)
                                            time.sleep(0.15)
                                        except Exception as e:
                                            print(f"    Error downloading {vote_url}: {e}")

                            except (ET.ParseError, Exception) as e:
                                continue

            elif response.status_code == 404:
                print(f"  {zip_filename} not found.")
                continue


def main(START, END):
    download_bulk_bills(START, END)


if __name__ == "__main__":
    START = 113
    END = 119
    main(START, END)
