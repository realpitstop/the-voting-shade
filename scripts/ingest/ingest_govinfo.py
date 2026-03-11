import gzip
import io
import os
import time
import zipfile
import zlib

import requests
from headers import headersENC

BASE_DIR = "./"
RAW_DIR = os.path.join(BASE_DIR, "data/raw/govinfo/bills/")
os.makedirs(RAW_DIR, exist_ok=True)

BILL_TYPES = ['hr', 's']


def download_bulk_bills(start_congress, end_congress):
    headers = headersENC

    for congress in range(start_congress, end_congress + 1):
        for session in ["1", "2"]:
            for btype in BILL_TYPES:
                zip_filename = f"BILLS-{congress}-{session}-{btype}.zip"
                url = f"https://www.govinfo.gov/bulkdata/BILLS/{congress}/{session}/{btype}/{zip_filename}"

                print(f"Checking {congress}-{session} type {btype}...")
                try:
                    response = requests.get(url, headers=headers)
                    time.sleep(0.15)

                    if response.status_code == 200:
                        content = response.content
                        content_encoding = response.headers.get('Content-Encoding', '').lower()

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
                                if not file_info.is_dir():
                                    filename = os.path.basename(file_info.filename)
                                    if not filename:
                                        continue
                                    target_path = os.path.join(RAW_DIR, filename)

                                    # Write the file content to the flat directory
                                    with open(target_path, "wb") as f:
                                        f.write(z.read(file_info.filename))

                    elif response.status_code == 404:
                        print("Error")
                        continue
                    else:
                        print(f"  Unexpected status {response.status_code}")

                except Exception as e:
                    print(f"  Error accessing {url}: {e}")


def main():
    # 113th Congress (2013) to 119th (Current)
    download_bulk_bills(113, 119)


if __name__ == "__main__":
    main()
