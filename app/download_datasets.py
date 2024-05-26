import os
import urllib.request
import gzip
import shutil

BASE_URL = "https://datasets.imdbws.com/"


def download_and_extract(file_name: str, dir_path: str) -> None:
    url = BASE_URL + file_name
    file_path = os.path.join(dir_path, file_name)
    extracted_path = file_path.replace(".gz", "")
    
    if os.path.exists(extracted_path):
        print(f"{extracted_path} already exists. Skipping download.")
        return
    else:
        os.makedirs(dir_path, exist_ok=True)

    # Download the file
    print(f"Downloading {file_name}...")
    urllib.request.urlretrieve(url, file_path)
    
    # Extract the file
    print(f"Extracting {file_name}...")
    with gzip.open(file_path, 'rb') as f_in:
        with open(extracted_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    # Remove the .gz file after extraction
    os.remove(file_path)
    print(f"Finished {file_name}")
    