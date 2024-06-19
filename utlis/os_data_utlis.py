import requests
import zipfile
from zipfile import ZipFile
import os
import shutil

def download_zip(zip_path, extract_folder_path, download_url):
    headers = {'Host': 'www.sec.gov', 'Connection': 'close',
            'Accept': 'application/json, text/javascript, */*; q=0.01', 'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
            }
    
    response = requests.get(download_url, headers=headers)

    if response.status_code == 200:
        with open(zip_path, 'wb') as file:
            file.write(response.content)
        print(f"{download_url} downloaded successfully.")
        try:
            with ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_folder_path)
            print("Files extracted successfully.")
        except zipfile.BadZipFile:
            print("Downloaded file is not a valid ZIP file.")
    else:
        print(f"Failed to download. HTTP status code: {response.status_code}")


def cleanup_files(zip_path, extract_folder_path):
    if os.path.exists(zip_path):
        os.remove(zip_path)
        print(f"Removed file: {zip_path}")
    if os.path.exists(extract_folder_path):
        shutil.rmtree(extract_folder_path)
        print(f"Removed directory: {extract_folder_path}")
