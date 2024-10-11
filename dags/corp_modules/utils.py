import requests
import zipfile
import os
import tempfile
import logging

__log = logging.getLogger(__name__)

def fetch_and_unzip(url, target):
    """
    Downloads a zip file, extracts contents,
    and handles potential seek issues.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        __log.info(f"Dowloading zip file to {temp_dir}")
        temp_zip = os.path.join(temp_dir, 'temp.zip') 

        # Download the zip file
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with open(temp_zip, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        __log.info(f"File downloaded! Extracting to {target}")

        # Now, open the temporary zip file for extraction
        os.makedirs(target, exist_ok=True)
        with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
            zip_ref.extractall(target)
        __log.info(f"Files extracted into {target}")

def get_zip_basename(file_url):
    """
    Extracts the base name of a .zip file from its URL.
    """
    file_name = os.path.basename(file_url)
    return os.path.splitext(file_name)[0]