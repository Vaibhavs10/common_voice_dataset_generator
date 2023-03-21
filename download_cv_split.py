import urllib
import sys
import requests
import os
import logging
import shutil
import json
from tqdm import tqdm
import time
from pathlib import Path
from datasets.download import DownloadConfig, DownloadManager


logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("cv13_download.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

#Step 1: Update the BUNDLE URL -> You can get this by trying to manually download a split and looking for the download URL.
_BUNDLE_URL_TEMPLATE_DELTA = 'cv-corpus-13.0-2023-03-09/cv-corpus-13.0-2023-03-09-{locale}.tar.gz'
_BUNDLE_VERSION = _BUNDLE_URL_TEMPLATE_DELTA.split("/")[0]
_API_URL = "https://commonvoice.mozilla.org/api/v1"

#Step 2: Place the path to the CV release JSON from https://github.com/common-voice/cv-dataset/tree/main/datasets
_CV_DATASET_RELEASE_JSON = "cv-corpus-13.0-2023-03-09.json"

def _get_bundle_url(locale, url_template):
    path = url_template.replace("{locale}", locale)
    path = urllib.parse.quote(path.encode("utf-8"), safe="~()*!.'")
    response = requests.get(f"{_API_URL}/bucket/dataset/{path}", timeout=10.0).json()
    return response["url"]


def _log_download(locale, bundle_version):
    email = "vaibhav@huggingface.co"
    payload = {"email": email, "locale": locale, "dataset": bundle_version}
    requests.post(f"{_API_URL}/{locale}/downloaders", json=payload).json()


def download_language(dl_manager, lang, root_dir):
    _log_download(lang, _BUNDLE_VERSION)
    url = _get_bundle_url(lang, _BUNDLE_URL_TEMPLATE_DELTA)
    i = 1
    while url == "https://s3.dualstack.us-west-2.amazonaws.com/":
        if i == 6:
            raise ConnectionError(f"Cannot download '{lang.upper()}' data, fetched url: {url}. ")
        i += 1
        logging.warning(f"Unsuccessful attempt to fetch data url. Trying {i} time. ")
        time.sleep(15)
        _log_download(lang, _BUNDLE_VERSION)
        url = _get_bundle_url(lang, _BUNDLE_URL_TEMPLATE_DELTA)

    logging.info(f"Trying to download data for '{lang.upper()}'... ")
    path = dl_manager.download_and_extract(url)
    if os.path.isdir(path):
        logging.info(f"'{lang.upper()}' data downloaded to {path}. ")
        shutil.move(path, root_dir / f"data/{lang}")
    else:  # if it's not a dir, there was no data update in the release
        logging.info(f"No data for '{lang.upper()}' found. ")


def main():
    root_dir = Path("")
    with open(_CV_DATASET_RELEASE_JSON, "r") as f:
        languages = json.load(f)["locales"].keys()

    if (root_dir / "langs_ok.txt").exists():
        with open(root_dir / "langs_ok.txt") as f:
            langs_to_skip = set([line.strip().split("_")[1] for line in f.read().split("\n") if line])
        logging.info(f"Already downloaded languages: {langs_to_skip}")
    else:
        langs_to_skip = set()

    dl_config = DownloadConfig(
        cache_dir=root_dir / "cache",
        resume_download=True,
        max_retries=5,
    )
    dl_manager = DownloadManager(
        download_config=dl_config,
        record_checksums=False,
    )

    for lang_id, lang in enumerate(tqdm(languages, desc="Processing languages...")):
        if lang in langs_to_skip:
            logging.info(f"Data for '{lang.upper()}' language already downloaded, skipping it. ")
            continue
        try:
            download_language(dl_manager, lang, root_dir=root_dir)
            with open(root_dir / "langs_ok.txt", "a") as f:
                f.write(f"{lang_id}_{lang}\n")
        except ConnectionError as e:
            logging.error(e.strerror)
            with open(root_dir / "langs_failed.txt", "a") as f:
                f.write(f"{lang_id}_{lang}\n")
        time.sleep(10)


if __name__ == "__main__":
    main()
