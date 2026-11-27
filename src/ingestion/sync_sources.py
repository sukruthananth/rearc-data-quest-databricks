import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
POPULATION_URL = (
    "https://tesseract.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Nation&measures=Population&year=all"
)

VOLUME_ROOT = "/Volumes/workspace/rearc_raw/bls_population_raw"
BLS_DIR = os.path.join(VOLUME_ROOT, "bls")
POP_DIR = os.path.join(VOLUME_ROOT, "population")
POP_FILE = os.path.join(POP_DIR, "us_population.json")
MANIFEST_PATH = os.path.join(BLS_DIR, "_manifest.json")

CONTACT_EMAIL = "data-engineering@example.com"
REQUEST_DELAY_SEC = 0.2


class _HrefParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            for k, v in attrs:
                if k.lower() == "href" and v:
                    self.hrefs.append(v)


def make_session(contact_email: str) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": f"RearcDataQuest/1.0 ({contact_email})",
        "Accept": "*/*",
    })
    return session


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"files": {}}


def save_manifest_atomic(path: str, manifest: dict) -> None:
    tmp_path = f"{path}.part"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    os.replace(tmp_path, path)


def list_bls_files(session: requests.Session, base_url: str) -> list[str]:
    resp = session.get(base_url, timeout=30)
    resp.raise_for_status()
    parser = _HrefParser()
    parser.feed(resp.text)

    files = []
    seen = set()
    for href in parser.hrefs:
        clean_name = href.split("?")[0].rstrip("/").split("/")[-1]
        if not clean_name or clean_name in {".", ".."} or href.endswith("/"):
            continue
        if re.match(r"^[A-Za-z0-9._-]+$", clean_name) and clean_name not in seen:
            seen.add(clean_name)
            files.append(clean_name)
    return sorted(files)


def download_file_atomic(session: requests.Session, url: str, dest_path: str) -> None:
    tmp_path = f"{dest_path}.part"
    with session.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(tmp_path, "wb") as out:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                if chunk:
                    out.write(chunk)
    os.replace(tmp_path, dest_path)


def sync_bls_directory(session: requests.Session) -> dict:
    os.makedirs(BLS_DIR, exist_ok=True)
    manifest = load_manifest(MANIFEST_PATH)
    tracked = manifest.get("files", {})

    remote_files = list_bls_files(session, BLS_BASE_URL)
    remote_set = set(remote_files)

    summary = {"downloaded": [], "skipped": [], "deleted": []}

    for file_name in remote_files:
        file_url = urljoin(BLS_BASE_URL, file_name)
        local_path = os.path.join(BLS_DIR, file_name)
        prev = tracked.get(file_name)

        head_resp = session.head(file_url, timeout=30)
        remote_last_mod = head_resp.headers.get("Last-Modified")
        remote_len = head_resp.headers.get("Content-Length")

        unchanged = (
            prev is not None
            and os.path.exists(local_path)
            and (
                (remote_last_mod and remote_last_mod == prev.get("last_modified"))
                or (
                    not remote_last_mod
                    and remote_len
                    and str(remote_len) == str(prev.get("content_length"))
                    and os.path.getsize(local_path) == int(remote_len)
                )
            )
        )

        if unchanged:
            summary["skipped"].append(file_name)
            time.sleep(REQUEST_DELAY_SEC)
            continue

        download_file_atomic(session, file_url, local_path)
        tracked[file_name] = {
            "last_modified": remote_last_mod,
            "content_length": int(remote_len) if remote_len and remote_len.isdigit() else os.path.getsize(local_path),
            "sha256": sha256_file(local_path),
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }
        summary["downloaded"].append(file_name)
        time.sleep(REQUEST_DELAY_SEC)

    for stale_name in [k for k in list(tracked.keys()) if k not in remote_set]:
        stale_path = os.path.join(BLS_DIR, stale_name)
        if os.path.exists(stale_path):
            os.remove(stale_path)
        del tracked
