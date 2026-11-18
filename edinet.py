import gzip
import io
import json
import re
import time
import zipfile
from email.utils import parsedate_to_datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from common import atomic, canonical, digest, now

API = "https://api.edinet-fsa.go.jp/api/v2"
START, END = "2016-11-14", "2026-11-13"
TYPES = {"120", "130", "140", "150", "160", "170"}
JST = ZoneInfo("Asia/Tokyo")
PARSER_VERSION = 1
MAX_ZIP = 80 * 1024 * 1024
MAX_EXPANDED = 400 * 1024 * 1024


def eligible(row):
    return (
        str(row.get("docTypeCode")) in TYPES
        and row.get("ordinanceCode") == "010"
        and bool(re.fullmatch(r"[0-9A-Z]{5}", row.get("secCode") or ""))
        and str(row.get("withdrawalStatus", "0")) == "0"
        and str(row.get("disclosureStatus", "0")) == "0"
        and str(row.get("xbrlFlag")) == "1"
    )


class APIError(RuntimeError):
    """Safe diagnostic: never includes URLs, request bodies or API keys."""


class ProviderResponseError(APIError):
    """A response affecting the run, rather than evidence against any one filing."""


def provider_status(body):
    """Extract an HTTP-style numeric status only; never expose the provider body."""
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeError):
        return None
    if not isinstance(payload, dict):
        return None
    fields = {k.lower(): v for k, v in payload.items()}
    metadata = fields.get("metadata")
    nested = metadata.get("status") if isinstance(metadata, dict) else None
    for value in (nested, fields.get("statuscode"), fields.get("status")):
        if re.fullmatch(r"[1-5][0-9]{2}", str(value)):
            return int(value)
    return None


class Client:
    def __init__(self, key, transport=None, interval=2.0, deadline=None):
        key = (key or "").strip()
        if not key:
            raise APIError("missing_edinet_key")
        self.key, self.interval, self.requests, self.last = key, interval, 0, 0.0
        self.deadline = deadline
        self.http = httpx.Client(
            timeout=90, follow_redirects=False, transport=transport
        )

    def get(self, path, params):
        for attempt in range(5):
            if self.deadline is not None and time.monotonic() > self.deadline - 3:
                raise APIError("budget_exhausted")
            time.sleep(max(0, self.interval - (time.monotonic() - self.last)))
            try:
                self.last = time.monotonic()
                self.requests += 1
                with self.http.stream(
                    "GET",
                    API + path,
                    params={**params, "Subscription-Key": self.key},
                    timeout=min(90, max(1, self.deadline - time.monotonic()))
                    if self.deadline
                    else 90,
                ) as r:
                    if r.status_code in {429, 500, 502, 503, 504}:
                        if attempt == 4:
                            raise APIError(f"http_{r.status_code}_retry_exhausted")
                        wait = r.headers.get("Retry-After", "")
                        delay = 2**attempt
                        if wait.isdigit():
                            delay = max(delay, int(wait))
                        elif wait:
                            try:
                                delay = max(
                                    delay,
                                    parsedate_to_datetime(wait).timestamp()
                                    - time.time(),
                                )
                            except (ValueError, TypeError, OverflowError):
                                pass
                        if (
                            self.deadline is not None
                            and time.monotonic() + delay >= self.deadline
                        ):
                            raise APIError("budget_exhausted")
                        time.sleep(delay)
                        continue
                    if r.status_code != 200:
                        raise APIError(f"http_{r.status_code}")
                    body = bytearray()
                    for chunk in r.iter_bytes():
                        body.extend(chunk)
                        if len(body) > MAX_ZIP:
                            raise APIError("response_cap_exceeded")
                    return bytes(body)
            except httpx.HTTPError:
                if attempt == 4:
                    raise APIError("transport_retry_exhausted") from None
                time.sleep(2**attempt)
        raise APIError("retry_exhausted")

    def listing(self, day):
        payload = json.loads(self.get("/documents.json", {"date": day, "type": 2}))
        meta = payload.get("metadata", {})
        if str(meta.get("status")) != "200":
            raise APIError("listing_status_" + str(meta.get("status", "missing")))
        rows = payload.get("results")
        if not isinstance(rows, list) or meta.get("resultset", {}).get("count") != len(
            rows
        ):
            raise APIError("listing_count_or_schema_mismatch")
        return payload

    def archive(self, doc_id, typ=1):
        if not re.fullmatch(r"S[0-9A-Z]{7}", doc_id):
            raise APIError("invalid_document_id")
        body = self.get("/documents/" + doc_id, {"type": typ})
        if not zipfile.is_zipfile(io.BytesIO(body)):
            status = provider_status(body)
            if status in {404, 410}:
                raise APIError(f"http_{status}")
            raise ProviderResponseError(f"archive_response_{status or 'not_zip'}")
        checked_zip(body).close()
        return body


def checked_zip(body):
    if len(body) > MAX_ZIP or not zipfile.is_zipfile(io.BytesIO(body)):
        raise ValueError("invalid_zip")
    z = zipfile.ZipFile(io.BytesIO(body))
    if sum(i.file_size for i in z.infolist()) > MAX_EXPANDED:
        z.close()
        raise ValueError("expanded_archive_cap")
    return z


class Lake:
    def __init__(self, root):
        self.root = Path(root)
        path = self.root / "state/checkpoint.json"
        compressed = self.root / "state/checkpoint.json.gz"
        self.compressed_state = compressed.exists()
        self.loader = None
        self.state = (
            json.loads(gzip.decompress(compressed.read_bytes()))
            if compressed.exists()
            else json.loads(path.read_text())
            if path.exists()
            else {"version": 1, "lists": {}, "documents": {}}
        )

    def save(self):
        body = canonical(self.state)
        if self.compressed_state:
            atomic(self.root / "state/checkpoint.json.gz", gzip.compress(body, mtime=0))
        else:
            atomic(self.root / "state/checkpoint.json", body)

    def read_bytes(self, relative):
        path = self.root / relative
        if not path.exists() and self.loader:
            self.loader(relative)
        return path.read_bytes()

    def raw(self, category, body, suffix):
        sha = digest(body)
        path = self.root / "raw" / category / (sha + suffix)
        if path.exists():
            if digest(path.read_bytes()) != sha:
                raise ValueError("immutable_raw_checksum_mismatch")
        else:
            atomic(path, body)
        return path.relative_to(self.root).as_posix(), sha

    def listing(self, client, day, refresh=False, *, persist=True):
        cached = self.state["lists"].get(day)
        if cached and not refresh:
            return json.loads(gzip.decompress(self.read_bytes(cached["path"])))
        payload = client.listing(day)
        path, sha = self.raw(
            "lists", gzip.compress(canonical(payload), mtime=0), ".json.gz"
        )
        self.state["lists"][day] = {"path": path, "hash": sha, "observed_at": now()}
        for row in payload["results"]:
            self.record_status(row)
        if persist:
            self.save()
        return payload

    def record_status(self, meta):
        d = self.state["documents"].get(meta.get("docID"))
        if d is None:
            return
        active = (
            str(meta.get("withdrawalStatus", "0")) == "0"
            and str(meta.get("disclosureStatus", "0")) == "0"
        )
        events = d.setdefault("status_events", [])
        if not events or events[-1]["active"] != active:
            events.append(
                {
                    "at": now(),
                    "active": active,
                    "withdrawal_status": str(meta.get("withdrawalStatus", "0")),
                    "disclosure_status": str(meta.get("disclosureStatus", "0")),
                }
            )

    def download_archive(self, client, meta, fingerprint):
        """Store a new filing version in the same archive cache used by the parser."""
        body = client.archive(meta["docID"])
        observed = now()
        raw_path, sha = self.raw("xbrl", body, ".zip")
        metadata_path, _ = self.raw(
            "metadata", canonical({"metadata": meta, "observed_at": observed}), ".json"
        )
        record = {
            "path": raw_path,
            "hash": sha,
            "metadata_path": metadata_path,
            "observed_at": observed,
        }
        self.state.setdefault("archives", {})[meta["docID"] + "-" + fingerprint] = (
            record
        )
        return record, body
