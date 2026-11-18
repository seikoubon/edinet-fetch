import argparse
import gzip
import json
import os
import time
from datetime import UTC, date, datetime, timedelta
from tempfile import TemporaryDirectory
from xml.etree.ElementTree import ParseError

import httpx

from common import atomic, canonical, digest, now
from edinet import (
    END,
    JST,
    PARSER_VERSION,
    START,
    TYPES,
    APIError,
    Client,
    Lake,
    eligible,
)
from storage import Remote

ENGINE_VERSION = 1
TERMINAL = {"unavailable", "parse_failed", "retry_exhausted", "invalid_request"}


def utc(value):
    return datetime.fromisoformat(value).astimezone(UTC)


def days_between(start, end):
    a, b = date.fromisoformat(start), date.fromisoformat(end)
    return [
        (a + timedelta(days=i)).isoformat() for i in range(max(0, (b - a).days + 1))
    ]


def emit(event, **values):
    print(
        json.dumps({"event": event, **values}, ensure_ascii=False, default=str),
        flush=True,
    )


def automation_state(lake):
    a = lake.state.setdefault("automation", {})
    for name, default in {
        "version": ENGINE_VERSION,
        "phase": "backfill",
        "doc_outcomes": {},
        "list_outcomes": {},
        "indexed_lists": {},
        "catalog_files": {},
        "daily_pending": [],
        "daily_cursor": END,
        "history": [],
    }.items():
        a.setdefault(name, default)
    return a


def classify_failure(exc, previous, signature, at=None, is_day=False, day=None):
    stamp = at or now()
    attempts = (
        previous.get("attempts", 0) + 1 if previous.get("signature") == signature else 1
    )
    code = str(exc) if isinstance(exc, APIError) else type(exc).__name__
    if code == "budget_exhausted":
        return {
            "status": "paused",
            "attempts": 0,
            "signature": signature,
            "code": code,
            "at": stamp,
        }
    if code in {
        "http_401",
        "http_403",
        "listing_status_401",
        "listing_status_403",
        "missing_edinet_key",
    }:
        raise APIError("authentication_or_configuration_failure") from None
    if isinstance(exc, (ParseError, UnicodeError)) or (
        isinstance(exc, ValueError) and not isinstance(exc, APIError)
    ):
        status = "parse_failed" if attempts >= 2 else "retry_wait"
        reason = "structured_data_parse_failure"
    elif code in {"http_404", "http_410"} and not is_day:
        status = "unavailable" if attempts >= 2 else "retry_wait"
        reason = "provider_archive_unavailable_after_bounded_retries"
    elif is_day and code in {
        "http_400",
        "listing_status_400",
        "http_404",
        "listing_status_404",
    }:
        # Only classify a provably old date as outside the rolling service window.
        today = datetime.now(JST).date()
        import calendar

        floor = date(
            today.year - 10,
            today.month,
            min(today.day, calendar.monthrange(today.year - 10, today.month)[1]),
        ) - timedelta(days=14)
        expired = day is not None and date.fromisoformat(day) < floor
        status = "unavailable" if expired else "invalid_request"
        reason = (
            "outside_rolling_window"
            if expired
            else "listing_request_rejected_requires_review"
        )
    else:
        status = "retry_exhausted" if attempts >= 5 else "retry_wait"
        reason = "transient_http_or_transport_failure"
    return {
        "status": status,
        "attempts": attempts,
        "signature": signature,
        "code": code,
        "reason": reason,
        "at": stamp,
        "parser_version": PARSER_VERSION,
        "next_retry_at": (
            utc(stamp) + timedelta(minutes=min(1440, 15 * 4 ** (attempts - 1)))
        ).isoformat(),
    }


def can_attempt(outcome, signature, at=None):
    if (
        not outcome
        or outcome.get("signature") != signature
        or outcome.get("parser_version") != PARSER_VERSION
    ):
        return True
    if outcome["status"] in TERMINAL:
        return False
    return not outcome.get("next_retry_at") or utc(outcome["next_retry_at"]) <= utc(
        at or now()
    )


def catalog(lake, remote, changed_paths=None):
    a = automation_state(lake)
    records = {}
    remote.fetch_many(p for p in a["catalog_files"].values() if p in remote.baseline)
    months = {
        month: json.loads(gzip.decompress((lake.root / path).read_bytes()))
        for month, path in a["catalog_files"].items()
    }
    for month in sorted(months):
        for doc, entry in months[month].items():
            if (
                doc not in records
                or entry["observed_at"] >= records[doc]["observed_at"]
            ):
                records[doc] = entry
    changed = set()
    for day, info in sorted(lake.state["lists"].items()):
        if a["indexed_lists"].get(day) == info["hash"]:
            continue
        remote.fetch(info["path"]) if not (lake.root / info["path"]).exists() else None
        payload = json.loads(gzip.decompress((lake.root / info["path"]).read_bytes()))
        month = day[:7]
        target = months.setdefault(month, {})
        for m in payload["results"]:
            doc = m.get("docID")
            if not doc:
                continue
            previous = records.get(doc)
            if not (
                (str(m.get("docTypeCode")) in TYPES and m.get("ordinanceCode") == "010")
                or previous
            ):
                continue
            # Null identities in withdrawal notices retain prior identity evidence;
            # the notice's active/withdrawal flags still determine eligibility.
            combined = (
                {
                    **previous["metadata"],
                    **{k: v for k, v in m.items() if v is not None},
                }
                if previous and m.get("secCode") is None
                else dict(m)
            )
            entry = {
                "metadata": combined,
                "observed_at": info["observed_at"],
                "source_day": day,
                "list_hash": info["hash"],
            }
            target[doc] = entry
            if not previous or entry["observed_at"] >= previous["observed_at"]:
                records[doc] = entry
        a["indexed_lists"][day] = info["hash"]
        changed.add(month)
    for month in changed:
        path = "catalog/" + month + ".json.gz"
        atomic(lake.root / path, gzip.compress(canonical(months[month]), mtime=0))
        a["catalog_files"][month] = path
        if changed_paths is not None:
            changed_paths.add(path)
    codes = set(lake.state.get("historical_tse_codes", []))
    return {
        doc: e for doc, e in records.items() if e["metadata"].get("secCode") in codes
    }


class Controller:
    def __init__(self, lake, remote, client, deadline):
        self.lake, self.remote, self.client, self.deadline = (
            lake,
            remote,
            client,
            deadline,
        )
        self.a = automation_state(lake)
        self.docs = self.list_days = 0
        self.last_publish = time.monotonic()
        self.published_docs = 0
        self.changes = set()
        self.failures = 0
        self.outage = False
        self.lake.compressed_state = True
        self.lake.loader = remote.fetch

    def checkpoint(self, force=False):
        if not self.changes:
            return
        batch_size = 100
        if (
            force
            or time.monotonic() - self.last_publish >= 120
            or self.docs - self.published_docs >= batch_size
        ):
            self.lake.save()
            self.changes.add("state/checkpoint.json.gz")
            self.remote.push(changed_paths=self.changes)
            self.changes.clear()
            self.published_docs = self.docs
            self.last_publish = time.monotonic()
            emit(
                "automation_checkpoint",
                documents_this_run=self.docs,
                listed_days=len(self.lake.state["lists"]),
            )

    def listing(self, day, refresh=False):
        if time.monotonic() > self.deadline:
            return False
        signature = day
        if not can_attempt(self.a["list_outcomes"].get(day), signature):
            return False
        try:
            self.lake.listing(self.client, day, refresh=refresh, persist=False)
            self.changes.add(self.lake.state["lists"][day]["path"])
            self.a["list_outcomes"].pop(day, None)
            self.list_days += 1
            self.checkpoint()
            return True
        except (APIError, ValueError, httpx.HTTPError) as exc:
            outcome = classify_failure(
                exc,
                self.a["list_outcomes"].get(day, {}),
                signature,
                is_day=True,
                day=day,
            )
            if outcome["status"] == "paused":
                self.deadline = time.monotonic()
                return False
            self.a["list_outcomes"][day] = outcome
            self.failures += 1
            self.changes.add("state/checkpoint.json.gz")
            self.outage = outcome["reason"] == "transient_http_or_transport_failure"
            emit("listing_failure", code=outcome["code"])
            return False

    def process(self, work, limit, deadline=None):
        count = 0
        for entry in work:
            m = entry["metadata"]
            doc = m["docID"]
            if (
                time.monotonic() > min(deadline or self.deadline, self.deadline)
                or count >= limit
            ):
                break
            signature = digest(canonical(m))
            previous = self.lake.state["documents"].get(doc)
            if (
                previous
                and previous["metadata_hash"] == signature
                or doc + "-" + signature in self.lake.state.get("archives", {})
            ):
                continue
            old = self.a["doc_outcomes"].get(doc, {})
            if not can_attempt(old, signature):
                continue
            try:
                archive, _ = self.lake.download_archive(self.client, m, signature)
                self.changes.update((archive["path"], archive["metadata_path"]))
            except (
                APIError,
                ValueError,
                ParseError,
                UnicodeError,
                httpx.HTTPError,
            ) as exc:
                result = classify_failure(exc, old, signature)
                if result["status"] == "paused":
                    self.deadline = time.monotonic()
                    break
                self.a["doc_outcomes"][doc] = result
                self.failures += 1
                self.changes.add("state/checkpoint.json.gz")
                emit("document_failure", code=result["code"])
                if result["reason"] == "transient_http_or_transport_failure":
                    self.outage = True
                    break
                continue
            count += 1
            self.docs += 1
            self.a["doc_outcomes"].pop(doc, None)
            self.checkpoint()
        return count

    def backfill(self, entries, max_docs, max_days):
        if self.a["phase"] == "daily":
            return entries
        max_docs -= self.process(self.historical_work(entries), max_docs)
        if max_docs <= 0 or self.outage:
            return entries
        count = 0
        for day in days_between(START, END):
            if count >= max_days or time.monotonic() > self.deadline - 60:
                break
            if day in self.lake.state["lists"]:
                continue
            outcome = self.a["list_outcomes"].get(day, {})
            if not can_attempt(outcome, day):
                continue
            self.listing(day)
            count += 1
            if self.outage:
                return entries
        if count:
            entries = catalog(self.lake, self.remote, self.changes)
            self.process(self.historical_work(entries), max_docs)
        return entries

    @staticmethod
    def historical_work(entries):
        return sorted(
            (
                e
                for e in entries.values()
                if eligible(e["metadata"])
                and START <= e["metadata"]["submitDateTime"][:10] <= END
            ),
            key=lambda e: (e["metadata"]["submitDateTime"], e["metadata"]["docID"]),
        )


def download_history(max_docs, max_days, minutes):
    """Fetch original filings only. Parsed documents and daily cursors are independent."""
    started = time.monotonic()
    deadline = started + minutes * 60 - 30
    # A local rerun must read the latest checkpoint, never a stale file from yesterday.
    with TemporaryDirectory(prefix="edinet-download-") as root:
        remote = Remote(root)
        remote.open()
        lake = Lake(root)
        if not lake.state.get("historical_tse_codes"):
            raise ValueError("missing_historical_tse_universe")
        client = Client(
            os.environ.get("EDINET_API_KEY"), interval=0.5, deadline=deadline
        )
        ctl = Controller(lake, remote, client, deadline)
        try:
            entries = catalog(lake, remote, ctl.changes)
            ctl.backfill(entries, max_docs, max_days)
        except (APIError, KeyboardInterrupt):
            ctl.checkpoint(force=True)
            raise
        finally:
            client.http.close()
        ctl.checkpoint(force=True)
        emit(
            "download_result",
            documents_downloaded=ctl.docs,
            lists_downloaded=ctl.list_days,
            failures=ctl.failures,
            requests=client.requests,
            elapsed_seconds=round(time.monotonic() - started, 2),
        )
        return 1 if ctl.failures else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download historical EDINET filing archives into an existing R2 snapshot."
    )
    parser.add_argument("--max-docs", type=int, default=500)
    parser.add_argument("--max-days", type=int, default=100)
    parser.add_argument("--minutes", type=int, default=25)
    args = parser.parse_args()
    if not (
        1 <= args.max_docs <= 1000
        and 1 <= args.max_days <= 1000
        and 1 <= args.minutes <= 80
    ):
        parser.error("Use 1–1000 documents/days and 1–80 minutes.")
    try:
        status = download_history(args.max_docs, args.max_days, args.minutes)
    except Exception as exc:
        # Network exceptions can include authenticated URLs. Never print them in public logs.
        print("Download failed:", type(exc).__name__, flush=True)
        status = 1
    raise SystemExit(status)
