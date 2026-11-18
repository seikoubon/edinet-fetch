import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from common import atomic, canonical, digest, now

PREFIX = "japan-swing/edinet/v1"


class Storage:
    def __init__(self, root, client=None, bucket=None, prefix=None):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.bucket = bucket or os.environ.get("SWING_S3_BUCKET")
        self.prefix = (prefix or PREFIX).strip("/")
        if not self.bucket:
            raise ValueError("SWING_S3_BUCKET is not configured")
        self.workers = max(
            1, min(16, int(os.environ.get("SWING_TRANSFER_WORKERS", "8")))
        )
        self.s3 = client or boto3.session.Session().client(
            "s3",
            endpoint_url=os.environ.get("SWING_S3_ENDPOINT") or None,
            region_name=os.environ.get("AWS_DEFAULT_REGION", "auto"),
            config=Config(
                max_pool_connections=self.workers,
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )
        self.marker = self.root / ".remote.json"

    def key(self, suffix):
        return f"{self.prefix}/{suffix}"


class Remote(Storage):
    def __init__(self, root, **kwargs):
        super().__init__(root, prefix=PREFIX, **kwargs)
        self.manifest = None
        self.baseline = {}
        self.uploaded = []

    def open(self):
        r = self.s3.get_object(Bucket=self.bucket, Key=self.key("latest.json"))
        self.manifest = json.loads(r["Body"].read())
        self.baseline = dict(self.manifest["files"])
        atomic(self.marker, canonical({"etag": r["ETag"], "files": self.baseline}))
        state = (
            "state/checkpoint.json.gz"
            if "state/checkpoint.json.gz" in self.baseline
            else "state/checkpoint.json"
        )
        self.fetch(state)
        return self.manifest

    def fetch(self, relative):
        path = self.root / relative
        if Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise ValueError("unsafe_manifest_path")
        if path.exists():
            return path
        sha = self.baseline[relative]
        body = self.s3.get_object(Bucket=self.bucket, Key=self.key("blobs/" + sha))[
            "Body"
        ].read()
        if digest(body) != sha:
            raise ValueError("remote_checksum_mismatch")
        atomic(path, body)
        return path

    def fetch_many(self, paths):
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            list(pool.map(self.fetch, paths))

    def push(self, changed_paths):
        old = json.loads(self.marker.read_text())
        merged = dict(old["files"])
        tasks = {}
        paths = (self.root / p for p in changed_paths)
        for path in paths:
            if not path.is_file():
                raise FileNotFoundError(path)
            if path.name.startswith(".") or path.name.endswith((".tmp", ".lock")):
                continue
            relative = path.relative_to(self.root).as_posix()
            body = path.read_bytes()
            sha = digest(body)
            merged[relative] = sha
            if old["files"].get(relative) != sha:
                tasks[sha] = (path, relative)

        if any(
            merged.get(p) != h for p, h in old["files"].items() if p.startswith("raw/")
        ):
            raise ValueError("immutable_raw_replacement_rejected")

        def upload(item):
            sha, (path, relative) = item
            body = path.read_bytes()
            if digest(body) != sha:
                raise ValueError("local_changed_during_publication")
            self.s3.put_object(
                Bucket=self.bucket, Key=self.key("blobs/" + sha), Body=body
            )
            return relative, sha, len(body)

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            self.uploaded = list(pool.map(upload, tasks.items()))
        snapshot = canonical({"version": 1, "created_at": now(), "files": merged})
        self.s3.put_object(
            Bucket=self.bucket,
            Key=self.key("snapshots/" + digest(snapshot) + ".json"),
            Body=snapshot,
        )
        condition = {"IfMatch": old["etag"]} if old["etag"] else {"IfNoneMatch": "*"}
        try:
            r = self.s3.put_object(
                Bucket=self.bucket,
                Key=self.key("latest.json"),
                Body=snapshot,
                **condition,
            )
        except ClientError:
            raise RuntimeError("snapshot_conflict_or_publish_failure") from None
        assert all(
            merged.get(p) == h for p, h in old["files"].items() if p.startswith("raw/")
        )
        self.baseline = merged
        self.manifest = json.loads(snapshot)
        atomic(self.marker, canonical({"etag": r["ETag"], "files": merged}))
        self.metrics = {"files": len(tasks), "bytes": sum(x[2] for x in self.uploaded)}
        return len(tasks)
