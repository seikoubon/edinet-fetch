"""Fetch Queria's processed EDINET tables for private research."""
import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from common import canonical, digest, now

API = "https://api.queria.io/v1/datasets/queria/edinet"
START = date(2016, 11, 14)
TABLES = {
    "business_results": "t.*",
    "company_profiles": "t.doc_id, t.consolidated_subsidiaries, t.research_and_development_expenses",
    "human_capital": "t.*",
}


def extract(root, end):
    import duckdb

    request = Request(API, headers={"User-Agent": "edinet-fetch (+https://github.com/seikoubon/edinet-fetch)", "Accept": "application/json"})
    with urlopen(request, timeout=30) as response:
        meta = json.load(response)
    url = meta["versioned_catalog_url"]
    parsed = urlparse(url)
    if (meta.get("visibility") != "public" or parsed.scheme != "https"
            or parsed.netloc != "data.queria.io"
            or not parsed.path.startswith("/datasets/queria/edinet/") or not parsed.query):
        raise ValueError("Unexpected Queria catalog")

    observed = now()
    root.mkdir(parents=True, exist_ok=True)
    files = {}
    with duckdb.connect(config={"threads": 1, "memory_limit": "2GB"}) as db:
        db.execute("INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs")
        db.execute("SET http_timeout=60; SET http_retries=1")
        db.execute("ATTACH 'ducklake:" + url.replace("'", "''") + "' AS edinet (READ_ONLY)")
        db.execute("""
            CREATE TEMP TABLE selected_docs AS
            SELECT * FROM edinet.main.mart_documents
            WHERE submit_date BETWEEN ? AND ? AND ordinance_code = '010'
              AND doc_type_code IN ('120','130','140','150','160','170')
              AND nullif(trim(sec_code),'') IS NOT NULL
        """, [START, end])
        # Several source listings can refer to the same filing. Preserve all of
        # them, but join each financial observation to the filing only once.
        db.execute("""
            CREATE TEMP TABLE filing_keys AS
            SELECT doc_id,
                   CASE WHEN count(DISTINCT submit_datetime) = 1 THEN min(submit_datetime) END AS submit_datetime,
                   CASE WHEN count(DISTINCT sec_code) = 1 THEN min(sec_code) END AS sec_code,
                   CASE WHEN count(DISTINCT doc_type_code) = 1 THEN min(doc_type_code) END AS doc_type_code
            FROM selected_docs WHERE doc_id IS NOT NULL GROUP BY doc_id
        """)
        queries = {"documents": "SELECT * FROM selected_docs"}
        for table, columns in TABLES.items():
            queries[table] = f"""
                SELECT {columns}, d.submit_datetime AS filing_submitted_at,
                       d.sec_code AS filing_sec_code, d.doc_type_code AS filing_type
                FROM edinet.main.mart_{table} AS t
                JOIN filing_keys AS d USING (doc_id)
            """
        seen, unique = db.execute("SELECT count(*), count(DISTINCT doc_id) FROM selected_docs").fetchone()
        print(f"Filing index: {seen} rows, {unique} distinct filings", flush=True)
        if not unique:
            raise ValueError("No filings in the selected interval")
        for name, sql in queries.items():
            path = root / (name + ".parquet")
            db.sql(sql).write_parquet(str(path), compression="zstd")
            count, docs = db.execute("SELECT count(*), count(DISTINCT doc_id) FROM read_parquet(?)", [str(path)]).fetchone()
            if not count:
                raise ValueError(f"No rows for {name}")
            files[path.name] = {"rows": count, "documents": docs, "bytes": path.stat().st_size, "sha256": digest(path.read_bytes())}
            print(f"{name}: {count} rows, {docs} filings", flush=True)
        first, last, companies = db.execute("SELECT min(submit_date), max(submit_date), count(DISTINCT sec_code) FROM selected_docs").fetchone()
    manifest = {
        "origin": "金融庁 EDINET", "processor": "Queria", "dataset": "https://queria.io/datasets/queria/edinet",
        "catalog": url, "updated_at": meta["updated_at"], "observed_at": observed,
        "start": str(START), "end": str(end), "first_filing": str(first), "last_filing": str(last),
        "securities": companies, "files": files, "backtest_approved": False,
    }
    (root / "manifest.json").write_bytes(canonical(manifest))


def publish(root):
    from botocore.exceptions import ClientError
    from storage import Storage

    manifest = json.loads((root / "manifest.json").read_bytes())
    store = Storage(root, prefix="japan-swing/queria/bulk/v1")
    version = digest(manifest["catalog"].encode())[:24]
    snapshot = f"{manifest['start']}_{manifest['end']}/{version}"
    key = store.key(snapshot + "/manifest.json")
    try:
        prior = json.loads(store.s3.get_object(Bucket=store.bucket, Key=key)["Body"].read())
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
    else:
        if prior["catalog"] != manifest["catalog"]:
            raise ValueError("Catalog mismatch in existing snapshot")
        print(f"Already published: {snapshot}")
        return

    for filename, info in manifest["files"].items():
        object_key = store.key(snapshot + "/" + filename)
        body = (root / filename).read_bytes()
        if digest(body) != info["sha256"]:
            raise ValueError(f"Local checksum mismatch: {filename}")
        store.s3.put_object(Bucket=store.bucket, Key=object_key, Body=body, Metadata={"sha256": info["sha256"]})
        stored = store.s3.head_object(Bucket=store.bucket, Key=object_key)
        if stored["ContentLength"] != info["bytes"] or stored["Metadata"].get("sha256") != info["sha256"]:
            raise ValueError(f"R2 upload mismatch: {filename}")
    store.s3.put_object(Bucket=store.bucket, Key=key, Body=canonical(manifest), ContentType="application/json")
    print(f"R2 published: {snapshot}, {manifest['securities']} security codes, "
          f"{manifest['first_filing']} to {manifest['last_filing']}, "
          f"{sum(f['bytes'] for f in manifest['files'].values())} Parquet bytes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["extract", "publish"])
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    if args.operation == "extract":
        extract(args.directory, datetime.now(ZoneInfo("Asia/Tokyo")).date() - timedelta(days=1))
    else:
        publish(args.directory)
