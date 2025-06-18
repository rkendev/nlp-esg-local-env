# pipelines/bronze_ingest.py

from pathlib import Path
from datetime import datetime
from dagster import op, job, get_dagster_logger
import pyarrow as pa
import pyarrow.parquet as pq

PILOT_CIKS = [
    "0000320193", "0000789019", "0000034088", "0000019617", "0000320187",
]

@op
def list_raw_docs():
    records = []
    base = Path("data/raw")
    for cik in PILOT_CIKS:
        for year_dir in (base / cik).iterdir():
            if not year_dir.is_dir():
                continue
            year = int(year_dir.name)
            for f in year_dir.iterdir():
                if f.name not in ("csr.pdf", "csr.txt"):
                    continue
                doc_type = "csr_pdf" if f.suffix == ".pdf" else "csr_txt"
                records.append({
                    "cik": cik,
                    "year": year,
                    "doc_type": doc_type,
                    "path": str(f),
                    "ingested_at": datetime.utcnow().isoformat(),
                })
    return records

@op
def ingest_to_bronze(context, records: list[dict]):
    logger = get_dagster_logger()
    out_dir = Path("data/bronze/documents")
    out_dir.mkdir(parents=True, exist_ok=True)

    # write a single Parquet file with all records
    table = pa.Table.from_pylist(records)
    target = out_dir / f"part-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.parquet"
    pq.write_table(table, target)

    logger.info(f"Wrote {len(records)} rows to {target}")

@job
def bronze_ingest():
    recs = list_raw_docs()
    ingest_to_bronze(recs)
