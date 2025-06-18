import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from dagster import build_init_resource_context
from pipelines.bronze_ingest import bronze_ingest

@ pytest.fixture(autouse=True)
def setup_and_teardown(tmp_path, monkeypatch):
    # Copy fixture raw CSR files into a temporary data/raw directory
    project_root = Path(__file__).parent.parent
    raw_src = project_root / "data" / "raw"
    raw_dst = tmp_path / "data" / "raw"
    shutil.copytree(raw_src, raw_dst)
    # Ensure bronze output dir exists
    bronze_dst = tmp_path / "data" / "bronze" / "documents"
    bronze_dst.mkdir(parents=True, exist_ok=True)
    # Monkeypatch cwd to tmp, *and* ensure pipelines/ package is still importable
    monkeypatch.chdir(tmp_path)
    # Prepend the real project root so "import pipelines..." still works
    # monkeypatch.syspath_prepend(str(project_root.resolve()))
    yield


def test_bronze_ingest_creates_parquet(tmp_path):
    # Execute the job in-process
    result = bronze_ingest.execute_in_process()
    assert result.success, "bronze_ingest job failed"

    # Verify at least one parquet file was written
    out_dir = Path("data") / "bronze" / "documents"
    files = list(out_dir.glob("*.parquet"))
    assert files, f"No parquet files found in {out_dir}"

    # Read and inspect schema
    table = pq.read_table(str(files[0]))
    expected_cols = {"cik", "year", "doc_type", "path", "ingested_at"}
    assert set(table.schema.names) == expected_cols

    # Spot-check row count
    assert table.num_rows >= 1
