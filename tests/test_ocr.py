# tests/test_ocr.py

import glob
import subprocess
import os
import pytest

CIKs = [
    "0000320193",
    "0000789019",
    "0000034088",
    "0000019617",
    "0000320187",
]

@pytest.fixture(scope="session", autouse=True)
def run_ocr(tmp_path_factory):
    # point OCR output at a temp dir if you want, or just run in place
    subprocess.run(
        ["poetry", "run", "python", "scripts/ocr_csr_standalone.py"],
        check=True,
    )

def test_five_txt_files_exist():
    paths = glob.glob("data/raw/*/2024/csr.txt")
    assert len(paths) >= 5, f"Only found {len(paths)} .txt files"

@pytest.mark.parametrize("cik", CIKs)
def test_txt_not_empty(cik):
    path = f"data/raw/{cik}/2024/csr.txt"
    assert os.path.exists(path), f"{path} not found"
    assert os.path.getsize(path) > 0, f"{path} is empty"
