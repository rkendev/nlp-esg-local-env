# pipelines/ocr_csr_per_page.py

import os
from pathlib import Path
from pdf2image import convert_from_path
from pytesseract import image_to_string
from dagster import op, job, DynamicOut, DynamicOutput

PILOT_CIKS = [
    "0000320193", "0000789019", "0000034088", "0000019617", "0000320187"
]

@op(out=DynamicOut())
def split_to_pages(_):
    for cik in PILOT_CIKS:
        pdf_path = Path(f"data/raw/{cik}/2024/csr.pdf")
        images = convert_from_path(str(pdf_path), dpi=300)
        for idx, img in enumerate(images, start=1):
            yield DynamicOutput(
                {"cik": cik, "page": idx, "img": img},
                mapping_key=f"{cik}_p{idx}"
            )

@op
def ocr_page(context, payload):
    cik = payload["cik"]
    idx = payload["page"]
    img = payload["img"]
    text = image_to_string(img)
    return {"cik": cik, "page": idx, "text": text}

@op
def assemble_csr(context, page_texts: list[dict]):
    # page_texts is an unordered list; sort and write one CSR per CIK
    from collections import defaultdict
    by_cik = defaultdict(list)
    for rec in page_texts:
        by_cik[rec["cik"]].append((rec["page"], rec["text"]))
    for cik, pages in by_cik.items():
        pages.sort()
        txt_path = Path(f"data/raw/{cik}/2024/csr.txt")
        txt_path.parent.mkdir(parents=True, exist_ok=True)
        with open(txt_path, "w", encoding="utf-8") as f:
            for _, t in pages:
                f.write(t)
        context.log.info(f"Wrote OCR text to {txt_path}")

@job(name="ocr_csr_per_page_job")
def ocr_csr_per_page_job():
    assembled = split_to_pages().map(ocr_page).collect()
    assemble_csr(assembled)
