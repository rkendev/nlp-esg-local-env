#!/usr/bin/env python
"""
Memory-friendly CSR→TXT OCR:
  • Renders one page at a time
  • Uses 200 dpi
  • Frees each PIL image immediately
"""

import logging
from pathlib import Path
from pdf2image import pdfinfo_from_path, convert_from_path
from pytesseract import image_to_string

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

PILOT_CIKS = [
    "0000320193", "0000789019", "0000034088", "0000019617", "0000320187"
]

def ocr_pdf(cik: str, year: int = 2024):
    pdf_path = Path(f"data/raw/{cik}/{year}/csr.pdf")
    txt_path = pdf_path.with_suffix(".txt")
    logger.info(f"[{cik}] → {txt_path}")

    info = pdfinfo_from_path(str(pdf_path))
    num_pages = int(info["Pages"])
    text_chunks = []

    for i in range(1, num_pages + 1):
        logger.info(f"[{cik}] Page {i}/{num_pages}")
        # Only render page i
        pages = convert_from_path(
            str(pdf_path),
            dpi=200,
            first_page=i,
            last_page=i,
            fmt="jpeg",  # slightly lighter than PNG
        )
        img = pages[0]
        text_chunks.append(image_to_string(img))
        # explicitly free
        img.close()
        del pages, img

    txt_path.parent.mkdir(parents=True, exist_ok=True)
    txt_path.write_text("\n\n".join(text_chunks), encoding="utf-8")
    logger.info(f"[{cik}] Done.")

if __name__ == "__main__":
    for cik in PILOT_CIKS:
        try:
            ocr_pdf(cik)
        except Exception:
            logger.exception(f"[{cik}] FAILED, skipping.")
