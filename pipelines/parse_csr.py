# pipelines/parse_csr.py

from pathlib import Path
from dagster import op, graph, DynamicOut, DynamicOutput, job

PILOT_CIKS = [
    "0000320193", "0000789019", "0000034088", "0000019617", "0000320187"
]

@op(out=DynamicOut())
def split_to_ciks(_):
    for cik in PILOT_CIKS:
        yield DynamicOutput(cik, mapping_key=cik)

@op
def load_csr_text(context, cik: str) -> str:
    path = Path(f"data/raw/{cik}/2024/csr.txt")
    context.log.info(f"Loading OCR text for {cik}")
    return path.read_text(encoding="utf-8")

@op
def extract_esg_metrics(context, cik: str, text: str) -> dict:
    # placeholder: run your NLP/keyword extraction here
    context.log.info(f"Extracting ESG metrics for {cik}")
    return {"cik": cik, "mention_count": text.lower().count("sustainability")}

@graph
def csr_parsing_pipeline():
    parsed = split_to_ciks().map(lambda c: extract_esg_metrics(c, load_csr_text(c)))

parsing_job = csr_parsing_pipeline.to_job(name="parse_csr_job")
