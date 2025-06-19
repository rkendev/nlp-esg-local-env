from pathlib import Path
import yaml
from dagster import op, graph, DynamicOut, DynamicOutput, job

PILOT_CIKS = [
    "0000320193", "0000789019", "0000034088", "0000019617", "0000320187"
]

@op(out=DynamicOut())
def split_to_ciks(_):
    for cik in PILOT_CIKS:
        yield DynamicOutput(cik, mapping_key=cik)

@op
def load_esg_lexicon() -> dict:
    with open("resources/esg_lexicon.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)

@op
def load_csr_text(context, cik: str) -> str:
    path = Path(f"data/raw/{cik}/2024/csr.txt")
    context.log.info(f"Loading OCR text for {cik}")
    return path.read_text(encoding="utf-8")

@op
def extract_esg_metrics(context, cik: str, text: str, lexicon: dict) -> dict:
    text_lower = text.lower()
    pos_hits = sum(text_lower.count(term) for term in lexicon.get("positive", []))
    neg_hits = sum(text_lower.count(term) for term in lexicon.get("negative", []))
    context.log.info(f"{cik}: +{pos_hits}, -{neg_hits}")
    return {
        "cik": cik,
        "positive_mentions": pos_hits,
        "negative_mentions": neg_hits,
        "net_score": pos_hits - neg_hits,
    }

@graph
def csr_parsing_pipeline():
    lex = load_esg_lexicon()
    # branch dynamically per CIK
    split_to_ciks().map(
        lambda c: extract_esg_metrics(c, load_csr_text(c), lex)
    )

parsing_job = csr_parsing_pipeline.to_job(name="parse_csr_job")
