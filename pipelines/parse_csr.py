from pathlib import Path
import yaml
from dagster import op, graph, DynamicOut, DynamicOutput

# List of pilot CIKs to process dynamically
PILOT_CIKS = [
    "0000320193",
    "0000789019",
    "0000034088",
    "0000019617",
    "0000320187",
]

@op(out=DynamicOut())
def split_to_ciks(_):
    """
    Dynamically emit outputs for each pilot CIK.
    """
    for cik in PILOT_CIKS:
        yield DynamicOutput(value=cik, mapping_key=cik)

@op
def load_esg_lexicon() -> dict:
    """
    Load and flatten the ESG lexicon from resources/esg_lexicon.yaml.
    The YAML has top-level pillars (environment, social, governance),
    each with "positive" and "negative" lists. This op flattens them
    into a single dict with top-level "positive" and "negative" keys.
    """
    path = Path(__file__).parent.parent / "resources" / "esg_lexicon.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))

    positive = []
    negative = []
    # Flatten across all pillars
    for pillar in raw.values():
        positive.extend(pillar.get("positive", []))
        negative.extend(pillar.get("negative", []))

    return {"positive": positive, "negative": negative}

@op
def load_csr_text(context, cik: str) -> str:
    """
    Load the OCR-extracted CSR text for a given CIK from the data/raw directory.
    """
    path = Path(f"data/raw/{cik}/2024/csr.txt")
    context.log.info(f"Loading OCR text for {cik}")
    return path.read_text(encoding="utf-8")

@op
def extract_esg_metrics(context, cik: str, text: str, lexicon: dict) -> dict:
    """
    Count occurrences of positive and negative ESG terms in the CSR text.
    """
    text_lower = text.lower()
    pos_hits = sum(text_lower.count(term.lower()) for term in lexicon.get("positive", []))
    neg_hits = sum(text_lower.count(term.lower()) for term in lexicon.get("negative", []))
    context.log.info(f"{cik}: +{pos_hits}, -{neg_hits}")
    return {
        "cik": cik,
        "positive_mentions": pos_hits,
        "negative_mentions": neg_hits,
        "net_score": pos_hits - neg_hits,
    }

@graph
def csr_parsing_pipeline():
    """
    DAG: load lexicon, then for each CIK load text and extract metrics.
    """
    lex = load_esg_lexicon()
    # Dynamically map over each CIK
    split_to_ciks().map(
        lambda c: extract_esg_metrics(c, load_csr_text(c), lex)
    )

# Create the Dagster job from the graph
parsing_job = csr_parsing_pipeline.to_job(name="parse_csr_job")
