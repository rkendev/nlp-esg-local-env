# pipelines/edgar_pipeline.py

import os
import requests
from dagster import op, job
from sec_api import QueryApi
from dotenv import load_dotenv

# Load environment variables from .env (must include SEC_API_KEY)
load_dotenv()

PILOT_CIKS = [
    "0000320193",  # AAPL
    "0000789019",  # MSFT
    "0000034088",  # XOM
    "0000019617",  # JPM
    "0000320187",  # NKE
]

@op
def fetch_10k_html(context):
    api_key = os.getenv("SEC_API_KEY")
    if not api_key:
        raise RuntimeError("SEC_API_KEY not set in environment")

    query_api = QueryApi(api_key=api_key)
    results = {}

    for cik in PILOT_CIKS:
        context.log.info(f"Fetching 2024 10-K for CIK={cik}…")

        # Build a query to retrieve the latest 10-K in 2024 for this CIK
        query = {
            "query": f"cik:{cik} AND formType:\"10-K\" AND filedAt:[2024-01-01 TO 2024-12-31]",
            "from": "0",
            "size": "1",
            "sort": [{ "filedAt": { "order": "desc" }}]
        }
        response = query_api.get_filings(query)
        filings = response.get("filings", [])

        if not filings:
            context.log.warning(f"No filings found for {cik}")
            continue

        # Extract the filing detail URL and fetch the HTML page
        url = filings[0]["linkToFilingDetails"]
        html = requests.get(url).text

        # Write out the HTML to data/raw/{cik}/2024/10-K.html
        out_path = os.path.join("data", "raw", cik, "2024", "10-K.html")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)

        results[cik] = out_path
        context.log.info(f"Wrote {out_path}")

    return results

@job
def ingest_10k_job():
    fetch_10k_html()
