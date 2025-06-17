import os
import requests
from dagster import op, job

# Updated mapping: live CSR PDF URLs as of June 2025
CSR_PDF_URLS = {
    "0000320193": "https://www.apple.com/environment/pdf/Apple_Environmental_Progress_Report_2024.pdf",  # AAPL
    "0000789019": "https://cdn-dynmedia-1.microsoft.com/is/content/microsoftcorp/microsoft/msc/documents/presentations/CSR/Microsoft-2024-Environmental-Sustainability-Report.pdf",  # MSFT
    "0000034088": "https://corporate.exxonmobil.com/-/media/global/files/sustainability-report/2024/sustainability-report-executive-summary.pdf",  # XOM :contentReference[oaicite:0]{index=0}
    "0000019617": "https://www.jpmorganchase.com/content/dam/jpmc/jpmorgan-chase-and-co/documents/Climate-Report-2024.pdf",  # JPM
    "0000320187": "https://media.about.nike.com/files/2fd5f76d-50a2-4906-b30b-3b6046f36ebf/FY23_Nike_Impact_Report.pdf"  # NKE :contentReference[oaicite:1]{index=1}
}

USER_AGENT = "MyESGApp/0.1 (data-team@mycompany.com)"

@op
def download_csr_reports(context):
    for cik, url in CSR_PDF_URLS.items():
        context.log.info(f"Downloading CSR PDF for CIK={cik} from {url}…")
        resp = requests.get(url, headers={"User-Agent": USER_AGENT})
        if resp.status_code == 200 and url.lower().endswith(".pdf"):
            out_dir = os.path.join("data", "raw", cik, "2024")
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, "csr.pdf")
            with open(out_path, "wb") as f:
                f.write(resp.content)
            context.log.info(f"Wrote {out_path}")
        else:
            context.log.error(f"Failed to download CSR PDF for {cik}: HTTP {resp.status_code}")

@job
def ingest_csr_from_ir():
    download_csr_reports()
