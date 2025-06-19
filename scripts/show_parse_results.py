#!/usr/bin/env python
import sys
from pathlib import Path

# add project root (one level up from scripts/) to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

from pipelines.parse_csr import parsing_job

if __name__ == "__main__":
    # Run the job in-process
    result = parsing_job.execute_in_process()

    # Pull back the dynamic outputs from your extract_esg_metrics op
    metrics_map = result.output_for_node("extract_esg_metrics")
    # metrics_map is a dict: { "0000320193": {...}, "0000789019": {...}, … }

    print("\nESG metrics per CIK:")
    for cik, metrics in metrics_map.items():
        print(f"{cik} → {metrics}")



