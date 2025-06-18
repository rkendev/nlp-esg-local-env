# pipelines/ocr_csr.py

import subprocess
from dagster import op, job, Failure

@op
def run_ocr_standalone(context):
    cmd = ["poetry", "run", "python", "scripts/ocr_csr_standalone.py"]
    context.log.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Failure("Standalone OCR script failed")

@job(name="ocr_csr_job")
def ocr_csr_job():
    run_ocr_standalone()