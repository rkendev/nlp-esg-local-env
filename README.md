# NLP ESG Project

This repo:
- **Resources**: `resources/esg_lexicon.yaml` defines an ESG lexicon, grouped by pillar (`environment`, `social`, `governance`), each with `positive`/`negative` term lists.
- **Loader**: `pipelines/parse_csr.py` flattens that YAML into a dict of two top-level lists via the `load_esg_lexicon` op.
- **Pipeline**: `parse_csr_job` loads OCR’d CSR text for a handful of CIKs and counts ESG mentions.
- **Tests**:
  - `test_lexicon.py` verifies the raw YAML structure.
  - `test_load_esg_lexicon_op.py` (you should add) verifies the loader’s output shape and size.
- **How to run**:
  - `pytest`
  - `poetry run dagster job execute -f pipelines/parse_csr.py -j parse_csr_job`
  - `python scripts/show_parse_results.py`
