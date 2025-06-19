import yaml
import pytest
from pathlib import Path

def test_lexicon_loads_and_counts():
    path = Path(__file__).parent.parent / "resources" / "esg_lexicon.yaml"
    lex = yaml.safe_load(path.read_text())
    # must have both sections…
    assert "positive" in lex and "negative" in lex
    # …and at least 200 each
    assert len(lex["positive"]) >= 200
    assert len(lex["negative"]) >= 200
    # total entries ≥400
    assert len(lex["positive"]) + len(lex["negative"]) >= 400
