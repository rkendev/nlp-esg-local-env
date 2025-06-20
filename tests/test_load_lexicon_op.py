# tests/test_load_lexicon_op.py
from pipelines.parse_csr import load_esg_lexicon

def test_load_esg_lexicon_op_returns_flat_dict():
    # call the op directly (it returns a dict)
    lex = load_esg_lexicon()

    # it should have exactly these two top-level keys:
    assert set(lex.keys()) == {"positive", "negative"}

    # and at least 200 of each
    assert len(lex["positive"]) >= 200
    assert len(lex["negative"]) >= 200

    # all entries must be simple strings (no nested lists/dicts)
    assert all(isinstance(t, str) for t in lex["positive"])
    assert all(isinstance(t, str) for t in lex["negative"])
