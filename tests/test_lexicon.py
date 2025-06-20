import yaml
from pathlib import Path

def test_lexicon_loads_and_counts():
    # Load the YAML lexicon
    path = Path(__file__).parent.parent / "resources" / "esg_lexicon.yaml"
    lex = yaml.safe_load(path.read_text())

    # Ensure top-level pillars exist and have both positive/negative lists
    for pillar in ("environment", "social", "governance"):
        assert pillar in lex, f"Missing pillar section: {pillar}"
        section = lex[pillar]
        assert isinstance(section, dict), f"{pillar} section should be a dict"
        assert "positive" in section and "negative" in section, (
            f"Both 'positive' and 'negative' lists required under {pillar}"
        )

    # Count total positive and negative terms across all pillars
    total_positive = sum(len(lex[p]["positive"]) for p in ("environment", "social", "governance"))
    total_negative = sum(len(lex[p]["negative"]) for p in ("environment", "social", "governance"))

    # Must have at least 200 of each polarity
    assert total_positive >= 200, f"Not enough positive terms: {total_positive}"
    assert total_negative >= 200, f"Not enough negative terms: {total_negative}"
