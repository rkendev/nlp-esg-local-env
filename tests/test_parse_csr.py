from pipelines.parse_csr import parsing_job

def test_parse_csr_job_runs_and_emits_one_metric_per_cik():
    result = parsing_job.execute_in_process()
    assert result.success
    outputs = result.output_for_node("extract_esg_metrics")
    assert isinstance(outputs, dict)
    assert len(outputs) == 5  # one per pilot CIK
    # Spot-check that metrics include the new keys:
    sample = next(iter(outputs.values()))
    assert {"positive_mentions","negative_mentions","net_score"}.issubset(sample.keys())

