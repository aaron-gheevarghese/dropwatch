"""Fast, permanent regression test against the committed fixture
(tests/fixtures/zscore_fixture.json) — deliberately does NOT run the full ~50s sweep
across all 118 real pairs and thresholds that scripts/measure_zscore_filter.py does;
that's a one-off measurement, not something every `pytest` run should pay for. This
just proves the fixture is intact, loadable, and the filter behaves sanely against a
small slice of real data plus the full synthetic edge-case suite.
"""

import json
from decimal import Decimal
from pathlib import Path

from scripts.measure_zscore_filter import MAX_PRICES_NEEDED, MIN_OBSERVATIONS, WINDOW, run_synthetic_cases
from workers.zscore_filter import compute_zscore

FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "zscore_fixture.json"


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def test_fixture_file_exists_and_has_real_data() -> None:
    fixture = _load_fixture()
    assert len(fixture["real_pairs"]) > 0
    assert len(fixture["synthetic_cases"]) > 0
    for pair in fixture["real_pairs"]:
        assert len(pair["prices"]) >= 500, f"{pair['pair_name']} has fewer rows than the fixture's own minimum"


def test_synthetic_edge_cases_all_pass() -> None:
    fixture = _load_fixture()
    outcomes = run_synthetic_cases(fixture["synthetic_cases"])
    failures = [o for o in outcomes if not o["passed"]]
    assert failures == [], f"synthetic edge case(s) failed: {failures}"


def test_sample_of_real_pairs_score_without_error() -> None:
    fixture = _load_fixture()
    # A handful of pairs, not all 118 — this is a smoke test, not the measurement.
    sample = fixture["real_pairs"][:5]

    for pair in sample:
        prices = [Decimal(p) for p in pair["prices"][-MAX_PRICES_NEEDED:]]
        result = compute_zscore(prices, window=WINDOW, min_observations=MIN_OBSERVATIONS)

        assert result is not None, f"{pair['pair_name']}: expected enough history to score (fixture guarantees 500+ rows)"
        if result.is_zero_variance:
            assert result.z is None
        else:
            assert result.z is not None
            assert result.z == result.z  # not NaN
