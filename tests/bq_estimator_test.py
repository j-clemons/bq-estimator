from __future__ import annotations

import pytest

from bq_estimator import bq_estimate
from bq_estimator import format_data

working_query = '''\
SELECT name, COUNT(*) as name_count
FROM `bigquery-public-data.usa_names.usa_1910_2013`
WHERE state = 'WA'
GROUP BY name
'''


@pytest.mark.parametrize(
    ('test_query', 'expected'),
    (
        (working_query, 65.94),
    ),
)
def test_bq_estimate(test_query: str, expected: float) -> None:
    comp_val = round(bq_estimate(test_query) / 1_000_000, 2)

    assert comp_val == expected


@pytest.mark.parametrize(
    ('bytes', 'expected_string'),
    (
        (0.0, '0.00 KB'),
        (12_345, '12.35 KB'),
        (12_345_678, '12.35 MB'),
        (999_345_678_901_234_000, '999345.68 TB'),
    ),
)
def test_format_data(bytes: float, expected_string: str) -> None:
    assert format_data(bytes) == expected_string
