from __future__ import annotations
import argparse
from google.cloud import bigquery
from typing import Sequence


def bq_estimate(query_text: str) -> int:
    client = bigquery.Client()

    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    query_job = client.query(query_text, job_config=job_config,)

    return query_job.total_bytes_processed / 1_000_000


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('filenames', nargs='*')

    args = parser.parse_args(argv)

    for file in args.filenames:
        with open(file) as f:
            print(bq_estimate(f.read()))


if __name__ == '__main__':
    raise SystemExit(main())
