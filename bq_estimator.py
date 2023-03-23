from __future__ import annotations
import argparse

from google.cloud import bigquery
from typing import Sequence
from subprocess import run
from re import findall


def bq_estimate(query_text: str) -> int:
    client = bigquery.Client()

    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    query_job = client.query(query_text, job_config=job_config,)

    return query_job.total_bytes_processed / 1_000_000


def dbt_process(dbt_selection: str) -> Sequence[str]:
    list_models = run(
        f'dbt ls --select {dbt_selection} --resource-type model',
        shell=True,
        capture_output=True,
        text=True
    )

    result = []
    for m in list_models.stdout.split('\n'):
        model = findall(r'^[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)+', m)
        if model != []:
            ele = model[0].split('.')
            result.append(f'target/compiled/{ele[0]}/models/'
                          f'{"/".join(ele[1:])}.sql')

        if 'No nodes selected' in m:
            return []

    run(f'dbt compile -s {dbt_selection}', shell=True, capture_output=True)

    return result


def process_files(filenames: Sequence[str]) -> str:
    total_est = 0.0
    for file in filenames:
        with open(file) as f:
            est = bq_estimate(f.read())
            total_est += est
            print(f'{file} ----- {est}')

    print(f'total estimated usage: {total_est}')
    return total_est


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('filenames', nargs='*')
    parser.add_argument('--dbt', dest='dbt', nargs='*')

    args = parser.parse_args(argv)

    if args.dbt is not None:
        for darg in args.dbt:
            process_files(dbt_process(darg))
    else:
        process_files(args.filenames)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
