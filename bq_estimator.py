from __future__ import annotations

import argparse
from re import findall
from subprocess import run
from typing import Sequence

from google.cloud import bigquery


def bq_estimate(query_text: str) -> int:
    client = bigquery.Client()

    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    query_job = client.query(query_text, job_config=job_config)

    return query_job.total_bytes_processed


def dbt_process(dbt_selection: str) -> Sequence[str]:
    list_models = run(
        f'dbt ls --select {dbt_selection} --resource-type model',
        shell=True,
        capture_output=True,
        text=True,
    )

    result = []
    for m in list_models.stdout.split('\n'):
        model = findall(r'^[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)+', m)
        if model != []:
            ele = model[0].split('.')
            result.append(
                f'target/compiled/{ele[0]}/models/'
                f'{"/".join(ele[1:])}.sql',
            )

        if 'No nodes selected' in m:
            return []

    run(f'dbt compile -s {dbt_selection}', shell=True, capture_output=True)

    return result


def format_data(raw_bytes: float) -> str:
    if raw_bytes / 2**10 < 1000:
        return f'{raw_bytes/2**10:.2f} KB'
    elif raw_bytes / 2**20 < 1000:
        return f'{raw_bytes/2**20:.2f} MB'
    elif raw_bytes / 2**30 < 1000:
        return f'{raw_bytes/2**30:.2f} GB'
    else:
        return f'{raw_bytes/2**40:.2f} TB'


def print_result(string: str, num: str, char_width: int = 60) -> None:
    dash_count = char_width - len(string) - len(num) \
        if len(string) + len(num) < char_width else 1
    print(f'{string} {dash_count*"-"} {num}')


def process_files(filenames: Sequence[str]) -> float:
    total_est = 0.0
    for file in filenames:
        f_name = file.split('/')[-1]
        with open(file) as f:
            est = bq_estimate(f.read())
            print_result(f_name, format_data(est))
            total_est += est

    print('Total Estimated Usage: ' + format_data(total_est))
    return total_est


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('filenames', nargs='*')
    parser.add_argument('--dbt', dest='dbt', nargs='*')

    args = parser.parse_args(argv)

    if args.dbt is not None:
        dbt_files: list[str] = []
        for darg in args.dbt:
            dbt_files.extend(dbt_process(darg))
        process_files(dbt_files)
    else:
        process_files(args.filenames)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
