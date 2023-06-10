from __future__ import annotations

import argparse
import sys
from io import StringIO
from re import findall
from typing import Sequence

from dbt.cli.main import dbtRunner
from dbt.cli.main import dbtRunnerResult
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery


RED = '\033[91m'
NORMAL = '\033[m'


def bq_estimate(query_text: str) -> tuple[float, str]:
    client = bigquery.Client()

    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    try:
        query_job = client.query(query_text, job_config=job_config)
    except BadRequest as e:
        return -1, e

    return query_job.total_bytes_processed, ''


class NullIO(StringIO):
    def write(self, txt):
        pass


def dbt_process(dbt_selection: str) -> Sequence[str]:
    dbt = dbtRunner()

    ls_args = ['ls', '--select', dbt_selection, '--resource-type', 'model']

    sys.stdout = NullIO()
    res: dbtRunnerResult = dbt.invoke(ls_args)

    if res.success is False:
        return []

    result = []
    for m in res.result:
        model = findall(r'^[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)+', m)
        if model != []:
            ele = model[0].split('.')
            result.append(
                f'target/compiled/{ele[0]}/models/'
                f'{"/".join(ele[1:])}.sql',
            )

        if 'No nodes selected' in m:
            return []

    dbt.invoke(['compile', '-s', dbt_selection])
    sys.stdout = sys.__stdout__

    return result


def format_data(raw_bytes: float) -> str:
    if raw_bytes == -1.0:
        return 'ERROR'
    elif raw_bytes / 2**10 < 1000:
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
    if num == 'ERROR':
        print(f'{RED}{string} {dash_count*"-"} {num}{NORMAL}')
    else:
        print(f'{string} {dash_count*"-"} {num}')


def process_files(filenames: Sequence[str], verbose: bool) -> float:
    total_est = 0.0
    for file in filenames:
        f_name = file.split('/')[-1]
        with open(file) as f:
            est, txt = bq_estimate(f.read())
            if verbose:
                print_result(f_name, format_data(est))
                print(txt)
            else:
                print_result(f_name, format_data(est))

            total_est += est

    print('Total Estimated Usage: ' + format_data(total_est))
    return total_est


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('filenames', nargs='*')
    parser.add_argument('--dbt', dest='dbt', nargs='*')
    parser.add_argument('--verbose', '-v', action='store_true', default=False)

    args = parser.parse_args(argv)

    if args.dbt is not None:
        dbt_files: list[str] = []
        for darg in args.dbt:
            dbt_files.extend(dbt_process(darg))
        process_files(dbt_files, args.verbose)
    else:
        process_files(args.filenames, args.verbose)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
