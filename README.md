# bq-estimator
Estimates the data usage of SQL queries executed in BigQuery.
<br>Returns the estimated usage in KB, MB, GB, or TB dependent on the closest multiplier.

# Installation

`pip install bq-estimator`

Google [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
must be set up for this tool to function.

# Usage

Ready to execute SQL queries.

`bq-estimator query.sql [query2.sql query3.sql …]`

## Options
## --dbt

Takes any model selection command that is compatible with dbt

`bq-estimator --dbt [dbt model selection]`

## --verbose -v
If errors are present the error details from BigQuery will be printed to the terminal.
