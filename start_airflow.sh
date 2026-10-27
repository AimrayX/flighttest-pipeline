#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow standalone