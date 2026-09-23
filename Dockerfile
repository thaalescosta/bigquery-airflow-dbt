FROM astrocrpublic.azurecr.io/runtime:3.3-7

ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate