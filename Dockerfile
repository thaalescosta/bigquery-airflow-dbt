FROM astrocrpublic.azurecr.io/runtime:3.3-7

RUN python - venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate