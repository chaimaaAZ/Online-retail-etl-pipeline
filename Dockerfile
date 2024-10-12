FROM quay.io/astronomer/astro-runtime:12.1.1
RUN python -m venv dbt_venv2 && source dbt_venv2/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate