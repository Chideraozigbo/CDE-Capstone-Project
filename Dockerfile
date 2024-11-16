FROM quay.io/astronomer/astro-runtime:12.3.0

WORKDIR /cde-capstone

COPY . .

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate


ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

USER astro




