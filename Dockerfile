FROM quay.io/astronomer/astro-runtime:9.6.0
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-clickhouse && deactivate