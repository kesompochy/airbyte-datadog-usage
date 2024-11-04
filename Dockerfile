
FROM airbyte/python-connector-base:1.1.0

WORKDIR /airbyte/integration_code
COPY pyproject.toml poetry.lock ./
COPY main.py ./
COPY airbyte_source_datadog_usage ./airbyte_source_datadog_usage
COPY metadata.yaml ./
RUN pip install ./airbyte/integration_code
