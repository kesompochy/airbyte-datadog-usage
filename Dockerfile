
FROM airbyte/python-connector-base:1.1.0

WORKDIR /airbyte/integration_code
COPY pyproject.toml poetry.lock ./
COPY main.py ./
COPY airbyte_source_datadog_usage ./airbyte_source_datadog_usage
COPY metadata.yaml ./
COPY README.md ./
RUN pip install .

ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
