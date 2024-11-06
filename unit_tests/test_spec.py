from airbyte_source_datadog_usage.source import SourceDatadogUsage


def test_spec_matches_source():
    source = SourceDatadogUsage()

    spec = source.spec(None)
    product_families_spec = spec.connectionSpecification["properties"][
        "hourly_usage_by_product"
    ]["properties"]["product_families"]

    assert product_families_spec["type"] == "array"
    assert product_families_spec["items"]["type"] == "string"

    config = {
        "api_key": "test_api_key",
        "application_key": "test_app_key",
        "site": "datadoghq.com",
        "hourly_usage_by_product": {
            "product_families": [
                "infra_hosts",
                "analyzed_logs",
            ],
            "start_date": "2024-01-01T00",
        },
    }

    stream = source.streams(config)[0]
    assert stream.product_families == ["infra_hosts", "analyzed_logs"]
