from airbyte_cdk.models import SyncMode
from pytest import fixture

from airbyte_source_datadog_usage.source import (
    HourlyUsageByProductStream,
    IncrementalDatadogUsageStream,
)


@fixture
def patch_incremental_base_class(mocker):
    mocker.patch.object(IncrementalDatadogUsageStream, "path", "v0/example_endpoint")
    mocker.patch.object(
        IncrementalDatadogUsageStream, "primary_key", "test_primary_key"
    )
    mocker.patch.object(IncrementalDatadogUsageStream, "__abstractmethods__", set())


def test_cursor_field(patch_incremental_base_class):
    stream = IncrementalDatadogUsageStream()
    assert stream.cursor_field == "timestamp"


def test_get_updated_state(patch_incremental_base_class):
    stream = IncrementalDatadogUsageStream()

    inputs = {
        "current_stream_state": {},
        "latest_record": {"timestamp": "2024-03-20T00:00:00Z"},
    }
    assert stream.get_updated_state(**inputs) == {"timestamp": "2024-03-20T00:00:00Z"}

    inputs = {
        "current_stream_state": {"timestamp": "2024-03-19T00:00:00Z"},
        "latest_record": {"timestamp": "2024-03-20T00:00:00Z"},
    }
    assert stream.get_updated_state(**inputs) == {"timestamp": "2024-03-20T00:00:00Z"}


def test_supports_incremental(patch_incremental_base_class):
    stream = IncrementalDatadogUsageStream()
    assert stream.supports_incremental


def test_source_defined_cursor(patch_incremental_base_class):
    stream = IncrementalDatadogUsageStream()
    assert stream.source_defined_cursor


def test_stream_checkpoint_interval(patch_incremental_base_class):
    stream = IncrementalDatadogUsageStream()
    assert stream.state_checkpoint_interval == 500


def test_hourly_usage_stream_properties():
    config = {
        "api_key": "test_api_key",
        "application_key": "test_app_key",
        "site": "datadoghq.com",
        "product_families": ["all"],
    }
    stream = HourlyUsageByProductStream(**config)

    assert stream.url_base == "https://api.datadoghq.com"
    assert stream.path == "/api/v2/usage/hourly_usage"
    assert stream.primary_key == "timestamp"

    assert stream.cursor_field == "timestamp"
    assert stream.supports_incremental
    assert stream.source_defined_cursor

    expected_headers = {
        "DD-API-KEY": config["api_key"],
        "DD-APPLICATION-KEY": config["application_key"],
    }
    assert stream.request_headers() == expected_headers


def test_hourly_usage_stream_request_params():
    config = {
        "api_key": "test_api_key",
        "application_key": "test_app_key",
        "site": "datadoghq.com",
        "product_families": ["all"],
    }
    stream = HourlyUsageByProductStream(**config)
    params = stream.request_params(
        stream_state={}, stream_slice=None, next_page_token=None
    )
    assert params == {"filter[product_families]": "all", "page[limit]": 500}

    params = stream.request_params(
        stream_state={"timestamp": "2024-03-19T00:00:00Z"},
        stream_slice=None,
        next_page_token=None,
    )
    assert params == {
        "filter[product_families]": "all",
        "page[limit]": 500,
        "filter[timestamp][start]": "2024-03-19T00:00:00Z",
    }


def test_hourly_usage_stream_parse_response(mocker):
    config = {
        "api_key": "test_api_key",
        "application_key": "test_app_key",
        "site": "datadoghq.com",
        "product_families": ["all"],
    }
    stream = HourlyUsageByProductStream(**config)

    response = mocker.Mock()
    response.json.return_value = {
        "usage": [
            {
                "attributes": {
                    "timestamp": "2024-03-20T00:00:00Z",
                    "org_name": "test_org",
                    "product_family": "logs",
                }
            }
        ]
    }

    records = list(stream.parse_response(response))
    assert records == [
        {
            "timestamp": "2024-03-20T00:00:00Z",
            "org_name": "test_org",
            "product_family": "logs",
        }
    ]
