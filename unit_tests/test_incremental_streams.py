from datetime import datetime

from airbyte_cdk.models import SyncMode
from pytest import fixture

from airbyte_source_datadog_usage.source import (
    EstimatedCostStream,
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
        "start_date": "2024-01-01T00",
    }
    stream = HourlyUsageByProductStream(**config)

    assert stream.url_base == "https://api.datadoghq.com"
    assert stream.path() == "/api/v2/usage/hourly_usage"
    assert stream.primary_key == ["timestamp", "product_family"]

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
        "start_date": "2024-01-01T00",
    }
    stream = HourlyUsageByProductStream(**config)

    # Initial sync
    params = stream.request_params(
        stream_state={}, stream_slice=None, next_page_token=None
    )
    assert params == {
        "filter[product_families]": "all",
        "page[limit]": 500,
        "filter[timestamp][start]": "2024-01-01T00",  # from config
    }

    # Incremental sync
    params = stream.request_params(
        stream_state={"timestamp": "2024-03-19T00:00:00Z"},
        stream_slice=None,
        next_page_token=None,
    )
    assert params == {
        "filter[product_families]": "all",
        "page[limit]": 500,
        "filter[timestamp][start]": "2024-03-19T00",  # ISO-8601 format
    }


def test_hourly_usage_stream_parse_response(mocker):
    config = {
        "api_key": "test_api_key",
        "application_key": "test_app_key",
        "site": "datadoghq.com",
        "product_families": ["all"],
        "start_date": "2024-01-01T00",
    }
    stream = HourlyUsageByProductStream(**config)

    response = mocker.Mock()
    response.json.return_value = {  # https://docs.datadoghq.com/api/latest/usage-metering/#get-hourly-usage-by-product-family
        "data": [
            {
                "attributes": {
                    "account_name": "test_account",
                    "account_public_id": "abc123",
                    "measurements": [
                        {"usage_type": "host_count", "value": 100},
                        {"usage_type": "container_count", "value": None},
                    ],
                    "org_name": "test_org",
                    "product_family": "infra_hosts",
                    "public_id": "def456",
                    "region": "us1",
                    "timestamp": "2019-09-19T10:00:00.000Z",
                },
                "id": "6564d4299b5ac14acd51b709",
                "type": "usage_timeseries",
            }
        ],
        "meta": {"pagination": {"next_record_id": "next_page_token"}},
    }

    records = list(stream.parse_response(response))
    assert records == [
        {
            "timestamp": "2019-09-19T10:00:00.000Z",
            "product_family": "infra_hosts",
            "org_name": "test_org",
            "measurements": [{"usage_type": "host_count", "value": 100}],
            "type": "usage_timeseries",
        }
    ]


def test_estimated_cost(mocker):
    config = {
        "api_key": "test_api_key",
        "application_key": "test_app_key",
        "site": "datadoghq.com",
    }
    stream = EstimatedCostStream(**config)

    assert stream.url_base == "https://api.datadoghq.com"
    assert stream.path() == "/api/v2/usage/estimated_cost"
    assert stream.primary_key == ["sync_date", "month"]

    assert stream.cursor_field == "sync_date"
    assert stream.supports_incremental
    assert stream.source_defined_cursor

    expected_headers = {
        "DD-API-KEY": config["api_key"],
        "DD-APPLICATION-KEY": config["application_key"],
    }

    assert stream.request_headers() == expected_headers

    mock_now = mocker.patch("airbyte_source_datadog_usage.source.datetime")
    mock_now.now.return_value = datetime(2024, 10, 15)
    params = stream.request_params({})
    assert params == {"start_month": "2024-10"}

    response = mocker.Mock()
    response.json.return_value = {  # https://docs.datadoghq.com/ja/api/latest/usage-metering/#get-estimated-cost-across-your-account
        "data": [
            {
                "type": "cost_by_org",
                "id": "333c20c962c7f38662f9093df1e51aaf40ad17f42cec70cd022e6bebb23ec1c0",
                "attributes": {
                    "account_name": "test_account",
                    "account_public_id": "hoge",
                    "org_name": "my_org",
                    "public_id": "bbhogefuga",
                    "region": "us",
                    "total_cost": 2500,
                    "date": "2024-10-01T00:00:00Z",
                    "charges": [
                        {
                            "product_name": "apm_fargate",
                            "charge_type": "committed",
                            "cost": 2500,
                            "last_aggregation_function": "average",
                        },
                        {
                            "product_name": "apm_fargate",
                            "charge_type": "on_demand",
                            "cost": 100,
                            "last_aggregation_function": "average",
                        },
                        {
                            "product_name": "siem",
                            "charge_type": "total",
                            "cost": 0,
                            "last_aggregation_function": "sum",
                        },
                    ],
                },
            }
        ]
    }
    records = list(stream.parse_response(response))
    assert records == [
        {
            "sync_date": "2024-10-15",
            "month": "2024-10",
            "org_name": "my_org",
            "total_cost": 2500,
            "charges": [
                {
                    "product_name": "apm_fargate",
                    "charge_type": "committed",
                    "cost": 2500,
                    "last_aggregation_function": "average",
                },
                {
                    "product_name": "apm_fargate",
                    "charge_type": "on_demand",
                    "cost": 100,
                    "last_aggregation_function": "average",
                },
                {
                    "product_name": "siem",
                    "charge_type": "total",
                    "cost": 0,
                    "last_aggregation_function": "sum",
                },
            ],
        }
    ]
