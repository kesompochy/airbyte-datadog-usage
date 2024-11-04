#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from airbyte_source_datadog_usage.source import SourceDatadogUsage


def test_check_connection(mocker):
    source = SourceDatadogUsage()
    logger_mock = MagicMock()
    config = {
        "api_key": "test_api_key",
        "application_key": "test_app_key",
        "site": "datadoghq.com",
        "start_date": "2024-01-01T00",
    }

    requests_mock = mocker.patch("requests.get")
    requests_mock.return_value.status_code = 200
    assert source.check_connection(logger_mock, config) == (True, None)

    requests_mock.return_value.status_code = 403
    requests_mock.return_value.text = "Invalid API key"
    assert source.check_connection(logger_mock, config) == (
        False,
        "HTTP 403: Invalid API key",
    )

    requests_mock.side_effect = Exception("Connection error")
    assert source.check_connection(logger_mock, config) == (False, "Connection error")


def test_streams(mocker):
    source = SourceDatadogUsage()
    config = {
        "api_key": "test_api_key",
        "application_key": "test_application_key",
        "site": "datadoghq.com",
        "product_families": ["all"],
        "start_date": "2024-01-01T00",
    }
    streams = source.streams(config)
    expected_streams_number = 1
    assert len(streams) == expected_streams_number
