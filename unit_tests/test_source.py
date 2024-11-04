#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from airbyte_source_datadog_usage.source import SourceDatadogUsage


def test_check_connection(mocker):
    source = SourceDatadogUsage()
    logger_mock, config_mock = MagicMock(), MagicMock()
    assert source.check_connection(logger_mock, config_mock) == (True, None)


def test_streams(mocker):
    source = SourceDatadogUsage()
    config = {
        "api_key": "test_api_key",
        "application_key": "test_application_key",
        "site": "datadoghq.com",
        "product_families": ["all"],
    }
    streams = source.streams(config)
    expected_streams_number = 1
    assert len(streams) == expected_streams_number
