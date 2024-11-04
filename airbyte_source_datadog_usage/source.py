#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator


# Basic full refresh stream
class DatadogUsageStream(HttpStream, ABC):

    @property
    def url_base(self) -> str:
        return self._url_base

    @property
    @abstractmethod
    def path(self) -> str:
        pass

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        json_response = response.json()
        next_record_id = (
            json_response.get("meta", {}).get("pagination", {}).get("next_record_id")
        )

        if next_record_id:
            return {"next_record_id": next_record_id}
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        yield {}


# Basic incremental stream
class IncrementalDatadogUsageStream(DatadogUsageStream, ABC):
    state_checkpoint_interval = 500

    @property
    def cursor_field(self) -> str:
        return "timestamp"

    @property
    def supports_incremental(self) -> bool:
        return True

    @property
    def source_defined_cursor(self) -> bool:
        return True

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        latest_timestamp = latest_record.get(self.cursor_field)
        current_timestamp = current_stream_state.get(self.cursor_field)

        if current_timestamp and latest_timestamp:
            return {self.cursor_field: max(latest_timestamp, current_timestamp)}
        return {self.cursor_field: latest_timestamp or current_timestamp}


class HourlyUsageByProductStream(IncrementalDatadogUsageStream):
    primary_key = "timestamp"

    def __init__(
        self, api_key: str, application_key: str, site: str, product_families: List[str]
    ):
        super().__init__()
        self.api_key = api_key
        self.application_key = application_key
        self.site = site
        self.product_families = product_families
        self._url_base = f"https://api.{site}"

    @property
    def path(self) -> str:
        return "/api/v2/usage/hourly_usage"

    def request_headers(self, **kwargs) -> Mapping[str, Any]:
        return {
            "DD-API-KEY": self.api_key,
            "DD-APPLICATION-KEY": self.application_key,
        }

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "filter[product_families]": ",".join(self.product_families),
            "page[limit]": 500,
        }
        if stream_state.get(self.cursor_field):
            params["filter[timestamp][start]"] = stream_state[self.cursor_field]

        if next_page_token:
            params.update(next_page_token)

        return params

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        for record in data.get("usage", []):
            yield {
                "timestamp": record["attributes"]["timestamp"],
                "org_name": record["attributes"]["org_name"],
                "product_family": record["attributes"]["product_family"],
            }


# Source
class SourceDatadogUsage(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            HourlyUsageByProductStream(
                api_key=config["api_key"],
                application_key=config["application_key"],
                site=config["site"],
                product_families=config["product_families"],
            )
        ]
