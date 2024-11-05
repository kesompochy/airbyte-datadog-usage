#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

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

    @abstractmethod
    def path(self, **kwargs) -> str:
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
    primary_key = ["timestamp", "product_family"]

    def __init__(
        self,
        api_key: str,
        application_key: str,
        site: str,
        product_families: List[str],
        start_date: str,
    ):
        super().__init__()
        self.api_key = api_key
        self.application_key = application_key
        self.site = site
        self.product_families = product_families
        self.start_date = start_date
        self._url_base = f"https://api.{site}"

    def path(self, **kwargs) -> str:
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

        start_time = stream_state.get(self.cursor_field)
        if start_time:
            params["filter[timestamp][start]"] = start_time[:13]
        else:
            params["filter[timestamp][start]"] = self.start_date

        if next_page_token:
            params.update(next_page_token)

        return params

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        for record in data.get("data", []):
            attributes = record["attributes"]
            yield {
                "timestamp": attributes["timestamp"],
                "product_family": attributes["product_family"],
                "org_name": attributes["org_name"],
                "measurements": [
                    {"usage_type": m["usage_type"], "value": m["value"]}
                    for m in attributes["measurements"]
                    if m["value"] is not None  # nullの値は除外
                ],
                "type": record["type"],
            }

    def get_json_schema(self) -> Dict[str, Any]:
        schema_path = (
            Path(__file__).parent / "schemas" / "hourly_usage_by_products.json"
        )
        return json.loads(schema_path.read_text())


class EstimatedCostStream(IncrementalDatadogUsageStream):
    primary_key = ["sync_date", "month"]

    def __init__(
        self,
        api_key: str,
        application_key: str,
        site: str,
        start_month: Optional[str] = None,
    ):
        super().__init__()
        self.api_key = api_key
        self.application_key = application_key
        self.site = site
        self._url_base = f"https://api.{site}"
        self.start_month = start_month or datetime.now().strftime("%Y-%m")

    def path(self, **kwargs) -> str:
        return "/api/v2/usage/estimated_cost"

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
        current_month = datetime.now().strftime("%Y-%m")
        params = {"start_month": current_month}

        return params

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        for record in data.get("data", []):
            attributes = record["attributes"]
            month = attributes["date"][:7]
            yield {
                "sync_date": datetime.now().strftime("%Y-%m-%d"),
                "month": month,
                "org_name": attributes["org_name"],
                "total_cost": attributes["total_cost"],
                "charges": [
                    {
                        "product_name": c["product_name"],
                        "charge_type": c["charge_type"],
                        "cost": c["cost"],
                        "last_aggregation_function": c["last_aggregation_function"],
                    }
                    for c in attributes["charges"]
                ],
            }

    @property
    def cursor_field(self) -> str:
        return "sync_date"

    def get_json_schema(self) -> Dict[str, Any]:
        schema_path = Path(__file__).parent / "schemas" / "estimated_cost.json"
        return json.loads(schema_path.read_text())


# Source
class SourceDatadogUsage(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            url = f"https://api.{config['site']}/api/v1/validate"

            headers = {
                "DD-API-KEY": config["api_key"],
                "DD-APPLICATION-KEY": config["application_key"],
            }

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                return True, None

            return False, f"HTTP {response.status_code}: {response.text}"

        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            HourlyUsageByProductStream(
                api_key=config["api_key"],
                application_key=config["application_key"],
                site=config["site"],
                product_families=config["hourly_usage_by_product"]["product_families"],
                start_date=config["hourly_usage_by_product"]["start_date"],
            ),
            EstimatedCostStream(
                api_key=config["api_key"],
                application_key=config["application_key"],
                site=config["site"],
            ),
        ]
