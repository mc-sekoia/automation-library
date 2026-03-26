import collections
import time
from datetime import datetime, timedelta
from typing import Any, Generator, Sequence, Tuple

import orjson
from sekoia_automation.connector import Connector

from .models import SaviyntConnectorConfiguration
from requests import HTTPError

import requests
from sekoia_automation.storage import PersistentJSON
import re
from . import SaviyntModule
from .client import ApiClient
from .models import SaviyntConnectorConfiguration

class SaviyntEventsConnector(Connector):
    module: SaviyntModule
    configuration: SaviyntConnectorConfiguration

    def __init__(self, *args: Any, **kwargs: dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)
        self.log(level="info", message="Initiating Connector")
        self.context = PersistentJSON("context.json", self._data_path)
        self.limit: int = 500

    def _fetch_events(self) -> None:
        """
        Successively queries the pages while more are available
        and the current batch is not too big.
        """
        all_events: list[dict[str, Any]] = []
        for analytic in self.configuration.analytics_name:
            #Get cached last event fetched
            last_event_date: str |None = None
            last_event_id: str | None= None
            last_event: str = self.get_event_analytic_context(analytic)
            if last_event:
                #Handling two id formats : id_date and date_id
                if re.match("[0-9]+_[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}",last_event):
                    last_event_id = last_event.split("_")[0]
                    last_event_date = last_event.split("_")[1]
                else:
                    last_event_date = last_event.split("_")[0]
                    last_event_id = last_event.split("_")[1]
                #Elapsed time since last event fetch
                timedelta_minutes = int((datetime.utcnow() - datetime.strptime(last_event_date,"%Y-%m-%d %H:%M:%S")).seconds / 60) +1
                #Adapt the timeframe if the connector has been launched earlier than its frequency
                timeframe = timedelta_minutes
            else:
                timeframe = self.configuration.frequency
            offset = 0
            events_to_fetch = True
            result: list[dict[str, Any]] = []
            while events_to_fetch:
                payload_json = {
                    "analyticsname": analytic,
                    "attributes": {"timeFrame": timeframe+5},
                    "max": self.limit,
                    "offset": offset
                }
                response = self.client.post(url=f"{self.module.configuration.base_url}/ECM/api/v5/fetchRuntimeControlsData",json=payload_json, timeout=60)
                if response.ok:
                    total: int = int(response.json()["total"])
                    displaycount: int = int(response.json()["displaycount"])
                    if (displaycount < total):
                        self.log(message=(f"Max api count reached. {total - displaycount} events have been lost. Consider lowering the frequency parameter"),level="error")
                    self.log(message=(f"Found {total} messages for analytic : {analytic}"),level="info",
                )
                    #Empty result handler
                    if total == 0 or not("result" in response.json()):
                        events_to_fetch = False
                        break
                    else:
                        result.extend(response.json()["result"])
                        #Offset pages handling
                        offset+=1
                        if total > offset*self.limit:
                            events_to_fetch = True
                        else: 
                            events_to_fetch = False
                #Error handling
                else:
                    if response.status_code in [401, 403, 500]:
                        raise APIException(response.status_code, response.reason, response.text)
                    else:
                        # Exit trigger if we can't authenticate against the server
                        level = "critical" if response.status_code in [403] else "error"
                        self.log(
                            message=(
                                f"Request on Saviynt API to fetch events failed with status {response.status_code} - {response.reason}"
                            ),
                            level=level,
                        )
                        return []
            if len(result) > 0:
                #Cleaning events by removing duplicates
                if last_event_id:
                    for i,event in enumerate(result):
                        if last_event in event.values():
                            result = result[i+1:]
                            break
                last_event_id = result[-1].get("ID")
                self.update_event_analytic_context(last_event_id, analytic)
                batch_of_events = [orjson.dumps(event).decode("utf-8") for event in result]
                self.log(
                        message=f"Sending a batch of {len(result)} messages from {analytic}",
                        level="info",
                    )
                self.push_events_to_intakes(events=batch_of_events)
            else:
                self.log(
                    message=f"No events to forward for {analytic}",
                    level="info",
                )
        self.log(
                    message=f"Sleeping until next batch in {self.configuration.frequency} minutes",
                    level="info",
                )
        time.sleep(self.configuration.frequency * 60)

        

    def create_client(self) -> ApiClient:
        try:
            return ApiClient(
                auth_url=self.module.configuration.base_url + '/ECM/api/login',
                client_id=self.module.configuration.username,
                client_secret=self.module.configuration.password,
            )

        except requests.exceptions.HTTPError as error:
            response = error.response
            level = "critical" if response.status_code in [401, 403] else "error"
            self.log(
                f"OAuth2 server responded {response.status_code} - {response.reason}",
                level=level,
            )
            raise error

        except TimeoutError as error:
            self.log(message="Failed to authorize due to timeout", level="error")
            raise error

    def get_event_analytic_context(self, analytic: str) -> str:
        """
        Get last event date and id.

        Returns:
            Tuple[datetime, str | None]:
        """
        with self.context as cache:
            event_type_context = cache.get(analytic)
            if not event_type_context:
                event_type_context = {}
            last_event_id = event_type_context.get("last_event_id")
            return last_event_id

    def update_event_analytic_context(
        self, last_event_id: str | None, analytic: str) -> None:
        """
        Set last event id.

        Args:
            last_event_id: str
            analytic: str
        """
        with self.context as cache:
            cache[analytic] = {"last_event_id": last_event_id}

    
    def handle_api_exception(self, error: HTTPError) -> None:
        message = f"Unexpected API error {error.response.status_code} - {str(error.response)}"
        if error.response.status_code == 401 or error.response.status_code == 403:
            message = "Saviynt API raised an authentication issue. Please check our credentials"
        elif error.response.status_code == 500:
            message = (
                "Saviynt API raised an internal error"
            )
        self.log(level="error", message=message)
        self.log(level="info", message="Waiting for next poll in {self.configuration.frequency} minutes")
        #Timer to prevent spamming
        time.sleep(60)

    def run(self) -> None:  # pragma: no cover
        """Run the trigger."""
        self.log(level="info", message="Starting Connector")        
        while self.running:
            try:
                self.log(level="info", message="Authentication to saviynt")
                self.client = self.create_client()
                self._fetch_events()
            except HTTPError as ex:
                self.handle_api_exception(ex)
            except Exception as ex:
                self.log_exception(ex, message="An unknown exception occurred")
                self.log_exception(ex, message="Retrying in 60 seconds")
                #Timer to prevent spamming
                time.sleep(60)
                raise