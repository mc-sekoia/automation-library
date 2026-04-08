import signal
import time
from typing import Any
from datetime import datetime, timedelta
import orjson
from sekoia_automation.connector import Connector

from .models import SaviyntConnectorConfiguration
from requests import HTTPError
from cachetools import Cache, LRUCache
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
        #Cache initialization
        self.cache_size = 2000
        self.events_cache: Cache[str, bool] = self.load_events_cache()
    
    def _fetch_events(self) -> None:
        """
        Successively queries the pages while more are available
        and the current batch is not too big.
        """
        for analytic in self.configuration.analytics_name:
            #Get cached last event fetched
            last_event_date: str |None = None
            last_event_id: str | None= None
            #Retrieving last event info
            last_event: str = self.get_analytic_last_event(analytic)
            if last_event:
                #Handling two id formats : id_date and date_id
                if re.match("[0-9]+_[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}",last_event):
                    last_event_id = last_event.split("_")[0]
                    last_event_date = last_event.split("_")[1]
                elif re.match("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}_[0-9]+",last_event):
                    last_event_date = last_event.split("_")[0]
                    last_event_id = last_event.split("_")[1]
                else:
                    self.log(message=f"Saved persistent last id has a bad format",level="error")
                    last_event_date = None
                    last_event_id = None
            #Setting pagination processing flags
            offset = 0
            events_to_fetch = True
            analytic_events: list[dict[str, Any]] = []
            while events_to_fetch:
                #For each event page, compute an adapted timeframe
                if last_event_date and last_event_id:
                    elapsed_time_minutes = int((datetime.utcnow() - datetime.strptime(last_event_date,"%Y-%m-%d %H:%M:%S")).total_seconds() / 60) +1
                    timeframe = elapsed_time_minutes
                else:
                    #Backs to frequency parameter as a default
                    timeframe = self.configuration.frequency
                #Widen the timeframe to overlap previous fetches
                timeframe += 5
                payload_json = {
                    "analyticsname": analytic,
                    "attributes": {"timeFrame": timeframe},
                    "max": self.limit,
                    "offset": offset
                }
                self.log(message=(f"Fetching events for {analytic} from {timeframe} minutes ago"),level="info")
                response = self.client.post(url=f"{self.module.configuration.base_url}/ECM/api/v5/fetchRuntimeControlsData",json=payload_json, timeout=60)
                if response.ok:
                    total: int = int(response.json()["total"])
                    #Empty result handler
                    if total == 0 or not("result" in response.json()):
                        events_to_fetch = False
                        break
                    else:
                        analytic_events.extend(response.json()["result"])
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
                        self.log(message=(f"Request on Saviynt API to fetch events failed with status {response.status_code} - {response.reason}"),level=level)
                        return None
            if len(analytic_events) > 0:
                #Removing duplicates (due to the offset handling in a relative timerange query)
                analytic_events = list(dict.fromkeys(analytic_events))
                #Cleaning events by removing events that has already been sent before
                filtered_events = [
                    event for event in analytic_events if event.get("ID") is not None and event["ID"] not in self.events_cache
                ]
                #Formating events for intake sending
                batch_of_events = [orjson.dumps(event).decode("utf-8") for event in filtered_events]
                self.log(message=f"Sending a batch of {len(filtered_events)} messages from {analytic}",level="info")
                last_event_id = filtered_events[-1].get("ID")
                self.update_event_analytic_context(last_event_id, analytic)
                self.push_events_to_intakes(events=batch_of_events)
                #Saving events to cache
                for event in filtered_events:
                    self.events_cache[event["ID"]] = True
                self.save_events_cache()
            else:
                self.log(message=f"No events to forward for {analytic}",level="info")
        self.log(message=f"Sleeping until next batch in {self.configuration.frequency} minutes",level="info")
        time.sleep(self.configuration.frequency * 60)

    def create_client(self) -> ApiClient:
        """
        Initiates the APIClient connection.

        Returns:
            str:
        """
        try:
            self.log(level="info", message="API Authentication")
            return ApiClient(
                auth_url=self.module.configuration.base_url + '/ECM/api/login',
                client_id=self.module.configuration.username,
                client_secret=self.module.configuration.password,
            )

        except HTTPError as ex:
                self.handle_api_exception(ex)

        except TimeoutError as error:
            self.log(message="Failed to authorize due to timeout", level="error")
            raise error

    def load_events_cache(self) -> Cache[str, bool]:
        """
        Load the events cache.
        """
        cache: Cache[str, bool] = LRUCache(maxsize=self.cache_size)
        with self.context as context:
            # load the cache from the context
            cached_event_ids = context.get("cached_event_ids", [])
        for uuid in cached_event_ids:
            cache[uuid] = True
        self.log(message=(f"Loading cached content : {cache}"),level="info",)
        return cache
    
    def save_events_cache(self) -> None:
        """
        Save the events cache.
        """
        with self.context as context:
            # save the events cache to the context
            context["cached_event_ids"] = list(self.events_cache.keys())
            debug = context["cached_event_ids"]
            self.log(message=(f"Saving cached content : {debug}"),level="info",)

    def get_analytic_last_event(self, analytic: str) -> str:
        """
        Get last event id from persistent storage.

        Returns:
            str:
        """
        with self.context as cache:
            analytic_context = cache.get(analytic)
            if not analytic_context:
                analytic_context = {}
            last_event_id = analytic_context.get("last_event_id")
            return last_event_id

    def update_event_analytic_context(
        self, last_event_id: str | None, analytic: str) -> None:
        """
        Save last_event_id as persistent.

        Args:
            last_event_id: str
            analytic: str
        """
        with self.context as cache:
            cache[analytic] = {"last_event_id": last_event_id}

    def handle_api_exception(self, error: HTTPError) -> None:
        """
        Handles API errors gracefully.

        Args:
            error: HTTPError
        """
        if error.response.status_code == 401 or error.response.status_code == 403:
            message = "Saviynt API raised an authentication issue. Please check our credentials"
        elif error.response.status_code == 500:
            message = (
                "Saviynt API raised an internal error"
            )
        else:
            message = f"Unexpected API error {error.response.status_code} - {str(error.response)}"
        self.log(level="error", message=message)
        self.log(level="info", message="Waiting for next poll in {self.configuration.frequency} minutes")
        #Timer to prevent spamming
        time.sleep(10)

    def run(self) -> None:  # pragma: no cover
        """Run the trigger."""
        self.log(level="info", message="Starting Connector") 
        self.client = self.create_client()
        while self.running:
            try:
                self._fetch_events()
            except HTTPError as ex:
                self.handle_api_exception(ex)
            except Exception as ex:
                self.log_exception(ex, message="An unknown exception occurred")
                self.log_exception(ex, message="Retrying in 10 seconds")
                #Timer to prevent spamming
                time.sleep(10)
                raise
        self.log(level="info", message="Connector has been shut down")