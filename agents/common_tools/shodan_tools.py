from google.adk.tools import BaseTool
import shodan
from typing import Iterator, Optional
import logging
import time

class ShodanStreamTool(BaseTool):
    """
    Tool to stream events from the Shodan Streaming API.
    Use `.stream(filter)` to get a Python iterator over matching events.
    """
    def __init__(self, api_key: str):
        super().__init__(
            name="shodan_stream",
            description="Stream real-time events from Shodan via its Streaming API"
        )
        self.client = shodan.Shodan(api_key)
        # retry/backoff config
        self.max_retries = 5
        self.backoff_factor = 2.0
        # logger for stream events
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def stream(self, filter: Optional[str] = None) -> Iterator[dict]:
        """
        Start streaming events. Pass an optional filter string (e.g., "port:22").
        Returns an iterator of event dicts.
        """
        # If no filter provided, use empty string (all events)
        flt = filter or ""
        attempt = 0
        while True:
            try:
                for event in self.client.stream.banners(raw=False, timeout=flt if flt else None):
                    yield event
                # if stream closes without error, reset attempts and reconnect
                attempt = 0
                self.logger.warning("Shodan stream ended, reconnecting...")
            except shodan.APIError as e:
                attempt += 1
                self.logger.error("Shodan APIError (attempt %d/%d): %s", attempt, self.max_retries, e)
                if attempt > self.max_retries:
                    self.logger.critical("Max retries reached, terminating stream.")
                    break
                sleep = self.backoff_factor ** (attempt - 1)
                self.logger.info("Sleeping %s seconds before retry...", sleep)
                time.sleep(sleep)
            except Exception as e:
                attempt += 1
                self.logger.error("Unexpected error in Shodan stream: %s", e, exc_info=True)
                if attempt > self.max_retries:
                    self.logger.critical("Max retries reached on exception, terminating stream.")
                    break
                sleep = self.backoff_factor ** (attempt - 1)
                time.sleep(sleep)
        self.logger.info("Shodan stream stopped.")

    # alias call to stream for ADK compatibility
    call = stream 