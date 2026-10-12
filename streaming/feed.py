import logging
import time

import requests

log = logging.getLogger(__name__)


class FeedUnavailable(Exception):
    """The feed could not be fetched or returned something unusable."""


def fetch_feed(
    url: str, timeout_s: float, max_retries: int, backoff_base_s: float
) -> dict:
    """Fetch one GTFS-RT snapshot as JSON.

    Retries transient failures with exponential backoff. Raises FeedUnavailable
    when all attempts fail: the caller decides whether to skip this cycle.
    """
    last_error: Exception | None = None

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout_s)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            wait = backoff_base_s * (2**attempt)
            log.warning(
                "feed fetch failed (attempt %d/%d): %s — retrying in %.1fs",
                attempt + 1,
                max_retries,
                exc,
                wait,
            )
            time.sleep(wait)
            continue

        if not isinstance(payload, dict) or not {"Entities", "Header"}.issubset(
            payload
        ):
            raise FeedUnavailable(
                f"unexpected feed structure: keys={list(payload)[:5]}"
            )

        return payload

    raise FeedUnavailable(f"all {max_retries} attempts failed: {last_error}")
