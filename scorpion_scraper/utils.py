import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional

def setup_logging(level=logging.INFO):
    """
    Sets up basic logging configuration.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def get_retry_session(
    retries: int = 5,
    backoff_factor: float = 1.0,  # increased backoff factor for better robustness
    status_forcelist: tuple = (500, 502, 503, 504),
    session: Optional[requests.Session] = None
) -> requests.Session:
    """
    Returns a requests.Session (or modifies an existing one) with
    automatic retry logic.
    """
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
