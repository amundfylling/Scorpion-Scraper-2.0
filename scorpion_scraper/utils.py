import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, Optional

DEFAULT_TIMEOUT = 20
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}

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
    status_forcelist: tuple = (429, 500, 502, 503, 504),
    pool_connections: int = 20,
    pool_maxsize: int = 20,
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
        allowed_methods=frozenset(["HEAD", "GET", "OPTIONS"]),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )
    session.headers.update(DEFAULT_HEADERS)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_with_status(
    session: requests.Session,
    url: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> requests.Response:
    """
    Fetch a URL with a timeout and fail fast on HTTP errors.
    """
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response

def parse_info_table(soup, table_class: str = "iTable") -> Dict[str, str]:
    """
    Parse SportScorpion key/value info tables into a dictionary.
    """
    data: Dict[str, str] = {}
    for table in soup.find_all("table", class_=table_class):
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if not th or not td:
                continue
            key = th.get_text(" ", strip=True)
            value = td.get_text(" ", strip=True)
            if key:
                data[key] = value
    return data
