"""
Dynatrace API Client Module

Handles HTTP communication with Dynatrace Monitored Entities API v2,
including pagination and exponential backoff retry logic.
"""

import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

try:
    import requests
except ImportError:
    raise ImportError("'requests' library is required. Install with: pip install requests")


@dataclass
class ClientConfig:
    """Configuration for the Dynatrace API client."""
    base_url: str
    api_token: str
    verify_ssl: bool = True
    page_size: int = 500
    from_time: Optional[str] = None
    to_time: Optional[str] = None
    max_retries: int = 5
    initial_backoff: float = 1.0
    max_backoff: float = 60.0


class DynatraceAPIError(Exception):
    """Custom exception for Dynatrace API errors."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"HTTP {status_code}: {message}")


class DynatraceClient:
    """
    HTTP client for Dynatrace Monitored Entities API v2.
    
    Implements cursor-based pagination and exponential backoff
    retry logic for rate limiting and transient errors.
    """

    def __init__(self, config: ClientConfig, log_callback: Optional[Callable[[str], None]] = None):
        """
        Initialize the client.
        
        Args:
            config: Client configuration
            log_callback: Optional callback for logging messages
        """
        self.config = config
        self.log = log_callback or (lambda msg: None)
        self._session: Optional[requests.Session] = None

    def _get_session(self) -> requests.Session:
        """Get or create HTTP session with configured headers."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                "Authorization": f"Api-Token {self.config.api_token}",
                "Accept": "application/json",
            })
            self._session.verify = self.config.verify_ssl
        return self._session

    def close(self) -> None:
        """Close the HTTP session."""
        if self._session:
            self._session.close()
            self._session = None

    def _execute_with_retry(
        self,
        url: str,
        params: Optional[Dict[str, str]] = None,
        check_cancelled: Optional[Callable[[], bool]] = None
    ) -> Dict[str, Any]:
        """
        Execute GET request with exponential backoff retry.
        
        Args:
            url: Full URL to request
            params: Query parameters
            check_cancelled: Optional callback to check if operation was cancelled
            
        Returns:
            Parsed JSON response
            
        Raises:
            DynatraceAPIError: For non-retryable errors or exhausted retries
        """
        session = self._get_session()
        retries = 0
        backoff = self.config.initial_backoff

        while retries <= self.config.max_retries:
            # Check for cancellation
            if check_cancelled and check_cancelled():
                raise DynatraceAPIError(0, "Operation cancelled by user")

            try:
                response = session.get(url, params=params, timeout=60)
                
                if response.status_code == 200:
                    return response.json()
                
                if response.status_code == 429:
                    retries += 1
                    if retries > self.config.max_retries:
                        raise DynatraceAPIError(429, "Rate limit exceeded after maximum retries")
                    
                    self.log(f"⚠ Rate limited (429). Retry {retries}/{self.config.max_retries} after {backoff:.1f}s")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, self.config.max_backoff)
                    continue
                
                if response.status_code >= 500:
                    retries += 1
                    if retries > self.config.max_retries:
                        raise DynatraceAPIError(
                            response.status_code,
                            f"Server error after maximum retries: {response.text[:300]}"
                        )
                    
                    self.log(f"⚠ Server error ({response.status_code}). Retry {retries}/{self.config.max_retries} after {backoff:.1f}s")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, self.config.max_backoff)
                    continue
                
                # 4xx errors (except 429) are not retryable
                error_msg = response.text[:500] if response.text else "No error details"
                raise DynatraceAPIError(response.status_code, error_msg)
                
            except requests.exceptions.SSLError as e:
                raise DynatraceAPIError(0, f"SSL Error: {str(e)}. Try disabling SSL verification if using self-signed certificates.")
            except requests.exceptions.ConnectionError as e:
                retries += 1
                if retries > self.config.max_retries:
                    raise DynatraceAPIError(0, f"Connection failed after maximum retries: {str(e)}")
                
                self.log(f"⚠ Connection error. Retry {retries}/{self.config.max_retries} after {backoff:.1f}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, self.config.max_backoff)
            except requests.exceptions.Timeout:
                retries += 1
                if retries > self.config.max_retries:
                    raise DynatraceAPIError(0, "Request timeout after maximum retries")
                
                self.log(f"⚠ Request timeout. Retry {retries}/{self.config.max_retries} after {backoff:.1f}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, self.config.max_backoff)
            except requests.exceptions.RequestException as e:
                raise DynatraceAPIError(0, f"Request failed: {str(e)}")
        
        raise DynatraceAPIError(0, "Unexpected retry loop exit")

    def test_connection(self) -> Dict[str, Any]:
        """
        Test connection to Dynatrace API with a minimal request.
        
        Returns:
            Response containing a small sample of entities
            
        Raises:
            DynatraceAPIError: If connection fails
        """
        url = f"{self.config.base_url.rstrip('/')}/entities"
        params = {
            "entitySelector": 'type("SERVICE")',
            "pageSize": "1",
        }
        return self._execute_with_retry(url, params)

    def fetch_entities_page(
        self,
        next_page_key: Optional[str] = None,
        check_cancelled: Optional[Callable[[], bool]] = None
    ) -> Dict[str, Any]:
        """
        Fetch a page of SERVICE entities.
        
        For the first page, all filter parameters are used.
        For subsequent pages, ONLY nextPageKey is used (API requirement).
        
        Args:
            next_page_key: Cursor for pagination
            check_cancelled: Optional callback to check cancellation
            
        Returns:
            API response as dictionary
        """
        url = f"{self.config.base_url.rstrip('/')}/entities"
        
        if next_page_key:
            # Subsequent pages: ONLY use nextPageKey
            params = {"nextPageKey": next_page_key}
        else:
            # First page: use all filter parameters
            params = {
                "entitySelector": 'type("SERVICE")',
                "fields": "+fromRelationships.calls,+toRelationships.called_by",
                "pageSize": str(self.config.page_size),
            }
            if self.config.from_time:
                params["from"] = self.config.from_time
            if self.config.to_time:
                params["to"] = self.config.to_time
        
        return self._execute_with_retry(url, params, check_cancelled)

    def fetch_entity_by_id(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single entity by ID for name resolution.
        
        Args:
            entity_id: Dynatrace entity ID
            
        Returns:
            Entity data or None if not found
        """
        url = f"{self.config.base_url.rstrip('/')}/entities/{entity_id}"
        try:
            return self._execute_with_retry(url)
        except DynatraceAPIError as e:
            if e.status_code == 404:
                return None
            raise

