"""
Dynatrace API Client Module - Recursive Topology Discovery

Handles HTTP communication with Dynatrace Monitored Entities API v2,
supporting batch ID fetching for recursive topology traversal.
"""

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

try:
    import requests
except ImportError:
    raise ImportError("'requests' library is required. Install with: pip install requests")


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class ClientConfig:
    """Configuration for the Dynatrace API client."""
    base_url: str
    api_token: str
    verify_ssl: bool = False  # Default to False for on-prem environments
    batch_size: int = 50      # Number of IDs to fetch per API call
    max_retries: int = 5
    initial_backoff: float = 1.0
    max_backoff: float = 60.0


# =============================================================================
# Exceptions
# =============================================================================

class DynatraceAPIError(Exception):
    """Custom exception for Dynatrace API errors."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"HTTP {status_code}: {message}")


# =============================================================================
# Data Models
# =============================================================================

@dataclass
class ServiceEntity:
    """
    Represents a Dynatrace SERVICE entity with enriched properties.
    
    Contains all fields needed for the flat table export model.
    """
    entity_id: str
    display_name: str
    # Enriched properties
    process_group: str = ""
    web_application_id: str = ""
    web_server_name: str = ""
    remote_endpoint: str = ""
    service_type: str = ""
    # Relationships
    calls: List[str] = field(default_factory=list)  # List of target service IDs

    @classmethod
    def from_api_response(cls, entity_data: Dict[str, Any]) -> "ServiceEntity":
        """
        Factory method to create ServiceEntity from API response JSON.
        
        Safely extracts all properties with fallback defaults.
        """
        entity_id = entity_data.get("entityId", "")
        display_name = entity_data.get("displayName", "")
        
        # Extract properties with safe navigation
        properties = entity_data.get("properties", {})
        process_group = properties.get("processGroup", "")
        web_application_id = properties.get("webApplicationId", "")
        web_server_name = properties.get("webServerName", "")
        remote_endpoint = properties.get("remoteEndpoint", "")
        service_type = properties.get("serviceType", "")
        
        # Extract outgoing calls (fromRelationships.calls)
        from_relationships = entity_data.get("fromRelationships", {})
        calls_data = from_relationships.get("calls", [])
        calls = []
        if isinstance(calls_data, list):
            for rel in calls_data:
                target_id = rel.get("id", "")
                target_type = rel.get("type", "")
                # Only include SERVICE-to-SERVICE relationships
                if target_id and target_type == "SERVICE":
                    calls.append(target_id)
        
        return cls(
            entity_id=entity_id,
            display_name=display_name,
            process_group=process_group,
            web_application_id=web_application_id,
            web_server_name=web_server_name,
            remote_endpoint=remote_endpoint,
            service_type=service_type,
            calls=calls,
        )


# =============================================================================
# API Client
# =============================================================================

class DynatraceClient:
    """
    HTTP client for Dynatrace Monitored Entities API v2.
    
    Supports:
    - Batch ID fetching for recursive topology discovery
    - Exponential backoff retry for rate limiting
    - Enriched field fetching for detailed service properties
    """

    # Fields to request for enriched service data
    ENRICHED_FIELDS = (
        "+properties.processGroup,"
        "+properties.webApplicationId,"
        "+properties.webServerName,"
        "+properties.remoteEndpoint,"
        "+properties.serviceType,"
        "+fromRelationships.calls"
    )

    def __init__(
        self,
        config: ClientConfig,
        log_callback: Optional[Callable[[str], None]] = None
    ):
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
        """Close the HTTP session and release resources."""
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
        
        Handles:
        - HTTP 429 (Rate Limiting) with exponential backoff
        - HTTP 5xx (Server Errors) with retry
        - Connection errors with retry
        
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
            # Check for cancellation before each attempt
            if check_cancelled and check_cancelled():
                raise DynatraceAPIError(0, "Operation cancelled by user")

            try:
                response = session.get(url, params=params, timeout=60)
                
                # Success
                if response.status_code == 200:
                    return response.json()
                
                # Rate Limited - retry with backoff
                if response.status_code == 429:
                    retries += 1
                    if retries > self.config.max_retries:
                        raise DynatraceAPIError(429, "Rate limit exceeded after maximum retries")
                    
                    self.log(f"⚠ Rate limited (429). Retry {retries}/{self.config.max_retries} after {backoff:.1f}s")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, self.config.max_backoff)
                    continue
                
                # Server Error - retry with backoff
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
                
                # Client Error (4xx except 429) - not retryable
                error_msg = response.text[:500] if response.text else "No error details"
                raise DynatraceAPIError(response.status_code, error_msg)
                
            except requests.exceptions.SSLError as e:
                raise DynatraceAPIError(
                    0, 
                    f"SSL Error: {str(e)}. Try disabling SSL verification for self-signed certificates."
                )
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
        
        Fetches a single SERVICE entity to validate connectivity and authentication.
        
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

    def fetch_services_by_ids(
        self,
        entity_ids: List[str],
        check_cancelled: Optional[Callable[[], bool]] = None
    ) -> List[ServiceEntity]:
        """
        Fetch SERVICE entities by specific IDs with enriched properties.
        
        This is the core method for recursive topology discovery.
        Uses entitySelector with specific IDs to fetch only the needed services.
        
        API Query Format:
            entitySelector=type("SERVICE"),entityId("ID1","ID2","ID3"...)
            fields=+properties.processGroup,+properties.webApplicationId,...
        
        Args:
            entity_ids: List of Dynatrace SERVICE entity IDs to fetch
            check_cancelled: Optional callback to check cancellation
            
        Returns:
            List of ServiceEntity objects with enriched data
        """
        if not entity_ids:
            return []
        
        url = f"{self.config.base_url.rstrip('/')}/entities"
        
        # Build entitySelector with specific IDs
        # Format: type("SERVICE"),entityId("ID1","ID2","ID3"...)
        ids_quoted = ",".join(f'"{eid}"' for eid in entity_ids)
        entity_selector = f'type("SERVICE"),entityId({ids_quoted})'
        
        params = {
            "entitySelector": entity_selector,
            "fields": self.ENRICHED_FIELDS,
            "pageSize": str(len(entity_ids)),  # Request exactly the number of IDs
        }
        
        response = self._execute_with_retry(url, params, check_cancelled)
        
        # Parse response and convert to ServiceEntity objects
        entities = response.get("entities", [])
        if not isinstance(entities, list):
            return []
        
        result = []
        for entity_data in entities:
            try:
                service = ServiceEntity.from_api_response(entity_data)
                result.append(service)
            except Exception as e:
                self.log(f"⚠ Failed to parse entity: {e}")
                continue
        
        return result

    def fetch_single_service(
        self,
        entity_id: str,
        check_cancelled: Optional[Callable[[], bool]] = None
    ) -> Optional[ServiceEntity]:
        """
        Fetch a single SERVICE entity by ID.
        
        Convenience method for fetching individual services.
        
        Args:
            entity_id: Dynatrace SERVICE entity ID
            check_cancelled: Optional callback to check cancellation
            
        Returns:
            ServiceEntity if found, None otherwise
        """
        try:
            services = self.fetch_services_by_ids([entity_id], check_cancelled)
            return services[0] if services else None
        except DynatraceAPIError as e:
            if e.status_code == 404:
                return None
            raise
