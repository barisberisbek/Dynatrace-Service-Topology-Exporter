#!/usr/bin/env python3
"""
Dynatrace Service Topology Extractor

Extracts service-to-service dependencies from Dynatrace Managed using
the Monitored Entities API v2 and exports as a CSV edge list.

Usage:
    python dynatrace_service_topology.py --base-url <URL> --output <FILE>

Environment Variables:
    DYNATRACE_API_TOKEN: API token with entities.read scope (required)
"""

import argparse
import csv
import logging
import os
import sys
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import requests
except ImportError:
    print("ERROR: 'requests' library is required. Install with: pip install requests")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Script configuration."""
    base_url: str
    api_token: str
    output_file: str
    page_size: int = 500
    from_time: Optional[str] = None
    to_time: Optional[str] = None
    verify_ssl: bool = True
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
    """HTTP client for Dynatrace API with retry logic."""

    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Api-Token {config.api_token}",
            "Accept": "application/json",
        })
        self.session.verify = config.verify_ssl

    def _execute_with_retry(self, url: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Execute GET request with exponential backoff retry for 429 and 5xx errors.
        
        Args:
            url: Full URL to request
            params: Query parameters
            
        Returns:
            Parsed JSON response
            
        Raises:
            DynatraceAPIError: For non-retryable errors
        """
        retries = 0
        backoff = self.config.initial_backoff

        while retries <= self.config.max_retries:
            try:
                response = self.session.get(url, params=params, timeout=60)
                
                if response.status_code == 200:
                    return response.json()
                
                if response.status_code == 429:
                    # Rate limited - apply exponential backoff
                    retries += 1
                    if retries > self.config.max_retries:
                        raise DynatraceAPIError(429, "Rate limit exceeded after max retries")
                    
                    logger.warning(f"Rate limited (429). Retry {retries}/{self.config.max_retries} after {backoff:.1f}s")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, self.config.max_backoff)
                    continue
                
                if response.status_code >= 500:
                    # Server error - retry with backoff
                    retries += 1
                    if retries > self.config.max_retries:
                        raise DynatraceAPIError(
                            response.status_code,
                            f"Server error after max retries: {response.text[:500]}"
                        )
                    
                    logger.warning(f"Server error ({response.status_code}). Retry {retries}/{self.config.max_retries} after {backoff:.1f}s")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, self.config.max_backoff)
                    continue
                
                # 4xx errors (except 429) are not retryable
                error_msg = response.text[:500] if response.text else "No error details"
                raise DynatraceAPIError(response.status_code, error_msg)
                
            except requests.exceptions.RequestException as e:
                retries += 1
                if retries > self.config.max_retries:
                    raise DynatraceAPIError(0, f"Request failed after max retries: {str(e)}")
                
                logger.warning(f"Request error: {e}. Retry {retries}/{self.config.max_retries} after {backoff:.1f}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, self.config.max_backoff)
        
        raise DynatraceAPIError(0, "Unexpected retry loop exit")

    def fetch_entities_page(
        self,
        next_page_key: Optional[str] = None,
        entity_selector: Optional[str] = None,
        fields: Optional[str] = None,
        page_size: Optional[int] = None,
        from_time: Optional[str] = None,
        to_time: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Fetch a page of entities from the API.
        
        For the first page, all filter parameters are used.
        For subsequent pages, ONLY nextPageKey is used (API requirement).
        
        Args:
            next_page_key: Cursor for pagination (if present, other params are ignored)
            entity_selector: Entity selector query
            fields: Fields to include in response
            page_size: Number of entities per page
            from_time: Start of timeframe
            to_time: End of timeframe
            
        Returns:
            API response as dictionary
        """
        url = f"{self.config.base_url.rstrip('/')}/entities"
        
        if next_page_key:
            # Subsequent pages: ONLY use nextPageKey
            params = {"nextPageKey": next_page_key}
        else:
            # First page: use all filter parameters
            params = {}
            if entity_selector:
                params["entitySelector"] = entity_selector
            if fields:
                params["fields"] = fields
            if page_size:
                params["pageSize"] = str(page_size)
            if from_time:
                params["from"] = from_time
            if to_time:
                params["to"] = to_time
        
        return self._execute_with_retry(url, params)

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


class TopologyExtractor:
    """Extracts service topology from Dynatrace entities."""

    def __init__(self, client: DynatraceClient, config: Config):
        self.client = client
        self.config = config
        self.id_to_name: Dict[str, str] = {}
        self.edges: Set[Tuple[str, str, str, str, str]] = set()
        self.unknown_ids: Set[str] = set()

    def fetch_all_services(self) -> List[Dict[str, Any]]:
        """
        Fetch all SERVICE entities using pagination.
        
        Returns:
            List of all service entity objects
        """
        all_entities: List[Dict[str, Any]] = []
        page_count = 0
        next_page_key: Optional[str] = None
        
        logger.info("Starting to fetch SERVICE entities...")
        
        while True:
            page_count += 1
            
            if next_page_key:
                logger.info(f"Fetching page {page_count} (using nextPageKey)...")
                response = self.client.fetch_entities_page(next_page_key=next_page_key)
            else:
                logger.info(f"Fetching page {page_count} (initial request)...")
                response = self.client.fetch_entities_page(
                    entity_selector='type("SERVICE")',
                    fields="+fromRelationships.calls,+toRelationships.called_by",
                    page_size=self.config.page_size,
                    from_time=self.config.from_time,
                    to_time=self.config.to_time,
                )
            
            # Extract entities from response
            entities = response.get("entities", [])
            if not isinstance(entities, list):
                logger.warning(f"Unexpected 'entities' type: {type(entities)}. Treating as empty.")
                entities = []
            
            all_entities.extend(entities)
            logger.info(f"Page {page_count}: Retrieved {len(entities)} entities (total: {len(all_entities)})")
            
            # Check for next page
            next_page_key = response.get("nextPageKey")
            if not next_page_key:
                break
        
        logger.info(f"Pagination complete. Total pages: {page_count}, Total entities: {len(all_entities)}")
        return all_entities

    def build_id_to_name_map(self, entities: List[Dict[str, Any]]) -> None:
        """
        Build mapping from entity ID to display name.
        
        Args:
            entities: List of entity objects
        """
        for entity in entities:
            entity_id = entity.get("entityId")
            display_name = entity.get("displayName", "")
            if entity_id:
                self.id_to_name[entity_id] = display_name
        
        logger.info(f"Built ID-to-name mapping with {len(self.id_to_name)} entries")

    def resolve_unknown_ids(self) -> None:
        """
        Attempt to resolve unknown entity IDs by fetching them individually.
        """
        if not self.unknown_ids:
            return
        
        logger.info(f"Resolving {len(self.unknown_ids)} unknown entity IDs...")
        resolved = 0
        
        for entity_id in list(self.unknown_ids):
            if entity_id in self.id_to_name:
                self.unknown_ids.discard(entity_id)
                continue
            
            try:
                entity = self.client.fetch_entity_by_id(entity_id)
                if entity:
                    display_name = entity.get("displayName", "UNKNOWN")
                    self.id_to_name[entity_id] = display_name
                    self.unknown_ids.discard(entity_id)
                    resolved += 1
            except DynatraceAPIError as e:
                logger.warning(f"Could not resolve ID {entity_id}: {e}")
        
        logger.info(f"Resolved {resolved} unknown IDs. Remaining unknown: {len(self.unknown_ids)}")

    def extract_relationships(self, entities: List[Dict[str, Any]]) -> None:
        """
        Extract service-to-service relationships from entities.
        
        Args:
            entities: List of service entity objects
        """
        for entity in entities:
            entity_id = entity.get("entityId")
            if not entity_id:
                continue
            
            # Extract outgoing calls (this service CALLS others)
            from_rels = entity.get("fromRelationships", {})
            calls = from_rels.get("calls", [])
            if isinstance(calls, list):
                for target in calls:
                    target_id = target.get("id")
                    target_type = target.get("type", "")
                    
                    # Only include SERVICE-to-SERVICE relationships
                    if target_id and target_type == "SERVICE":
                        self._add_edge(entity_id, target_id, "CALLS")
            
            # Extract incoming calls (others CALL this service)
            to_rels = entity.get("toRelationships", {})
            called_by = to_rels.get("called_by", [])
            if isinstance(called_by, list):
                for source in called_by:
                    source_id = source.get("id")
                    source_type = source.get("type", "")
                    
                    # Only include SERVICE-to-SERVICE relationships
                    if source_id and source_type == "SERVICE":
                        self._add_edge(source_id, entity_id, "CALLED_BY")

    def _add_edge(self, source_id: str, target_id: str, relationship: str) -> None:
        """
        Add an edge to the collection, tracking unknown IDs.
        
        Args:
            source_id: Source entity ID
            target_id: Target entity ID  
            relationship: Relationship type (CALLS or CALLED_BY)
        """
        # Track unknown IDs for later resolution
        if source_id not in self.id_to_name:
            self.unknown_ids.add(source_id)
        if target_id not in self.id_to_name:
            self.unknown_ids.add(target_id)
        
        # Get names (will be resolved later)
        source_name = self.id_to_name.get(source_id, "UNKNOWN")
        target_name = self.id_to_name.get(target_id, "UNKNOWN")
        
        self.edges.add((source_id, source_name, target_id, target_name, relationship))

    def finalize_edges(self) -> List[Tuple[str, str, str, str, str]]:
        """
        Finalize edges with resolved names.
        
        Returns:
            List of edge tuples with resolved names
        """
        finalized = []
        for source_id, _, target_id, _, relationship in self.edges:
            source_name = self.id_to_name.get(source_id, "UNKNOWN")
            target_name = self.id_to_name.get(target_id, "UNKNOWN")
            finalized.append((source_id, source_name, target_id, target_name, relationship))
        return finalized

    def run(self) -> List[Tuple[str, str, str, str, str]]:
        """
        Execute the full extraction pipeline.
        
        Returns:
            List of edge tuples (source_id, source_name, target_id, target_name, relationship)
        """
        # Fetch all service entities
        entities = self.fetch_all_services()
        
        if not entities:
            logger.warning("No SERVICE entities found!")
            return []
        
        # Build ID-to-name mapping
        self.build_id_to_name_map(entities)
        
        # Extract relationships
        self.extract_relationships(entities)
        logger.info(f"Extracted {len(self.edges)} unique edges")
        
        # Resolve unknown IDs if any
        if self.unknown_ids:
            self.resolve_unknown_ids()
        
        # Finalize with resolved names
        final_edges = self.finalize_edges()
        
        # Log any remaining unknown IDs
        unknown_count = sum(1 for _, sn, _, tn, _ in final_edges if sn == "UNKNOWN" or tn == "UNKNOWN")
        if unknown_count > 0:
            logger.warning(f"{unknown_count} edges have UNKNOWN entity names")
        
        return final_edges


def write_csv(edges: List[Tuple[str, str, str, str, str]], output_file: str) -> None:
    """
    Write edges to CSV file.
    
    Args:
        edges: List of edge tuples
        output_file: Path to output CSV file
    """
    logger.info(f"Writing {len(edges)} edges to {output_file}...")
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source_id", "source_name", "target_id", "target_name", "relationship"])
        
        for edge in sorted(edges):
            writer.writerow(edge)
    
    logger.info(f"CSV file written successfully: {output_file}")


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract Dynatrace service-to-service topology as CSV edge list.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Basic usage
    python dynatrace_service_topology.py \\
        --base-url https://my-activegate:9999/e/ENV_ID/api/v2 \\
        --output service_topology.csv

    # With custom timeframe
    python dynatrace_service_topology.py \\
        --base-url https://my-activegate:9999/e/ENV_ID/api/v2 \\
        --output topology.csv \\
        --from now-7d --to now

    # Disable SSL verification (for self-signed certs)
    python dynatrace_service_topology.py \\
        --base-url https://my-activegate:9999/e/ENV_ID/api/v2 \\
        --output topology.csv \\
        --no-verify-ssl

Environment Variables:
    DYNATRACE_API_TOKEN    API token with entities.read scope (required)
        """,
    )
    
    parser.add_argument(
        "--base-url",
        required=True,
        help="Dynatrace API base URL (e.g., https://host:9999/e/ENV_ID/api/v2)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output CSV file path",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=500,
        help="Page size for API requests (default: 500, max: 500)",
    )
    parser.add_argument(
        "--from",
        dest="from_time",
        help="Start of timeframe (e.g., now-3d, now-72h). Optional.",
    )
    parser.add_argument(
        "--to",
        dest="to_time",
        help="End of timeframe (e.g., now). Optional.",
    )
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="Disable SSL certificate verification (for self-signed certs)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum retry attempts for rate limiting (default: 5)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose/debug logging",
    )
    
    return parser.parse_args()


def load_api_token() -> str:
    """
    Load API token from environment variable.
    
    Returns:
        API token string
        
    Raises:
        SystemExit: If token is not found
    """
    token = os.environ.get("DYNATRACE_API_TOKEN")
    if not token:
        logger.error(
            "DYNATRACE_API_TOKEN environment variable is not set.\n"
            "Please set it with: set DYNATRACE_API_TOKEN=your_token_here (Windows)\n"
            "                 or: export DYNATRACE_API_TOKEN=your_token_here (Linux/Mac)"
        )
        sys.exit(1)
    return token


def validate_config(args: argparse.Namespace) -> Config:
    """
    Validate arguments and build configuration.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        Validated Config object
    """
    # Validate base URL
    base_url = args.base_url.rstrip("/")
    if not base_url.startswith("http"):
        logger.error(f"Invalid base URL: {base_url}. Must start with http:// or https://")
        sys.exit(1)
    
    # Validate page size
    if args.page_size < 1 or args.page_size > 500:
        logger.error(f"Invalid page size: {args.page_size}. Must be between 1 and 500.")
        sys.exit(1)
    
    # Load API token
    api_token = load_api_token()
    
    return Config(
        base_url=base_url,
        api_token=api_token,
        output_file=args.output,
        page_size=args.page_size,
        from_time=args.from_time,
        to_time=args.to_time,
        verify_ssl=not args.no_verify_ssl,
        max_retries=args.max_retries,
    )


def main() -> None:
    """Main entry point."""
    args = parse_arguments()
    
    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Suppress SSL warnings if verification is disabled
    if args.no_verify_ssl:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        logger.warning("SSL certificate verification is DISABLED")
    
    logger.info("=" * 60)
    logger.info("Dynatrace Service Topology Extractor")
    logger.info("=" * 60)
    
    # Build and validate configuration
    config = validate_config(args)
    logger.info(f"Base URL: {config.base_url}")
    logger.info(f"Output file: {config.output_file}")
    logger.info(f"Page size: {config.page_size}")
    if config.from_time:
        logger.info(f"From: {config.from_time}")
    if config.to_time:
        logger.info(f"To: {config.to_time}")
    
    try:
        # Initialize client and extractor
        client = DynatraceClient(config)
        extractor = TopologyExtractor(client, config)
        
        # Run extraction
        edges = extractor.run()
        
        if not edges:
            logger.warning("No edges found. The output file will be empty (header only).")
        
        # Write output
        write_csv(edges, config.output_file)
        
        # Summary
        logger.info("=" * 60)
        logger.info("EXTRACTION COMPLETE")
        logger.info(f"  Services discovered: {len(extractor.id_to_name)}")
        logger.info(f"  Edges exported: {len(edges)}")
        logger.info(f"  Output file: {config.output_file}")
        logger.info("=" * 60)
        
    except DynatraceAPIError as e:
        logger.error(f"Dynatrace API error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

