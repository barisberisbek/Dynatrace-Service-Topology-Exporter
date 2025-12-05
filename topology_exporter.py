"""
Recursive Topology Exporter Module

Implements BFS (Breadth-First Search) traversal to discover service-to-service
topology starting from user-provided root service IDs.

Exports to multiple formats: Excel (.xlsx), CSV (.csv), GraphML (.graphml)
"""

from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import pandas as pd
import networkx as nx

from dynatrace_client import ClientConfig, DynatraceClient, DynatraceAPIError, ServiceEntity


# =============================================================================
# Result Models
# =============================================================================

@dataclass
class ExportResult:
    """Result of a topology export operation."""
    success: bool
    message: str
    total_services: int = 0
    total_edges: int = 0
    output_files: List[str] = field(default_factory=list)
    traversal_depth: int = 0


@dataclass
class ExportProgress:
    """Progress information during BFS traversal."""
    current_depth: int = 0
    services_discovered: int = 0
    edges_found: int = 0
    queue_size: int = 0
    status: str = "Idle"


@dataclass
class EdgeRecord:
    """
    Represents a single edge (relationship) in the topology.
    
    Contains all fields for the flat table export model.
    """
    # Source service details
    source_id: str
    source_name: str
    source_pg: str
    source_web_app_id: str
    source_remote_name: str
    source_web_server: str
    # Relationship type
    relation: str  # Always "CALLS" for this implementation
    # Target service details
    target_id: str
    target_name: str
    target_pg: str
    target_web_app_id: str
    target_remote_name: str
    target_web_server: str

    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary for DataFrame creation."""
        return {
            "Source_ID": self.source_id,
            "Source_Name": self.source_name,
            "Source_PG": self.source_pg,
            "Source_WebAppID": self.source_web_app_id,
            "Source_RemoteName": self.source_remote_name,
            "Source_WebServer": self.source_web_server,
            "RELATION": self.relation,
            "Target_ID": self.target_id,
            "Target_Name": self.target_name,
            "Target_PG": self.target_pg,
            "Target_WebAppID": self.target_web_app_id,
            "Target_RemoteName": self.target_remote_name,
            "Target_WebServer": self.target_web_server,
        }


# =============================================================================
# Topology Exporter (BFS Implementation)
# =============================================================================

class TopologyExporter:
    """
    Recursive Topology Discoverer using BFS traversal.
    
    Starting from user-provided root service IDs, discovers the complete
    downstream service topology by following CALLS relationships.
    
    Algorithm:
    1. Initialize queue with root service IDs
    2. For each batch of IDs in queue:
       a. Fetch service details from Dynatrace API
       b. Extract CALLS relationships
       c. Add unvisited target IDs to queue
       d. Record edges for export
    3. Continue until queue is empty
    4. Export collected edges to selected formats
    """

    def __init__(
        self,
        client: DynatraceClient,
        log_callback: Optional[Callable[[str], None]] = None,
        progress_callback: Optional[Callable[[ExportProgress], None]] = None,
    ):
        """
        Initialize the exporter.
        
        Args:
            client: Configured DynatraceClient instance
            log_callback: Optional callback for log messages
            progress_callback: Optional callback for progress updates
        """
        self.client = client
        self.log = log_callback or (lambda msg: None)
        self.on_progress = progress_callback or (lambda p: None)
        
        # BFS state
        self._visited: Set[str] = set()
        self._queue: deque = deque()
        self._services: Dict[str, ServiceEntity] = {}  # ID -> ServiceEntity
        self._edges: List[EdgeRecord] = []
        
        # Control
        self._cancelled = False
        self._batch_size = 50  # IDs per API call

    def cancel(self) -> None:
        """Request cancellation of the current export."""
        self._cancelled = True

    def _is_cancelled(self) -> bool:
        """Check if cancellation was requested."""
        return self._cancelled

    def _reset(self) -> None:
        """Reset internal state for a new export."""
        self._visited.clear()
        self._queue.clear()
        self._services.clear()
        self._edges.clear()
        self._cancelled = False

    def _bfs_traverse(self, root_ids: List[str]) -> int:
        """
        Perform BFS traversal starting from root service IDs.
        
        Args:
            root_ids: List of starting service IDs
            
        Returns:
            Maximum depth reached during traversal
        """
        # Initialize queue with root IDs (depth 0)
        for rid in root_ids:
            if rid and rid not in self._visited:
                self._queue.append((rid, 0))  # (entity_id, depth)
                self._visited.add(rid)
        
        max_depth = 0
        progress = ExportProgress(status="Starting BFS traversal...")
        self.on_progress(progress)
        
        self.log("🔍 Starting BFS traversal...")
        self.log(f"   Root services: {len(root_ids)}")
        
        while self._queue:
            if self._is_cancelled():
                raise DynatraceAPIError(0, "Export cancelled by user")
            
            # Collect a batch of IDs from the queue
            batch: List[Tuple[str, int]] = []
            while self._queue and len(batch) < self._batch_size:
                batch.append(self._queue.popleft())
            
            if not batch:
                break
            
            # Extract IDs and track max depth
            batch_ids = [item[0] for item in batch]
            batch_depths = [item[1] for item in batch]
            current_max_depth = max(batch_depths)
            max_depth = max(max_depth, current_max_depth)
            
            # Update progress
            progress.current_depth = current_max_depth
            progress.services_discovered = len(self._services)
            progress.edges_found = len(self._edges)
            progress.queue_size = len(self._queue)
            progress.status = f"Depth {current_max_depth}: Fetching {len(batch_ids)} services..."
            self.on_progress(progress)
            
            self.log(f"   Depth {current_max_depth}: Fetching {len(batch_ids)} services...")
            
            # Fetch service details from API
            try:
                services = self.client.fetch_services_by_ids(
                    batch_ids,
                    check_cancelled=self._is_cancelled
                )
            except DynatraceAPIError as e:
                self.log(f"   ⚠ API error fetching batch: {e.message}")
                continue
            
            # Process fetched services
            for service in services:
                self._services[service.entity_id] = service
                
                # Find the depth for this service
                service_depth = 0
                for bid, bdepth in batch:
                    if bid == service.entity_id:
                        service_depth = bdepth
                        break
                
                # Process outgoing CALLS relationships
                for target_id in service.calls:
                    # Add target to queue if not visited
                    if target_id not in self._visited:
                        self._visited.add(target_id)
                        self._queue.append((target_id, service_depth + 1))
            
            self.log(f"      Retrieved: {len(services)} services, Queue: {len(self._queue)}")
        
        self.log(f"✓ BFS traversal complete. Max depth: {max_depth}")
        return max_depth

    def _build_edges(self) -> None:
        """
        Build edge records from discovered services.
        
        Creates EdgeRecord for each CALLS relationship where both
        source and target services have been discovered.
        """
        self.log("🔗 Building edge records...")
        
        for service_id, service in self._services.items():
            for target_id in service.calls:
                # Get target service (may or may not be in our discovered set)
                target = self._services.get(target_id)
                
                edge = EdgeRecord(
                    # Source details
                    source_id=service.entity_id,
                    source_name=service.display_name,
                    source_pg=service.process_group,
                    source_web_app_id=service.web_application_id,
                    source_remote_name=service.remote_endpoint,
                    source_web_server=service.web_server_name,
                    # Relationship
                    relation="CALLS",
                    # Target details (use UNKNOWN if target not discovered)
                    target_id=target_id,
                    target_name=target.display_name if target else "UNKNOWN",
                    target_pg=target.process_group if target else "",
                    target_web_app_id=target.web_application_id if target else "",
                    target_remote_name=target.remote_endpoint if target else "",
                    target_web_server=target.web_server_name if target else "",
                )
                self._edges.append(edge)
        
        self.log(f"   Total edges: {len(self._edges)}")

    def _create_dataframe(self) -> pd.DataFrame:
        """
        Create pandas DataFrame from edge records.
        
        Returns:
            DataFrame with all edge data
        """
        if not self._edges:
            # Return empty DataFrame with correct columns
            return pd.DataFrame(columns=[
                "Source_ID", "Source_Name", "Source_PG", "Source_WebAppID",
                "Source_RemoteName", "Source_WebServer", "RELATION",
                "Target_ID", "Target_Name", "Target_PG", "Target_WebAppID",
                "Target_RemoteName", "Target_WebServer"
            ])
        
        records = [edge.to_dict() for edge in self._edges]
        return pd.DataFrame(records)

    def _export_excel(self, df: pd.DataFrame, output_path: str) -> str:
        """Export DataFrame to Excel format."""
        excel_path = output_path.replace(".csv", "").replace(".graphml", "") + ".xlsx"
        df.to_excel(excel_path, index=False, sheet_name="Topology", engine="openpyxl")
        self.log(f"💾 Excel exported: {excel_path}")
        return excel_path

    def _export_csv(self, df: pd.DataFrame, output_path: str) -> str:
        """Export DataFrame to CSV format."""
        csv_path = output_path.replace(".xlsx", "").replace(".graphml", "") + ".csv"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        self.log(f"💾 CSV exported: {csv_path}")
        return csv_path

    def _export_graphml(self, output_path: str) -> str:
        """
        Export topology to GraphML format.
        
        Creates a directed graph with service nodes and CALLS edges.
        Node attributes include all service properties.
        """
        graphml_path = output_path.replace(".xlsx", "").replace(".csv", "") + ".graphml"
        
        # Create directed graph
        G = nx.DiGraph()
        
        # Add nodes with attributes
        for service_id, service in self._services.items():
            G.add_node(
                service_id,
                label=service.display_name,
                displayName=service.display_name,
                processGroup=service.process_group,
                webApplicationId=service.web_application_id,
                webServerName=service.web_server_name,
                remoteEndpoint=service.remote_endpoint,
                serviceType=service.service_type,
            )
        
        # Add edges
        for edge in self._edges:
            # Ensure target node exists (add minimal node if not discovered)
            if edge.target_id not in G:
                G.add_node(
                    edge.target_id,
                    label=edge.target_name,
                    displayName=edge.target_name,
                )
            
            G.add_edge(
                edge.source_id,
                edge.target_id,
                relation=edge.relation,
            )
        
        # Write GraphML
        nx.write_graphml(G, graphml_path)
        self.log(f"💾 GraphML exported: {graphml_path}")
        return graphml_path

    def run(
        self,
        root_ids: List[str],
        output_path: str,
        export_excel: bool = True,
        export_csv: bool = False,
        export_graphml: bool = False,
    ) -> ExportResult:
        """
        Execute the recursive topology discovery and export.
        
        Args:
            root_ids: List of root service IDs to start traversal from
            output_path: Base path for output files (extension will be adjusted)
            export_excel: Export to Excel format (.xlsx)
            export_csv: Export to CSV format (.csv)
            export_graphml: Export to GraphML format (.graphml)
            
        Returns:
            ExportResult with success status and statistics
        """
        self._reset()
        
        # Validate inputs
        if not root_ids:
            return ExportResult(
                success=False,
                message="No root service IDs provided"
            )
        
        # Filter empty IDs
        root_ids = [rid.strip() for rid in root_ids if rid and rid.strip()]
        if not root_ids:
            return ExportResult(
                success=False,
                message="No valid root service IDs provided"
            )
        
        self.log("=" * 60)
        self.log("🚀 RECURSIVE TOPOLOGY DISCOVERY")
        self.log("=" * 60)
        self.log(f"   Root Service IDs: {len(root_ids)}")
        for rid in root_ids[:5]:  # Show first 5
            self.log(f"      • {rid}")
        if len(root_ids) > 5:
            self.log(f"      ... and {len(root_ids) - 5} more")
        self.log("")
        
        try:
            # Phase 1: BFS Traversal
            max_depth = self._bfs_traverse(root_ids)
            
            if not self._services:
                self.log("⚠ No services discovered!")
                return ExportResult(
                    success=True,
                    message="No services found. Check if root IDs are valid.",
                    total_services=0,
                    total_edges=0,
                )
            
            # Phase 2: Build Edges
            self._build_edges()
            
            # Phase 3: Create DataFrame
            df = self._create_dataframe()
            
            # Phase 4: Export to selected formats
            output_files = []
            
            if export_excel:
                path = self._export_excel(df, output_path)
                output_files.append(path)
            
            if export_csv:
                path = self._export_csv(df, output_path)
                output_files.append(path)
            
            if export_graphml:
                path = self._export_graphml(output_path)
                output_files.append(path)
            
            # Summary
            self.log("")
            self.log("=" * 60)
            self.log("✅ EXPORT COMPLETED SUCCESSFULLY")
            self.log(f"   Services Discovered: {len(self._services)}")
            self.log(f"   Edges (CALLS): {len(self._edges)}")
            self.log(f"   Max Traversal Depth: {max_depth}")
            self.log(f"   Output Files: {len(output_files)}")
            for f in output_files:
                self.log(f"      • {f}")
            self.log("=" * 60)
            
            return ExportResult(
                success=True,
                message="Export completed successfully",
                total_services=len(self._services),
                total_edges=len(self._edges),
                output_files=output_files,
                traversal_depth=max_depth,
            )
            
        except DynatraceAPIError as e:
            self.log(f"❌ API Error: {e}")
            return ExportResult(
                success=False,
                message=str(e),
                total_services=len(self._services),
                total_edges=len(self._edges),
            )
        except IOError as e:
            self.log(f"❌ File Error: {e}")
            return ExportResult(
                success=False,
                message=f"Failed to write output file: {e}",
                total_services=len(self._services),
                total_edges=len(self._edges),
            )
        except Exception as e:
            self.log(f"❌ Unexpected Error: {e}")
            return ExportResult(
                success=False,
                message=f"Unexpected error: {e}",
            )
