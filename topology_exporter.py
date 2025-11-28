"""
Topology Exporter Module

Extracts service-to-service topology from Dynatrace entities
and exports as a CSV edge list.
"""

import csv
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from dynatrace_client import ClientConfig, DynatraceClient, DynatraceAPIError


@dataclass
class ExportResult:
    """Result of a topology export operation."""
    success: bool
    message: str
    total_services: int = 0
    total_edges: int = 0
    output_file: str = ""
    unknown_ids_count: int = 0


@dataclass
class ExportProgress:
    """Progress information during export."""
    page: int = 0
    entities_fetched: int = 0
    edges_found: int = 0
    status: str = "Idle"


class TopologyExporter:
    """
    Extracts service topology from Dynatrace and exports to CSV.
    
    Handles pagination, relationship extraction, ID-to-name mapping,
    and CSV generation.
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
        
        self._id_to_name: Dict[str, str] = {}
        self._edges: Set[Tuple[str, str, str]] = set()  # (source_id, target_id, relationship)
        self._unknown_ids: Set[str] = set()
        self._cancelled = False

    def cancel(self) -> None:
        """Request cancellation of the current export."""
        self._cancelled = True

    def _is_cancelled(self) -> bool:
        """Check if cancellation was requested."""
        return self._cancelled

    def _reset(self) -> None:
        """Reset internal state for a new export."""
        self._id_to_name.clear()
        self._edges.clear()
        self._unknown_ids.clear()
        self._cancelled = False

    def _fetch_all_services(self) -> List[Dict[str, Any]]:
        """
        Fetch all SERVICE entities using pagination.
        
        Returns:
            List of all service entity objects
        """
        all_entities: List[Dict[str, Any]] = []
        page_count = 0
        next_page_key: Optional[str] = None
        progress = ExportProgress(status="Fetching services...")
        
        self.log("📡 Starting to fetch SERVICE entities...")
        
        while True:
            if self._is_cancelled():
                raise DynatraceAPIError(0, "Export cancelled by user")
            
            page_count += 1
            progress.page = page_count
            
            if next_page_key:
                self.log(f"   Fetching page {page_count} (continuation)...")
            else:
                self.log(f"   Fetching page {page_count} (initial request)...")
            
            response = self.client.fetch_entities_page(
                next_page_key=next_page_key,
                check_cancelled=self._is_cancelled
            )
            
            # Extract entities from response
            entities = response.get("entities", [])
            if not isinstance(entities, list):
                self.log(f"   ⚠ Unexpected 'entities' type: {type(entities)}. Treating as empty.")
                entities = []
            
            all_entities.extend(entities)
            progress.entities_fetched = len(all_entities)
            self.on_progress(progress)
            
            self.log(f"   Page {page_count}: {len(entities)} services (total: {len(all_entities)})")
            
            # Check for next page
            next_page_key = response.get("nextPageKey")
            if not next_page_key:
                break
        
        self.log(f"✓ Pagination complete. Pages: {page_count}, Total services: {len(all_entities)}")
        return all_entities

    def _build_id_to_name_map(self, entities: List[Dict[str, Any]]) -> None:
        """Build mapping from entity ID to display name."""
        for entity in entities:
            entity_id = entity.get("entityId")
            display_name = entity.get("displayName", "")
            if entity_id:
                self._id_to_name[entity_id] = display_name
        
        self.log(f"📋 Built ID-to-name mapping: {len(self._id_to_name)} services")

    def _extract_relationships(self, entities: List[Dict[str, Any]]) -> None:
        """Extract service-to-service relationships from entities."""
        progress = ExportProgress(
            entities_fetched=len(entities),
            status="Extracting relationships..."
        )
        self.on_progress(progress)
        
        for entity in entities:
            if self._is_cancelled():
                raise DynatraceAPIError(0, "Export cancelled by user")
            
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
                    
                    if target_id and target_type == "SERVICE":
                        self._add_edge(entity_id, target_id, "CALLS")
            
            # Extract incoming calls (others CALL this service)
            to_rels = entity.get("toRelationships", {})
            called_by = to_rels.get("called_by", [])
            if isinstance(called_by, list):
                for source in called_by:
                    source_id = source.get("id")
                    source_type = source.get("type", "")
                    
                    if source_id and source_type == "SERVICE":
                        self._add_edge(source_id, entity_id, "CALLED_BY")
        
        progress.edges_found = len(self._edges)
        self.on_progress(progress)
        self.log(f"🔗 Extracted {len(self._edges)} unique edges")

    def _add_edge(self, source_id: str, target_id: str, relationship: str) -> None:
        """Add an edge, tracking unknown IDs for later resolution."""
        if source_id not in self._id_to_name:
            self._unknown_ids.add(source_id)
        if target_id not in self._id_to_name:
            self._unknown_ids.add(target_id)
        
        self._edges.add((source_id, target_id, relationship))

    def _resolve_unknown_ids(self) -> None:
        """Attempt to resolve unknown entity IDs."""
        if not self._unknown_ids:
            return
        
        self.log(f"🔍 Resolving {len(self._unknown_ids)} unknown entity IDs...")
        resolved = 0
        
        for entity_id in list(self._unknown_ids):
            if self._is_cancelled():
                break
            
            if entity_id in self._id_to_name:
                self._unknown_ids.discard(entity_id)
                continue
            
            try:
                entity = self.client.fetch_entity_by_id(entity_id)
                if entity:
                    display_name = entity.get("displayName", "UNKNOWN")
                    self._id_to_name[entity_id] = display_name
                    self._unknown_ids.discard(entity_id)
                    resolved += 1
            except DynatraceAPIError:
                # Keep as unknown, will use "UNKNOWN" as name
                pass
        
        if resolved > 0:
            self.log(f"   Resolved {resolved} IDs")
        if self._unknown_ids:
            self.log(f"   ⚠ {len(self._unknown_ids)} IDs remain unresolved (will use 'UNKNOWN')")

    def _write_csv(self, output_path: str) -> int:
        """
        Write edges to CSV file.
        
        Returns:
            Number of edges written
        """
        self.log(f"💾 Writing CSV to: {output_path}")
        
        # Build final edge list with resolved names
        final_edges: List[Tuple[str, str, str, str, str]] = []
        for source_id, target_id, relationship in self._edges:
            source_name = self._id_to_name.get(source_id, "UNKNOWN")
            target_name = self._id_to_name.get(target_id, "UNKNOWN")
            final_edges.append((source_id, source_name, target_id, target_name, relationship))
        
        # Sort for consistent output
        final_edges.sort()
        
        # Write CSV
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["source_id", "source_name", "target_id", "target_name", "relationship"])
            writer.writerows(final_edges)
        
        self.log(f"✓ CSV written: {len(final_edges)} edges")
        return len(final_edges)

    def run(self, output_path: str) -> ExportResult:
        """
        Execute the full topology extraction and export.
        
        Args:
            output_path: Path to output CSV file
            
        Returns:
            ExportResult with success status and statistics
        """
        self._reset()
        
        try:
            # Fetch all services
            entities = self._fetch_all_services()
            
            if not entities:
                self.log("⚠ No SERVICE entities found!")
                return ExportResult(
                    success=True,
                    message="No SERVICE entities found. Empty CSV created.",
                    total_services=0,
                    total_edges=0,
                    output_file=output_path,
                )
            
            # Build ID-to-name mapping
            self._build_id_to_name_map(entities)
            
            # Extract relationships
            self._extract_relationships(entities)
            
            # Resolve unknown IDs
            if self._unknown_ids:
                self._resolve_unknown_ids()
            
            # Write CSV
            edge_count = self._write_csv(output_path)
            
            # Count edges with unknown names
            unknown_count = sum(
                1 for src, tgt, _ in self._edges
                if self._id_to_name.get(src, "UNKNOWN") == "UNKNOWN" or
                   self._id_to_name.get(tgt, "UNKNOWN") == "UNKNOWN"
            )
            
            self.log("=" * 50)
            self.log("✅ EXPORT COMPLETED SUCCESSFULLY")
            self.log(f"   Services: {len(self._id_to_name)}")
            self.log(f"   Edges: {edge_count}")
            self.log(f"   Output: {output_path}")
            if unknown_count > 0:
                self.log(f"   ⚠ Edges with unknown names: {unknown_count}")
            self.log("=" * 50)
            
            return ExportResult(
                success=True,
                message="Export completed successfully",
                total_services=len(self._id_to_name),
                total_edges=edge_count,
                output_file=output_path,
                unknown_ids_count=unknown_count,
            )
            
        except DynatraceAPIError as e:
            self.log(f"❌ API Error: {e}")
            return ExportResult(
                success=False,
                message=str(e),
                total_services=len(self._id_to_name),
                total_edges=len(self._edges),
            )
        except IOError as e:
            self.log(f"❌ File Error: {e}")
            return ExportResult(
                success=False,
                message=f"Failed to write CSV: {e}",
                total_services=len(self._id_to_name),
                total_edges=len(self._edges),
            )
        except Exception as e:
            self.log(f"❌ Unexpected Error: {e}")
            return ExportResult(
                success=False,
                message=f"Unexpected error: {e}",
            )

