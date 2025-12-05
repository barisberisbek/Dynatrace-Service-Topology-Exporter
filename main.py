"""
Dynatrace Recursive Topology Discoverer - Desktop Application

A modern PySide6-based GUI application with Garanti BBVA corporate theme
for discovering service-to-service topology using BFS traversal.
"""

import os
import sys
import subprocess
import platform
from pathlib import Path
from typing import List, Optional

from PySide6.QtCore import Qt, QThread, Signal, Slot
from PySide6.QtGui import QFont, QColor, QPalette
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QFormLayout,
    QGroupBox,
    QLabel,
    QLineEdit,
    QTextEdit,
    QSpinBox,
    QCheckBox,
    QPushButton,
    QProgressBar,
    QFileDialog,
    QMessageBox,
    QFrame,
    QSizePolicy,
)

from dynatrace_client import ClientConfig, DynatraceClient, DynatraceAPIError
from topology_exporter import TopologyExporter, ExportResult


# =============================================================================
# Garanti BBVA Corporate Colors
# =============================================================================

class GarantiColors:
    """Garanti BBVA corporate color palette."""
    PRIMARY_GREEN = "#008558"       # Main brand green
    DARK_GREEN = "#006847"          # Darker green for hover states
    LIGHT_GREEN = "#00A86B"         # Lighter green for accents
    WHITE = "#FFFFFF"               # Clean white background
    LIGHT_GREY = "#F4F6F6"          # Light grey for sections
    MEDIUM_GREY = "#E8ECEC"         # Medium grey for borders
    DARK_GREY = "#2C3E50"           # Dark grey for text
    TEXT_SECONDARY = "#5D6D7E"      # Secondary text color
    ERROR_RED = "#E74C3C"           # Error state
    WARNING_ORANGE = "#E67E22"      # Warning state
    SUCCESS_GREEN = "#27AE60"       # Success state (different from brand)


# =============================================================================
# Worker Thread for Background Export
# =============================================================================

class ExportWorker(QThread):
    """Background worker thread for recursive topology discovery."""
    
    log_message = Signal(str)
    progress_update = Signal(int, int, int, str)  # depth, services, edges, status
    finished = Signal(object)  # ExportResult
    
    def __init__(
        self,
        config: ClientConfig,
        root_ids: List[str],
        output_path: str,
        export_excel: bool,
        export_csv: bool,
        export_graphml: bool,
    ):
        super().__init__()
        self.config = config
        self.root_ids = root_ids
        self.output_path = output_path
        self.export_excel = export_excel
        self.export_csv = export_csv
        self.export_graphml = export_graphml
        self._exporter: Optional[TopologyExporter] = None
        self._client: Optional[DynatraceClient] = None

    def run(self):
        """Execute the export in background thread."""
        try:
            self._client = DynatraceClient(
                self.config,
                log_callback=self._emit_log
            )
            
            self._exporter = TopologyExporter(
                self._client,
                log_callback=self._emit_log,
                progress_callback=self._emit_progress
            )
            
            result = self._exporter.run(
                root_ids=self.root_ids,
                output_path=self.output_path,
                export_excel=self.export_excel,
                export_csv=self.export_csv,
                export_graphml=self.export_graphml,
            )
            self.finished.emit(result)
            
        except Exception as e:
            self.finished.emit(ExportResult(
                success=False,
                message=f"Unexpected error: {e}"
            ))
        finally:
            if self._client:
                self._client.close()

    def _emit_log(self, message: str):
        self.log_message.emit(message)

    def _emit_progress(self, progress):
        self.progress_update.emit(
            progress.current_depth,
            progress.services_discovered,
            progress.edges_found,
            progress.status
        )

    def cancel(self):
        """Request cancellation of the export."""
        if self._exporter:
            self._exporter.cancel()


class TestConnectionWorker(QThread):
    """Background worker for testing API connection."""
    
    finished = Signal(bool, str)  # success, message
    
    def __init__(self, config: ClientConfig):
        super().__init__()
        self.config = config

    def run(self):
        try:
            client = DynatraceClient(self.config)
            response = client.test_connection()
            client.close()
            
            entities = response.get("entities", [])
            total = response.get("totalCount", len(entities))
            
            self.finished.emit(
                True,
                f"✓ Connection successful!\n\nTotal SERVICE entities available: {total}"
            )
        except DynatraceAPIError as e:
            self.finished.emit(False, f"Connection failed:\n\n{e.message}")
        except Exception as e:
            self.finished.emit(False, f"Unexpected error:\n\n{str(e)}")


# =============================================================================
# Main Application Window
# =============================================================================

class MainWindow(QMainWindow):
    """Main application window with Garanti BBVA theme."""

    def __init__(self):
        super().__init__()
        self._export_worker: Optional[ExportWorker] = None
        self._test_worker: Optional[TestConnectionWorker] = None
        self._setup_ui()
        self._connect_signals()
        self._apply_garanti_theme()

    def _setup_ui(self):
        """Build the user interface."""
        self.setWindowTitle("Dynatrace Recursive Topology Discoverer")
        self.setMinimumSize(800, 850)
        self.resize(850, 900)

        # Central widget and main layout
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setSpacing(16)
        main_layout.setContentsMargins(20, 20, 20, 20)

        # ===== Header =====
        header = QLabel("Dynatrace Service Topology Discoverer")
        header.setObjectName("header")
        header.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(header)

        # ===== Connection Settings Group =====
        conn_group = QGroupBox("Dynatrace Connection")
        conn_group.setObjectName("settingsGroup")
        conn_layout = QFormLayout()
        conn_layout.setSpacing(12)
        conn_layout.setContentsMargins(16, 20, 16, 16)

        # Base URL
        self.base_url_input = QLineEdit()
        self.base_url_input.setPlaceholderText("https://activegate-host:9999/e/environment-id/api/v2")
        self.base_url_input.setObjectName("inputField")
        conn_layout.addRow("Base URL:", self.base_url_input)

        # Batch Size
        batch_widget = QWidget()
        batch_layout = QHBoxLayout(batch_widget)
        batch_layout.setContentsMargins(0, 0, 0, 0)
        batch_layout.setSpacing(8)
        
        self.batch_size_spin = QSpinBox()
        self.batch_size_spin.setRange(10, 100)
        self.batch_size_spin.setValue(50)
        self.batch_size_spin.setFixedWidth(80)
        self.batch_size_spin.setObjectName("inputField")
        batch_layout.addWidget(self.batch_size_spin)
        batch_layout.addWidget(QLabel("IDs per API call (10-100)"))
        batch_layout.addStretch()
        conn_layout.addRow("Batch Size:", batch_widget)

        # SSL Verification (default unchecked)
        ssl_widget = QWidget()
        ssl_layout = QHBoxLayout(ssl_widget)
        ssl_layout.setContentsMargins(0, 0, 0, 0)
        ssl_layout.setSpacing(12)

        self.ssl_checkbox = QCheckBox("Verify SSL certificates")
        self.ssl_checkbox.setChecked(False)  # Default to unchecked for on-prem
        ssl_layout.addWidget(self.ssl_checkbox)

        self.ssl_warning = QLabel("⚠ SSL verification disabled")
        self.ssl_warning.setObjectName("warningLabel")
        self.ssl_warning.setVisible(True)
        ssl_layout.addWidget(self.ssl_warning)
        ssl_layout.addStretch()
        conn_layout.addRow("", ssl_widget)

        conn_group.setLayout(conn_layout)
        main_layout.addWidget(conn_group)

        # ===== Root Services Group =====
        root_group = QGroupBox("Root Service IDs")
        root_group.setObjectName("settingsGroup")
        root_layout = QVBoxLayout()
        root_layout.setSpacing(8)
        root_layout.setContentsMargins(16, 20, 16, 16)

        root_label = QLabel("Enter Service IDs to start topology discovery (one per line):")
        root_label.setObjectName("helpText")
        root_layout.addWidget(root_label)

        self.root_ids_input = QTextEdit()
        self.root_ids_input.setPlaceholderText(
            "SERVICE-1234567890ABCDEF\n"
            "SERVICE-FEDCBA0987654321\n"
            "SERVICE-..."
        )
        self.root_ids_input.setObjectName("multiLineInput")
        self.root_ids_input.setMinimumHeight(100)
        self.root_ids_input.setMaximumHeight(150)
        root_layout.addWidget(self.root_ids_input)

        root_group.setLayout(root_layout)
        main_layout.addWidget(root_group)

        # ===== Authentication Group =====
        auth_group = QGroupBox("Authentication")
        auth_group.setObjectName("settingsGroup")
        auth_layout = QVBoxLayout()
        auth_layout.setSpacing(8)
        auth_layout.setContentsMargins(16, 20, 16, 16)

        auth_info = QLabel(
            "API token is read from environment variable: DYNATRACE_API_TOKEN\n"
            "Required scope: entities.read"
        )
        auth_info.setObjectName("helpText")
        auth_info.setWordWrap(True)
        auth_layout.addWidget(auth_info)

        self.check_token_btn = QPushButton("Check Token")
        self.check_token_btn.setObjectName("secondaryButton")
        self.check_token_btn.setFixedWidth(120)
        auth_layout.addWidget(self.check_token_btn)

        auth_group.setLayout(auth_layout)
        main_layout.addWidget(auth_group)

        # ===== Export Settings Group =====
        export_group = QGroupBox("Export Settings")
        export_group.setObjectName("settingsGroup")
        export_layout = QVBoxLayout()
        export_layout.setSpacing(12)
        export_layout.setContentsMargins(16, 20, 16, 16)

        # Output path
        path_widget = QWidget()
        path_layout = QHBoxLayout(path_widget)
        path_layout.setContentsMargins(0, 0, 0, 0)
        path_layout.setSpacing(8)

        self.output_path_input = QLineEdit()
        self.output_path_input.setPlaceholderText("Select output file location...")
        self.output_path_input.setObjectName("inputField")
        path_layout.addWidget(self.output_path_input)

        self.browse_btn = QPushButton("Browse...")
        self.browse_btn.setObjectName("secondaryButton")
        self.browse_btn.setFixedWidth(100)
        path_layout.addWidget(self.browse_btn)
        export_layout.addWidget(path_widget)

        # Export format checkboxes
        format_widget = QWidget()
        format_layout = QHBoxLayout(format_widget)
        format_layout.setContentsMargins(0, 0, 0, 0)
        format_layout.setSpacing(24)

        format_label = QLabel("Export Formats:")
        format_layout.addWidget(format_label)

        self.excel_checkbox = QCheckBox("Excel (.xlsx)")
        self.excel_checkbox.setChecked(True)
        format_layout.addWidget(self.excel_checkbox)

        self.csv_checkbox = QCheckBox("CSV (.csv)")
        self.csv_checkbox.setChecked(False)
        format_layout.addWidget(self.csv_checkbox)

        self.graphml_checkbox = QCheckBox("GraphML (.graphml)")
        self.graphml_checkbox.setChecked(False)
        format_layout.addWidget(self.graphml_checkbox)

        format_layout.addStretch()
        export_layout.addWidget(format_widget)

        export_group.setLayout(export_layout)
        main_layout.addWidget(export_group)

        # ===== Control Buttons =====
        control_widget = QWidget()
        control_layout = QHBoxLayout(control_widget)
        control_layout.setContentsMargins(0, 8, 0, 8)
        control_layout.setSpacing(12)

        self.test_conn_btn = QPushButton("Test Connection")
        self.test_conn_btn.setObjectName("secondaryButton")
        self.test_conn_btn.setFixedHeight(40)
        control_layout.addWidget(self.test_conn_btn)

        self.run_export_btn = QPushButton("▶  Discover Topology")
        self.run_export_btn.setObjectName("primaryButton")
        self.run_export_btn.setFixedHeight(40)
        control_layout.addWidget(self.run_export_btn)

        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setObjectName("dangerButton")
        self.cancel_btn.setFixedHeight(40)
        self.cancel_btn.setEnabled(False)
        control_layout.addWidget(self.cancel_btn)

        self.open_folder_btn = QPushButton("Open Output Folder")
        self.open_folder_btn.setObjectName("secondaryButton")
        self.open_folder_btn.setFixedHeight(40)
        self.open_folder_btn.setEnabled(False)
        control_layout.addWidget(self.open_folder_btn)

        control_layout.addStretch()
        main_layout.addWidget(control_widget)

        # ===== Progress Section =====
        progress_widget = QWidget()
        progress_layout = QHBoxLayout(progress_widget)
        progress_layout.setContentsMargins(0, 0, 0, 0)
        progress_layout.setSpacing(16)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # Indeterminate
        self.progress_bar.setVisible(False)
        self.progress_bar.setFixedHeight(8)
        self.progress_bar.setObjectName("progressBar")
        progress_layout.addWidget(self.progress_bar)

        self.stats_label = QLabel("")
        self.stats_label.setObjectName("statsLabel")
        self.stats_label.setMinimumWidth(300)
        progress_layout.addWidget(self.stats_label)

        main_layout.addWidget(progress_widget)

        # ===== Log Area =====
        log_group = QGroupBox("Discovery Log")
        log_group.setObjectName("settingsGroup")
        log_layout = QVBoxLayout()
        log_layout.setSpacing(8)
        log_layout.setContentsMargins(16, 20, 16, 16)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setObjectName("logArea")
        self.log_text.setMinimumHeight(180)
        log_layout.addWidget(self.log_text)

        log_btn_layout = QHBoxLayout()
        self.clear_log_btn = QPushButton("Clear Logs")
        self.clear_log_btn.setObjectName("secondaryButton")
        self.clear_log_btn.setFixedWidth(100)
        log_btn_layout.addWidget(self.clear_log_btn)
        log_btn_layout.addStretch()
        log_layout.addLayout(log_btn_layout)

        log_group.setLayout(log_layout)
        main_layout.addWidget(log_group)

        # ===== Status Bar =====
        self.status_label = QLabel("Ready")
        self.status_label.setObjectName("statusBar")
        main_layout.addWidget(self.status_label)

    def _connect_signals(self):
        """Connect UI signals to slots."""
        self.ssl_checkbox.stateChanged.connect(self._on_ssl_changed)
        self.check_token_btn.clicked.connect(self._check_token)
        self.browse_btn.clicked.connect(self._browse_output)
        self.test_conn_btn.clicked.connect(self._test_connection)
        self.run_export_btn.clicked.connect(self._run_export)
        self.cancel_btn.clicked.connect(self._cancel_export)
        self.open_folder_btn.clicked.connect(self._open_output_folder)
        self.clear_log_btn.clicked.connect(self.log_text.clear)

    def _apply_garanti_theme(self):
        """Apply Garanti BBVA corporate theme stylesheet."""
        self.setStyleSheet(f"""
            /* Main Window */
            QMainWindow {{
                background-color: {GarantiColors.LIGHT_GREY};
            }}
            
            QWidget {{
                font-family: 'Segoe UI', 'Arial', sans-serif;
                font-size: 13px;
                color: {GarantiColors.DARK_GREY};
            }}
            
            /* Header */
            QLabel#header {{
                font-size: 22px;
                font-weight: bold;
                color: {GarantiColors.PRIMARY_GREEN};
                padding: 8px 0 16px 0;
            }}
            
            /* Group Boxes */
            QGroupBox#settingsGroup {{
                background-color: {GarantiColors.WHITE};
                border: 1px solid {GarantiColors.MEDIUM_GREY};
                border-radius: 8px;
                margin-top: 16px;
                padding-top: 12px;
                font-weight: bold;
                color: {GarantiColors.PRIMARY_GREEN};
            }}
            
            QGroupBox#settingsGroup::title {{
                subcontrol-origin: margin;
                left: 16px;
                padding: 0 8px;
                background-color: {GarantiColors.WHITE};
            }}
            
            /* Input Fields */
            QLineEdit#inputField, QSpinBox#inputField {{
                padding: 10px 12px;
                border: 2px solid {GarantiColors.MEDIUM_GREY};
                border-radius: 6px;
                background-color: {GarantiColors.WHITE};
                font-size: 13px;
            }}
            
            QLineEdit#inputField:focus, QSpinBox#inputField:focus {{
                border-color: {GarantiColors.PRIMARY_GREEN};
            }}
            
            /* Multi-line Input */
            QTextEdit#multiLineInput {{
                padding: 8px;
                border: 2px solid {GarantiColors.MEDIUM_GREY};
                border-radius: 6px;
                background-color: {GarantiColors.WHITE};
                font-family: 'Consolas', 'Courier New', monospace;
                font-size: 12px;
            }}
            
            QTextEdit#multiLineInput:focus {{
                border-color: {GarantiColors.PRIMARY_GREEN};
            }}
            
            /* Log Area */
            QTextEdit#logArea {{
                padding: 8px;
                border: 2px solid {GarantiColors.MEDIUM_GREY};
                border-radius: 6px;
                background-color: #FAFAFA;
                font-family: 'Consolas', 'Courier New', monospace;
                font-size: 11px;
                color: {GarantiColors.DARK_GREY};
            }}
            
            /* Primary Button (Green) */
            QPushButton#primaryButton {{
                background-color: {GarantiColors.PRIMARY_GREEN};
                color: {GarantiColors.WHITE};
                font-weight: bold;
                font-size: 14px;
                border: none;
                border-radius: 6px;
                padding: 0 24px;
                min-width: 160px;
            }}
            
            QPushButton#primaryButton:hover {{
                background-color: {GarantiColors.DARK_GREEN};
            }}
            
            QPushButton#primaryButton:disabled {{
                background-color: {GarantiColors.MEDIUM_GREY};
                color: {GarantiColors.TEXT_SECONDARY};
            }}
            
            /* Secondary Button */
            QPushButton#secondaryButton {{
                background-color: {GarantiColors.WHITE};
                color: {GarantiColors.PRIMARY_GREEN};
                font-weight: 600;
                border: 2px solid {GarantiColors.PRIMARY_GREEN};
                border-radius: 6px;
                padding: 0 16px;
            }}
            
            QPushButton#secondaryButton:hover {{
                background-color: {GarantiColors.LIGHT_GREY};
            }}
            
            QPushButton#secondaryButton:disabled {{
                border-color: {GarantiColors.MEDIUM_GREY};
                color: {GarantiColors.TEXT_SECONDARY};
            }}
            
            /* Danger Button */
            QPushButton#dangerButton {{
                background-color: {GarantiColors.WHITE};
                color: {GarantiColors.ERROR_RED};
                font-weight: 600;
                border: 2px solid {GarantiColors.ERROR_RED};
                border-radius: 6px;
                padding: 0 16px;
            }}
            
            QPushButton#dangerButton:hover {{
                background-color: #FDEDEC;
            }}
            
            QPushButton#dangerButton:disabled {{
                border-color: {GarantiColors.MEDIUM_GREY};
                color: {GarantiColors.TEXT_SECONDARY};
            }}
            
            /* Checkboxes */
            QCheckBox {{
                spacing: 8px;
                color: {GarantiColors.DARK_GREY};
            }}
            
            QCheckBox::indicator {{
                width: 18px;
                height: 18px;
                border: 2px solid {GarantiColors.MEDIUM_GREY};
                border-radius: 4px;
                background-color: {GarantiColors.WHITE};
            }}
            
            QCheckBox::indicator:checked {{
                background-color: {GarantiColors.PRIMARY_GREEN};
                border-color: {GarantiColors.PRIMARY_GREEN};
            }}
            
            /* Labels */
            QLabel#helpText {{
                color: {GarantiColors.TEXT_SECONDARY};
                font-size: 12px;
            }}
            
            QLabel#warningLabel {{
                color: {GarantiColors.WARNING_ORANGE};
                font-weight: bold;
                font-size: 12px;
            }}
            
            QLabel#statsLabel {{
                color: {GarantiColors.PRIMARY_GREEN};
                font-weight: bold;
                font-size: 13px;
            }}
            
            /* Status Bar */
            QLabel#statusBar {{
                padding: 8px 12px;
                background-color: {GarantiColors.WHITE};
                border: 1px solid {GarantiColors.MEDIUM_GREY};
                border-radius: 4px;
                color: {GarantiColors.TEXT_SECONDARY};
            }}
            
            /* Progress Bar */
            QProgressBar#progressBar {{
                border: none;
                border-radius: 4px;
                background-color: {GarantiColors.MEDIUM_GREY};
            }}
            
            QProgressBar#progressBar::chunk {{
                background-color: {GarantiColors.PRIMARY_GREEN};
                border-radius: 4px;
            }}
            
            /* Spin Box */
            QSpinBox {{
                padding: 8px;
                border: 2px solid {GarantiColors.MEDIUM_GREY};
                border-radius: 6px;
            }}
            
            QSpinBox:focus {{
                border-color: {GarantiColors.PRIMARY_GREEN};
            }}
        """)

    # =========================================================================
    # Slot Methods
    # =========================================================================

    @Slot()
    def _on_ssl_changed(self):
        """Show/hide SSL warning based on checkbox state."""
        self.ssl_warning.setVisible(not self.ssl_checkbox.isChecked())

    @Slot()
    def _check_token(self):
        """Check if API token environment variable is set."""
        token = os.environ.get("DYNATRACE_API_TOKEN")
        if token:
            QMessageBox.information(
                self,
                "Token Status",
                f"✓ API token detected\n\n"
                f"Length: {len(token)} characters\n"
                f"(Token value is not displayed for security)"
            )
        else:
            QMessageBox.warning(
                self,
                "Token Not Found",
                "API token not found!\n\n"
                "Please set the environment variable:\n\n"
                "Windows (PowerShell):\n"
                '$env:DYNATRACE_API_TOKEN = "your_token"\n\n'
                "Linux/macOS:\n"
                'export DYNATRACE_API_TOKEN="your_token"'
            )

    @Slot()
    def _browse_output(self):
        """Open file dialog to select output file path."""
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Topology Export",
            str(Path.home() / "service_topology.xlsx"),
            "Excel Files (*.xlsx);;CSV Files (*.csv);;GraphML Files (*.graphml);;All Files (*)"
        )
        if path:
            self.output_path_input.setText(path)

    def _get_root_ids(self) -> List[str]:
        """Parse root service IDs from the text input."""
        text = self.root_ids_input.toPlainText()
        lines = text.strip().split("\n")
        # Filter empty lines and strip whitespace
        ids = [line.strip() for line in lines if line.strip()]
        return ids

    def _get_config(self) -> Optional[ClientConfig]:
        """Build ClientConfig from UI inputs, or None if validation fails."""
        # Check API token
        token = os.environ.get("DYNATRACE_API_TOKEN")
        if not token:
            QMessageBox.critical(
                self,
                "Missing Token",
                "API token not found!\n\n"
                "Please set the DYNATRACE_API_TOKEN environment variable."
            )
            return None

        # Validate base URL
        base_url = self.base_url_input.text().strip()
        if not base_url:
            QMessageBox.critical(self, "Validation Error", "Base URL is required.")
            self.base_url_input.setFocus()
            return None

        if not base_url.startswith(("http://", "https://")):
            QMessageBox.critical(
                self,
                "Validation Error",
                "Base URL must start with http:// or https://"
            )
            self.base_url_input.setFocus()
            return None

        return ClientConfig(
            base_url=base_url.rstrip("/"),
            api_token=token,
            verify_ssl=self.ssl_checkbox.isChecked(),
            batch_size=self.batch_size_spin.value(),
        )

    @Slot()
    def _test_connection(self):
        """Test connection to Dynatrace API."""
        config = self._get_config()
        if not config:
            return

        self._set_ui_running(True, "Testing connection...")
        self.log_text.append("🔌 Testing connection to Dynatrace API...")

        self._test_worker = TestConnectionWorker(config)
        self._test_worker.finished.connect(self._on_test_finished)
        self._test_worker.start()

    @Slot(bool, str)
    def _on_test_finished(self, success: bool, message: str):
        """Handle test connection result."""
        self._set_ui_running(False)
        
        if success:
            self.log_text.append("✓ Connection test successful")
            QMessageBox.information(self, "Connection Test", message)
        else:
            self.log_text.append(f"✗ Connection test failed")
            QMessageBox.critical(self, "Connection Test Failed", message)

    @Slot()
    def _run_export(self):
        """Start the recursive topology discovery."""
        config = self._get_config()
        if not config:
            return

        # Validate root IDs
        root_ids = self._get_root_ids()
        if not root_ids:
            QMessageBox.critical(
                self,
                "Validation Error",
                "Please enter at least one Root Service ID."
            )
            self.root_ids_input.setFocus()
            return

        # Validate output path
        output_path = self.output_path_input.text().strip()
        if not output_path:
            QMessageBox.critical(self, "Validation Error", "Output file path is required.")
            self.browse_btn.click()
            return

        # Check at least one export format is selected
        if not (self.excel_checkbox.isChecked() or 
                self.csv_checkbox.isChecked() or 
                self.graphml_checkbox.isChecked()):
            QMessageBox.critical(
                self,
                "Validation Error",
                "Please select at least one export format."
            )
            return

        # Ensure directory exists
        output_dir = Path(output_path).parent
        if not output_dir.exists():
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                QMessageBox.critical(
                    self,
                    "Error",
                    f"Cannot create output directory:\n{e}"
                )
                return

        # Start export
        self._set_ui_running(True, "Discovering topology...")
        self.log_text.clear()

        self._export_worker = ExportWorker(
            config=config,
            root_ids=root_ids,
            output_path=output_path,
            export_excel=self.excel_checkbox.isChecked(),
            export_csv=self.csv_checkbox.isChecked(),
            export_graphml=self.graphml_checkbox.isChecked(),
        )
        self._export_worker.log_message.connect(self._on_log_message)
        self._export_worker.progress_update.connect(self._on_progress_update)
        self._export_worker.finished.connect(self._on_export_finished)
        self._export_worker.start()

    @Slot(str)
    def _on_log_message(self, message: str):
        """Append log message to the log area."""
        self.log_text.append(message)
        # Auto-scroll to bottom
        scrollbar = self.log_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    @Slot(int, int, int, str)
    def _on_progress_update(self, depth: int, services: int, edges: int, status: str):
        """Update progress statistics."""
        self.stats_label.setText(
            f"Depth: {depth}  |  Services: {services}  |  Edges: {edges}"
        )
        self.status_label.setText(status)
        self.status_label.setStyleSheet(
            f"padding: 8px 12px; background-color: {GarantiColors.WHITE}; "
            f"border: 1px solid {GarantiColors.MEDIUM_GREY}; border-radius: 4px; "
            f"color: {GarantiColors.PRIMARY_GREEN}; font-weight: bold;"
        )

    @Slot(object)
    def _on_export_finished(self, result: ExportResult):
        """Handle export completion."""
        self._set_ui_running(False)

        if result.success:
            self.status_label.setText(
                f"✓ Completed: {result.total_services} services, {result.total_edges} edges"
            )
            self.status_label.setStyleSheet(
                f"padding: 8px 12px; background-color: {GarantiColors.WHITE}; "
                f"border: 1px solid {GarantiColors.SUCCESS_GREEN}; border-radius: 4px; "
                f"color: {GarantiColors.SUCCESS_GREEN}; font-weight: bold;"
            )
            self.open_folder_btn.setEnabled(True)
            
            files_list = "\n".join(f"• {f}" for f in result.output_files)
            QMessageBox.information(
                self,
                "Export Complete",
                f"Topology discovery completed successfully!\n\n"
                f"Services Discovered: {result.total_services}\n"
                f"Edges (CALLS): {result.total_edges}\n"
                f"Max Depth: {result.traversal_depth}\n\n"
                f"Output Files:\n{files_list}"
            )
        else:
            self.status_label.setText(f"✗ Error: {result.message[:60]}...")
            self.status_label.setStyleSheet(
                f"padding: 8px 12px; background-color: {GarantiColors.WHITE}; "
                f"border: 1px solid {GarantiColors.ERROR_RED}; border-radius: 4px; "
                f"color: {GarantiColors.ERROR_RED}; font-weight: bold;"
            )
            
            QMessageBox.critical(
                self,
                "Export Failed",
                f"Topology discovery failed:\n\n{result.message}"
            )

    @Slot()
    def _cancel_export(self):
        """Cancel the running export."""
        if self._export_worker and self._export_worker.isRunning():
            self.log_text.append("⚠ Cancelling discovery...")
            self._export_worker.cancel()

    @Slot()
    def _open_output_folder(self):
        """Open the folder containing the output file."""
        output_path = self.output_path_input.text().strip()
        if not output_path:
            return

        folder = Path(output_path).parent
        if not folder.exists():
            return

        # Platform-aware folder opening
        system = platform.system()
        try:
            if system == "Windows":
                os.startfile(str(folder))
            elif system == "Darwin":  # macOS
                subprocess.run(["open", str(folder)], check=True)
            else:  # Linux
                subprocess.run(["xdg-open", str(folder)], check=True)
        except Exception as e:
            QMessageBox.warning(
                self,
                "Error",
                f"Could not open folder:\n{e}"
            )

    def _set_ui_running(self, running: bool, status: str = ""):
        """Enable/disable UI elements based on running state."""
        # Disable inputs while running
        self.base_url_input.setEnabled(not running)
        self.batch_size_spin.setEnabled(not running)
        self.ssl_checkbox.setEnabled(not running)
        self.root_ids_input.setEnabled(not running)
        self.output_path_input.setEnabled(not running)
        self.browse_btn.setEnabled(not running)
        self.excel_checkbox.setEnabled(not running)
        self.csv_checkbox.setEnabled(not running)
        self.graphml_checkbox.setEnabled(not running)

        # Toggle buttons
        self.test_conn_btn.setEnabled(not running)
        self.check_token_btn.setEnabled(not running)
        self.run_export_btn.setEnabled(not running)
        self.cancel_btn.setEnabled(running)
        
        if running:
            self.open_folder_btn.setEnabled(False)

        # Progress bar
        self.progress_bar.setVisible(running)

        # Status
        if status:
            self.status_label.setText(status)
            self.status_label.setStyleSheet(
                f"padding: 8px 12px; background-color: {GarantiColors.WHITE}; "
                f"border: 1px solid {GarantiColors.PRIMARY_GREEN}; border-radius: 4px; "
                f"color: {GarantiColors.PRIMARY_GREEN}; font-weight: bold;"
            )
        elif not running:
            self.status_label.setText("Ready")
            self.status_label.setStyleSheet(
                f"padding: 8px 12px; background-color: {GarantiColors.WHITE}; "
                f"border: 1px solid {GarantiColors.MEDIUM_GREY}; border-radius: 4px; "
                f"color: {GarantiColors.TEXT_SECONDARY};"
            )

    def closeEvent(self, event):
        """Handle window close - cancel any running workers."""
        if self._export_worker and self._export_worker.isRunning():
            self._export_worker.cancel()
            self._export_worker.wait(3000)
        if self._test_worker and self._test_worker.isRunning():
            self._test_worker.wait(3000)
        event.accept()


# =============================================================================
# Application Entry Point
# =============================================================================

def main():
    """Application entry point."""
    # Suppress SSL warnings when verification is disabled
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    app = QApplication(sys.argv)
    app.setApplicationName("Dynatrace Topology Discoverer")
    app.setOrganizationName("Garanti BBVA")
    
    # Use Fusion style for consistent cross-platform look
    app.setStyle("Fusion")

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
