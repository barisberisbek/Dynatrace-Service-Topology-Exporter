"""
Dynatrace Service Topology Exporter - Desktop Application

A modern PySide6-based GUI application for extracting service-to-service
topology from Dynatrace Managed environments.
"""

import os
import sys
import subprocess
import platform
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, QThread, Signal, Slot, QSize
from PySide6.QtGui import QFont, QIcon, QPalette, QColor
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
    QSpinBox,
    QComboBox,
    QCheckBox,
    QPushButton,
    QTextEdit,
    QProgressBar,
    QFileDialog,
    QMessageBox,
    QFrame,
    QSizePolicy,
)

from dynatrace_client import ClientConfig, DynatraceClient, DynatraceAPIError
from topology_exporter import TopologyExporter, ExportResult


# ============================================================================
# Worker Thread for Background Export
# ============================================================================

class ExportWorker(QThread):
    """Background worker thread for topology export operation."""
    
    log_message = Signal(str)
    progress_update = Signal(int, int, str)  # entities, edges, status
    finished = Signal(object)  # ExportResult
    
    def __init__(self, config: ClientConfig, output_path: str):
        super().__init__()
        self.config = config
        self.output_path = output_path
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
            
            result = self._exporter.run(self.output_path)
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
            progress.entities_fetched,
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
            
            # Check if we got any entities back
            entities = response.get("entities", [])
            total = response.get("totalCount", len(entities))
            
            self.finished.emit(
                True,
                f"Connection successful!\n\nTotal SERVICE entities available: {total}"
            )
        except DynatraceAPIError as e:
            self.finished.emit(False, f"Connection failed:\n\n{e.message}")
        except Exception as e:
            self.finished.emit(False, f"Unexpected error:\n\n{str(e)}")


# ============================================================================
# Main Application Window
# ============================================================================

class MainWindow(QMainWindow):
    """Main application window for Dynatrace Topology Exporter."""

    def __init__(self):
        super().__init__()
        self._export_worker: Optional[ExportWorker] = None
        self._test_worker: Optional[TestConnectionWorker] = None
        self._setup_ui()
        self._connect_signals()
        self._apply_styles()

    def _setup_ui(self):
        """Build the user interface."""
        self.setWindowTitle("Dynatrace Service Topology Exporter")
        self.setMinimumSize(700, 750)
        self.resize(750, 800)

        # Central widget and main layout
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setSpacing(12)
        main_layout.setContentsMargins(16, 16, 16, 16)

        # ===== Connection Settings Group =====
        conn_group = QGroupBox("Dynatrace Connection Settings")
        conn_layout = QFormLayout()
        conn_layout.setSpacing(8)

        self.base_url_input = QLineEdit()
        self.base_url_input.setPlaceholderText("https://my-activegate:9999/e/environment-id/api/v2")
        conn_layout.addRow("Base URL:", self.base_url_input)

        # Timeframe selector
        timeframe_widget = QWidget()
        timeframe_layout = QHBoxLayout(timeframe_widget)
        timeframe_layout.setContentsMargins(0, 0, 0, 0)
        timeframe_layout.setSpacing(8)

        self.timeframe_combo = QComboBox()
        self.timeframe_combo.addItems(["Default (Dynatrace default)", "Custom"])
        self.timeframe_combo.setFixedWidth(200)
        timeframe_layout.addWidget(self.timeframe_combo)
        timeframe_layout.addStretch()
        conn_layout.addRow("Timeframe:", timeframe_widget)

        # Custom timeframe inputs (initially hidden)
        self.custom_time_widget = QWidget()
        custom_time_layout = QHBoxLayout(self.custom_time_widget)
        custom_time_layout.setContentsMargins(0, 0, 0, 0)
        custom_time_layout.setSpacing(8)

        self.from_input = QLineEdit()
        self.from_input.setPlaceholderText("e.g., now-7d")
        self.from_input.setFixedWidth(150)
        custom_time_layout.addWidget(QLabel("From:"))
        custom_time_layout.addWidget(self.from_input)

        self.to_input = QLineEdit()
        self.to_input.setPlaceholderText("e.g., now")
        self.to_input.setFixedWidth(150)
        custom_time_layout.addWidget(QLabel("To:"))
        custom_time_layout.addWidget(self.to_input)
        custom_time_layout.addStretch()

        self.custom_time_widget.setVisible(False)
        conn_layout.addRow("", self.custom_time_widget)

        # Page size
        page_size_widget = QWidget()
        page_size_layout = QHBoxLayout(page_size_widget)
        page_size_layout.setContentsMargins(0, 0, 0, 0)
        page_size_layout.setSpacing(8)

        self.page_size_spin = QSpinBox()
        self.page_size_spin.setRange(1, 500)
        self.page_size_spin.setValue(500)
        self.page_size_spin.setFixedWidth(100)
        page_size_layout.addWidget(self.page_size_spin)
        page_size_layout.addWidget(QLabel("(max 500)"))
        page_size_layout.addStretch()
        conn_layout.addRow("Page Size:", page_size_widget)

        # SSL verification
        ssl_widget = QWidget()
        ssl_layout = QHBoxLayout(ssl_widget)
        ssl_layout.setContentsMargins(0, 0, 0, 0)
        ssl_layout.setSpacing(8)

        self.ssl_checkbox = QCheckBox("Verify SSL certificates")
        self.ssl_checkbox.setChecked(True)
        ssl_layout.addWidget(self.ssl_checkbox)

        self.ssl_warning = QLabel("⚠ SSL verification disabled - use only in trusted networks")
        self.ssl_warning.setStyleSheet("color: #e67e22; font-weight: bold;")
        self.ssl_warning.setVisible(False)
        ssl_layout.addWidget(self.ssl_warning)
        ssl_layout.addStretch()
        conn_layout.addRow("", ssl_widget)

        conn_group.setLayout(conn_layout)
        main_layout.addWidget(conn_group)

        # ===== Authentication Group =====
        auth_group = QGroupBox("Authentication")
        auth_layout = QVBoxLayout()
        auth_layout.setSpacing(8)

        auth_info = QLabel(
            "API token is read from environment variable: DYNATRACE_API_TOKEN\n"
            "The token requires 'entities.read' scope."
        )
        auth_info.setWordWrap(True)
        auth_layout.addWidget(auth_info)

        self.check_token_btn = QPushButton("Check Token")
        self.check_token_btn.setFixedWidth(120)
        auth_layout.addWidget(self.check_token_btn)

        auth_group.setLayout(auth_layout)
        main_layout.addWidget(auth_group)

        # ===== Output Settings Group =====
        output_group = QGroupBox("Output Settings")
        output_layout = QHBoxLayout()
        output_layout.setSpacing(8)

        self.output_path_input = QLineEdit()
        self.output_path_input.setPlaceholderText("Select output CSV file...")
        output_layout.addWidget(self.output_path_input)

        self.browse_btn = QPushButton("Browse...")
        self.browse_btn.setFixedWidth(100)
        output_layout.addWidget(self.browse_btn)

        output_group.setLayout(output_layout)
        main_layout.addWidget(output_group)

        # ===== Control Buttons =====
        control_widget = QWidget()
        control_layout = QHBoxLayout(control_widget)
        control_layout.setContentsMargins(0, 0, 0, 0)
        control_layout.setSpacing(12)

        self.test_conn_btn = QPushButton("Test Connection")
        self.test_conn_btn.setFixedHeight(36)
        control_layout.addWidget(self.test_conn_btn)

        self.run_export_btn = QPushButton("▶  Run Topology Export")
        self.run_export_btn.setFixedHeight(36)
        self.run_export_btn.setStyleSheet("""
            QPushButton {
                background-color: #27ae60;
                color: white;
                font-weight: bold;
                border: none;
                border-radius: 4px;
                padding: 0 20px;
            }
            QPushButton:hover {
                background-color: #2ecc71;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        control_layout.addWidget(self.run_export_btn)

        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setFixedHeight(36)
        self.cancel_btn.setEnabled(False)
        control_layout.addWidget(self.cancel_btn)

        self.open_folder_btn = QPushButton("Open Output Folder")
        self.open_folder_btn.setFixedHeight(36)
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
        self.progress_bar.setFixedHeight(20)
        progress_layout.addWidget(self.progress_bar)

        self.stats_label = QLabel("")
        self.stats_label.setMinimumWidth(200)
        progress_layout.addWidget(self.stats_label)

        main_layout.addWidget(progress_widget)

        # ===== Log Area =====
        log_group = QGroupBox("Logs")
        log_layout = QVBoxLayout()
        log_layout.setSpacing(4)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Consolas", 9))
        self.log_text.setMinimumHeight(200)
        log_layout.addWidget(self.log_text)

        clear_btn_layout = QHBoxLayout()
        self.clear_log_btn = QPushButton("Clear Logs")
        self.clear_log_btn.setFixedWidth(100)
        clear_btn_layout.addWidget(self.clear_log_btn)
        clear_btn_layout.addStretch()
        log_layout.addLayout(clear_btn_layout)

        log_group.setLayout(log_layout)
        main_layout.addWidget(log_group)

        # ===== Status Bar =====
        self.status_label = QLabel("Ready")
        self.status_label.setStyleSheet("padding: 4px; color: #7f8c8d;")
        main_layout.addWidget(self.status_label)

    def _connect_signals(self):
        """Connect UI signals to slots."""
        self.timeframe_combo.currentIndexChanged.connect(self._on_timeframe_changed)
        self.ssl_checkbox.stateChanged.connect(self._on_ssl_changed)
        self.check_token_btn.clicked.connect(self._check_token)
        self.browse_btn.clicked.connect(self._browse_output)
        self.test_conn_btn.clicked.connect(self._test_connection)
        self.run_export_btn.clicked.connect(self._run_export)
        self.cancel_btn.clicked.connect(self._cancel_export)
        self.open_folder_btn.clicked.connect(self._open_output_folder)
        self.clear_log_btn.clicked.connect(self.log_text.clear)

    def _apply_styles(self):
        """Apply custom styling to the application."""
        self.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                border: 1px solid #bdc3c7;
                border-radius: 6px;
                margin-top: 12px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 12px;
                padding: 0 6px;
            }
            QLineEdit, QSpinBox, QComboBox {
                padding: 6px;
                border: 1px solid #bdc3c7;
                border-radius: 4px;
            }
            QLineEdit:focus, QSpinBox:focus, QComboBox:focus {
                border-color: #3498db;
            }
            QPushButton {
                padding: 6px 16px;
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: #ecf0f1;
            }
            QPushButton:hover {
                background-color: #d5dbdb;
            }
            QPushButton:disabled {
                color: #95a5a6;
            }
            QTextEdit {
                border: 1px solid #bdc3c7;
                border-radius: 4px;
            }
        """)

    # =========================================================================
    # Slot Methods
    # =========================================================================

    @Slot()
    def _on_timeframe_changed(self):
        """Toggle custom timeframe inputs visibility."""
        is_custom = self.timeframe_combo.currentIndex() == 1
        self.custom_time_widget.setVisible(is_custom)

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
                "✓ API token detected (not displayed for security).\n\n"
                f"Token length: {len(token)} characters"
            )
        else:
            QMessageBox.warning(
                self,
                "Token Not Found",
                "API token not found!\n\n"
                "Please set the environment variable:\n"
                "DYNATRACE_API_TOKEN=your_token_here\n\n"
                "Windows (PowerShell):\n"
                '$env:DYNATRACE_API_TOKEN = "your_token"\n\n'
                "Linux/macOS:\n"
                'export DYNATRACE_API_TOKEN="your_token"'
            )

    @Slot()
    def _browse_output(self):
        """Open file dialog to select output CSV path."""
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Topology CSV",
            str(Path.home() / "service_topology.csv"),
            "CSV Files (*.csv);;All Files (*)"
        )
        if path:
            self.output_path_input.setText(path)

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

        # Get timeframe
        from_time = None
        to_time = None
        if self.timeframe_combo.currentIndex() == 1:  # Custom
            from_time = self.from_input.text().strip() or None
            to_time = self.to_input.text().strip() or None

        return ClientConfig(
            base_url=base_url.rstrip("/"),
            api_token=token,
            verify_ssl=self.ssl_checkbox.isChecked(),
            page_size=self.page_size_spin.value(),
            from_time=from_time,
            to_time=to_time,
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
            self.log_text.append(f"✗ Connection test failed: {message}")
            QMessageBox.critical(self, "Connection Test Failed", message)

    @Slot()
    def _run_export(self):
        """Start the topology export process."""
        config = self._get_config()
        if not config:
            return

        # Validate output path
        output_path = self.output_path_input.text().strip()
        if not output_path:
            QMessageBox.critical(self, "Validation Error", "Output file path is required.")
            self.browse_btn.click()
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
        self._set_ui_running(True, "Exporting topology...")
        self.log_text.append("=" * 50)
        self.log_text.append("🚀 Starting Topology Export")
        self.log_text.append("=" * 50)
        self.log_text.append(f"   Base URL: {config.base_url}")
        self.log_text.append(f"   Page Size: {config.page_size}")
        self.log_text.append(f"   SSL Verify: {config.verify_ssl}")
        self.log_text.append(f"   Output: {output_path}")
        if config.from_time:
            self.log_text.append(f"   From: {config.from_time}")
        if config.to_time:
            self.log_text.append(f"   To: {config.to_time}")
        self.log_text.append("")

        self._export_worker = ExportWorker(config, output_path)
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

    @Slot(int, int, str)
    def _on_progress_update(self, entities: int, edges: int, status: str):
        """Update progress statistics."""
        self.stats_label.setText(f"Services: {entities}  |  Edges: {edges}")
        self.status_label.setText(status)

    @Slot(object)
    def _on_export_finished(self, result: ExportResult):
        """Handle export completion."""
        self._set_ui_running(False)

        if result.success:
            self.status_label.setText(f"✓ Completed: {result.total_edges} edges exported")
            self.status_label.setStyleSheet("padding: 4px; color: #27ae60; font-weight: bold;")
            self.open_folder_btn.setEnabled(True)
            
            QMessageBox.information(
                self,
                "Export Complete",
                f"Topology export completed successfully!\n\n"
                f"Services: {result.total_services}\n"
                f"Edges: {result.total_edges}\n"
                f"Output: {result.output_file}"
            )
        else:
            self.status_label.setText(f"✗ Error: {result.message[:50]}...")
            self.status_label.setStyleSheet("padding: 4px; color: #e74c3c; font-weight: bold;")
            
            QMessageBox.critical(
                self,
                "Export Failed",
                f"Topology export failed:\n\n{result.message}"
            )

    @Slot()
    def _cancel_export(self):
        """Cancel the running export."""
        if self._export_worker and self._export_worker.isRunning():
            self.log_text.append("⚠ Cancelling export...")
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
        self.timeframe_combo.setEnabled(not running)
        self.from_input.setEnabled(not running)
        self.to_input.setEnabled(not running)
        self.page_size_spin.setEnabled(not running)
        self.ssl_checkbox.setEnabled(not running)
        self.output_path_input.setEnabled(not running)
        self.browse_btn.setEnabled(not running)

        # Toggle buttons
        self.test_conn_btn.setEnabled(not running)
        self.run_export_btn.setEnabled(not running)
        self.cancel_btn.setEnabled(running)
        
        if running:
            self.open_folder_btn.setEnabled(False)

        # Progress bar
        self.progress_bar.setVisible(running)

        # Status
        if status:
            self.status_label.setText(status)
            self.status_label.setStyleSheet("padding: 4px; color: #3498db; font-weight: bold;")
        elif not running:
            self.status_label.setText("Ready")
            self.status_label.setStyleSheet("padding: 4px; color: #7f8c8d;")

    def closeEvent(self, event):
        """Handle window close - cancel any running workers."""
        if self._export_worker and self._export_worker.isRunning():
            self._export_worker.cancel()
            self._export_worker.wait(3000)
        if self._test_worker and self._test_worker.isRunning():
            self._test_worker.wait(3000)
        event.accept()


# ============================================================================
# Application Entry Point
# ============================================================================

def main():
    """Application entry point."""
    # Suppress SSL warnings when verification is disabled
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    app = QApplication(sys.argv)
    app.setApplicationName("Dynatrace Topology Exporter")
    app.setOrganizationName("Dynatrace Tools")
    
    # Set application style
    app.setStyle("Fusion")

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()

