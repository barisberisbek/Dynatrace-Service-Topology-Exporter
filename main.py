"""
Dynatrace Recursive Topology Discoverer - Desktop Application

A simple PySide6-based GUI application with Garanti BBVA corporate theme
for discovering service-to-service topology using BFS traversal.
"""

import os
import sys
import subprocess
import platform
from pathlib import Path
from typing import List, Optional

from PySide6.QtCore import Qt, QThread, Signal, Slot
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
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
)

from dynatrace_client import ClientConfig, DynatraceClient, DynatraceAPIError
from topology_exporter import TopologyExporter, ExportResult


# =============================================================================
# Worker Threads
# =============================================================================

class ExportWorker(QThread):
    """Background worker for topology discovery."""
    
    log_message = Signal(str)
    progress_update = Signal(int, int, int, str)
    finished = Signal(object)
    
    def __init__(self, config, root_ids, output_path, export_excel, export_csv, export_graphml):
        super().__init__()
        self.config = config
        self.root_ids = root_ids
        self.output_path = output_path
        self.export_excel = export_excel
        self.export_csv = export_csv
        self.export_graphml = export_graphml
        self._exporter = None
        self._client = None

    def run(self):
        try:
            self._client = DynatraceClient(self.config, log_callback=self._emit_log)
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
            self.finished.emit(ExportResult(success=False, message=f"Error: {e}"))
        finally:
            if self._client:
                self._client.close()

    def _emit_log(self, msg):
        self.log_message.emit(msg)

    def _emit_progress(self, p):
        self.progress_update.emit(p.current_depth, p.services_discovered, p.edges_found, p.status)

    def cancel(self):
        if self._exporter:
            self._exporter.cancel()


class TestWorker(QThread):
    """Background worker for connection test."""
    
    finished = Signal(bool, str)
    
    def __init__(self, config):
        super().__init__()
        self.config = config

    def run(self):
        try:
            client = DynatraceClient(self.config)
            response = client.test_connection()
            client.close()
            total = response.get("totalCount", len(response.get("entities", [])))
            self.finished.emit(True, f"Connection OK!\nServices available: {total}")
        except DynatraceAPIError as e:
            self.finished.emit(False, f"Failed: {e.message}")
        except Exception as e:
            self.finished.emit(False, f"Error: {str(e)}")


# =============================================================================
# Main Window
# =============================================================================

class MainWindow(QMainWindow):
    """Simple, compact main window with clear colors."""

    def __init__(self):
        super().__init__()
        self._export_worker = None
        self._test_worker = None
        self._setup_ui()

    def _setup_ui(self):
        self.setWindowTitle("Dynatrace Topology Discoverer")
        self.setMinimumSize(720, 620)
        self.resize(750, 650)
        self.setStyleSheet("QMainWindow { background-color: #FFFFFF; }")

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        # === Header ===
        header = QLabel("🔍 Dynatrace Service Topology Discoverer")
        header.setStyleSheet("""
            font-size: 20px; 
            font-weight: bold; 
            color: #006A4E;
            padding: 8px;
            background-color: #E8F5E9;
            border-radius: 6px;
            border-left: 5px solid #006A4E;
        """)
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)

        # === Connection Section ===
        conn_section = self._create_section("⚡ Connection Settings", "#1565C0", "#E3F2FD")
        conn_layout = QGridLayout()
        conn_layout.setSpacing(10)
        conn_layout.setContentsMargins(12, 12, 12, 12)

        # Base URL
        url_label = QLabel("Base URL:")
        url_label.setStyleSheet("font-weight: bold; color: #1565C0;")
        conn_layout.addWidget(url_label, 0, 0)
        
        self.base_url_input = QLineEdit()
        self.base_url_input.setPlaceholderText("https://activegate:9999/e/env-id/api/v2")
        self.base_url_input.setStyleSheet("""
            QLineEdit {
                padding: 8px 10px;
                border: 2px solid #90CAF9;
                border-radius: 5px;
                background-color: #FFFFFF;
                font-size: 13px;
            }
            QLineEdit:focus {
                border-color: #1565C0;
                background-color: #FAFAFA;
            }
        """)
        conn_layout.addWidget(self.base_url_input, 0, 1, 1, 3)

        # Batch Size
        batch_label = QLabel("Batch:")
        batch_label.setStyleSheet("font-weight: bold; color: #1565C0;")
        conn_layout.addWidget(batch_label, 1, 0)
        
        self.batch_size_spin = QSpinBox()
        self.batch_size_spin.setRange(10, 100)
        self.batch_size_spin.setValue(50)
        self.batch_size_spin.setFixedWidth(70)
        self.batch_size_spin.setStyleSheet("""
            QSpinBox {
                padding: 6px;
                border: 2px solid #90CAF9;
                border-radius: 5px;
                font-weight: bold;
            }
            QSpinBox:focus { border-color: #1565C0; }
        """)
        conn_layout.addWidget(self.batch_size_spin, 1, 1)

        self.ssl_checkbox = QCheckBox("🔒 Verify SSL")
        self.ssl_checkbox.setChecked(False)
        self.ssl_checkbox.setStyleSheet("font-weight: bold; color: #E65100;")
        conn_layout.addWidget(self.ssl_checkbox, 1, 2)

        self.check_token_btn = QPushButton("🔑 Check Token")
        self.check_token_btn.setStyleSheet("""
            QPushButton {
                padding: 6px 12px;
                background-color: #FFF3E0;
                border: 2px solid #FF9800;
                border-radius: 5px;
                color: #E65100;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #FFE0B2; }
        """)
        self.check_token_btn.clicked.connect(self._check_token)
        conn_layout.addWidget(self.check_token_btn, 1, 3)

        conn_section.setLayout(conn_layout)
        layout.addWidget(conn_section)

        # === Root Service IDs Section ===
        root_section = self._create_section("📋 Root Service IDs", "#7B1FA2", "#F3E5F5")
        root_layout = QVBoxLayout()
        root_layout.setSpacing(8)
        root_layout.setContentsMargins(12, 12, 12, 12)

        root_hint = QLabel("Enter Service IDs to start discovery (one per line):")
        root_hint.setStyleSheet("color: #7B1FA2; font-style: italic;")
        root_layout.addWidget(root_hint)

        self.root_ids_input = QTextEdit()
        self.root_ids_input.setPlaceholderText("SERVICE-1234567890ABCDEF\nSERVICE-FEDCBA0987654321")
        self.root_ids_input.setMaximumHeight(80)
        self.root_ids_input.setStyleSheet("""
            QTextEdit {
                padding: 8px;
                border: 2px solid #CE93D8;
                border-radius: 5px;
                background-color: #FFFFFF;
                font-family: Consolas, monospace;
                font-size: 12px;
            }
            QTextEdit:focus {
                border-color: #7B1FA2;
                background-color: #FAFAFA;
            }
        """)
        root_layout.addWidget(self.root_ids_input)

        root_section.setLayout(root_layout)
        layout.addWidget(root_section)

        # === Output Section ===
        output_section = self._create_section("💾 Export Settings", "#00695C", "#E0F2F1")
        output_layout = QGridLayout()
        output_layout.setSpacing(10)
        output_layout.setContentsMargins(12, 12, 12, 12)

        output_label = QLabel("Output:")
        output_label.setStyleSheet("font-weight: bold; color: #00695C;")
        output_layout.addWidget(output_label, 0, 0)

        self.output_path_input = QLineEdit()
        self.output_path_input.setPlaceholderText("Select output file location...")
        self.output_path_input.setStyleSheet("""
            QLineEdit {
                padding: 8px 10px;
                border: 2px solid #80CBC4;
                border-radius: 5px;
                background-color: #FFFFFF;
            }
            QLineEdit:focus { border-color: #00695C; }
        """)
        output_layout.addWidget(self.output_path_input, 0, 1)

        self.browse_btn = QPushButton("📁 Browse...")
        self.browse_btn.setStyleSheet("""
            QPushButton {
                padding: 6px 12px;
                background-color: #E0F2F1;
                border: 2px solid #00695C;
                border-radius: 5px;
                color: #00695C;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #B2DFDB; }
        """)
        self.browse_btn.clicked.connect(self._browse_output)
        output_layout.addWidget(self.browse_btn, 0, 2)

        # Export format checkboxes
        format_layout = QHBoxLayout()
        format_label = QLabel("Formats:")
        format_label.setStyleSheet("font-weight: bold; color: #00695C;")
        format_layout.addWidget(format_label)

        self.excel_cb = QCheckBox("📊 Excel")
        self.excel_cb.setChecked(True)
        self.excel_cb.setStyleSheet("font-weight: bold; color: #1B5E20;")
        format_layout.addWidget(self.excel_cb)

        self.csv_cb = QCheckBox("📄 CSV")
        self.csv_cb.setStyleSheet("font-weight: bold; color: #0D47A1;")
        format_layout.addWidget(self.csv_cb)

        self.graphml_cb = QCheckBox("🔗 GraphML")
        self.graphml_cb.setStyleSheet("font-weight: bold; color: #4A148C;")
        format_layout.addWidget(self.graphml_cb)

        format_layout.addStretch()
        output_layout.addLayout(format_layout, 1, 0, 1, 3)

        output_section.setLayout(output_layout)
        layout.addWidget(output_section)

        # === Control Buttons ===
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(12)

        self.test_btn = QPushButton("🔌 Test Connection")
        self.test_btn.setStyleSheet("""
            QPushButton {
                padding: 10px 16px;
                background-color: #E3F2FD;
                border: 2px solid #1976D2;
                border-radius: 6px;
                color: #1565C0;
                font-weight: bold;
                font-size: 13px;
            }
            QPushButton:hover { background-color: #BBDEFB; }
            QPushButton:disabled { background-color: #E0E0E0; color: #9E9E9E; border-color: #BDBDBD; }
        """)
        self.test_btn.clicked.connect(self._test_connection)
        btn_layout.addWidget(self.test_btn)

        self.run_btn = QPushButton("▶  DISCOVER TOPOLOGY")
        self.run_btn.setStyleSheet("""
            QPushButton {
                padding: 10px 24px;
                background-color: #006A4E;
                border: none;
                border-radius: 6px;
                color: white;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover { background-color: #004D40; }
            QPushButton:disabled { background-color: #A5D6A7; }
        """)
        self.run_btn.clicked.connect(self._run_export)
        btn_layout.addWidget(self.run_btn)

        self.cancel_btn = QPushButton("⏹ Cancel")
        self.cancel_btn.setEnabled(False)
        self.cancel_btn.setStyleSheet("""
            QPushButton {
                padding: 10px 16px;
                background-color: #FFEBEE;
                border: 2px solid #D32F2F;
                border-radius: 6px;
                color: #C62828;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #FFCDD2; }
            QPushButton:disabled { background-color: #E0E0E0; color: #9E9E9E; border-color: #BDBDBD; }
        """)
        self.cancel_btn.clicked.connect(self._cancel_export)
        btn_layout.addWidget(self.cancel_btn)

        self.open_folder_btn = QPushButton("📂 Open Folder")
        self.open_folder_btn.setEnabled(False)
        self.open_folder_btn.setStyleSheet("""
            QPushButton {
                padding: 10px 16px;
                background-color: #FFF8E1;
                border: 2px solid #FFA000;
                border-radius: 6px;
                color: #E65100;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #FFECB3; }
            QPushButton:disabled { background-color: #E0E0E0; color: #9E9E9E; border-color: #BDBDBD; }
        """)
        self.open_folder_btn.clicked.connect(self._open_output_folder)
        btn_layout.addWidget(self.open_folder_btn)

        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        # === Progress ===
        progress_layout = QHBoxLayout()
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setVisible(False)
        self.progress_bar.setFixedHeight(10)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: none;
                background-color: #E0E0E0;
                border-radius: 5px;
            }
            QProgressBar::chunk {
                background-color: #006A4E;
                border-radius: 5px;
            }
        """)
        progress_layout.addWidget(self.progress_bar)

        self.stats_label = QLabel("")
        self.stats_label.setStyleSheet("color: #006A4E; font-weight: bold; font-size: 13px;")
        progress_layout.addWidget(self.stats_label)
        layout.addLayout(progress_layout)

        # === Log Area ===
        log_label = QLabel("📜 Log Output:")
        log_label.setStyleSheet("font-weight: bold; color: #37474F;")
        layout.addWidget(log_label)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Consolas", 10))
        self.log_text.setStyleSheet("""
            QTextEdit {
                background-color: #263238;
                color: #ECEFF1;
                border: 2px solid #455A64;
                border-radius: 6px;
                padding: 8px;
            }
        """)
        layout.addWidget(self.log_text, 1)

        # === Status Bar ===
        self.status_label = QLabel("✓ Ready")
        self.status_label.setStyleSheet("""
            padding: 8px 12px;
            background-color: #E8F5E9;
            border: 2px solid #4CAF50;
            border-radius: 5px;
            color: #2E7D32;
            font-weight: bold;
        """)
        layout.addWidget(self.status_label)

    def _create_section(self, title: str, color: str, bg_color: str) -> QFrame:
        """Create a styled section frame with a title."""
        frame = QFrame()
        frame.setStyleSheet(f"""
            QFrame {{
                background-color: {bg_color};
                border: 2px solid {color};
                border-radius: 8px;
            }}
        """)
        return frame

    # === Slots ===

    @Slot()
    def _check_token(self):
        token = os.environ.get("DYNATRACE_API_TOKEN")
        if token:
            QMessageBox.information(self, "Token Status", f"✓ Token found!\n\nLength: {len(token)} characters")
        else:
            QMessageBox.warning(self, "Token Missing", "❌ Token not found!\n\nSet DYNATRACE_API_TOKEN environment variable.")

    @Slot()
    def _browse_output(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Save As", str(Path.home() / "topology.xlsx"),
            "Excel (*.xlsx);;CSV (*.csv);;GraphML (*.graphml);;All (*)"
        )
        if path:
            self.output_path_input.setText(path)

    def _get_config(self) -> Optional[ClientConfig]:
        token = os.environ.get("DYNATRACE_API_TOKEN")
        if not token:
            QMessageBox.critical(self, "Error", "❌ DYNATRACE_API_TOKEN not set!")
            return None

        base_url = self.base_url_input.text().strip()
        if not base_url:
            QMessageBox.critical(self, "Error", "❌ Base URL is required!")
            return None

        if not base_url.startswith(("http://", "https://")):
            QMessageBox.critical(self, "Error", "❌ Base URL must start with http:// or https://")
            return None

        return ClientConfig(
            base_url=base_url.rstrip("/"),
            api_token=token,
            verify_ssl=self.ssl_checkbox.isChecked(),
            batch_size=self.batch_size_spin.value(),
        )

    @Slot()
    def _test_connection(self):
        config = self._get_config()
        if not config:
            return

        self._set_running(True, "🔌 Testing connection...")
        self.log_text.append("🔌 Testing connection to Dynatrace API...")

        self._test_worker = TestWorker(config)
        self._test_worker.finished.connect(self._on_test_done)
        self._test_worker.start()

    @Slot(bool, str)
    def _on_test_done(self, success, msg):
        self._set_running(False)
        if success:
            self.log_text.append(f"✅ {msg}")
            QMessageBox.information(self, "Success", f"✅ {msg}")
        else:
            self.log_text.append(f"❌ {msg}")
            QMessageBox.critical(self, "Failed", f"❌ {msg}")

    @Slot()
    def _run_export(self):
        config = self._get_config()
        if not config:
            return

        root_ids = [x.strip() for x in self.root_ids_input.toPlainText().split("\n") if x.strip()]
        if not root_ids:
            QMessageBox.critical(self, "Error", "❌ Enter at least one Root Service ID!")
            return

        output_path = self.output_path_input.text().strip()
        if not output_path:
            QMessageBox.critical(self, "Error", "❌ Select output file!")
            return

        if not (self.excel_cb.isChecked() or self.csv_cb.isChecked() or self.graphml_cb.isChecked()):
            QMessageBox.critical(self, "Error", "❌ Select at least one export format!")
            return

        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        self._set_running(True, "🔍 Discovering topology...")
        self.log_text.clear()

        self._export_worker = ExportWorker(
            config, root_ids, output_path,
            self.excel_cb.isChecked(),
            self.csv_cb.isChecked(),
            self.graphml_cb.isChecked(),
        )
        self._export_worker.log_message.connect(self._on_log)
        self._export_worker.progress_update.connect(self._on_progress)
        self._export_worker.finished.connect(self._on_export_done)
        self._export_worker.start()

    @Slot(str)
    def _on_log(self, msg):
        self.log_text.append(msg)
        self.log_text.verticalScrollBar().setValue(self.log_text.verticalScrollBar().maximum())

    @Slot(int, int, int, str)
    def _on_progress(self, depth, services, edges, status):
        self.stats_label.setText(f"📊 Depth: {depth} | Services: {services} | Edges: {edges}")
        self.status_label.setText(f"🔄 {status}")
        self.status_label.setStyleSheet("""
            padding: 8px 12px;
            background-color: #E3F2FD;
            border: 2px solid #1976D2;
            border-radius: 5px;
            color: #1565C0;
            font-weight: bold;
        """)

    @Slot(object)
    def _on_export_done(self, result):
        self._set_running(False)
        if result.success:
            self.status_label.setText(f"✅ Done: {result.total_services} services, {result.total_edges} edges")
            self.status_label.setStyleSheet("""
                padding: 8px 12px;
                background-color: #E8F5E9;
                border: 2px solid #4CAF50;
                border-radius: 5px;
                color: #2E7D32;
                font-weight: bold;
            """)
            self.open_folder_btn.setEnabled(True)
            files = "\n".join(result.output_files)
            QMessageBox.information(self, "Success", f"✅ Export complete!\n\nFiles:\n{files}")
        else:
            self.status_label.setText(f"❌ Error: {result.message[:50]}")
            self.status_label.setStyleSheet("""
                padding: 8px 12px;
                background-color: #FFEBEE;
                border: 2px solid #D32F2F;
                border-radius: 5px;
                color: #C62828;
                font-weight: bold;
            """)
            QMessageBox.critical(self, "Error", f"❌ {result.message}")

    @Slot()
    def _cancel_export(self):
        if self._export_worker and self._export_worker.isRunning():
            self.log_text.append("⚠️ Cancelling operation...")
            self._export_worker.cancel()

    @Slot()
    def _open_output_folder(self):
        path = self.output_path_input.text().strip()
        if not path:
            return
        folder = Path(path).parent
        if not folder.exists():
            return
        try:
            if platform.system() == "Windows":
                os.startfile(str(folder))
            elif platform.system() == "Darwin":
                subprocess.run(["open", str(folder)])
            else:
                subprocess.run(["xdg-open", str(folder)])
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _set_running(self, running, status=""):
        self.base_url_input.setEnabled(not running)
        self.batch_size_spin.setEnabled(not running)
        self.ssl_checkbox.setEnabled(not running)
        self.root_ids_input.setEnabled(not running)
        self.output_path_input.setEnabled(not running)
        self.browse_btn.setEnabled(not running)
        self.excel_cb.setEnabled(not running)
        self.csv_cb.setEnabled(not running)
        self.graphml_cb.setEnabled(not running)
        self.test_btn.setEnabled(not running)
        self.check_token_btn.setEnabled(not running)
        self.run_btn.setEnabled(not running)
        self.cancel_btn.setEnabled(running)
        if running:
            self.open_folder_btn.setEnabled(False)
        self.progress_bar.setVisible(running)
        if status:
            self.status_label.setText(status)
            self.status_label.setStyleSheet("""
                padding: 8px 12px;
                background-color: #E3F2FD;
                border: 2px solid #1976D2;
                border-radius: 5px;
                color: #1565C0;
                font-weight: bold;
            """)
        elif not running:
            self.status_label.setText("✓ Ready")
            self.status_label.setStyleSheet("""
                padding: 8px 12px;
                background-color: #E8F5E9;
                border: 2px solid #4CAF50;
                border-radius: 5px;
                color: #2E7D32;
                font-weight: bold;
            """)

    def closeEvent(self, event):
        if self._export_worker and self._export_worker.isRunning():
            self._export_worker.cancel()
            self._export_worker.wait(2000)
        if self._test_worker and self._test_worker.isRunning():
            self._test_worker.wait(2000)
        event.accept()


# =============================================================================
# Entry Point
# =============================================================================

def main():
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
