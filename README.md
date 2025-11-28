# Dynatrace Service Topology Exporter

A production-grade desktop application for extracting service-to-service topology from **Dynatrace Managed (On-Premise)** environments and exporting it as a CSV edge list.

Built with Python 3 and PySide6, designed for enterprise/banking environments where security and reliability are paramount.

---

## Summary

This tool connects to the Dynatrace Monitored Entities API v2 to:

- **Fetch all SERVICE entities** using cursor-based pagination
- **Extract service-to-service dependencies** (both incoming and outgoing calls)
- **Build an ID-to-name mapping** for human-readable output
- **Export a CSV edge list** suitable for topology visualization or CMDB synchronization

### Key Features

| Feature | Description |
|---------|-------------|
| **Modern GUI** | Clean, responsive PySide6 interface with real-time logging |
| **Secure Token Handling** | API token read from environment variable, never displayed or logged |
| **Robust Error Handling** | Exponential backoff retry for rate limiting (429) and server errors (5xx) |
| **SSL Flexibility** | Toggle SSL verification for self-signed certificates in on-prem environments |
| **Background Processing** | Export runs in a background thread, keeping the UI responsive |
| **CLI Alternative** | Original command-line script included for automation scenarios |

---

## Requirements

- Python 3.8+
- Dynatrace API token with `entities.read` scope
- Network access to Dynatrace ActiveGate/Cluster

---

## Installation

```bash
# Clone or download the repository
cd DynatracePythonOtomasyon

# Install dependencies
pip install -r requirements.txt
```

---

## Example Usage

### GUI Application

```powershell
# 1. Set API token (Windows PowerShell)
$env:DYNATRACE_API_TOKEN = "dt0c01.XXXXXX.YYYYYY"

# 2. Launch the application
python main.py
```

```bash
# 1. Set API token (Linux/macOS)
export DYNATRACE_API_TOKEN="dt0c01.XXXXXX.YYYYYY"

# 2. Launch the application
python main.py
```

### CLI Script (Alternative)

```powershell
# Windows PowerShell
$env:DYNATRACE_API_TOKEN = "dt0c01.XXXXXX.YYYYYY"

python dynatrace_service_topology.py `
    --base-url https://my-activegate.example.com:9999/e/abc12345/api/v2 `
    --output service_topology.csv
```

```bash
# Linux/macOS
export DYNATRACE_API_TOKEN="dt0c01.XXXXXX.YYYYYY"

python dynatrace_service_topology.py \
    --base-url https://my-activegate.example.com:9999/e/abc12345/api/v2 \
    --output service_topology.csv
```

### CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--base-url` | Dynatrace API base URL (required) | - |
| `--output` | Output CSV file path (required) | - |
| `--page-size` | Entities per API request | 500 |
| `--from` | Start of timeframe (e.g., `now-7d`) | API default |
| `--to` | End of timeframe (e.g., `now`) | API default |
| `--no-verify-ssl` | Disable SSL certificate verification | Enabled |
| `--verbose` | Enable debug logging | Off |

---

## GUI Screenshot Description

The application features a single-window interface with the following sections:

```
┌─────────────────────────────────────────────────────────────────┐
│  Dynatrace Service Topology Exporter                            │
├─────────────────────────────────────────────────────────────────┤
│  ┌─ Dynatrace Connection Settings ────────────────────────────┐ │
│  │  Base URL:    [https://activegate:9999/e/env-id/api/v2   ] │ │
│  │  Timeframe:   [Default (Dynatrace default)    ▼]           │ │
│  │  Page Size:   [500    ] (max 500)                          │ │
│  │  ☑ Verify SSL certificates                                 │ │
│  └────────────────────────────────────────────────────────────┘ │
│  ┌─ Authentication ───────────────────────────────────────────┐ │
│  │  API token is read from environment variable:              │ │
│  │  DYNATRACE_API_TOKEN                                       │ │
│  │  [Check Token]                                             │ │
│  └────────────────────────────────────────────────────────────┘ │
│  ┌─ Output Settings ──────────────────────────────────────────┐ │
│  │  [C:\output\service_topology.csv              ] [Browse...] │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                 │
│  [Test Connection] [▶ Run Topology Export] [Cancel] [Open Folder]│
│                                                                 │
│  ████████████████░░░░░░░░░  Services: 1,234  |  Edges: 5,678   │
│                                                                 │
│  ┌─ Logs ─────────────────────────────────────────────────────┐ │
│  │ 📡 Starting to fetch SERVICE entities...                   │ │
│  │    Fetching page 1 (initial request)...                    │ │
│  │    Page 1: 500 services (total: 500)                       │ │
│  │    Fetching page 2 (continuation)...                       │ │
│  │    Page 2: 500 services (total: 1000)                      │ │
│  │ ✓ Pagination complete. Pages: 3, Total services: 1234     │ │
│  │ 📋 Built ID-to-name mapping: 1234 services                │ │
│  │ 🔗 Extracted 5678 unique edges                            │ │
│  │ 💾 Writing CSV to: C:\output\service_topology.csv         │ │
│  │ ✅ EXPORT COMPLETED SUCCESSFULLY                          │ │
│  └────────────────────────────────────────────────────────────┘ │
│  [Clear Logs]                                                   │
│                                                                 │
│  ✓ Completed: 5,678 edges exported                             │
└─────────────────────────────────────────────────────────────────┘
```

### Section Details

| Section | Purpose |
|---------|---------|
| **Connection Settings** | Configure Dynatrace API endpoint, timeframe, page size, and SSL options |
| **Authentication** | Displays token source info; "Check Token" validates without revealing the secret |
| **Output Settings** | Select destination CSV file with a file browser dialog |
| **Control Buttons** | Test connection, start/cancel export, open output folder |
| **Progress Area** | Indeterminate progress bar with real-time entity/edge counters |
| **Logs** | Scrolling log with timestamped messages and status icons |
| **Status Bar** | Current state: Ready, Running, Completed, or Error details |

---

## Output Format

The exported CSV contains a directed edge list with the following columns:

| Column | Description | Example |
|--------|-------------|---------|
| `source_id` | Dynatrace entity ID of the calling service | `SERVICE-1234567890ABCDEF` |
| `source_name` | Display name of the calling service | `PaymentService` |
| `target_id` | Dynatrace entity ID of the called service | `SERVICE-FEDCBA0987654321` |
| `target_name` | Display name of the called service | `DatabaseService` |
| `relationship` | Direction indicator | `CALLS` or `CALLED_BY` |

### Example Output

```csv
source_id,source_name,target_id,target_name,relationship
SERVICE-ABC123,OrderService,SERVICE-DEF456,PaymentService,CALLS
SERVICE-DEF456,PaymentService,SERVICE-GHI789,FraudCheckService,CALLS
SERVICE-GHI789,FraudCheckService,SERVICE-DEF456,PaymentService,CALLED_BY
```

---

## Notes for Banking/Production Environments

### Security Considerations

| Topic | Recommendation |
|-------|----------------|
| **API Token Scope** | Use a dedicated service account token with **minimum required scope** (`entities.read` only). Never use personal or admin tokens. |
| **Token Rotation** | Rotate tokens per your organization's security policy (e.g., every 90 days). |
| **Token Storage** | Store tokens in a secrets manager or password vault. Set the environment variable only for the session duration. |
| **SSL Verification** | Keep SSL verification **enabled** in production. If using self-signed certificates, add the internal CA to your system trust store instead of disabling verification. |
| **Output Handling** | The CSV may contain sensitive service names. Handle according to your data classification policies. |

### Performance & Impact

| Topic | Recommendation |
|-------|----------------|
| **Timing** | Run during **off-peak hours** to minimize load on the Dynatrace cluster (e.g., early morning or weekends). |
| **Timeframe** | Use a limited timeframe (e.g., `--from now-24h`) to reduce data volume and API load. |
| **Large Environments** | For environments with 10,000+ services, expect the export to take 5-15 minutes. The UI remains responsive. |
| **Rate Limiting** | The tool handles HTTP 429 automatically with exponential backoff. If you see frequent rate limit hits, reduce page size to 200. |
| **Network** | Ensure stable network connectivity to the ActiveGate. Transient errors are retried automatically. |

### Compliance & Audit

| Topic | Recommendation |
|-------|----------------|
| **Logging** | Copy logs from the GUI for audit trails. Consider redirecting CLI output to a file. |
| **Change Management** | Document the tool in your CMDB and follow change management procedures for first-time use. |
| **Data Retention** | Apply your organization's data retention policies to exported CSV files. |

---

## Caveats for Banking/Production Environments

1. **Environment Variable Persistence**
   - The `DYNATRACE_API_TOKEN` environment variable is session-scoped by default.
   - Do **not** persist it in system-wide environment variables or shell profiles.
   - For scheduled automation, use a secrets manager to inject the token at runtime.

2. **Self-Signed Certificates**
   - Many on-prem Dynatrace deployments use internal PKI or self-signed certificates.
   - The "Disable SSL verification" option is provided for testing only.
   - For production: add your internal CA certificate to the system trust store or Python's `certifi` bundle.

3. **Proxy Configuration**
   - If your environment requires a proxy, set the standard environment variables:
     ```powershell
     $env:HTTPS_PROXY = "http://proxy.example.com:8080"
     ```
   - The `requests` library will honor these settings automatically.

4. **Firewall Rules**
   - Ensure outbound access to the ActiveGate on the configured port (typically 9999 or 443).
   - The tool only makes outbound HTTPS GET requests to `/api/v2/entities`.

5. **Entity Staleness**
   - By default, Dynatrace returns entities seen in the last 72 hours.
   - Use the timeframe options to adjust this window based on your topology freshness requirements.

---

## Packaging as Standalone Executable

For distribution to users without Python installed:

```bash
# Install PyInstaller
pip install pyinstaller

# Create standalone executable
pyinstaller --onefile --windowed --name "DynatraceTopologyExporter" main.py

# Output will be in dist/DynatraceTopologyExporter.exe
```

**Note:** The executable still requires the `DYNATRACE_API_TOKEN` environment variable to be set.

---

## Project Structure

```
DynatracePythonOtomasyon/
├── main.py                         # PySide6 GUI application
├── dynatrace_client.py             # HTTP client with retry logic
├── topology_exporter.py            # Business logic for extraction
├── dynatrace_service_topology.py   # CLI script (alternative)
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```

---

## License

Internal use only. Contact your Dynatrace administrator for API access and token provisioning.

---

## Support

For issues related to:
- **Dynatrace API**: Consult the [Dynatrace API Documentation](https://www.dynatrace.com/support/help/dynatrace-api)
- **This Tool**: Open an issue in the repository or contact the maintainer

