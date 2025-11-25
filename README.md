"""
# SAP IBP XYZ Analysis API

FastAPI application for fetching SAP IBP data and performing XYZ segmentation analysis.

## Features

- Fetch product data from SAP IBP OData API
- Perform XYZ segmentation based on demand variability
- Export results in CSV, JSON, or Excel format
- Structured logging with JSON format
- Modular architecture with separated concerns

## Installation

1. Clone the repository
2. Create virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\\Scripts\\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create `.env` file from `.env.example`:
   ```bash
   cp .env.example .env
   ```

5. Update `.env` with your SAP credentials

## Running the Application

```bash
# Development
python -m app.main

# Or with uvicorn directly
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## API Endpoints

- `GET /` - Health check
- `GET /health` - Health check
- `GET /api/v1/xyz-analysis` - Perform XYZ analysis
- `GET /api/v1/xyz-analysis/export` - Export analysis results
- `GET /api/v1/xyz-analysis/summary` - Get segment summary

## Documentation

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Project Structure

```
sap_xyz_api/
├── app/
│   ├── __init__.py
│   ├── main.py                 # Application entry point
│   ├── config.py               # Configuration management
│   ├── models/
│   │   └── schemas.py          # Pydantic models
│   ├── services/
│   │   ├── sap_service.py      # SAP API integration
│   │   └── analysis_service.py # XYZ analysis logic
│   ├── api/
│   │   ├── dependencies.py     # Dependency injection
│   │   └── routes/
│   │       ├── health.py       # Health check routes
│   │       └── xyz_analysis.py # Analysis routes
│   └── utils/
│       └── logger.py           # Logging utilities
├── requirements.txt
├── .env
└── README.md
```

## XYZ Segmentation

- **X Segment**: Stable demand (CV ≤ 10%)
- **Y Segment**: Moderate variability (10% < CV ≤ 25%)
- **Z Segment**: High variability (CV > 25%)

# XYZ Segment Write-Back to SAP IBP

## Overview

The write-back functionality allows you to write XYZ segmentation results directly back to SAP IBP using the `/IBP/PLANNING_DATA_API_SRV` OData service.

## Prerequisites

### 1. SAP IBP Configuration

**Create Custom Key Figure:**
- Navigate to SAP IBP Configuration
- Create a new key figure named `XYZ_SEGMENT` (or your preferred name)
- Set it as a string/text field to store "X", "Y", or "Z"
- Assign to appropriate planning level (e.g., LOCPRODWEEKLY)

**Communication Arrangement:**
- Set up communication arrangement based on `SAP_COM_0720` scenario
- Grant appropriate authorization for data integration

**Global Configuration (Optional):**
- Set `ENABLE_NULL_INFO` parameter if you need NULL handling
- Set `MAX_RECORD_IN_SIM_TABLE` if writing to scenarios

### 2. Application Configuration

Update your `.env` file:

```bash
# Enable write operations
ENABLE_WRITE_OPERATIONS=true

# Write API configuration
SAP_WRITE_API_URL=https://your-tenant.sap.com/sap/opu/odata/sap/IBP_PLANNING_DATA_API_SRV
SAP_PLANNING_AREA=SAP1
SAP_XYZ_KEY_FIGURE=XYZ_SEGMENT
```

## Write Modes

### 1. Simple Mode (Recommended for ≤5,000 products)

Single POST request with auto-commit.

**Use when:**
- Total products ≤ 5,000
- Simple, straightforward write
- No need for batch tracking

**Example:**
```bash
curl -X POST "http://localhost:8000/api/v1/xyz-write/write-segments" \
  -H "Content-Type: application/json" \
  -d '{
    "x_threshold": 10.0,
    "y_threshold": 25.0,
    "write_mode": "simple",
    "version_id": "UPSIDE"
  }'
```

### 2. Batched Mode (For >5,000 products)

Multiple POST requests with explicit commit.

**Use when:**
- Total products > 5,000
- Need batch tracking
- Want to control batch size

**Features:**
- Configurable batch size (default: 5,000)
- Sequential batch processing
- Explicit commit after all batches

**Example:**
```bash
curl -X POST "http://localhost:8000/api/v1/xyz-write/write-segments" \
  -H "Content-Type: application/json" \
  -d '{
    "x_threshold": 10.0,
    "y_threshold": 25.0,
    "write_mode": "batched",
    "batch_size": 5000,
    "version_id": "UPSIDE"
  }'
```

### 3. Parallel Mode (For very large datasets)

Multiple POST requests sent in parallel.

**Use when:**
- Total products > 20,000
- Need maximum performance
- System can handle concurrent requests

**Features:**
- Parallel batch processing
- Configurable worker threads (default: 4)
- Best performance for high volumes

**Example:**
```bash
curl -X POST "http://localhost:8000/api/v1/xyz-write/write-segments" \
  -H "Content-Type: application/json" \
  -d '{
    "x_threshold": 10.0,
    "y_threshold": 25.0,
    "write_mode": "parallel",
    "batch_size": 5000,
    "max_workers": 4,
    "version_id": "UPSIDE"
  }'
```

## API Endpoints

### POST /api/v1/xyz-write/write-segments

Perform XYZ analysis and write results to SAP IBP.

**Request Body:**
```json
{
  "x_threshold": 10.0,
  "y_threshold": 25.0,
  "filters": "PRDID eq 'IBP-100' or PRDID eq 'IBP-110'",
  "write_mode": "simple",
  "version_id": "UPSIDE",
  "scenario_id": null,
  "location_id": "1720",
  "period_field": "PERIODID3_TSTAMP",
  "batch_size": 5000,
  "max_workers": 4
}
```

**Response:**
```json
{
  "status": "success",
  "transaction_id": "A1B2C3D4E5F6G7H8I9J0K1L2M3N4O5P6",
  "total_products": 150,
  "segments_written": {
    "X": 45,
    "Y": 60,
    "Z": 45
  },
  "analysis_params": {
    "x_threshold": 10.0,
    "y_threshold": 25.0
  },
  "write_mode": "simple",
  "version_id": "UPSIDE",
  "records_sent": 150,
  "message": "Data written and committed successfully",
  "timestamp": "2024-01-15T10:30:00"
}
```

### POST /api/v1/xyz-write/write-custom

Write pre-calculated or custom segment assignments.

**Request Body:**
```json
{
  "segments": [
    {
      "PRDID": "IBP-100",
      "XYZ_Segment": "X",
      "LOCID": "1720"
    },
    {
      "PRDID": "IBP-110",
      "XYZ_Segment": "Y"
    }
  ],
  "version_id": "UPSIDE",
  "write_mode": "simple"
}
```

### GET /api/v1/xyz-write/status/{transaction_id}

Check the status of a write transaction.

**Response:**
```json
{
  "transaction_id": "A1B2C3D4E5F6G7H8I9J0K1L2M3N4O5P6",
  "status": "completed",
  "export_result": {
    "Status": "Success",
    "RecordsProcessed": 150
  },
  "messages": [],
  "timestamp": "2024-01-15T10:35:00"
}
```

### GET /api/v1/xyz-write/validate-config

Validate write configuration.

**Response:**
```json
{
  "configured": true,
  "configuration": {
    "sap_write_api_url": true,
    "planning_area": true,
    "xyz_key_figure": true,
    "credentials_configured": true
  },
  "message": "All settings configured",
  "timestamp": "2024-01-15T10:30:00"
}
```

## Complete Workflow Example

### Step 1: Validate Configuration
```bash
curl http://localhost:8000/api/v1/xyz-write/validate-config
```

### Step 2: Perform Analysis & Write
```bash
curl -X POST "http://localhost:8000/api/v1/xyz-write/write-segments" \
  -H "Content-Type: application/json" \
  -d '{
    "x_threshold": 10.0,
    "y_threshold": 25.0,
    "write_mode": "simple",
    "version_id": "UPSIDE"
  }'
```

### Step 3: Check Status (Optional)
```bash
curl http://localhost:8000/api/v1/xyz-write/status/A1B2C3D4E5F6G7H8I9J0K1L2M3N4O5P6
```

## Version and Scenario Management

### Writing to Base Version
```json
{
  "version_id": null
}
```
Data is written to the base version.

### Writing to Specific Version
```json
{
  "version_id": "UPSIDE"
}
```
Only version-specific key figures are written.

### Writing to Scenario
```json
{
  "version_id": "UPSIDE",
  "scenario_id": "11111111CCCCCCCC22222222DDDDDDDD"
}
```
**Note:** Requires `MAX_RECORD_IN_SIM_TABLE` parameter to be set in IBP.

## Error Handling

### Common Errors

**403 Forbidden:**
```json
{
  "detail": "Write operations are disabled. Set ENABLE_WRITE_OPERATIONS=true"
}
```
**Solution:** Enable write operations in `.env`

**500 Internal Server Error:**
```json
{
  "detail": "SAP_PLANNING_AREA not configured"
}
```
**Solution:** Configure missing settings in `.env`

**Timeout Error:**
```json
{
  "detail": "SAP write request timeout"
}
```
**Solution:** 
- Increase `SAP_TIMEOUT` value
- Use batched or parallel mode for large datasets

### Getting Error Details

After a write operation, check for errors:
```bash
curl http://localhost:8000/api/v1/xyz-write/status/{transaction_id}
```

The `messages` field contains any errors from SAP IBP.

## Performance Guidelines

| Product Count | Recommended Mode | Batch Size | Workers |
|--------------|------------------|------------|---------|
| < 5,000      | Simple           | N/A        | N/A     |
| 5,000-20,000 | Batched          | 5,000      | N/A     |
| 20,000-50,000| Parallel         | 5,000      | 4       |
| > 50,000     | Parallel         | 5,000      | 6-8     |

## Security Considerations

1. **Enable only when needed:** Keep `ENABLE_WRITE_OPERATIONS=false` by default
2. **Use strong credentials:** Store SAP credentials securely
3. **Validate inputs:** API validates all segment values (X, Y, Z only)
4. **Audit logs:** All write operations are logged with transaction IDs
5. **Test first:** Use test versions/scenarios before production writes

## Testing

### Test Write Configuration
```bash
# Check if everything is configured
curl http://localhost:8000/api/v1/xyz-write/validate-config
```

### Test with Small Dataset
```bash
# Write with filters to limit products
curl -X POST "http://localhost:8000/api/v1/xyz-write/write-segments" \
  -H "Content-Type: application/json" \
  -d '{
    "filters": "PRDID eq '\''TEST-PRODUCT'\''",
    "write_mode": "simple",
    "version_id": "TEST_VERSION"
  }'
```

### Test Custom Write
```bash
# Write specific segments manually
curl -X POST "http://localhost:8000/api/v1/xyz-write/write-custom" \
  -H "Content-Type: application/json" \
  -d '{
    "segments": [
      {"PRDID": "TEST-001", "XYZ_Segment": "X"}
    ],
    "version_id": "TEST_VERSION"
  }'
```

## Troubleshooting

### Issue: Transaction not committed
**Symptom:** Data not visible in SAP IBP
**Solution:** 
- Check transaction status endpoint
- Verify `DoCommit` was set correctly
- For batched/parallel modes, ensure commit was called

### Issue: NULL values instead of segments
**Symptom:** Key figure shows NULL
**Solution:**
- Check `ENABLE_NULL_INFO` parameter in IBP
- Verify `_isNull` flags are set to `false`

### Issue: Performance degradation
**Symptom:** Slow writes, timeouts
**Solution:**
- Use batched or parallel mode
- Reduce batch size
- Increase timeout value
- Check SAP system load

### Issue: Version-specific key figure not updated
**Symptom:** Only base version updated
**Solution:**
- Ensure key figure is configured as version-specific in IBP
- Verify correct `version_id` is provided

## Best Practices

1. **Always validate configuration first** before production writes
2. **Start with small test datasets** to verify connectivity
3. **Use filters** to limit scope during testing
4. **Monitor transaction status** for large operations
5. **Keep transaction IDs** for audit trails
6. **Use test versions** before writing to production
7. **Enable write operations only when needed**
8. **Implement retry logic** for critical operations
9. **Set appropriate timeouts** based on data volume
10. **Log all write operations** for compliance

## Integration Examples

### Python Script
```python
import requests

API_BASE = "http://localhost:8000"

# Perform analysis and write
response = requests.post(
    f"{API_BASE}/api/v1/xyz-write/write-segments",
    json={
        "x_threshold": 10.0,
        "y_threshold": 25.0,
        "write_mode": "batched",
        "version_id": "UPSIDE"
    }
)

result = response.json()
transaction_id = result["transaction_id"]

# Check status
status = requests.get(
    f"{API_BASE}/api/v1/xyz-write/status/{transaction_id}"
)

print(status.json())
```

### Scheduled Job (Cron)
```bash
#!/bin/bash
# Daily XYZ segment update

curl -X POST "http://localhost:8000/api/v1/xyz-write/write-segments" \
  -H "Content-Type: application/json" \
  -d '{
    "write_mode": "batched",
    "version_id": "CONSENSUS"
  }' >> /var/log/xyz_write.log 2>&1
```

## Support

For issues or questions:
1. Check logs in JSON format for detailed error messages
2. Verify SAP IBP configuration and permissions
3. Test with small datasets first
4. Review SAP IBP documentation for PLANNING_DATA_API_SRV