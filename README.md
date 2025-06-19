# 🏪 Retail Plan Visualizer Backend

A FastAPI backend service for retail forecasting data visualization with Google BigQuery integration.

## 📋 Overview

This backend service provides RESTful APIs to fetch and analyze retail forecasting data stored in Google BigQuery. It supports filtering, pagination, and statistical analysis of forecast data across multiple dimensions including sites, brands, merchandise hierarchies, and time periods.

## 🏗️ Architecture

- **FastAPI**: Modern, fast web framework for building APIs
- **Google BigQuery**: Data warehouse for large-scale analytics
- **Pydantic**: Data validation and parsing using Python type annotations
- **Uvicorn**: ASGI server implementation

## 📊 Data Schema

The application works with forecast data containing the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `forecast_datetime` | datetime | Timestamp when forecast was generated |
| `forecast_run_id` | string | Unique identifier for the forecast run |
| `site_id` | integer | Site identifier |
| `brand` | integer | Brand identifier |
| `mh_segment` | string | Merchandise hierarchy segment |
| `mh_family` | string | Merchandise hierarchy family |
| `mh_class` | string | Merchandise hierarchy class |
| `mh_brick` | string | Merchandise hierarchy brick |
| `product_id` | string | Product identifier |
| `forecast_week` | date | Week for which forecast is made |
| `actual_qty` | float | Actual quantity sold |
| `predicted_qty` | float | Predicted quantity |
| `model_used` | string | ML model used for prediction |
| `qty_group` | integer | Quantity group classification |
| `forecast_week_number` | integer | Week number in forecast |
| `training_data_max_date` | date | Maximum date in training data |
| `forecast_horizon` | integer | Forecast horizon in weeks |

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Google Cloud Project with BigQuery API enabled
- Service Account with BigQuery permissions (optional, can use default credentials)

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd retail-plan-visualizer-backend
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Setup authentication and environment variables**:
   
   **Option A: Using Application Default Credentials (Recommended)**
   ```bash
   # Authenticate with Google Cloud
   gcloud auth application-default login
   
   # Setup environment variables
   cp env.example .env
   ```
   
   Edit `.env` file with your configuration:
   ```env
   GOOGLE_CLOUD_PROJECT_ID=your-project-id
   # GOOGLE_APPLICATION_CREDENTIALS=  # Leave empty for ADC
   BIGQUERY_DATASET_ID=your-dataset-id
   BIGQUERY_TABLE_ID=your-table-id
   ```
   
   **Option B: Using Service Account Key**
   ```bash
   cp env.example .env
   ```
   
   Edit `.env` file with your service account configuration:
   ```env
   GOOGLE_CLOUD_PROJECT_ID=your-project-id
   GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-key.json
   BIGQUERY_DATASET_ID=your-dataset-id
   BIGQUERY_TABLE_ID=your-table-id
   ```

5. **Run the application**:
   ```bash
   python run.py
   ```

The API will be available at `http://localhost:8000`

## 🔧 Configuration

### Environment Variables

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `GOOGLE_CLOUD_PROJECT_ID` | Yes | Your Google Cloud Project ID | - |
| `BIGQUERY_DATASET_ID` | Yes | BigQuery dataset containing forecast data | - |
| `BIGQUERY_TABLE_ID` | Yes | BigQuery table with forecast data | - |
| `GOOGLE_APPLICATION_CREDENTIALS` | No | Path to service account JSON file (leave empty for ADC) | None (uses Application Default Credentials) |
| `APP_NAME` | No | Application name | "Retail Plan Visualizer Backend" |
| `DEBUG` | No | Enable debug mode | True |
| `HOST` | No | Server host | "0.0.0.0" |
| `PORT` | No | Server port | 8000 |

### Google Cloud Authentication

You can authenticate with Google Cloud in several ways:

1. **Application Default Credentials** (Recommended for development):
   - Run `gcloud auth application-default login` in your terminal
   - Leave `GOOGLE_APPLICATION_CREDENTIALS` empty or unset in your `.env` file
   - This is the easiest method for local development

2. **Service Account Key** (Alternative for development):
   - Download service account JSON key from Google Cloud Console
   - Set `GOOGLE_APPLICATION_CREDENTIALS` to the file path in your `.env` file

3. **Default Credentials** (Recommended for production):
   - Use when running on Google Cloud (Cloud Run, Compute Engine, etc.)
   - Leave `GOOGLE_APPLICATION_CREDENTIALS` empty
   - Google Cloud services automatically use their default credentials

**For most users**: Simply run `gcloud auth application-default login` and leave the credentials path empty in your `.env` file.

## 🔗 API Endpoints

### Base URL: `/api/v1`

### Forecast Data

#### `GET /forecast/`
Get forecast data with optional filtering and pagination.

**Query Parameters:**
- `site_id` (integer): Filter by site ID
- `brand` (integer): Filter by brand ID
- `mh_segment` (string): Filter by merchandise segment
- `mh_family` (string): Filter by merchandise family
- `mh_class` (string): Filter by merchandise class
- `mh_brick` (string): Filter by merchandise brick
- `product_id` (string): Filter by product ID
- `forecast_run_id` (string): Filter by forecast run ID
- `model_used` (string): Filter by model used
- `start_date` (date): Start date for forecast week filter (YYYY-MM-DD)
- `end_date` (date): End date for forecast week filter (YYYY-MM-DD)
- `limit` (integer): Maximum number of records (1-1000, default: 100)
- `offset` (integer): Number of records to skip (default: 0)

**Example:**
```bash
curl "http://localhost:8000/api/v1/forecast/?site_id=8013&limit=10&offset=0"
```

#### `GET /forecast/summary`
Get summary statistics for forecast data.

**Example:**
```bash
curl "http://localhost:8000/api/v1/forecast/summary?site_id=8013"
```

#### `GET /forecast/unique-values/{column_name}`
Get unique values for a specific column.

**Supported columns:**
- `site_id`, `brand`, `mh_segment`, `mh_family`, `mh_class`, `mh_brick`, `product_id`, `model_used`, `qty_group`

**Example:**
```bash
curl "http://localhost:8000/api/v1/forecast/unique-values/mh_segment"
```

#### `GET /forecast/health`
Health check endpoint for the forecast service.

### System

#### `GET /health`
General health check endpoint.

#### `GET /`
Root endpoint with API documentation links.

## 📚 API Documentation

Once the application is running, you can access:

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

## 🐳 Docker Deployment

```dockerfile
# Build and run with Docker
docker build -t retail-plan-visualizer-backend .
docker run -p 8000:8000 --env-file .env retail-plan-visualizer-backend
```

## 🧪 Testing

```bash
# Install test dependencies
pip install pytest pytest-asyncio httpx

# Run tests
pytest
```

## 📝 Development

### Project Structure

```
retail-plan-visualizer-backend/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI application
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py      # Configuration settings
│   ├── models/
│   │   ├── __init__.py
│   │   └── forecast_model.py # Pydantic models
│   ├── routers/
│   │   ├── __init__.py
│   │   └── forecast_router.py # API routes
│   └── services/
│       ├── __init__.py
│       └── bigquery_service.py # BigQuery operations
├── requirements.txt         # Python dependencies
├── run.py                  # Application runner
├── env.example             # Environment variables template
└── README.md              # This file
```

### Adding New Features

1. **Models**: Add new Pydantic models in `app/models/`
2. **Services**: Add business logic in `app/services/`
3. **Routes**: Add new API endpoints in `app/routers/`
4. **Configuration**: Update settings in `app/config/settings.py`

## 🔒 Security Considerations

- Always use service accounts with minimal required BigQuery permissions
- Never commit credentials to version control
- Use environment variables for sensitive configuration
- Enable CORS restrictions for production deployments
- Consider implementing rate limiting for production use

## 📊 Performance Tips

- Use appropriate `limit` values to control response size
- Leverage BigQuery's partitioning and clustering for better performance
- Consider caching frequently requested data
- Monitor BigQuery costs and optimize queries

## 🐛 Troubleshooting

### Common Issues

1. **Authentication Errors**:
   - Verify Google Cloud credentials
   - Check service account permissions
   - Ensure BigQuery API is enabled

2. **Connection Errors**:
   - Verify project ID, dataset ID, and table ID
   - Check network connectivity to Google Cloud

3. **Performance Issues**:
   - Reduce query complexity
   - Use appropriate filters
   - Consider BigQuery optimization

### Logging

The application logs to stdout. Adjust logging level in `app/main.py`:

```python
logging.basicConfig(level=logging.DEBUG)  # For detailed logs
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:
- Create an issue in the repository
- Check the API documentation at `/docs`
- Review BigQuery documentation for data-related questions 