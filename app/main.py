from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import time
import logging
from app.config.settings import settings
from app.routers import forecast_router

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI application
app = FastAPI(
    title=settings.app_name,
    description="FastAPI backend for retail plan visualization with BigQuery integration",
    version=settings.app_version,
    debug=settings.debug
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# Include routers
app.include_router(forecast_router.router, prefix=settings.api_v1_prefix)

# Root endpoint
@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint with API documentation links"""
    environment = "Development" if settings.debug else "Production"
    
    return f"""
    <html>
        <head>
            <title>Retail Plan Visualizer Backend</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .header {{ color: #2c3e50; margin-bottom: 30px; }}
                .endpoint {{ margin: 10px 0; }}
                .endpoint a {{ color: #3498db; text-decoration: none; }}
                .endpoint a:hover {{ text-decoration: underline; }}
                .description {{ color: #7f8c8d; margin-left: 20px; }}
            </style>
        </head>
        <body>
            <h1 class="header">🏪 Retail Plan Visualizer Backend</h1>
            <p>FastAPI backend for retail forecasting data with BigQuery integration.</p>
            
            <h2>📚 API Documentation</h2>
            <div class="endpoint">
                <a href="/docs" target="_blank">📋 Interactive API Documentation (Swagger UI)</a>
                <div class="description">Interactive API documentation with request/response examples</div>
            </div>
            <div class="endpoint">
                <a href="/redoc" target="_blank">📖 Alternative API Documentation (ReDoc)</a>
                <div class="description">Clean, three-panel API documentation</div>
            </div>
            
            <h2>🔗 API Endpoints</h2>
            <div class="endpoint">
                <a href="/api/v1/forecast/" target="_blank">GET /api/v1/forecast/</a>
                <div class="description">Get forecast data with filtering and pagination</div>
            </div>
            <div class="endpoint">
                <a href="/api/v1/forecast/summary" target="_blank">GET /api/v1/forecast/summary</a>
                <div class="description">Get summary statistics for forecast data</div>
            </div>
            <div class="endpoint">
                <a href="/api/v1/forecast/unique-values/mh_segment" target="_blank">GET /api/v1/forecast/unique-values/{{column_name}}</a>
                <div class="description">Get unique values for a specific column</div>
            </div>
            <div class="endpoint">
                <a href="/api/v1/forecast/health" target="_blank">GET /api/v1/forecast/health</a>
                <div class="description">Health check endpoint</div>
            </div>
            
            <h2>⚙️ Configuration</h2>
            <p><strong>Environment:</strong> {environment}</p>
            <p><strong>Version:</strong> {settings.app_version}</p>
            <p><strong>Debug Mode:</strong> {settings.debug}</p>
            
            <h2>🚀 Getting Started</h2>
            <ol>
                <li>Configure your BigQuery credentials in the .env file</li>
                <li>Set up your project ID, dataset ID, and table ID</li>
                <li>Use the interactive documentation to explore the API</li>
            </ol>
        </body>
    </html>
    """

# Health check endpoint
@app.get("/health")
async def health_check():
    """General health check endpoint"""
    return {
        "status": "healthy",
        "message": "Retail Plan Visualizer Backend is running",
        "version": settings.app_version,
        "debug": settings.debug
    } 