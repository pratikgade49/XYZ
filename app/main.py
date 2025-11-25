"""
app/main.py

Updated FastAPI application with write-back functionality
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from datetime import datetime

from app.config import get_settings
from app.utils.logger import setup_logger, get_logger
from app.api.routes import health, xyz_analysis, xyz_write

from dotenv import load_dotenv
load_dotenv()

# Initialize settings
settings = get_settings()

# Setup logging
setup_logger("app", level=settings.LOG_LEVEL, format_type=settings.LOG_FORMAT)
logger = get_logger(__name__)

# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="""
    API for fetching SAP IBP data and performing XYZ segmentation analysis.
    
    ## Features
    - Fetch product data from SAP IBP
    - Perform XYZ segmentation analysis
    - Export analysis results (CSV, JSON, Excel)
    - Write XYZ segments back to SAP IBP
    
    ## XYZ Segmentation
    - **X Segment**: Stable demand (CV ≤ 10%)
    - **Y Segment**: Moderate variability (10% < CV ≤ 25%)
    - **Z Segment**: High variability (CV > 25%)
    """,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router)
app.include_router(xyz_analysis.router)
app.include_router(xyz_write.router)


@app.on_event("startup")
async def startup_event():
    """Application startup"""
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    logger.info(f"Debug mode: {settings.DEBUG}")
    logger.info(f"Write operations enabled: {settings.ENABLE_WRITE_OPERATIONS}")
    
    if settings.ENABLE_WRITE_OPERATIONS:
        logger.info(f"Write API URL: {settings.SAP_WRITE_API_URL}")
        logger.info(f"Planning Area: {settings.SAP_PLANNING_AREA}")
        logger.info(f"XYZ Key Figure: {settings.SAP_XYZ_KEY_FIGURE}")


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown"""
    logger.info("Shutting down application")


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if settings.DEBUG else "An error occurred",
            "timestamp": datetime.utcnow().isoformat()
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )