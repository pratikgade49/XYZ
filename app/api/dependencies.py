"""
app/api/dependencies.py

Dependency injection for services including write service
"""

from fastapi import HTTPException
from app.services.sap_service import SAPService
from app.services.sap_write_service import SAPWriteService
from app.services.analysis_service import AnalysisService
from app.config import get_settings


def get_sap_service() -> SAPService:
    """Dependency for SAP read service"""
    return SAPService()


def get_analysis_service() -> AnalysisService:
    """Dependency for analysis service"""
    return AnalysisService()


def get_sap_write_service() -> SAPWriteService:
    """Dependency for SAP write service"""
    settings = get_settings()
    
    # Check if write operations are enabled
    if not settings.ENABLE_WRITE_OPERATIONS:
        raise HTTPException(
            status_code=403,
            detail="Write operations are disabled. Set ENABLE_WRITE_OPERATIONS=true in configuration."
        )
    
    # Validate required write configuration
    if not settings.SAP_WRITE_API_URL:
        raise HTTPException(
            status_code=500,
            detail="SAP_WRITE_API_URL not configured"
        )
    
    if not settings.SAP_PLANNING_AREA:
        raise HTTPException(
            status_code=500,
            detail="SAP_PLANNING_AREA not configured"
        )
    
    if not settings.SAP_XYZ_KEY_FIGURE:
        raise HTTPException(
            status_code=500,
            detail="SAP_XYZ_KEY_FIGURE not configured"
        )
    
    return SAPWriteService()