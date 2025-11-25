"""
app/config.py

Updated configuration with write-back settings
"""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # API Configuration
    APP_NAME: str = "SAP IBP XYZ Analysis API"
    APP_VERSION: str = "2.0.0"
    DEBUG: bool = False
    
    # SAP IBP Read Configuration (existing)
    SAP_API_URL: str
    SAP_USERNAME: str
    SAP_PASSWORD: str
    SAP_TIMEOUT: int = 30
    
    # SAP IBP Write Configuration (new)
    SAP_WRITE_API_URL: str = ""  # e.g., https://your-tenant.sap.com/sap/opu/odata/sap/IBP_PLANNING_DATA_API_SRV
    SAP_PLANNING_AREA: str = ""  # e.g., SAP1, SAP2, YSAPIBP1
    SAP_XYZ_KEY_FIGURE: str = "XYZID"  # Name of the key figure in IBP to store segment
    SAP_ENABLE_NULL_HANDLING: bool = False  # Set to True if ENABLE_NULL_INFO parameter is set in SAP IBP
    
    # Analysis Configuration
    DEFAULT_X_THRESHOLD: float = 10.0
    DEFAULT_Y_THRESHOLD: float = 25.0
    
    # Write Configuration
    DEFAULT_BATCH_SIZE: int = 5000
    DEFAULT_MAX_WORKERS: int = 4
    ENABLE_WRITE_OPERATIONS: bool = False  # Safety flag - must be explicitly enabled
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()