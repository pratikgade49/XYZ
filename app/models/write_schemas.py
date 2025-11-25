"""
app/models/write_schemas.py

Pydantic schemas for XYZ write-back operations
Add these to your existing schemas.py file
"""

from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Any
from datetime import datetime


class XYZWriteRequest(BaseModel):
    """Request model for writing XYZ segments back to SAP"""
    
    # Analysis parameters
    x_threshold: Optional[float] = Field(None, description="CV threshold for X segment")
    y_threshold: Optional[float] = Field(None, description="CV threshold for Y segment")
    filters: Optional[str] = Field(None, description="Additional OData filters for data fetch")
    
    # Write parameters
    write_mode: str = Field("simple", description="Write mode: simple, batched, or parallel")
    version_id: Optional[str] = Field(None, description="Target version ID (None = base version)")
    scenario_id: Optional[str] = Field(None, description="Target scenario ID (None = baseline)")
    location_id: Optional[str] = Field(None, description="Location ID if location-specific")
    period_field: Optional[str] = Field("PERIODID3_TSTAMP", description="Period field name")
    
    # Batch parameters (for batched/parallel modes)
    batch_size: Optional[int] = Field(5000, description="Records per batch", ge=1, le=10000)
    max_workers: Optional[int] = Field(4, description="Parallel workers (parallel mode only)", ge=1, le=10)
    
    class Config:
        json_schema_extra = {
            "example": {
                "x_threshold": 10.0,
                "y_threshold": 25.0,
                "filters": "PRDID eq 'IBP-100' or PRDID eq 'IBP-110'",
                "write_mode": "simple",
                "version_id": "UPSIDE",
                "period_field": "PERIODID3_TSTAMP"
            }
        }


class XYZWriteResponse(BaseModel):
    """Response model for write operations"""
    status: str = Field(..., description="Operation status")
    transaction_id: str = Field(..., description="SAP transaction ID")
    total_products: int = Field(..., description="Total products analyzed")
    segments_written: Dict[str, int] = Field(..., description="Segment distribution")
    analysis_params: Dict[str, float] = Field(..., description="Analysis parameters used")
    write_mode: str = Field(..., description="Write mode used")
    version_id: Optional[str] = Field(None, description="Target version")
    scenario_id: Optional[str] = Field(None, description="Target scenario")
    records_sent: int = Field(..., description="Number of records sent to SAP")
    batch_count: Optional[int] = Field(None, description="Number of batches (if batched)")
    message: str = Field(..., description="Status message")
    timestamp: str = Field(..., description="Response timestamp")
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "transaction_id": "A1B2C3D4E5F6G7H8I9J0K1L2M3N4O5P6",
                "total_products": 150,
                "segments_written": {"X": 45, "Y": 60, "Z": 45},
                "analysis_params": {"x_threshold": 10.0, "y_threshold": 25.0},
                "write_mode": "simple",
                "version_id": "UPSIDE",
                "records_sent": 150,
                "message": "Data written and committed successfully",
                "timestamp": "2024-01-15T10:30:00"
            }
        }


class XYZWriteStatus(BaseModel):
    """Status of a write transaction"""
    transaction_id: str = Field(..., description="Transaction ID")
    status: str = Field(..., description="Transaction status")
    export_result: Dict[str, Any] = Field(..., description="Export result from SAP")
    messages: List[Dict[str, Any]] = Field(..., description="Error messages if any")
    timestamp: str = Field(..., description="Status check timestamp")


class BatchWriteResponse(BaseModel):
    """Response for batched write operations"""
    status: str
    transaction_id: str
    total_batches: int
    successful_batches: int
    failed_batches: List[int]
    records_sent: int
    message: str
    timestamp: str


class SegmentAssignment(BaseModel):
    """Individual segment assignment"""
    PRDID: str = Field(..., description="Product ID")
    XYZ_Segment: str = Field(..., description="Segment (X, Y, or Z)")
    LOCID: Optional[str] = Field(None, description="Location ID (optional)")
    PERIODID3_TSTAMP: Optional[str] = Field(None, description="Period timestamp (optional)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "PRDID": "IBP-100",
                "XYZ_Segment": "X",
                "LOCID": "1720",
                "PERIODID3_TSTAMP": "2024-01-15T10:30:00"
            }
        }


class CustomSegmentWriteRequest(BaseModel):
    """Request for writing custom segment assignments"""
    segments: List[SegmentAssignment] = Field(..., description="List of segment assignments")
    version_id: Optional[str] = Field(None, description="Target version")
    scenario_id: Optional[str] = Field(None, description="Target scenario")
    period_field: str = Field("PERIODID3_TSTAMP", description="Period field name")
    write_mode: str = Field("simple", description="Write mode: simple, batched, or parallel")
    
    class Config:
        json_schema_extra = {
            "example": {
                "segments": [
                    {"PRDID": "IBP-100", "XYZ_Segment": "X"},
                    {"PRDID": "IBP-110", "XYZ_Segment": "Y", "LOCID": "1720"}
                ],
                "version_id": "UPSIDE",
                "write_mode": "simple"
            }
        }