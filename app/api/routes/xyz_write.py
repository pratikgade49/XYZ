"""
app/api/routes/xyz_write.py

API routes for writing XYZ segmentation results back to SAP IBP
"""

from fastapi import APIRouter, Depends, Query, HTTPException, Body
from datetime import datetime
from typing import Optional
from enum import Enum

from app.models.write_schemas import (
    XYZWriteRequest,
    XYZWriteResponse,
    XYZWriteStatus,
    BatchWriteResponse
)
from app.services.sap_service import SAPService
from app.services.sap_write_service import SAPWriteService
from app.services.analysis_service import AnalysisService
from app.api.dependencies import get_sap_service, get_analysis_service, get_sap_write_service
from app.config import get_settings
from app.utils.logger import get_logger

router = APIRouter(prefix="/api/v1/xyz-write", tags=["XYZ Write-Back"])
logger = get_logger(__name__)


class WriteMode(str, Enum):
    """Write mode options"""
    SIMPLE = "simple"      # Single request with DoCommit (≤5000 records)
    BATCHED = "batched"    # Multiple requests with explicit commit
    PARALLEL = "parallel"  # Parallel processing for high volumes


@router.post("/write-segments", response_model=XYZWriteResponse)
async def write_xyz_segments(
    request: XYZWriteRequest = Body(...),
    sap_service: SAPService = Depends(get_sap_service),
    analysis_service: AnalysisService = Depends(get_analysis_service),
    write_service: SAPWriteService = Depends(get_sap_write_service)
):
    """
    Perform XYZ analysis and write segments back to SAP IBP
    
    This endpoint:
    1. Fetches product data from SAP IBP
    2. Performs XYZ segmentation analysis
    3. Writes the XYZ_Segment values back to SAP IBP
    
    **Write Modes:**
    - `simple`: Single request (recommended for ≤5,000 products)
    - `batched`: Multiple batches with commit (for >5,000 products)
    - `parallel`: Parallel processing (for very large datasets)
    
    **Note:** Ensure the target key figure exists in SAP IBP configuration.
    """
    settings = get_settings()
    x_thresh = request.x_threshold or settings.DEFAULT_X_THRESHOLD
    y_thresh = request.y_threshold or settings.DEFAULT_Y_THRESHOLD
    
    logger.info(
        f"XYZ write-back requested: mode={request.write_mode}, "
        f"version={request.version_id}, X={x_thresh}, Y={y_thresh}"
    )
    
    try:
        # Step 1: Fetch data from SAP
        logger.info("Step 1: Fetching data from SAP IBP")
        df = sap_service.fetch_data(additional_filters=request.filters)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found with given filters")
        
        logger.info(f"Fetched {len(df)} records")
        
        # Step 2: Perform XYZ analysis
        logger.info("Step 2: Performing XYZ segmentation")
        result_df = analysis_service.calculate_xyz_segmentation(df, x_thresh, y_thresh)
        
        # Prepare data for write-back
        # Keep only necessary columns: PRDID, XYZ_Segment, and optional period/location
        write_df = result_df[['PRDID', 'XYZ_Segment']].copy()
        
        # Add period field if specified in request
        if request.period_field and request.period_field in df.columns:
            # Get the first period for each product
            period_data = df.groupby('PRDID')[request.period_field].first().reset_index()
            write_df = write_df.merge(period_data, on='PRDID', how='left')
        
        # Add location if specified
        if request.location_id:
            write_df['LOCID'] = request.location_id
        
        logger.info(f"Prepared {len(write_df)} segments for write-back")
        
        # Step 3: Write to SAP based on mode
        logger.info(f"Step 3: Writing to SAP IBP using {request.write_mode} mode")
        
        if request.write_mode == WriteMode.SIMPLE:
            write_result = write_service.write_segments_simple(
                segment_data=write_df,
                version_id=request.version_id,
                scenario_id=request.scenario_id,
                period_field=request.period_field or "PERIODID3_TSTAMP"
            )
        
        elif request.write_mode == WriteMode.BATCHED:
            write_result = write_service.write_segments_batched(
                segment_data=write_df,
                version_id=request.version_id,
                scenario_id=request.scenario_id,
                period_field=request.period_field or "PERIODID3_TSTAMP",
                batch_size=request.batch_size or 5000
            )
        
        elif request.write_mode == WriteMode.PARALLEL:
            write_result = write_service.write_segments_parallel(
                segment_data=write_df,
                version_id=request.version_id,
                scenario_id=request.scenario_id,
                period_field=request.period_field or "PERIODID3_TSTAMP",
                batch_size=request.batch_size or 5000,
                max_workers=request.max_workers or 4
            )
        
        # Calculate segment distribution
        segment_counts = result_df['XYZ_Segment'].value_counts().to_dict()
        
        logger.info(f"Write operation completed successfully: {write_result.get('transaction_id')}")
        
        return XYZWriteResponse(
            status="success",
            transaction_id=write_result.get('transaction_id'),
            total_products=len(result_df),
            segments_written=segment_counts,
            analysis_params={
                "x_threshold": x_thresh,
                "y_threshold": y_thresh
            },
            write_mode=request.write_mode,
            version_id=request.version_id,
            scenario_id=request.scenario_id,
            records_sent=write_result.get('records_sent'),
            batch_count=write_result.get('batch_count'),
            message=write_result.get('message'),
            timestamp=datetime.utcnow().isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Write-back failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/write-custom", response_model=XYZWriteResponse)
async def write_custom_segments(
    segments: list = Body(..., description="List of segment assignments"),
    version_id: Optional[str] = Body(None),
    scenario_id: Optional[str] = Body(None),
    period_field: str = Body("PERIODID3_TSTAMP"),
    write_mode: WriteMode = Body(WriteMode.SIMPLE),
    write_service: SAPWriteService = Depends(get_sap_write_service)
):
    """
    Write custom XYZ segment assignments to SAP IBP
    
    Use this endpoint to write pre-calculated or manually adjusted segments.
    
    **Request body example:**
    ```json
    {
        "segments": [
            {"PRDID": "IBP-100", "XYZ_Segment": "X"},
            {"PRDID": "IBP-110", "XYZ_Segment": "Y", "LOCID": "1720"}
        ],
        "version_id": "UPSIDE",
        "write_mode": "simple"
    }
    ```
    """
    logger.info(f"Custom segment write requested: {len(segments)} segments")
    
    try:
        import pandas as pd
        
        # Convert to DataFrame
        write_df = pd.DataFrame(segments)
        
        # Validate required columns
        if 'PRDID' not in write_df.columns or 'XYZ_Segment' not in write_df.columns:
            raise HTTPException(
                status_code=400,
                detail="Each segment must have 'PRDID' and 'XYZ_Segment' fields"
            )
        
        # Validate segment values
        valid_segments = {'X', 'Y', 'Z'}
        invalid_segments = set(write_df['XYZ_Segment'].unique()) - valid_segments
        if invalid_segments:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid segment values: {invalid_segments}. Must be X, Y, or Z"
            )
        
        logger.info(f"Writing {len(write_df)} custom segments")
        
        # Write based on mode
        if write_mode == WriteMode.SIMPLE:
            write_result = write_service.write_segments_simple(
                segment_data=write_df,
                version_id=version_id,
                scenario_id=scenario_id,
                period_field=period_field
            )
        elif write_mode == WriteMode.BATCHED:
            write_result = write_service.write_segments_batched(
                segment_data=write_df,
                version_id=version_id,
                scenario_id=scenario_id,
                period_field=period_field
            )
        else:
            write_result = write_service.write_segments_parallel(
                segment_data=write_df,
                version_id=version_id,
                scenario_id=scenario_id,
                period_field=period_field
            )
        
        segment_counts = write_df['XYZ_Segment'].value_counts().to_dict()
        
        return XYZWriteResponse(
            status="success",
            transaction_id=write_result.get('transaction_id'),
            total_products=len(write_df),
            segments_written=segment_counts,
            analysis_params={},
            write_mode=write_mode,
            version_id=version_id,
            scenario_id=scenario_id,
            records_sent=write_result.get('records_sent'),
            batch_count=write_result.get('batch_count'),
            message=write_result.get('message'),
            timestamp=datetime.utcnow().isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Custom write failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{transaction_id}", response_model=XYZWriteStatus)
async def get_write_status(
    transaction_id: str,
    write_service: SAPWriteService = Depends(get_sap_write_service)
):
    """
    Get the status of a write transaction
    
    Use this to check if the write operation completed successfully
    and retrieve any error messages.
    """
    logger.info(f"Status check requested for transaction: {transaction_id}")
    
    try:
        # Get CSRF token and session for status check
        session, csrf_token = write_service._get_csrf_token()
        
        try:
            # Get export result
            export_result = write_service._get_export_result(session, csrf_token, transaction_id)
            
            # Get messages (errors if any)
            messages = []
            try:
                # Messages might need separate session
                msg_session, msg_csrf = write_service._get_csrf_token()
                try:
                    url = f"{write_service.api_url}/Message"
                    response = msg_session.get(
                        url,
                        params={"Transactionid": transaction_id},
                        headers={"X-CSRF-Token": msg_csrf},
                        timeout=write_service.timeout
                    )
                    if response.ok:
                        messages = response.json()
                except Exception as e:
                    logger.warning(f"Could not fetch messages: {str(e)}")
                finally:
                    msg_session.close()
            except Exception:
                pass
            
            return XYZWriteStatus(
                transaction_id=transaction_id,
                status="completed" if export_result else "unknown",
                export_result=export_result,
                messages=messages,
                timestamp=datetime.utcnow().isoformat()
            )
        
        finally:
            session.close()
        
    except Exception as e:
        logger.error(f"Status check failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/validate-config")
async def validate_write_config(
    write_service: SAPWriteService = Depends(get_sap_write_service)
):
    """
    Validate write configuration
    
    Checks if all required settings are configured:
    - SAP Write API URL
    - Planning Area
    - XYZ Key Figure name
    """
    settings = get_settings()
    
    config_status = {
        "sap_write_api_url": bool(settings.SAP_WRITE_API_URL),
        "planning_area": bool(settings.SAP_PLANNING_AREA),
        "xyz_key_figure": bool(settings.SAP_XYZ_KEY_FIGURE),
        "credentials_configured": bool(settings.SAP_USERNAME and settings.SAP_PASSWORD)
    }
    
    all_configured = all(config_status.values())
    
    return {
        "configured": all_configured,
        "configuration": config_status,
        "message": "All settings configured" if all_configured else "Missing required settings",
        "timestamp": datetime.utcnow().isoformat()
    }