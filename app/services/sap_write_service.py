"""
app/services/sap_write_service.py

Service for writing XYZ segmentation results back to SAP IBP
using the /IBP/PLANNING_DATA_API_SRV OData service
"""

import requests
import pandas as pd
from typing import Optional, Dict, List, Any
from datetime import datetime
import uuid
from app.config import get_settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class SAPWriteService:
    """Service for writing data back to SAP IBP via PLANNING_DATA_API_SRV"""
    
    def __init__(self):
        self.settings = get_settings()
        # Remove trailing slash from API URL if present
        self.api_url = self.settings.SAP_WRITE_API_URL.rstrip('/')
        self.username = self.settings.SAP_USERNAME
        self.password = self.settings.SAP_PASSWORD
        self.timeout = self.settings.SAP_TIMEOUT
        self.planning_area = self.settings.SAP_PLANNING_AREA
        self.xyz_key_figure = self.settings.SAP_XYZ_KEY_FIGURE
        self.enable_null_handling = self.settings.SAP_ENABLE_NULL_HANDLING
        self.session = None
        self.csrf_token = None

        
        logger.info(f"Initialized write service with URL: {self.api_url}")
        logger.info(f"Planning area: {self.planning_area}")
        logger.info(f"Key figure: {self.xyz_key_figure}")
        logger.info(f"NULL handling enabled: {self.enable_null_handling}")
    
    def _get_csrf_token(self) -> tuple[requests.Session, str]:
        """
        Fetch CSRF token required for POST operations
        
        Returns:
            Tuple of (session, csrf_token)
        """
        logger.debug("Fetching CSRF token from SAP")
        
        # Create session to maintain cookies
        session = requests.Session()
        session.auth = (self.username, self.password)
        
        # Fetch CSRF token with HEAD or GET request
        try:
            response = session.get(
                self.api_url,
                headers={
                    "X-CSRF-Token": "Fetch",
                    "Accept": "application/json"
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            
            csrf_token = response.headers.get("X-CSRF-Token")
            
            if not csrf_token:
                raise Exception("CSRF token not found in response headers")
            
            logger.info(f"CSRF token obtained successfully")
            return session, csrf_token
            
        except Exception as e:
            logger.error(f"Failed to get CSRF token: {str(e)}")
            raise Exception(f"Failed to obtain CSRF token: {str(e)}")
        
    def _generate_transaction_id(self) -> str:
        """Generate a unique transaction ID (max 32 chars)"""
        return uuid.uuid4().hex.upper()[:32]
    
    def _prepare_payload(
        self,
        segment_data: pd.DataFrame,
        transaction_id: str,
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
        period_field: str = "PERIODID3_TSTAMP",
        do_commit: bool = False
    ) -> Dict[str, Any]:
        """
        Prepare POST payload for SAP IBP
        
        Args:
            segment_data: DataFrame with PRDID, XYZ_Segment, and optional period columns
            transaction_id: Unique transaction identifier
            version_id: Target version (None = base version)
            scenario_id: Target scenario (None = baseline)
            period_field: Period field name (e.g., PERIODID3_TSTAMP, PERIODID4_TSTAMP)
            do_commit: If True, auto-commit in single request
            
        Returns:
            Dictionary payload for POST request
        """
        logger.debug(f"Preparing payload for {len(segment_data)} records")
        
        # Build aggregation level fields string
        # Format: PRDID,XYZ_SEGMENT,PERIODID_TSTAMP
        # Or with NULL handling: PRDID,XYZ_SEGMENT,XYZ_SEGMENT_isNull,PERIODID_TSTAMP
        
        if self.enable_null_handling:
            agg_fields = f"PRDID,{self.xyz_key_figure},{self.xyz_key_figure}_isNull,{period_field}"
        else:
            agg_fields = f"PRDID,{self.xyz_key_figure},{period_field}"
        
        # Add location if present
        if 'LOCID' in segment_data.columns:
            agg_fields = f"LOCID,{agg_fields}"
        
        # Build navigation property data
        nav_data = []
        for _, row in segment_data.iterrows():
            record = {
                "PRDID": row['PRDID'],
                self.xyz_key_figure: row['XYZ_Segment']
            }
            
            # Add NULL flag only if enabled
            if self.enable_null_handling:
                record[f"{self.xyz_key_figure}_isNull"] = False
            
            # Add location if present
            if 'LOCID' in row and pd.notna(row['LOCID']):
                record['LOCID'] = row['LOCID']
            
            # Add period timestamp
            if period_field in row and pd.notna(row[period_field]):
                record[period_field] = row[period_field]
            else:
                # Use current timestamp if not provided
                record[period_field] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            
            nav_data.append(record)
        
        # Navigation property name is based on planning area
        # Format: Nav<PlanningArea> (e.g., NavSAP1, NavYSAPIBP1)
        nav_property_name = f"Nav{self.planning_area}"
        
        # Build main payload
        payload = {
            "Transactionid": transaction_id,
            "AggregationLevelFieldsString": agg_fields,
            nav_property_name: nav_data  # Dynamic navigation property name
        }
        
        # Add optional fields
        if version_id:
            payload["VersionID"] = version_id
        
        if scenario_id:
            payload["ScenarioID"] = scenario_id
        
        if do_commit:
            payload["DoCommit"] = True
        
        logger.debug(f"Payload prepared with {len(nav_data)} records using {nav_property_name}")
        return payload
    
    def write_segments_simple(
        self,
        segment_data: pd.DataFrame,
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
        period_field: str = "PERIODID3_TSTAMP"
    ) -> Dict[str, Any]:
        """
        Write XYZ segments using simple single-request method (for ≤5000 records)
        
        Args:
            segment_data: DataFrame with PRDID and XYZ_Segment columns
            version_id: Target version
            scenario_id: Target scenario
            period_field: Period timestamp field name
            
        Returns:
            Response with transaction ID and status
        """
        record_count = len(segment_data)
        logger.info(f"Starting simple write for {record_count} segments")
        
        if record_count > 5000:
            logger.warning(f"Record count {record_count} exceeds recommended limit of 5000")
        
        # Generate transaction ID
        transaction_id = self._generate_transaction_id()
        logger.info(f"Generated transaction ID: {transaction_id}")
        
        # Prepare payload with DoCommit=True
        payload = self._prepare_payload(
            segment_data=segment_data,
            transaction_id=transaction_id,
            version_id=version_id,
            scenario_id=scenario_id,
            period_field=period_field,
            do_commit=True
        )
        
        # Get CSRF token
        session, csrf_token = self._get_csrf_token()
        
        # Send POST request
        url = f"{self.api_url}/{self.planning_area}Trans"
        
        try:
            logger.debug(f"Sending POST to: {url}")
            response = session.post(
                url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrf_token
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            logger.info(f"Write successful - Transaction ID: {transaction_id}")
            
            return {
                "status": "success",
                "transaction_id": transaction_id,
                "records_sent": record_count,
                "message": "Data written and committed successfully"
            }
            
        except requests.exceptions.Timeout:
            logger.error("Write request timeout")
            raise Exception("SAP write request timeout")
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Write request failed: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                logger.error(f"Response body: {e.response.text}")
            raise Exception(f"Failed to write data to SAP: {str(e)}")
        
        finally:
            # Close session
            session.close()
    
    def write_segments_batched(
        self,
        segment_data: pd.DataFrame,
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
        period_field: str = "PERIODID3_TSTAMP",
        batch_size: int = 5000
    ) -> Dict[str, Any]:
        """
        Write XYZ segments using multi-batch method with explicit commit
        
        Args:
            segment_data: DataFrame with PRDID and XYZ_Segment columns
            version_id: Target version
            scenario_id: Target scenario
            period_field: Period timestamp field name
            batch_size: Number of records per batch (default 5000)
            
        Returns:
            Response with transaction ID, batch info, and status
        """
        record_count = len(segment_data)
        logger.info(f"Starting batched write for {record_count} segments")
        
        # Get CSRF token and session
        session, csrf_token = self._get_csrf_token()
        
        try:
            # Get transaction ID from system
            transaction_id = self._get_transaction_id(session, csrf_token)
            logger.info(f"Retrieved transaction ID: {transaction_id}")
            
            # Split data into batches
            batches = [segment_data[i:i+batch_size] for i in range(0, record_count, batch_size)]
            batch_count = len(batches)
            logger.info(f"Split into {batch_count} batches of max {batch_size} records")
            
            url = f"{self.api_url}/{self.planning_area}Trans"
            
            # Send batches
            for idx, batch in enumerate(batches, 1):
                logger.info(f"Sending batch {idx}/{batch_count} ({len(batch)} records)")
                
                payload = self._prepare_payload(
                    segment_data=batch,
                    transaction_id=transaction_id,
                    version_id=version_id,
                    scenario_id=scenario_id,
                    period_field=period_field,
                    do_commit=False
                )
                
                try:
                    response = session.post(
                        url,
                        json=payload,
                        headers={
                            "Content-Type": "application/json",
                            "X-CSRF-Token": csrf_token
                        },
                        timeout=self.timeout
                    )
                    response.raise_for_status()
                    logger.info(f"Batch {idx}/{batch_count} sent successfully")
                    
                except requests.exceptions.RequestException as e:
                    logger.error(f"Batch {idx} failed: {str(e)}")
                    raise Exception(f"Failed to send batch {idx}: {str(e)}")
            
            # Commit transaction
            logger.info("All batches sent, committing transaction")
            commit_result = self._commit_transaction(session, csrf_token, transaction_id)
            
            # Get export result
            export_result = self._get_export_result(session, csrf_token, transaction_id)
            
            return {
                "status": "success",
                "transaction_id": transaction_id,
                "records_sent": record_count,
                "batch_count": batch_count,
                "batch_size": batch_size,
                "commit_status": commit_result,
                "export_result": export_result,
                "message": "Data written and committed in batches"
            }
        
        finally:
            # Close session
            session.close()
    
    def _get_transaction_id(self, session: requests.Session, csrf_token: str) -> str:
        """Get transaction ID from SAP system"""
        url = f"{self.api_url}/getTransactionID"
        
        try:
            logger.debug("Requesting transaction ID from SAP")
            response = session.get(
                url,
                headers={"X-CSRF-Token": csrf_token},
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Parse response to extract transaction ID
            data = response.json()
            transaction_id = data.get('d', {}).get('TransactionID')
            
            if not transaction_id:
                raise Exception("Transaction ID not found in response")
            
            logger.info(f"Transaction ID obtained: {transaction_id}")
            return transaction_id
            
        except Exception as e:
            logger.error(f"Failed to get transaction ID: {str(e)}")
            raise
    
    def _commit_transaction(self, session: requests.Session, csrf_token: str, transaction_id: str) -> Dict[str, Any]:
        """Commit a transaction"""
        url = f"{self.api_url}/commit"
        
        payload = {"Transactionid": transaction_id}
        
        try:
            logger.info(f"Committing transaction: {transaction_id}")
            response = session.post(
                url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrf_token
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            
            logger.info("Transaction committed successfully")
            return {
                "status": "committed",
                "transaction_id": transaction_id
            }
            
        except Exception as e:
            logger.error(f"Commit failed: {str(e)}")
            raise Exception(f"Failed to commit transaction: {str(e)}")
    
    def _get_export_result(self, session: requests.Session, csrf_token: str, transaction_id: str) -> Dict[str, Any]:
        """Get export/import result status"""
        url = f"{self.api_url}/GetExportResult"
        
        params = {"Transactionid": transaction_id}
        
        try:
            logger.debug(f"Getting export result for transaction: {transaction_id}")
            response = session.get(
                url,
                params=params,
                headers={"X-CSRF-Token": csrf_token},
                timeout=self.timeout
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info("Export result retrieved successfully")
            return result
            
        except Exception as e:
            logger.warning(f"Failed to get export result: {str(e)}")
            return {"status": "unknown", "error": str(e)}
    
    def get_messages(self, transaction_id: str) -> List[Dict[str, Any]]:
        """Get error messages for a transaction"""
        # Create new session for this request
        session, csrf_token = self._get_csrf_token()
        
        try:
            url = f"{self.api_url}/Message"
            params = {"Transactionid": transaction_id}
            
            logger.debug(f"Getting messages for transaction: {transaction_id}")
            response = session.get(
                url,
                params=params,
                headers={"X-CSRF-Token": csrf_token},
                timeout=self.timeout
            )
            response.raise_for_status()
            
            messages = response.json()
            logger.info("Messages retrieved successfully")
            return messages
            
        except Exception as e:
            logger.warning(f"Failed to get messages: {str(e)}")
            return []
        
        finally:
            session.close()
    
    def write_segments_parallel(
        self,
        segment_data: pd.DataFrame,
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
        period_field: str = "PERIODID3_TSTAMP",
        batch_size: int = 5000,
        max_workers: int = 4
    ) -> Dict[str, Any]:
        """
        Write XYZ segments using parallel processing for high volumes
        
        Args:
            segment_data: DataFrame with PRDID and XYZ_Segment columns
            version_id: Target version
            scenario_id: Target scenario
            period_field: Period timestamp field name
            batch_size: Number of records per batch
            max_workers: Maximum parallel threads
            
        Returns:
            Response with transaction ID and status
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        record_count = len(segment_data)
        logger.info(f"Starting parallel write for {record_count} segments")
        
        # Get CSRF token and session
        session, csrf_token = self._get_csrf_token()
        
        try:
            # Initiate parallel process
            transaction_id = self._initiate_parallel_process(
                session=session,
                csrf_token=csrf_token,
                version_id=version_id,
                scenario_id=scenario_id
            )
            
            # Split data into batches
            batches = [segment_data[i:i+batch_size] for i in range(0, record_count, batch_size)]
            batch_count = len(batches)
            logger.info(f"Split into {batch_count} batches for parallel processing")
            
            url = f"{self.api_url}/{self.planning_area}Trans"
            
            # Send batches in parallel
            results = []
            failed_batches = []
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {
                    executor.submit(
                        self._send_batch_parallel,
                        url,
                        batch,
                        transaction_id,
                        csrf_token,
                        period_field,
                        idx
                    ): idx for idx, batch in enumerate(batches, 1)
                }
                
                for future in as_completed(future_to_batch):
                    batch_idx = future_to_batch[future]
                    try:
                        result = future.result()
                        results.append(result)
                        logger.info(f"Batch {batch_idx} completed successfully")
                    except Exception as e:
                        logger.error(f"Batch {batch_idx} failed: {str(e)}")
                        failed_batches.append(batch_idx)
            
            if failed_batches:
                logger.error(f"Failed batches: {failed_batches}")
                raise Exception(f"Some batches failed: {failed_batches}")
            
            # Commit transaction
            logger.info("All batches sent, committing transaction")
            commit_result = self._commit_transaction(session, csrf_token, transaction_id)
            
            # Get export result
            export_result = self._get_export_result(session, csrf_token, transaction_id)
            
            return {
                "status": "success",
                "transaction_id": transaction_id,
                "records_sent": record_count,
                "batch_count": batch_count,
                "parallel_workers": max_workers,
                "commit_status": commit_result,
                "export_result": export_result,
                "message": "Data written in parallel and committed"
            }
        
        finally:
            session.close()
    
    def _initiate_parallel_process(
        self,
        session: requests.Session,
        csrf_token: str,
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None
    ) -> str:
        """Initiate parallel processing and get transaction ID"""
        url = f"{self.api_url}/InitiateParallelProcess"
        
        payload = {
            "PlanningArea": self.planning_area
        }
        
        if version_id:
            payload["VersionID"] = version_id
        
        if scenario_id:
            payload["ScenarioID"] = scenario_id
        
        try:
            logger.info("Initiating parallel process")
            response = session.post(
                url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrf_token
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            transaction_id = data.get('d', {}).get('TransactionID')
            
            if not transaction_id:
                raise Exception("Transaction ID not found in response")
            
            logger.info(f"Parallel process initiated with transaction ID: {transaction_id}")
            return transaction_id
            
        except Exception as e:
            logger.error(f"Failed to initiate parallel process: {str(e)}")
            raise
    
    def _send_batch_parallel(
        self,
        url: str,
        batch: pd.DataFrame,
        transaction_id: str,
        csrf_token: str,
        period_field: str,
        batch_idx: int
    ) -> Dict[str, Any]:
        """Send a single batch in parallel processing"""
        # Create new session for this thread
        session = requests.Session()
        session.auth = (self.username, self.password)
        
        try:
            payload = self._prepare_payload(
                segment_data=batch,
                transaction_id=transaction_id,
                period_field=period_field,
                do_commit=False
            )
            
            response = session.post(
                url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrf_token
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            
            return {
                "batch_idx": batch_idx,
                "records": len(batch),
                "status": "success"
            }
        
        finally:
            session.close()