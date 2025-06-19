import os
from typing import List, Optional, Dict, Any
from datetime import date, datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
from app.config.settings import settings
from app.models.forecast_model import ForecastRecord, ForecastQuery

# Configure logging
logger = logging.getLogger(__name__)


class BigQueryService:
    """Service class for BigQuery operations"""
    
    def __init__(self):
        """Initialize BigQuery client"""
        self.project_id = settings.google_cloud_project_id
        self.dataset_id = settings.bigquery_dataset_id
        self.table_id = settings.bigquery_table_id
        
        # Initialize client with credentials
        if settings.google_application_credentials and os.path.exists(settings.google_application_credentials):
            logger.info("Using service account credentials from file")
            credentials = service_account.Credentials.from_service_account_file(
                settings.google_application_credentials
            )
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        else:
            # Use Application Default Credentials (ADC)
            # This works with 'gcloud auth application-default login'
            logger.info("Using Application Default Credentials (ADC)")
            self.client = bigquery.Client(project=self.project_id)
        
        logger.info(f"BigQuery client initialized for project: {self.project_id}")
        logger.info(f"Target table: {self.project_id}.{self.dataset_id}.{self.table_id}")
    
    def _build_query(self, query_params: ForecastQuery) -> str:
        """Build SQL query based on query parameters"""
        
        base_query = f"""
        SELECT 
            forecast_datetime,
            forecast_run_id,
            site_id,
            brand,
            mh_segment,
            mh_family,
            mh_class,
            mh_brick,
            product_id,
            forecast_week,
            actual_qty,
            predicted_qty,
            model_used,
            qty_group,
            forecast_week_number,
            training_data_max_date,
            forecast_horizon,
            created_at
        FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
        """
        
        # Build WHERE conditions
        conditions = []
        
        if query_params.site_id is not None:
            conditions.append(f"site_id = '{query_params.site_id}'")
        
        if query_params.brand is not None:
            conditions.append(f"brand = '{query_params.brand}'")
        
        if query_params.mh_segment:
            conditions.append(f"mh_segment = '{query_params.mh_segment}'")
        
        if query_params.mh_family:
            conditions.append(f"mh_family = '{query_params.mh_family}'")
        
        if query_params.mh_class:
            conditions.append(f"mh_class = '{query_params.mh_class}'")
        
        if query_params.mh_brick:
            conditions.append(f"mh_brick = '{query_params.mh_brick}'")
        
        if query_params.product_id:
            conditions.append(f"product_id = '{query_params.product_id}'")
        
        if query_params.forecast_run_id:
            conditions.append(f"forecast_run_id = '{query_params.forecast_run_id}'")
        
        if query_params.model_used:
            conditions.append(f"model_used = '{query_params.model_used}'")
        
        if query_params.start_date:
            conditions.append(f"forecast_week >= '{query_params.start_date}'")
        
        if query_params.end_date:
            conditions.append(f"forecast_week <= '{query_params.end_date}'")
        
        # Add WHERE clause if there are conditions
        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)
        
        # Add ORDER BY
        base_query += " ORDER BY forecast_datetime DESC, forecast_week ASC"
        
        # Add LIMIT and OFFSET
        base_query += f" LIMIT {query_params.limit}"
        if query_params.offset > 0:
            base_query += f" OFFSET {query_params.offset}"
        
        return base_query
    
    def _build_count_query(self, query_params: ForecastQuery) -> str:
        """Build count query to get total records"""
        
        base_query = f"""
        SELECT COUNT(*) as total_count
        FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
        """
        
        # Build WHERE conditions (same as main query)
        conditions = []
        
        if query_params.site_id is not None:
            conditions.append(f"site_id = '{query_params.site_id}'")
        
        if query_params.brand is not None:
            conditions.append(f"brand = '{query_params.brand}'")
        
        if query_params.mh_segment:
            conditions.append(f"mh_segment = '{query_params.mh_segment}'")
        
        if query_params.mh_family:
            conditions.append(f"mh_family = '{query_params.mh_family}'")
        
        if query_params.mh_class:
            conditions.append(f"mh_class = '{query_params.mh_class}'")
        
        if query_params.mh_brick:
            conditions.append(f"mh_brick = '{query_params.mh_brick}'")
        
        if query_params.product_id:
            conditions.append(f"product_id = '{query_params.product_id}'")
        
        if query_params.forecast_run_id:
            conditions.append(f"forecast_run_id = '{query_params.forecast_run_id}'")
        
        if query_params.model_used:
            conditions.append(f"model_used = '{query_params.model_used}'")
        
        if query_params.start_date:
            conditions.append(f"forecast_week >= '{query_params.start_date}'")
        
        if query_params.end_date:
            conditions.append(f"forecast_week <= '{query_params.end_date}'")
        
        # Add WHERE clause if there are conditions
        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)
        
        return base_query
    
    async def get_forecast_data(self, query_params: ForecastQuery) -> tuple[List[ForecastRecord], int]:
        """Fetch forecast data from BigQuery"""
        
        try:
            # Get total count
            count_query = self._build_count_query(query_params)
            count_job = self.client.query(count_query)
            count_result = count_job.result()
            total_count = list(count_result)[0].total_count
            
            # Get actual data
            data_query = self._build_query(query_params)
            query_job = self.client.query(data_query)
            results = query_job.result()
            
            # Convert results to ForecastRecord objects
            forecast_records = []
            for row in results:
                record_dict = {
                    "forecast_datetime": row.forecast_datetime,
                    "forecast_run_id": row.forecast_run_id,
                    "site_id": row.site_id,
                    "brand": row.brand,
                    "mh_segment": row.mh_segment,
                    "mh_family": row.mh_family,
                    "mh_class": row.mh_class,
                    "mh_brick": row.mh_brick,
                    "product_id": row.product_id,
                    "forecast_week": row.forecast_week,
                    "actual_qty": row.actual_qty,
                    "predicted_qty": row.predicted_qty,
                    "model_used": row.model_used,
                    "qty_group": row.qty_group,
                    "forecast_week_number": row.forecast_week_number,
                    "training_data_max_date": row.training_data_max_date,
                    "forecast_horizon": row.forecast_horizon,
                    "created_at": row.created_at
                }
                forecast_records.append(ForecastRecord(**record_dict))
            
            return forecast_records, total_count
        
        except Exception as e:
            raise Exception(f"Error fetching data from BigQuery: {str(e)}")
    
    async def get_unique_values(self, column_name: str) -> List[str]:
        """Get unique values for a specific column"""
        
        try:
            query = f"""
            SELECT DISTINCT {column_name}
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE {column_name} IS NOT NULL
            ORDER BY {column_name}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            return [str(row[0]) for row in results]
        
        except Exception as e:
            raise Exception(f"Error fetching unique values for {column_name}: {str(e)}")
    
    async def get_summary_stats(self, query_params: ForecastQuery) -> Dict[str, Any]:
        """Get summary statistics for the filtered data"""
        
        try:
            base_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT site_id) as unique_sites,
                COUNT(DISTINCT brand) as unique_brands,
                COUNT(DISTINCT product_id) as unique_products,
                COUNT(DISTINCT forecast_run_id) as unique_forecast_runs,
                AVG(actual_qty) as avg_actual_qty,
                AVG(predicted_qty) as avg_predicted_qty,
                SUM(actual_qty) as total_actual_qty,
                SUM(predicted_qty) as total_predicted_qty,
                MIN(forecast_week) as min_forecast_week,
                MAX(forecast_week) as max_forecast_week
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            """
            
            # Apply same filters as main query
            conditions = []
            
            if query_params.site_id is not None:
                conditions.append(f"site_id = '{query_params.site_id}'")
            
            if query_params.brand is not None:
                conditions.append(f"brand = '{query_params.brand}'")
            
            if query_params.mh_segment:
                conditions.append(f"mh_segment = '{query_params.mh_segment}'")
            
            if query_params.mh_family:
                conditions.append(f"mh_family = '{query_params.mh_family}'")
            
            if query_params.mh_class:
                conditions.append(f"mh_class = '{query_params.mh_class}'")
            
            if query_params.mh_brick:
                conditions.append(f"mh_brick = '{query_params.mh_brick}'")
            
            if query_params.product_id:
                conditions.append(f"product_id = '{query_params.product_id}'")
            
            if query_params.forecast_run_id:
                conditions.append(f"forecast_run_id = '{query_params.forecast_run_id}'")
            
            if query_params.model_used:
                conditions.append(f"model_used = '{query_params.model_used}'")
            
            if query_params.start_date:
                conditions.append(f"forecast_week >= '{query_params.start_date}'")
            
            if query_params.end_date:
                conditions.append(f"forecast_week <= '{query_params.end_date}'")
            
            if conditions:
                base_query += " WHERE " + " AND ".join(conditions)
            
            query_job = self.client.query(base_query)
            results = query_job.result()
            
            row = list(results)[0]
            return {
                "total_records": row.total_records,
                "unique_sites": row.unique_sites,
                "unique_brands": row.unique_brands,
                "unique_products": row.unique_products,
                "unique_forecast_runs": row.unique_forecast_runs,
                "avg_actual_qty": float(row.avg_actual_qty) if row.avg_actual_qty else 0,
                "avg_predicted_qty": float(row.avg_predicted_qty) if row.avg_predicted_qty else 0,
                "total_actual_qty": float(row.total_actual_qty) if row.total_actual_qty else 0,
                "total_predicted_qty": float(row.total_predicted_qty) if row.total_predicted_qty else 0,
                "min_forecast_week": row.min_forecast_week,
                "max_forecast_week": row.max_forecast_week
            }
        
        except Exception as e:
            raise Exception(f"Error fetching summary statistics: {str(e)}")


# Global service instance
bigquery_service = BigQueryService() 