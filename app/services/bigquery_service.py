import os
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from datetime import date, datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import json
from app.config.settings import settings
from app.models.forecast_model import ForecastRecord, ForecastQuery

# Configure logging
logger = logging.getLogger(__name__)

# Global cache for frequently accessed data
_cache = {
    "unique_values": {},
    "summary_stats": {},
    "cache_timestamps": {}
}

# Cache TTL (Time To Live) in seconds
CACHE_TTL = 300  # 5 minutes

# Thread pool for concurrent BigQuery operations
THREAD_POOL = ThreadPoolExecutor(max_workers=10)


class BigQueryService:
    """Optimized BigQuery service with async operations and direct filtering"""
    
    def __init__(self):
        """Initialize BigQuery client with performance optimizations"""
        self.project_id = settings.google_cloud_project_id
        self.dataset_id = settings.bigquery_dataset_id
        self.table_id = settings.bigquery_table_id
        
        # Job configuration for optimal performance
        self.job_config = bigquery.QueryJobConfig(
            use_query_cache=True,
            use_legacy_sql=False,
            maximum_bytes_billed=2000000000,  # 2GB limit
        )
        
        # Initialize client with performance settings
        if settings.google_application_credentials and os.path.exists(settings.google_application_credentials):
            logger.info("Using service account credentials from file")
            credentials = service_account.Credentials.from_service_account_file(
                settings.google_application_credentials
            )
            self.client = bigquery.Client(
                credentials=credentials, 
                project=self.project_id,
                default_query_job_config=self.job_config
            )
        else:
            logger.info("Using Application Default Credentials (ADC)")
            self.client = bigquery.Client(
                project=self.project_id,
                default_query_job_config=self.job_config
            )
        
        logger.info(f"BigQuery client initialized for project: {self.project_id}")
        logger.info(f"Target table: {self.project_id}.{self.dataset_id}.{self.table_id}")
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache entry is still valid"""
        if cache_key not in _cache["cache_timestamps"]:
            return False
        
        cache_time = _cache["cache_timestamps"][cache_key]
        return (datetime.now() - cache_time).total_seconds() < CACHE_TTL
    
    def _set_cache(self, cache_key: str, data: Any, cache_type: str = "unique_values"):
        """Set cache entry with timestamp"""
        _cache[cache_type][cache_key] = data
        _cache["cache_timestamps"][cache_key] = datetime.now()
    
    def _get_cache(self, cache_key: str, cache_type: str = "unique_values") -> Optional[Any]:
        """Get cache entry if valid"""
        if self._is_cache_valid(cache_key):
            return _cache[cache_type].get(cache_key)
        return None
    
    def _build_parameterized_where_conditions(self, query_params: ForecastQuery) -> Tuple[str, Dict[str, Any]]:
        """Build parameterized WHERE conditions for BigQuery - prevents SQL injection and optimizes queries"""
        conditions = []
        params = {}
        
        if query_params.site_id is not None:
            conditions.append("site_id = @site_id")
            params["site_id"] = query_params.site_id
        
        if query_params.brand is not None:
            conditions.append("brand = @brand")
            params["brand"] = query_params.brand
        
        if query_params.mh_segment:
            conditions.append("mh_segment = @mh_segment")
            params["mh_segment"] = query_params.mh_segment
        
        if query_params.mh_family:
            conditions.append("mh_family = @mh_family")
            params["mh_family"] = query_params.mh_family
        
        if query_params.mh_class:
            conditions.append("mh_class = @mh_class")
            params["mh_class"] = query_params.mh_class
        
        if query_params.mh_brick:
            conditions.append("mh_brick = @mh_brick")
            params["mh_brick"] = query_params.mh_brick
        
        if query_params.product_id:
            conditions.append("product_id = @product_id")
            params["product_id"] = query_params.product_id
        
        if query_params.forecast_run_id:
            conditions.append("forecast_run_id = @forecast_run_id")
            params["forecast_run_id"] = query_params.forecast_run_id
        
        if query_params.model_used:
            conditions.append("model_used = @model_used")
            params["model_used"] = query_params.model_used
        
        if query_params.start_date:
            conditions.append("forecast_week >= @start_date")
            params["start_date"] = query_params.start_date
        
        if query_params.end_date:
            conditions.append("forecast_week <= @end_date")
            params["end_date"] = query_params.end_date
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        return where_clause, params
    
    def _build_optimized_query(self, query_params: ForecastQuery) -> Tuple[str, Dict[str, Any]]:
        """Build highly optimized parameterized SQL query with BigQuery best practices"""
        
        # Use column selection optimized for BigQuery
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
        
        # Build parameterized WHERE conditions
        where_clause, params = self._build_parameterized_where_conditions(query_params)
        base_query += where_clause
        
        # Optimized ORDER BY with clustering hints
        base_query += " ORDER BY forecast_datetime DESC, forecast_week ASC"
        
        # Add pagination parameters
        base_query += " LIMIT @limit OFFSET @offset"
        params["limit"] = query_params.limit
        params["offset"] = query_params.offset
        
        return base_query, params
    
    def _build_optimized_count_query(self, query_params: ForecastQuery) -> Tuple[str, Dict[str, Any]]:
        """Build optimized count query with parameterization"""
        
        base_query = f"""
        SELECT COUNT(*) as total_count
        FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
        """
        
        # Build parameterized WHERE conditions
        where_clause, params = self._build_parameterized_where_conditions(query_params)
        base_query += where_clause
        
        return base_query, params
    
    async def _execute_query_async(self, query: str, params: Dict[str, Any] = None) -> bigquery.QueryJob:
        """Execute BigQuery query asynchronously with parameters"""
        
        def run_query():
            query_params = []
            if params:
                for key, value in params.items():
                    # Properly map Python types to BigQuery types
                    if isinstance(value, str):
                        param_type = "STRING"
                    elif isinstance(value, int):
                        param_type = "INT64"
                    elif isinstance(value, float):
                        param_type = "FLOAT64"
                    elif isinstance(value, bool):
                        param_type = "BOOL"
                    elif isinstance(value, date):
                        param_type = "DATE"
                    elif isinstance(value, datetime):
                        param_type = "DATETIME"
                    else:
                        param_type = "STRING"  # Default fallback
                    
                    query_params.append(bigquery.ScalarQueryParameter(key, param_type, value))
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=query_params,
                use_query_cache=True,
                use_legacy_sql=False,
                dry_run=False,
            )
            
            return self.client.query(query, job_config=job_config)
        
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        query_job = await loop.run_in_executor(THREAD_POOL, run_query)
        
        # Wait for completion asynchronously with timeout
        def wait_for_completion():
            return query_job.result(timeout=60)  # 60 second timeout
        
        await loop.run_in_executor(THREAD_POOL, wait_for_completion)
        return query_job
    
    async def get_forecast_data(self, query_params: ForecastQuery) -> Tuple[List[ForecastRecord], int]:
        """Fetch forecast data with async operations and direct BigQuery filtering"""
        
        try:
            # Build optimized queries with parameters
            data_query, data_params = self._build_optimized_query(query_params)
            count_query, count_params = self._build_optimized_count_query(query_params)
            
            # Execute both queries concurrently for maximum performance
            async def execute_data_query():
                job = await self._execute_query_async(data_query, data_params)
                return list(job.result())
            
            async def execute_count_query():
                job = await self._execute_query_async(count_query, count_params)
                return list(job.result())[0].total_count
            
            # Run both queries in parallel
            data_results, total_count = await asyncio.gather(
                execute_data_query(),
                execute_count_query()
            )
            
            # Optimize record creation with direct attribute access
            forecast_records = [
                ForecastRecord(
                    forecast_datetime=row.forecast_datetime,
                    forecast_run_id=row.forecast_run_id,
                    site_id=row.site_id,
                    brand=row.brand,
                    mh_segment=row.mh_segment,
                    mh_family=row.mh_family,
                    mh_class=row.mh_class,
                    mh_brick=row.mh_brick,
                    product_id=row.product_id,
                    forecast_week=row.forecast_week,
                    actual_qty=row.actual_qty,
                    predicted_qty=row.predicted_qty,
                    model_used=row.model_used,
                    qty_group=row.qty_group,
                    forecast_week_number=row.forecast_week_number,
                    training_data_max_date=row.training_data_max_date,
                    forecast_horizon=row.forecast_horizon,
                    created_at=row.created_at
                )
                for row in data_results
            ]
            
            logger.info(f"Fetched {len(forecast_records)} records out of {total_count} total")
            return forecast_records, total_count
        
        except Exception as e:
            logger.error(f"Error in get_forecast_data: {str(e)}")
            raise Exception(f"Error fetching data from BigQuery: {str(e)}")
    
    async def get_unique_values(self, column_name: str) -> List[str]:
        """Get unique values with caching and direct BigQuery filtering"""
        
        # Check cache first
        cache_key = f"unique_{column_name}"
        cached_result = self._get_cache(cache_key)
        if cached_result is not None:
            logger.info(f"Cache hit for unique values of {column_name}")
            return cached_result
        
        try:
            # Optimized query with parameterization
            query = f"""
            SELECT DISTINCT {column_name}
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE {column_name} IS NOT NULL
            ORDER BY {column_name}
            LIMIT 1000
            """
            
            query_job = await self._execute_query_async(query)
            results = query_job.result()
            
            unique_values = [str(row[0]) for row in results]
            
            # Cache the result
            self._set_cache(cache_key, unique_values)
            logger.info(f"Cached {len(unique_values)} unique values for {column_name}")
            
            return unique_values
        
        except Exception as e:
            logger.error(f"Error in get_unique_values for {column_name}: {str(e)}")
            raise Exception(f"Error fetching unique values for {column_name}: {str(e)}")
    
    async def get_all_unique_values(self) -> Dict[str, List[str]]:
        """Get unique values for site_id and brand with parallel execution and caching"""
        
        # Check cache first
        cache_key = "all_unique_values"
        cached_result = self._get_cache(cache_key)
        if cached_result is not None:
            logger.info("Cache hit for all unique values")
            return cached_result
        
        columns = ["site_id", "brand"]
        
        try:
            # Execute all queries in parallel for maximum performance
            async def get_column_unique_values(column: str):
                query = f"""
                SELECT DISTINCT {column}
                FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
                WHERE {column} IS NOT NULL
                ORDER BY {column}
                LIMIT 1000
                """
                
                query_job = await self._execute_query_async(query)
                results = query_job.result()
                return column, [str(row[0]) for row in results]
            
            # Run all column queries concurrently
            column_tasks = [get_column_unique_values(column) for column in columns]
            column_results = await asyncio.gather(*column_tasks)
            
            # Build result dictionary
            result = {column: values for column, values in column_results}
            
            # Cache the result
            self._set_cache(cache_key, result)
            logger.info(f"Cached unique values for {len(columns)} columns")
            
            return result
        
        except Exception as e:
            logger.error(f"Error in get_all_unique_values: {str(e)}")
            raise Exception(f"Error fetching unique values for all columns: {str(e)}")
    
    async def get_summary_stats(self, query_params: ForecastQuery) -> Dict[str, Any]:
        """Get summary statistics with caching and direct BigQuery aggregation"""
        
        # Create cache key based on query parameters
        cache_key = f"summary_{abs(hash(str(query_params)))}"
        cached_result = self._get_cache(cache_key, "summary_stats")
        if cached_result is not None:
            logger.info("Cache hit for summary statistics")
            return cached_result
        
        try:
            # Highly optimized summary query with direct BigQuery aggregation
            base_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT site_id) as unique_sites,
                COUNT(DISTINCT brand) as unique_brands,
                COUNT(DISTINCT product_id) as unique_products,
                COUNT(DISTINCT forecast_run_id) as unique_forecast_runs,
                ROUND(AVG(actual_qty), 2) as avg_actual_qty,
                ROUND(AVG(predicted_qty), 2) as avg_predicted_qty,
                ROUND(SUM(actual_qty), 2) as total_actual_qty,
                ROUND(SUM(predicted_qty), 2) as total_predicted_qty,
                MIN(forecast_week) as min_forecast_week,
                MAX(forecast_week) as max_forecast_week
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            """
            
            # Add parameterized WHERE conditions
            where_clause, params = self._build_parameterized_where_conditions(query_params)
            base_query += where_clause
            
            query_job = await self._execute_query_async(base_query, params)
            results = query_job.result()
            
            row = list(results)[0]
            summary_data = {
                "total_records": int(row.total_records) if row.total_records else 0,
                "unique_sites": int(row.unique_sites) if row.unique_sites else 0,
                "unique_brands": int(row.unique_brands) if row.unique_brands else 0,
                "unique_products": int(row.unique_products) if row.unique_products else 0,
                "unique_forecast_runs": int(row.unique_forecast_runs) if row.unique_forecast_runs else 0,
                "avg_actual_qty": float(row.avg_actual_qty) if row.avg_actual_qty else 0,
                "avg_predicted_qty": float(row.avg_predicted_qty) if row.avg_predicted_qty else 0,
                "total_actual_qty": float(row.total_actual_qty) if row.total_actual_qty else 0,
                "total_predicted_qty": float(row.total_predicted_qty) if row.total_predicted_qty else 0,
                "min_forecast_week": row.min_forecast_week,
                "max_forecast_week": row.max_forecast_week
            }
            
            # Cache the result
            self._set_cache(cache_key, summary_data, "summary_stats")
            logger.info("Cached summary statistics")
            
            return summary_data
        
        except Exception as e:
            logger.error(f"Error in get_summary_stats: {str(e)}")
            raise Exception(f"Error fetching summary statistics: {str(e)}")
    
    def clear_cache(self, cache_type: str = None):
        """Clear cache entries"""
        if cache_type:
            _cache[cache_type].clear()
            # Clear corresponding timestamps
            keys_to_remove = [key for key in _cache["cache_timestamps"] 
                            if key.startswith(cache_type.replace("_", ""))]
            for key in keys_to_remove:
                del _cache["cache_timestamps"][key]
            logger.info(f"Cleared {cache_type} cache")
        else:
            # Clear all caches
            _cache["unique_values"].clear()
            _cache["summary_stats"].clear()
            _cache["cache_timestamps"].clear()
            logger.info("Cleared all caches")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            "unique_values_cached": len(_cache["unique_values"]),
            "summary_stats_cached": len(_cache["summary_stats"]),
            "total_cached_items": len(_cache["cache_timestamps"]),
            "cache_ttl_seconds": CACHE_TTL,
            "thread_pool_workers": THREAD_POOL._max_workers
        }


# Global service instance
bigquery_service = BigQueryService() 