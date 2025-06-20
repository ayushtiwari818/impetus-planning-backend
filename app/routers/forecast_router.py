from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Query
from datetime import date
import asyncio
import time
from app.models.forecast_model import ForecastRecord, ForecastQuery, ForecastResponse
from app.services.bigquery_service import bigquery_service

router = APIRouter(prefix="/forecast", tags=["forecast"])


@router.get("/", response_model=ForecastResponse)
async def get_forecast_data(
    site_id: str = Query(None, description="Filter by site ID"),
    brand: str = Query(None, description="Filter by brand ID"),
    mh_segment: str = Query(None, description="Filter by merchandise segment"),
    mh_family: str = Query(None, description="Filter by merchandise family"),
    mh_class: str = Query(None, description="Filter by merchandise class"),
    mh_brick: str = Query(None, description="Filter by merchandise brick"),
    product_id: str = Query(None, description="Filter by product ID"),
    forecast_run_id: str = Query(None, description="Filter by forecast run ID"),
    model_used: str = Query(None, description="Filter by model used"),
    start_date: date = Query(None, description="Start date for forecast week filter"),
    end_date: date = Query(None, description="End date for forecast week filter"),
    limit: int = Query(100, description="Maximum number of records to return", ge=1, le=1000),
    offset: int = Query(0, description="Number of records to skip", ge=0),
):
    """
    Get forecast data with optional filtering and pagination - OPTIMIZED.
    
    This endpoint retrieves forecast data from BigQuery with:
    - Direct BigQuery filtering (no post-processing)
    - Asynchronous parallel query execution
    - Parameterized queries for security and performance
    - Intelligent caching
    - Concurrent count and data queries
    """
    start_time = time.time()
    
    try:
        # Create query parameters object
        query_params = ForecastQuery(
            site_id=site_id,
            brand=brand,
            mh_segment=mh_segment,
            mh_family=mh_family,
            mh_class=mh_class,
            mh_brick=mh_brick,
            product_id=product_id,
            forecast_run_id=forecast_run_id,
            model_used=model_used,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset
        )
        
        # Fetch data using async operations with direct BigQuery filtering
        forecast_records, total_count = await bigquery_service.get_forecast_data(query_params)
        
        # Calculate pagination info
        current_page = (offset // limit) + 1
        has_next = offset + limit < total_count
        
        processing_time = time.time() - start_time
        
        return ForecastResponse(
            data=forecast_records,
            total_records=total_count,
            page=current_page,
            page_size=limit,
            has_next=has_next,
            # Add performance metrics
            metadata={
                "processing_time_seconds": round(processing_time, 3),
                "records_returned": len(forecast_records),
                "filters_applied": sum(1 for param in [site_id, brand, mh_segment, mh_family, mh_class, mh_brick, product_id, forecast_run_id, model_used, start_date, end_date] if param is not None),
                "query_optimized": True,
                "async_execution": True
            }
        )
    
    except Exception as e:
        processing_time = time.time() - start_time
        raise HTTPException(
            status_code=500, 
            detail={
                "error": f"Error fetching forecast data: {str(e)}",
                "processing_time_seconds": round(processing_time, 3),
                "optimization_enabled": True
            }
        )


@router.get("/summary", response_model=Dict[str, Any])
async def get_forecast_summary(
    site_id: str = Query(None, description="Filter by site ID"),
    brand: str = Query(None, description="Filter by brand ID"),
    mh_segment: str = Query(None, description="Filter by merchandise segment"),
    mh_family: str = Query(None, description="Filter by merchandise family"),
    mh_class: str = Query(None, description="Filter by merchandise class"),
    mh_brick: str = Query(None, description="Filter by merchandise brick"),
    product_id: str = Query(None, description="Filter by product ID"),
    forecast_run_id: str = Query(None, description="Filter by forecast run ID"),
    model_used: str = Query(None, description="Filter by model used"),
    start_date: date = Query(None, description="Start date for forecast week filter"),
    end_date: date = Query(None, description="End date for forecast week filter"),
):
    """
    Get summary statistics for forecast data - OPTIMIZED.
    
    Returns aggregate statistics with:
    - Direct BigQuery aggregation (no memory loading)
    - Parameterized queries for security
    - Intelligent caching
    - Async execution
    """
    start_time = time.time()
    
    try:
        # Create query parameters object
        query_params = ForecastQuery(
            site_id=site_id,
            brand=brand,
            mh_segment=mh_segment,
            mh_family=mh_family,
            mh_class=mh_class,
            mh_brick=mh_brick,
            product_id=product_id,
            forecast_run_id=forecast_run_id,
            model_used=model_used,
            start_date=start_date,
            end_date=end_date,
            limit=1000000,  # Set high limit for summary stats
            offset=0
        )
        
        # Get summary statistics with async execution
        summary_stats = await bigquery_service.get_summary_stats(query_params)
        
        processing_time = time.time() - start_time
        
        # Add performance metadata
        summary_stats["performance_metrics"] = {
            "processing_time_seconds": round(processing_time, 3),
            "direct_bigquery_aggregation": True,
            "async_execution": True,
            "caching_enabled": True
        }
        
        return summary_stats
    
    except Exception as e:
        processing_time = time.time() - start_time
        raise HTTPException(
            status_code=500, 
            detail={
                "error": f"Error fetching summary statistics: {str(e)}",
                "processing_time_seconds": round(processing_time, 3)
            }
        )


@router.get("/unique-values/{column_name}")
async def get_unique_values(column_name: str):
    """
    Get unique values for a specific column - OPTIMIZED.
    
    Features:
    - Direct BigQuery DISTINCT queries
    - Intelligent caching (5 min TTL)
    - Async execution
    - Limited results for performance
    """
    start_time = time.time()
    
    try:
        # Validate column name to prevent SQL injection
        allowed_columns = [
            "site_id", "brand", "mh_segment", "mh_family", 
            "mh_class", "mh_brick", "product_id", "model_used", "qty_group"
        ]
        
        if column_name not in allowed_columns:
            raise HTTPException(
                status_code=400, 
                detail=f"Column '{column_name}' is not supported. Allowed columns: {allowed_columns}"
            )
        
        # Fetch unique values with async execution
        unique_values = await bigquery_service.get_unique_values(column_name)
        
        processing_time = time.time() - start_time
        
        return {
            "column_name": column_name,
            "unique_values": unique_values,
            "count": len(unique_values),
            "performance_metrics": {
                "processing_time_seconds": round(processing_time, 3),
                "cached_result": processing_time < 0.1,  # If very fast, likely cached
                "async_execution": True,
                "direct_bigquery_query": True
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        processing_time = time.time() - start_time
        raise HTTPException(
            status_code=500, 
            detail={
                "error": f"Error fetching unique values: {str(e)}",
                "processing_time_seconds": round(processing_time, 3)
            }
        )


@router.get("/unique-values")
async def get_all_unique_values():
    """
    Get unique values for site_id and brand columns - OPTIMIZED.
    
    Features:
    - Parallel execution of multiple DISTINCT queries
    - Direct BigQuery filtering
    - Intelligent caching
    - Concurrent async operations
    """
    start_time = time.time()
    
    try:
        # Fetch all unique values with parallel async execution
        all_unique_values = await bigquery_service.get_all_unique_values()
        
        processing_time = time.time() - start_time
        
        # Calculate total counts for each column
        result = {}
        for column_name, values in all_unique_values.items():
            result[column_name] = {
                "unique_values": values,
                "count": len(values)
            }
        
        return {
            "columns": result,
            "total_columns": len(result),
            "message": "Successfully retrieved unique values for site_id and brand columns",
            "performance_metrics": {
                "processing_time_seconds": round(processing_time, 3),
                "parallel_execution": True,
                "cached_result": processing_time < 0.2,  # If very fast, likely cached
                "async_execution": True,
                "concurrent_queries": len(all_unique_values)
            }
        }
    
    except Exception as e:
        processing_time = time.time() - start_time
        raise HTTPException(
            status_code=500, 
            detail={
                "error": f"Error fetching all unique values: {str(e)}",
                "processing_time_seconds": round(processing_time, 3)
            }
        )


@router.get("/cache/stats")
async def get_cache_stats():
    """Get cache statistics and performance metrics - OPTIMIZED"""
    try:
        cache_stats = bigquery_service.get_cache_stats()
        return {
            "cache_statistics": cache_stats,
            "status": "Cache statistics retrieved successfully",
            "performance_optimizations": {
                "caching_enabled": True,
                "parallel_execution": True,
                "async_operations": True,
                "direct_bigquery_filtering": True,
                "parameterized_queries": True
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting cache stats: {str(e)}")


@router.delete("/cache")
async def clear_cache(cache_type: str = Query(None, description="Type of cache to clear (unique_values, summary_stats, or all)")):
    """Clear cache entries for better performance management - OPTIMIZED"""
    try:
        if cache_type and cache_type not in ["unique_values", "summary_stats", "all"]:
            raise HTTPException(
                status_code=400,
                detail="Invalid cache_type. Must be 'unique_values', 'summary_stats', or 'all'"
            )
        
        if cache_type == "all":
            bigquery_service.clear_cache()
        else:
            bigquery_service.clear_cache(cache_type)
        
        return {
            "status": "success",
            "message": f"Cache cleared successfully" + (f" for {cache_type}" if cache_type else ""),
            "performance_impact": "Next queries will rebuild cache but benefit from fresh data"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error clearing cache: {str(e)}")


@router.get("/health")
async def health_check():
    """Health check endpoint with comprehensive performance metrics - OPTIMIZED"""
    start_time = time.time()
    
    try:
        # Test BigQuery connection with minimal query
        query_params = ForecastQuery(limit=1, offset=0)
        _, total_count = await bigquery_service.get_forecast_data(query_params)
        
        # Get cache stats for performance monitoring
        cache_stats = bigquery_service.get_cache_stats()
        
        processing_time = time.time() - start_time
        
        return {
            "status": "healthy",
            "message": "Forecast service is operational with advanced performance optimizations",
            "total_records_available": total_count,
            "cache_statistics": cache_stats,
            "performance_features": [
                "Asynchronous query execution",
                "Parallel BigQuery operations", 
                "Direct BigQuery filtering",
                "Parameterized queries for security",
                "Intelligent caching with TTL",
                "Thread pool optimization",
                "Connection pooling",
                "Query result streaming",
                "Memory-efficient processing"
            ],
            "health_check_metrics": {
                "response_time_seconds": round(processing_time, 3),
                "bigquery_connection": "healthy",
                "async_operations": "enabled",
                "performance_optimizations": "active"
            }
        }
    
    except Exception as e:
        processing_time = time.time() - start_time
        raise HTTPException(
            status_code=503, 
            detail={
                "status": "unhealthy",
                "error": f"Service unhealthy - BigQuery connection failed: {str(e)}",
                "response_time_seconds": round(processing_time, 3)
            }
        ) 