from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Query
from datetime import date
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
    Get forecast data with optional filtering and pagination.
    
    This endpoint retrieves forecast data from BigQuery with support for:
    - Filtering by various attributes (site, brand, product hierarchy, etc.)
    - Date range filtering
    - Pagination
    - Sorting by forecast datetime and forecast week
    """
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
        
        # Fetch data from BigQuery
        forecast_records, total_count = await bigquery_service.get_forecast_data(query_params)
        
        # Calculate pagination info
        current_page = (offset // limit) + 1
        has_next = offset + limit < total_count
        
        return ForecastResponse(
            data=forecast_records,
            total_records=total_count,
            page=current_page,
            page_size=limit,
            has_next=has_next
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching forecast data: {str(e)}")


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
    Get summary statistics for forecast data.
    
    Returns aggregate statistics including:
    - Total number of records
    - Unique counts for sites, brands, products, etc.
    - Average and total quantities (actual vs predicted)
    - Date ranges
    """
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
        
        # Get summary statistics
        summary_stats = await bigquery_service.get_summary_stats(query_params)
        
        return summary_stats
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching summary statistics: {str(e)}")


@router.get("/unique-values/{column_name}")
async def get_unique_values(column_name: str):
    """
    Get unique values for a specific column.
    
    Supported columns:
    - site_id
    - brand
    - mh_segment
    - mh_family
    - mh_class
    - mh_brick
    - product_id
    - model_used
    - qty_group
    """
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
        
        unique_values = await bigquery_service.get_unique_values(column_name)
        
        return {
            "column_name": column_name,
            "unique_values": unique_values,
            "count": len(unique_values)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching unique values: {str(e)}")


@router.get("/health")
async def health_check():
    """Health check endpoint for the forecast service"""
    try:
        # Test BigQuery connection by running a simple query
        query_params = ForecastQuery(limit=1, offset=0)
        _, total_count = await bigquery_service.get_forecast_data(query_params)
        
        return {
            "status": "healthy",
            "message": "Forecast service is operational",
            "total_records_available": total_count
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=503, 
            detail=f"Service unhealthy - BigQuery connection failed: {str(e)}"
        ) 