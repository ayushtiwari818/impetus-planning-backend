from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel, Field
from uuid import UUID


class ForecastRecord(BaseModel):
    """Pydantic model for forecast data records from BigQuery"""
    
    forecast_datetime: datetime = Field(..., description="Timestamp when forecast was generated")
    forecast_run_id: str = Field(..., description="Unique identifier for the forecast run")
    site_id: str = Field(..., description="Site identifier")
    brand: str = Field(None, description="Brand identifier")
    mh_segment: str = Field(None, description="Merchandise hierarchy segment")
    mh_family: str = Field(None, description="Merchandise hierarchy family")
    mh_class: str = Field(None, description="Merchandise hierarchy class")
    mh_brick: str = Field(None, description="Merchandise hierarchy brick")
    product_id: str = Field(None, description="Product identifier")
    forecast_week: date = Field(..., description="Week for which forecast is made")
    actual_qty: Optional[float] = Field(None, description="Actual quantity sold")
    predicted_qty: float = Field(..., description="Predicted quantity")
    model_used: str = Field(None, description="ML model used for prediction")
    qty_group: Optional[int] = Field(None, description="Quantity group classification")
    forecast_week_number: Optional[int] = Field(None, description="Week number in forecast")
    training_data_max_date: Optional[date] = Field(None, description="Maximum date in training data")
    forecast_horizon: Optional[int] = Field(None, description="Forecast horizon in weeks")
    created_at: Optional[datetime] = Field(None, description="Record creation timestamp")

    class Config:
        """Pydantic configuration"""
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat()
        }


class ForecastQuery(BaseModel):
    """Query parameters for forecast data"""
    
    site_id: Optional[str] = Field(None, description="Filter by site ID")
    brand: Optional[str] = Field(None, description="Filter by brand ID")
    mh_segment: Optional[str] = Field(None, description="Filter by merchandise segment")
    mh_family: Optional[str] = Field(None, description="Filter by merchandise family")
    mh_class: Optional[str] = Field(None, description="Filter by merchandise class")
    mh_brick: Optional[str] = Field(None, description="Filter by merchandise brick")
    product_id: Optional[str] = Field(None, description="Filter by product ID")
    forecast_run_id: Optional[str] = Field(None, description="Filter by forecast run ID")
    model_used: Optional[str] = Field(None, description="Filter by model used")
    start_date: Optional[date] = Field(None, description="Start date for forecast week filter")
    end_date: Optional[date] = Field(None, description="End date for forecast week filter")
    limit: Optional[int] = Field(100, description="Maximum number of records to return")
    offset: Optional[int] = Field(0, description="Number of records to skip")


class ForecastResponse(BaseModel):
    """Response model for forecast data"""
    
    data: list[ForecastRecord]
    total_records: int
    page: int
    page_size: int
    has_next: bool 