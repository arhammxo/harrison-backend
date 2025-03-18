from fastapi import FastAPI, HTTPException, Query, Path, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict, Any
import sqlite3
from pydantic import BaseModel, Field, validator
import json
import math
from datetime import datetime
import os
import subprocess
from google.cloud import storage

app = FastAPI(
    title="Real Estate Investment API",
    description="API for analyzing real estate investment properties with detailed metrics and neighborhood analysis",
    version="1.1.0"
)

# Enable CORS for frontend development
# Fix the CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://harrison-realestate.vercel.app",  # Removed trailing slash
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

LOCAL_TESTING = False  # Changed to True for local testing


# Initialize at startup
def download_db_at_startup():
    """Download database file from GCS at startup with better error handling and retries"""
    db_path = 'final.db' if LOCAL_TESTING else '/tmp/final.db'
    
    # If running in local testing mode, skip download
    if LOCAL_TESTING:
        print("Running in local testing mode - using local database file")
        if not os.path.exists(db_path):
            print(f"WARNING: Local database '{db_path}' does not exist!")
            # Create empty database if needed for local testing
            conn = sqlite3.connect(db_path)
            conn.close()
        return True
    
    # For Cloud Run deployment
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Check if database already exists
            if os.path.exists(db_path):
                print(f"Database already exists at {db_path}")
                # Verify database is valid by trying a simple query
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.execute("PRAGMA integrity_check")
                    result = cursor.fetchone()
                    conn.close()
                    if result and result[0] == "ok":
                        print("Database integrity check passed")
                        return True
                    else:
                        print("Database integrity check failed, redownloading")
                        os.remove(db_path)
                except Exception as e:
                    print(f"Database verification failed: {str(e)}, redownloading")
                    os.remove(db_path)
            
            # Download database file
            print(f"Downloading database from GCS (attempt {retry_count + 1}/{max_retries})...")
            client = storage.Client()
            bucket = client.bucket('arhammxo-hdb')
            blob = bucket.blob('final.db')
            
            # Use a shorter timeout to allow for retries
            blob.download_to_filename(db_path, timeout=120)
            
            # Verify download was successful
            if os.path.exists(db_path) and os.path.getsize(db_path) > 0:
                print(f"Database downloaded successfully ({os.path.getsize(db_path)} bytes)")
                return True
            else:
                print("Downloaded file is empty or doesn't exist")
                retry_count += 1
                
        except Exception as e:
            print(f"Database download failed (attempt {retry_count + 1}/{max_retries}): {str(e)}")
            retry_count += 1
            # Wait before retrying
            import time
            time.sleep(2)
    
    print("All database download attempts failed")
    # At this point, we failed to download the database after multiple attempts
    # Return False to indicate failure
    return False

# Call this function early in your app initialization
db_initialized = download_db_at_startup()

# Add startup event to ensure database is ready
@app.on_event("startup")
async def startup_event():
    """Verify database connectivity on app startup"""
    if not db_initialized:
        # Log the error but allow app to start (optional - you can also raise an exception to prevent startup)
        print("WARNING: Database initialization failed during startup! Application may not function correctly.")
    
    try:
        # Test database connection
        conn = get_db_connection()
        cursor = conn.execute("SELECT sqlite_version();")
        version = cursor.fetchone()[0]
        print(f"Successfully connected to SQLite database (version {version})")
        conn.close()
    except Exception as e:
        print(f"ERROR: Database connection test failed: {str(e)}")
        # Optionally, you can raise the exception to prevent the app from starting
        # raise e

# Improve the health check to better report application status
@app.get("/health", tags=["System"])
async def health_check():
    """Check API health with detailed database status"""
    health_status = {
        "status": "unhealthy",
        "database_initialized": db_initialized,
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        conn = get_db_connection()
        cursor = conn.execute("SELECT COUNT(*) FROM properties")
        property_count = cursor.fetchone()[0]
        conn.close()
        
        health_status.update({
            "status": "healthy",
            "database": "connected",
            "property_count": property_count
        })
    except Exception as e:
        health_status.update({
            "database": "disconnected",
            "error": str(e)
        })
    
    return health_status

# Modify your connection function
def get_db_connection():
    db_path = 'final.db' if LOCAL_TESTING else '/tmp/final.db'
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

# Base property model with all requested fields
class PropertyBase(BaseModel):
    property_id: int
    text: Optional[str] = None
    style: Optional[str] = None
    full_street_line: str
    street: Optional[str] = None
    unit: Optional[str] = None
    city: str
    state: str
    zip_code: int
    beds: float
    full_baths: Optional[float] = 0
    half_baths: Optional[float] = 0
    baths: Optional[float] = None
    sqft: float
    year_built: Optional[int] = None
    list_price: float
    list_date: Optional[str] = None
    sold_price: Optional[float] = None
    last_sold_date: Optional[str] = None
    assessed_value: Optional[float] = None
    estimated_value: Optional[float] = None
    tax: Optional[float] = None
    tax_history: Optional[str] = None
    price_per_sqft: Optional[float] = None
    neighborhoods: Optional[str] = None
    hoa_fee: Optional[float] = None
    primary_photo: Optional[str] = None
    alt_photos: Optional[str] = None
    
    @validator('beds', 'full_baths', 'half_baths', 'sqft', 'list_price')
    def validate_non_negative(cls, v, values, **kwargs):
        if v is not None and v < 0:
            raise ValueError("Value cannot be negative")
        return v

    @validator('baths', always=True)
    def calculate_baths(cls, v, values):
        if v is not None:
            return v
        full = values.get('full_baths') or 0
        half = values.get('half_baths') or 0
        return full + (0.5 * half)

# Full property model with investment metrics
class Property(PropertyBase):
    days_on_mls: Optional[int] = None
    zori_monthly_rent: Optional[float] = None
    zori_annual_rent: Optional[float] = None
    zori_growth_rate: Optional[float] = None
    cap_rate: Optional[float] = None
    cash_yield: Optional[float] = None
    irr: Optional[float] = None
    cash_on_cash: Optional[float] = None
    total_return: Optional[float] = None
    monthly_payment: Optional[float] = None
    lcf_year1: Optional[float] = None
    equity_at_exit: Optional[float] = None
    cash_equity: Optional[float] = None
    investment_ranking: Optional[int] = None
    broker_name: Optional[str] = None
    broker_id: Optional[int] = None
    broker_email: Optional[str] = None
    broker_phones: Optional[str] = None
    
    class Config:
        schema_extra = {
            "example": {
                "property_id": 12345,
                "full_street_line": "123 Main St",
                "city": "New York",
                "state": "NY",
                "zip_code": 10001,
                "beds": 2,
                "full_baths": 2,
                "half_baths": 0,
                "baths": 2.0,
                "sqft": 1200,
                "list_price": 750000,
                "style": "Condo",
                "year_built": 2005,
                "price_per_sqft": 625,
                "primary_photo": "https://example.com/photos/123.jpg",
                "zori_monthly_rent": 4200,
                "cap_rate": 5.2,
                "cash_yield": 4.8,
                "irr": 12.5,
                "cash_on_cash": 1.8,
                "lcf_year1": 12000,
                "total_return": 2.35,
                "investment_ranking": 8,
                "broker_name": "ABC Realty",
                "broker_email": "broker@example.com",
                "broker_phones": "123-456-7890"
            }
        }

# Property detail model with all fields
class PropertyDetail(Property):
    noi_year1: Optional[float] = None
    noi_year2: Optional[float] = None
    noi_year3: Optional[float] = None
    noi_year4: Optional[float] = None
    noi_year5: Optional[float] = None
    ucf_year1: Optional[float] = None
    ucf_year2: Optional[float] = None
    ucf_year3: Optional[float] = None
    ucf_year4: Optional[float] = None
    ucf_year5: Optional[float] = None
    lcf_year1: Optional[float] = None
    lcf_year2: Optional[float] = None
    lcf_year3: Optional[float] = None
    lcf_year4: Optional[float] = None
    lcf_year5: Optional[float] = None
    loan_amount: Optional[float] = None
    annual_debt_service: Optional[float] = None
    down_payment_pct: Optional[float] = None
    interest_rate: Optional[float] = None
    loan_term: Optional[int] = None
    exit_cap_rate: Optional[float] = None
    exit_value: Optional[float] = None
    accumulated_cash_flow: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    parking_garage: Optional[int] = None
    lot_sqft: Optional[float] = None
    broker_email: Optional[str] = None
    broker_phones: Optional[str] = None
    agent_id: Optional[int] = None
    agent_name: Optional[str] = None
    agent_email: Optional[str] = None
    agent_phones: Optional[str] = None
    office_name: Optional[str] = None
    office_phones: Optional[str] = None
    
# Enhanced property search result model with essential data
class PropertySearchResult(BaseModel):
    property_id: int
    primary_photo: Optional[str] = None
    full_street_line: str
    city: str
    state: str
    zip_code: int
    beds: float
    baths: float
    sqft: float
    list_price: float
    price_per_sqft: Optional[float] = None
    investment_ranking: Optional[int] = None
    cap_rate: Optional[float] = None
    irr: Optional[float] = None
    cash_on_cash: Optional[float] = None
    total_return: Optional[float] = None
    broker_name: Optional[str] = None
    broker_id: Optional[int] = None
    
    class Config:
        schema_extra = {
            "example": {
                "property_id": 12345,
                "primary_photo": "https://example.com/photos/123.jpg",
                "full_street_line": "123 Main St",
                "city": "New York",
                "state": "NY",
                "zip_code": 10001,
                "beds": 2,
                "baths": 2.0,
                "sqft": 1200,
                "list_price": 750000,
                "price_per_sqft": 625,
                "investment_ranking": 8,
                "cap_rate": 5.2,
                "irr": 12.5,
                "cash_on_cash": 1.8,
                "total_return": 2.35,
                "broker_name": "ABC Realty",
                "broker_id": 123
            }
        }
    
# Model for calculation audit data
class CalculationAudit(BaseModel):
    property_id: int
    zori_monthly_rent: Optional[float] = None
    zori_annual_rent: Optional[float] = None
    zori_growth_rate: Optional[float] = None
    gross_rent_multiplier: Optional[float] = None
    tax_used: Optional[float] = None
    hoa_fee_used: Optional[float] = None
    noi_year1: Optional[float] = None
    cap_rate: Optional[float] = None
    down_payment_pct: Optional[float] = None
    interest_rate: Optional[float] = None
    loan_term: Optional[int] = None
    loan_amount: Optional[float] = None
    monthly_payment: Optional[float] = None
    annual_debt_service: Optional[float] = None
    exit_cap_rate: Optional[float] = None
    exit_value: Optional[float] = None
    equity_at_exit: Optional[float] = None
    irr: Optional[float] = None
    cash_on_cash: Optional[float] = None
    total_return: Optional[float] = None

# Enhanced calculation audit with additional details
class EnhancedCalculationAudit(CalculationAudit):
    # Rent calculation breakdown
    monthly_rent_per_sqft: Optional[float] = None
    annual_rent: Optional[float] = None
    
    # Expense breakdown
    property_tax_annual: Optional[float] = None
    insurance_annual: Optional[float] = None
    maintenance_annual: Optional[float] = None
    property_management_annual: Optional[float] = None
    hoa_annual: Optional[float] = None
    vacancy_annual: Optional[float] = None
    total_expenses_annual: Optional[float] = None
    
    # NOI calculation breakdown
    gross_income: Optional[float] = None
    operating_expenses: Optional[float] = None
    net_operating_income: Optional[float] = None
    
    # Cash flow calculation
    annual_cash_flow: Optional[float] = None
    monthly_cash_flow: Optional[float] = None
    
    # ROI components
    cash_investment: Optional[float] = None
    annual_roi_percentage: Optional[float] = None
    five_year_roi_percentage: Optional[float] = None
    
    # Investment ranking factors
    cap_rate_score: Optional[float] = None
    cash_flow_score: Optional[float] = None
    roi_score: Optional[float] = None
    location_score: Optional[float] = None
    final_score: Optional[float] = None

# City/Location models
class City(BaseModel):
    city: str
    state: str
    property_count: int

class ZipCode(BaseModel):
    zip_code: int
    city: str
    state: str
    property_count: int

# Market statistics models
class CityStats(BaseModel):
    city: str
    state: str
    property_count: int
    avg_price: float
    min_price: float
    max_price: float
    avg_rent: float
    avg_cap_rate: float
    avg_cash_yield: float
    avg_irr: float
    avg_cash_on_cash: float
    avg_total_return: Optional[float] = None
    avg_price_per_sqft: float
    avg_annual_cash_flow: Optional[float] = None
    avg_rent_growth_rate: Optional[float] = None
    avg_investment_ranking: Optional[float] = None

class ZipCodeStats(BaseModel):
    zip_code: int
    city: str
    state: str
    property_count: int
    avg_price: float
    min_price: float
    max_price: float
    avg_rent: float
    avg_cap_rate: float
    avg_cash_yield: float
    avg_irr: float
    avg_cash_on_cash: float
    avg_total_return: Optional[float] = None
    avg_price_per_sqft: float
    avg_annual_cash_flow: Optional[float] = None
    avg_rent_growth_rate: Optional[float] = None
    avg_investment_ranking: Optional[float] = None
    neighborhood_quality: Optional[float] = None

# Investment criteria model
class InvestmentCriteria(BaseModel):
    min_cap_rate: float = Field(default=0, ge=0, description="Minimum cap rate (percentage)")
    min_cash_yield: float = Field(default=0, ge=0, description="Minimum cash yield (percentage)")
    min_irr: float = Field(default=0, ge=0, description="Minimum IRR (percentage)")
    min_cash_on_cash: float = Field(default=0, ge=0, description="Minimum cash-on-cash return")
    min_total_return: float = Field(default=0, ge=0, description="Minimum total return multiplier")
    min_monthly_cash_flow: float = Field(default=0, description="Minimum monthly cash flow")
    min_investment_ranking: int = Field(default=0, ge=0, le=10, description="Minimum investment ranking (1-10)")
    max_price: Optional[int] = Field(default=None, gt=0, description="Maximum property price")
    min_price: Optional[int] = Field(default=None, ge=0, description="Minimum property price")
    min_beds: Optional[int] = Field(default=None, ge=0, description="Minimum number of bedrooms")
    min_baths: Optional[float] = Field(default=None, ge=0, description="Minimum number of bathrooms")
    min_sqft: Optional[int] = Field(default=None, gt=0, description="Minimum square footage")
    property_type: Optional[str] = Field(default=None, description="Property type/style")
    
    class Config:
        schema_extra = {
            "example": {
                "min_cap_rate": 5.0,
                "min_cash_yield": 4.0,
                "min_irr": 10.0,
                "min_cash_on_cash": 1.5,
                "min_total_return": 1.5,
                "min_monthly_cash_flow": 500,
                "min_investment_ranking": 7,
                "max_price": 750000,
                "min_price": 250000,
                "min_beds": 2
            }
        }

# ZORI data model
class ZoriData(BaseModel):
    RegionName: str  # ZIP code
    State: str
    City: Optional[str] = None
    Metro: Optional[str] = None
    latest_rent: float
    one_year_ago_rent: Optional[float] = None
    five_years_ago_rent: Optional[float] = None
    one_year_growth: Optional[float] = None
    five_year_cagr: Optional[float] = None

# Neighborhood quality model
class NeighborhoodQuality(BaseModel):
    zip_code: str
    quality_score: float  # Scale 0.75 - 0.95
    
    class Config:
        schema_extra = {
            "example": {
                "zip_code": "10001",
                "quality_score": 0.85
            }
        }

# Model for state overview
class StateOverview(BaseModel):
    state: str
    property_count: int
    city_count: int
    zipcode_count: int
    avg_price: float
    avg_rent: float
    avg_cap_rate: float
    avg_cash_yield: float
    avg_irr: float
    avg_total_return: Optional[float] = None
    avg_investment_ranking: Optional[float] = None
    top_cities: List[Dict[str, Any]]

# Model for investment comparison
class InvestmentComparison(BaseModel):
    property_id: int
    full_street_line: str
    city: str
    state: str
    zip_code: int
    list_price: float
    zori_monthly_rent: Optional[float] = None
    cap_rate: Optional[float] = None
    cash_yield: Optional[float] = None
    irr: Optional[float] = None
    cash_on_cash: Optional[float] = None
    total_return: Optional[float] = None
    lcf_year1: Optional[float] = None
    five_year_return: Optional[float] = None
    equity_at_exit: Optional[float] = None
    monthly_payment: Optional[float] = None
    investment_ranking: Optional[int] = None
    
    class Config:
        schema_extra = {
            "example": {
                "property_id": 12345,
                "full_street_line": "123 Main St",
                "city": "New York",
                "state": "NY",
                "zip_code": 10001,
                "list_price": 750000,
                "zori_monthly_rent": 4200,
                "cap_rate": 5.2,
                "cash_yield": 4.8,
                "irr": 12.5,
                "cash_on_cash": 1.8,
                "total_return": 2.1,
                "lcf_year1": 12000,
                "five_year_return": 240000,
                "equity_at_exit": 975000,
                "monthly_payment": 2800,
                "investment_ranking": 8
            }
        }

# Price range filter parameters
class PriceRangeParams:
    def __init__(
        self,
        min_price: Optional[int] = Query(None, ge=0, description="Minimum property price"),
        max_price: Optional[int] = Query(None, gt=0, description="Maximum property price")
    ):
        self.min_price = min_price
        self.max_price = max_price

# Pagination parameters
def pagination_params(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Number of items per page")
):
    return {"page": page, "page_size": page_size}

# Sorting parameters
def sorting_params(
    sort_by: str = Query("investment_ranking", description="Field to sort by"),
    sort_desc: bool = Query(True, description="Sort in descending order (true) or ascending (false)")
):
    return {"sort_by": sort_by, "sort_desc": sort_desc}

# Response with pagination
class PaginatedResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    results: List[Any]

# Helper function to apply investment criteria to query
def apply_investment_criteria(query, params, criteria: InvestmentCriteria):
    """
    Apply investment criteria to a SQL query and parameter list.
    
    Args:
        query: SQL query string
        params: List of query parameters
        criteria: InvestmentCriteria object
        
    Returns:
        Tuple of (updated query, updated params)
    """
    if criteria.min_cap_rate > 0:
        query += " AND cap_rate >= ?"
        params.append(criteria.min_cap_rate)
        
    if criteria.min_cash_yield > 0:
        query += " AND cash_yield >= ?"
        params.append(criteria.min_cash_yield)
        
    if criteria.min_irr > 0:
        query += " AND irr >= ?"
        params.append(criteria.min_irr)
        
    if criteria.min_cash_on_cash > 0:
        query += " AND cash_on_cash >= ?"
        params.append(criteria.min_cash_on_cash)
        
    if criteria.min_total_return > 0:
        query += " AND total_return >= ?"
        params.append(criteria.min_total_return)
        
    if criteria.min_monthly_cash_flow > 0:
        query += " AND lcf_year1 / 12 >= ?"
        params.append(criteria.min_monthly_cash_flow)
        
    if criteria.min_investment_ranking > 0:
        query += " AND investment_ranking >= ?"
        params.append(criteria.min_investment_ranking)
        
    if criteria.max_price:
        query += " AND list_price <= ?"
        params.append(criteria.max_price)
        
    if criteria.min_price:
        query += " AND list_price >= ?"
        params.append(criteria.min_price)
        
    if criteria.min_beds:
        query += " AND beds >= ?"
        params.append(criteria.min_beds)
        
    if criteria.min_baths:
        query += " AND (full_baths + (half_baths * 0.5)) >= ?"
        params.append(criteria.min_baths)
        
    if criteria.min_sqft:
        query += " AND sqft >= ?"
        params.append(criteria.min_sqft)
        
    if criteria.property_type:
        query += " AND style = ?"
        params.append(criteria.property_type)
        
    return query, params

# Helper function to apply price range filtering
def apply_price_range(query, params, price_range: PriceRangeParams):
    """
    Apply price range filtering to a SQL query and parameter list.
    
    Args:
        query: SQL query string
        params: List of query parameters
        price_range: PriceRangeParams object
        
    Returns:
        Tuple of (updated query, updated params)
    """
    if price_range.min_price:
        query += " AND list_price >= ?"
        params.append(price_range.min_price)
        
    if price_range.max_price:
        query += " AND list_price <= ?"
        params.append(price_range.max_price)
        
    return query, params

# Helper function to apply sorting
def apply_sorting(query, sort_by: str, sort_desc: bool = True):
    """
    Apply sorting to a SQL query based on specified field.
    
    Args:
        query: SQL query string
        sort_by: Field to sort by
        sort_desc: True for descending order, False for ascending
        
    Returns:
        Updated query string
    """
    # Validate sort field
    valid_sort_fields = [
        "investment_ranking", "cap_rate", "cash_yield", "irr", "cash_on_cash", 
        "total_return", "list_price", "lcf_year1", "zori_monthly_rent", 
        "sqft", "beds", "price_per_sqft"
    ]
    
    if sort_by not in valid_sort_fields:
        sort_by = "investment_ranking"  # Default to investment_ranking
    
    direction = "DESC" if sort_desc else "ASC"
    query += f" ORDER BY {sort_by} {direction}"
    
    # Add secondary sort for better consistency
    if sort_by == "investment_ranking":
        query += ", cap_rate DESC"
    elif sort_by == "cap_rate":
        query += ", investment_ranking DESC"
    elif sort_by == "list_price":
        query += ", investment_ranking DESC"
    elif sort_by == "price_per_sqft":
        query += ", list_price DESC"
    else:
        query += ", list_price ASC"
    
    return query

# Helper function to paginate results
def paginate_results(query, params, page, page_size):
    """
    Add pagination to a SQL query and execute it.
    
    Args:
        query: SQL query string
        params: List of query parameters
        page: Page number (1-based)
        page_size: Number of items per page
        
    Returns:
        Tuple of (paginated query, updated params)
    """
    # Add pagination
    offset = (page - 1) * page_size
    query += " LIMIT ? OFFSET ?"
    params.extend([page_size, offset])
    
    return query, params

# Helper function to map property data to PropertySearchResult model
def map_to_search_result(property_data):
    """
    Map property data to PropertySearchResult model.
    Ensures all required fields are present.
    
    Args:
        property_data: Dictionary with property data from database
        
    Returns:
        Dictionary with mapped data
    """
    return {
        "property_id": property_data.get("property_id"),
        "primary_photo": property_data.get("primary_photo"),
        "full_street_line": property_data.get("full_street_line"),
        "city": property_data.get("city"),
        "state": property_data.get("state"),
        "zip_code": property_data.get("zip_code"),
        "beds": property_data.get("beds"),
        "baths": property_data.get("baths") or (property_data.get("full_baths", 0) + 0.5 * property_data.get("half_baths", 0)),
        "sqft": property_data.get("sqft"),
        "list_price": property_data.get("list_price"),
        "price_per_sqft": property_data.get("price_per_sqft"),
        "investment_ranking": property_data.get("investment_ranking"),
        "cap_rate": property_data.get("cap_rate"),
        "irr": property_data.get("irr"),
        "cash_on_cash": property_data.get("cash_on_cash"),
        "total_return": property_data.get("total_return"),
        "broker_name": property_data.get("broker_name"),
        "broker_id": property_data.get("broker_id")
    }

# API Endpoints

@app.get("/", tags=["General"])
async def root():
    """API root endpoint with basic information"""
    return {
        "name": "Real Estate Investment API",
        "version": "1.1.0",
        "description": "API for analyzing real estate investment properties with detailed metrics",
        "endpoints": {
            "properties": "/properties/",
            "properties_by_state": "/properties/state/{state}",
            "properties_by_city": "/properties/city/{city}",
            "properties_by_zipcode": "/properties/zipcode/{zipcode}",
            "property_detail": "/properties/{property_id}",
            "property_calculations": "/properties/{property_id}/calculations",
            "markets": "/market-stats/",
            "investment_analysis": "/investment-analysis/"
        }
    }

@app.get("/states/", tags=["Locations"], response_model=List[str])
async def get_states():
    """Get list of all states with available properties"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT DISTINCT state FROM properties ORDER BY state")
        states = [row[0] for row in cursor.fetchall()]
        return states
    finally:
        conn.close()

@app.get("/cities/", tags=["Locations"], response_model=List[City])
async def get_cities(state: Optional[str] = None):
    """Get list of cities with available properties"""
    conn = get_db_connection()
    try:
        if state:
            cursor = conn.execute(
                "SELECT city, state, property_count FROM city_lookup WHERE state = ? ORDER BY city",
                (state,)
            )
        else:
            cursor = conn.execute(
                "SELECT city, state, property_count FROM city_lookup ORDER BY state, city"
            )
        
        cities = [dict(row) for row in cursor.fetchall()]
        return cities
    finally:
        conn.close()

@app.get("/zipcodes/", tags=["Locations"], response_model=List[ZipCode])
async def get_zipcodes(city: Optional[str] = None, state: Optional[str] = None):
    """Get list of ZIP codes with available properties"""
    conn = get_db_connection()
    try:
        query = "SELECT zip_code, city, state, property_count FROM zipcode_lookup"
        params = []
        
        if city:
            query += " WHERE city = ?"
            params.append(city)
            
            if state:
                query += " AND state = ?"
                params.append(state)
        elif state:
            query += " WHERE state = ?"
            params.append(state)
            
        query += " ORDER BY state, city, zip_code"
        
        cursor = conn.execute(query, params)
        zipcodes = [dict(row) for row in cursor.fetchall()]
        return zipcodes
    finally:
        conn.close()

@app.get("/property-types/", tags=["Properties"], response_model=List[Dict[str, Any]])
async def get_property_types():
    """Get list of available property types/styles"""
    conn = get_db_connection()
    try:
        cursor = conn.execute(
            "SELECT style AS property_type, property_count FROM style_lookup ORDER BY property_count DESC"
        )
        
        property_types = [dict(row) for row in cursor.fetchall()]
        return property_types
    finally:
        conn.close()

@app.get("/properties/", tags=["Properties"], response_model=PaginatedResponse)
async def get_properties(
    criteria: Optional[InvestmentCriteria] = None,
    price_range: PriceRangeParams = Depends(PriceRangeParams),
    sorting: Dict = Depends(sorting_params),
    pagination: Dict = Depends(pagination_params)
):
    """
    Get properties with filtering by investment criteria and price range.
    Allows searching across all properties with various filters.
    """
    if criteria is None:
        criteria = InvestmentCriteria()
    
    conn = get_db_connection()
    try:
        # Use the api_property_search view for better performance
        query = """
        SELECT * FROM api_property_search 
        WHERE 1=1
        """
        params = []
        
        # Apply price range filtering
        query, params = apply_price_range(query, params, price_range)
        
        # Apply investment criteria
        query, params = apply_investment_criteria(query, params, criteria)
        
        # Get total count before pagination
        count_query = f"SELECT COUNT(*) FROM ({query})"
        cursor = conn.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # Apply sorting
        query = apply_sorting(query, sorting["sort_by"], sorting["sort_desc"])
        
        # Apply pagination
        page = pagination["page"]
        page_size = pagination["page_size"]
        query, params = paginate_results(query, params, page, page_size)
        
        # Execute query
        cursor = conn.execute(query, params)
        properties = [map_to_search_result(dict(row)) for row in cursor.fetchall()]
        
        # Calculate total pages
        total_pages = math.ceil(total_count / page_size)
        
        return {
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "pages": total_pages,
            "results": properties
        }
    finally:
        conn.close()

@app.get("/properties/state/{state}", tags=["Properties"], response_model=PaginatedResponse)
async def get_properties_by_state(
    state: str,
    criteria: Optional[InvestmentCriteria] = None,
    price_range: PriceRangeParams = Depends(PriceRangeParams),
    sorting: Dict = Depends(sorting_params),
    pagination: Dict = Depends(pagination_params)
):
    """
    Get all properties in a specific state with filtering and sorting options.
    """
    if criteria is None:
        criteria = InvestmentCriteria()
    
    conn = get_db_connection()
    try:
        # Use the api_property_search view for better performance
        query = """
        SELECT * FROM api_property_search 
        WHERE state = ?
        """
        params = [state]
        
        # Apply price range filtering
        query, params = apply_price_range(query, params, price_range)
        
        # Apply investment criteria
        query, params = apply_investment_criteria(query, params, criteria)
        
        # Get total count before pagination
        count_query = f"SELECT COUNT(*) FROM ({query})"
        cursor = conn.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        if total_count == 0:
            raise HTTPException(status_code=404, detail=f"No properties found in {state}")
        
        # Apply sorting
        query = apply_sorting(query, sorting["sort_by"], sorting["sort_desc"])
        
        # Apply pagination
        page = pagination["page"]
        page_size = pagination["page_size"]
        query, params = paginate_results(query, params, page, page_size)
        
        # Execute query
        cursor = conn.execute(query, params)
        properties = [map_to_search_result(dict(row)) for row in cursor.fetchall()]
        
        # Calculate total pages
        total_pages = math.ceil(total_count / page_size)
        
        return {
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "pages": total_pages,
            "results": properties
        }
    finally:
        conn.close()

@app.get("/properties/city/{city}", tags=["Properties"], response_model=PaginatedResponse)
async def get_properties_by_city(
    city: str,
    state: Optional[str] = None,
    criteria: Optional[InvestmentCriteria] = None,
    price_range: PriceRangeParams = Depends(PriceRangeParams),
    sorting: Dict = Depends(sorting_params),
    pagination: Dict = Depends(pagination_params)
):
    """
    Get all properties in a specific city with filtering and sorting options.
    Optionally filter by state if city name exists in multiple states.
    """
    if criteria is None:
        criteria = InvestmentCriteria()
    
    conn = get_db_connection()
    try:
        # Use the api_property_search view for better performance
        query = """
        SELECT * FROM api_property_search 
        WHERE city = ?
        """
        params = [city]
        
        if state:
            query += " AND state = ?"
            params.append(state)
        
        # Apply price range filtering
        query, params = apply_price_range(query, params, price_range)
        
        # Apply investment criteria
        query, params = apply_investment_criteria(query, params, criteria)
        
        # Get total count before pagination
        count_query = f"SELECT COUNT(*) FROM ({query})"
        cursor = conn.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        if total_count == 0:
            if state:
                raise HTTPException(status_code=404, detail=f"No properties found in {city}, {state}")
            else:
                raise HTTPException(status_code=404, detail=f"No properties found in {city}")
        
        # Apply sorting
        query = apply_sorting(query, sorting["sort_by"], sorting["sort_desc"])
        
        # Apply pagination
        page = pagination["page"]
        page_size = pagination["page_size"]
        query, params = paginate_results(query, params, page, page_size)
        
        # Execute query
        cursor = conn.execute(query, params)
        properties = [map_to_search_result(dict(row)) for row in cursor.fetchall()]
        
        # Calculate total pages
        total_pages = math.ceil(total_count / page_size)
        
        return {
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "pages": total_pages,
            "results": properties
        }
    finally:
        conn.close()

@app.get("/properties/zipcode/{zipcode}", tags=["Properties"], response_model=PaginatedResponse)
async def get_properties_by_zipcode(
    zipcode: int = Path(..., description="ZIP code to search for"),
    criteria: Optional[InvestmentCriteria] = None,
    price_range: PriceRangeParams = Depends(PriceRangeParams),
    sorting: Dict = Depends(sorting_params),
    pagination: Dict = Depends(pagination_params)
):
    """
    Get all properties in a specific ZIP code with filtering and sorting options.
    """
    if criteria is None:
        criteria = InvestmentCriteria()
    
    conn = get_db_connection()
    try:
        # Use the api_property_search view for better performance
        query = """
        SELECT * FROM api_property_search 
        WHERE zip_code = ?
        """
        params = [zipcode]
        
        # Apply price range filtering
        query, params = apply_price_range(query, params, price_range)
        
        # Apply investment criteria
        query, params = apply_investment_criteria(query, params, criteria)
        
        # Get total count before pagination
        count_query = f"SELECT COUNT(*) FROM ({query})"
        cursor = conn.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        if total_count == 0:
            raise HTTPException(status_code=404, detail=f"No properties found with ZIP code {zipcode}")
        
        # Apply sorting
        query = apply_sorting(query, sorting["sort_by"], sorting["sort_desc"])
        
        # Apply pagination
        page = pagination["page"]
        page_size = pagination["page_size"]
        query, params = paginate_results(query, params, page, page_size)
        
        # Execute query
        cursor = conn.execute(query, params)
        properties = [map_to_search_result(dict(row)) for row in cursor.fetchall()]
        
        # Calculate total pages
        total_pages = math.ceil(total_count / page_size)
        
        return {
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "pages": total_pages,
            "results": properties
        }
    finally:
        conn.close()

@app.get("/properties/{property_id}", tags=["Properties"], response_model=PropertyDetail)
async def get_property_detail(property_id: int):
    """
    Get detailed information about a specific property including:
    - Basic property data (address, beds, baths, sqft, etc.)
    - Investment metrics (cap rate, ROI, cash-on-cash, etc.)
    - Photos and broker information
    """
    conn = get_db_connection()
    try:
        # Use the api_property_details view for full property details
        cursor = conn.execute(
            """
            SELECT * FROM api_property_details
            WHERE property_id = ?
            """,
            (property_id,)
        )
        
        property_data = cursor.fetchone()
        if not property_data:
            raise HTTPException(status_code=404, detail=f"Property ID {property_id} not found")
        
        return dict(property_data)
    finally:
        conn.close()

@app.get("/properties/{property_id}/calculations", tags=["Properties"], response_model=EnhancedCalculationAudit)
async def get_property_calculations(property_id: int):
    """
    Get detailed calculation audit data for a specific property with enhanced information.
    Provides transparency for all investment calculations with breakdowns of each component.
    """
    conn = get_db_connection()
    try:
        # Get calculation data from combined audit tables
        cursor = conn.execute(
            """
            SELECT 
                p.property_id,
                p.list_price,
                p.sqft,
                
                -- Rental income data
                r.zori_monthly_rent,
                r.zori_annual_rent,
                r.zori_growth_rate,
                r.gross_rent_multiplier,
                r.zori_monthly_rent / p.sqft AS monthly_rent_per_sqft,
                
                -- Cash flow data
                cf.tax_used AS property_tax_annual,
                cf.hoa_fee_used AS hoa_annual,
                cf.noi_year1 AS net_operating_income,
                
                -- Cap rate calculation
                cf.cap_rate,
                
                -- Mortgage details
                m.down_payment_pct,
                m.interest_rate,
                m.loan_term,
                m.loan_amount,
                m.monthly_payment,
                m.annual_debt_service,
                p.list_price * (m.down_payment_pct / 100) AS cash_investment,
                
                -- ROI components
                ret.exit_cap_rate,
                ret.exit_value,
                ret.equity_at_exit,
                ret.irr,
                ret.cash_on_cash,
                ret.total_return,
                ret.investment_ranking,
                
                -- Cash flow projections
                proj.lcf_year1 AS annual_cash_flow,
                proj.lcf_year1 / 12 AS monthly_cash_flow
                
            FROM properties p
            LEFT JOIN rental_income_audit r ON p.property_id = r.property_id
            LEFT JOIN cash_flow_audit cf ON p.property_id = cf.property_id
            LEFT JOIN mortgage_audit m ON p.property_id = m.property_id
            LEFT JOIN investment_returns_audit ret ON p.property_id = ret.property_id
            LEFT JOIN cash_flow_projections_audit proj ON p.property_id = proj.property_id
            WHERE p.property_id = ?
            """,
            (property_id,)
        )
        
        calc_data = cursor.fetchone()
        if not calc_data:
            raise HTTPException(status_code=404, detail=f"Calculation data for Property ID {property_id} not found")
        
        calc_dict = dict(calc_data)
        
        # Calculate estimated expenses if not directly available
        list_price = calc_dict.get('list_price', 0)
        
        # Estimate operating expense components if not directly available
        if 'property_tax_annual' not in calc_dict or not calc_dict['property_tax_annual']:
            calc_dict['property_tax_annual'] = list_price * 0.01  # Estimate 1% of property value for taxes
            
        if 'insurance_annual' not in calc_dict:
            calc_dict['insurance_annual'] = list_price * 0.005  # Estimate 0.5% for insurance
            
        if 'maintenance_annual' not in calc_dict:
            calc_dict['maintenance_annual'] = list_price * 0.01  # Estimate 1% for maintenance
            
        if 'property_management_annual' not in calc_dict:
            zori_annual = calc_dict.get('zori_annual_rent', 0)
            calc_dict['property_management_annual'] = zori_annual * 0.08  # Estimate 8% of rent
            
        if 'vacancy_annual' not in calc_dict:
            zori_annual = calc_dict.get('zori_annual_rent', 0)
            calc_dict['vacancy_annual'] = zori_annual * 0.05  # Estimate 5% vacancy rate
            
        # Calculate total expenses
        total_expenses = (
            calc_dict.get('property_tax_annual', 0) +
            calc_dict.get('insurance_annual', 0) +
            calc_dict.get('maintenance_annual', 0) +
            calc_dict.get('property_management_annual', 0) +
            calc_dict.get('hoa_annual', 0) +
            calc_dict.get('vacancy_annual', 0)
        )
        calc_dict['total_expenses_annual'] = total_expenses
        
        # Calculate gross income if not available
        if 'gross_income' not in calc_dict:
            calc_dict['gross_income'] = calc_dict.get('zori_annual_rent', 0)
            
        # Calculate operating expenses if not available
        if 'operating_expenses' not in calc_dict:
            calc_dict['operating_expenses'] = total_expenses
            
        # Calculate net operating income if not available
        if 'net_operating_income' not in calc_dict or not calc_dict['net_operating_income']:
            calc_dict['net_operating_income'] = calc_dict.get('gross_income', 0) - calc_dict.get('operating_expenses', 0)
            
        # Calculate annual ROI percentage
        cash_investment = calc_dict.get('cash_investment', 0)
        if cash_investment > 0:
            annual_cash_flow = calc_dict.get('annual_cash_flow', 0)
            calc_dict['annual_roi_percentage'] = (annual_cash_flow / cash_investment) * 100
            
            # Calculate five-year ROI (simplified)
            calc_dict['five_year_roi_percentage'] = calc_dict.get('total_return', 0) * 100
        
        # Add investment ranking factors (simplified estimates if not available)
        if 'cap_rate_score' not in calc_dict:
            cap_rate = calc_dict.get('cap_rate', 0)
            calc_dict['cap_rate_score'] = min(cap_rate / 10, 1) * 10  # Scale 0-10
            
        if 'cash_flow_score' not in calc_dict:
            monthly_cf = calc_dict.get('monthly_cash_flow', 0)
            calc_dict['cash_flow_score'] = min(monthly_cf / 1000, 1) * 10  # Scale 0-10 based on $1000/mo
            
        if 'roi_score' not in calc_dict:
            irr = calc_dict.get('irr', 0)
            calc_dict['roi_score'] = min(irr / 20, 1) * 10  # Scale 0-10 based on 20% IRR
            
        if 'location_score' not in calc_dict:
            # This would ideally come from neighborhood quality data
            calc_dict['location_score'] = 7.5  # Default mid-range score
            
        if 'final_score' not in calc_dict:
            # Simplified estimate of final investment score
            calc_dict['final_score'] = (
                calc_dict.get('cap_rate_score', 0) * 0.3 +
                calc_dict.get('cash_flow_score', 0) * 0.3 +
                calc_dict.get('roi_score', 0) * 0.3 +
                calc_dict.get('location_score', 0) * 0.1
            )
        
        return calc_dict
    finally:
        conn.close()

@app.get("/properties/{property_id}/cash-flow-projection", tags=["Investment Analysis"])
async def get_property_cash_flow_projection(property_id: int):
    """Get cash flow projections for a specific property"""
    conn = get_db_connection()
    try:
        # Use the cash_flow_projections_audit table for accurate data
        cursor = conn.execute("""
        SELECT p.property_id, p.full_street_line, p.city, p.state, p.zip_code,
            p.list_price, p.zori_monthly_rent, p.zori_annual_rent,
            cf.noi_year1, cf.noi_year2, cf.noi_year3, cf.noi_year4, cf.noi_year5,
            cf.ucf_year1, cf.ucf_year2, cf.ucf_year3, cf.ucf_year4, cf.ucf_year5,
            cf.lcf_year1, cf.lcf_year2, cf.lcf_year3, cf.lcf_year4, cf.lcf_year5,
            m.monthly_payment, m.annual_debt_service, m.cash_equity,
            p.accumulated_cash_flow, m.total_principal_paid, r.exit_value,
            r.equity_at_exit, r.cash_on_cash, r.irr
        FROM properties p
        LEFT JOIN cash_flow_projections_audit cf ON p.property_id = cf.property_id
        LEFT JOIN mortgage_audit m ON p.property_id = m.property_id
        LEFT JOIN investment_returns_audit r ON p.property_id = r.property_id
        WHERE p.property_id = ?
        """, (property_id,))
        
        property_data = cursor.fetchone()
        if not property_data:
            raise HTTPException(status_code=404, detail=f"Property ID {property_id} not found")
        
        property_dict = dict(property_data)
        
        # Format projection data
        projections = []
        for year in range(1, 6):
            projections.append({
                "year": year,
                "noi": property_dict.get(f"noi_year{year}"),
                "unlevered_cash_flow": property_dict.get(f"ucf_year{year}"),
                "levered_cash_flow": property_dict.get(f"lcf_year{year}")
            })
        
        # Format result
        result = {
            "property_id": property_dict["property_id"],
            "address": property_dict["full_street_line"],
            "city": property_dict["city"],
            "state": property_dict["state"],
            "zip_code": property_dict["zip_code"],
            "list_price": property_dict["list_price"],
            "monthly_rent": property_dict["zori_monthly_rent"],
            "annual_rent": property_dict["zori_annual_rent"],
            "monthly_payment": property_dict["monthly_payment"],
            "annual_debt_service": property_dict["annual_debt_service"],
            "cash_equity": property_dict["cash_equity"],
            "projections": projections,
            "accumulated_cash_flow": property_dict["accumulated_cash_flow"],
            "total_principal_paid": property_dict["total_principal_paid"],
            "exit_value": property_dict["exit_value"],
            "equity_at_exit": property_dict["equity_at_exit"],
            "cash_on_cash": property_dict["cash_on_cash"],
            "irr": property_dict["irr"]
        }
        
        return result
    finally:
        conn.close()

@app.get("/market-stats/city/{city}", tags=["Market Statistics"], response_model=CityStats)
async def get_city_stats(city: str, state: Optional[str] = None):
    """Get market statistics for a specific city"""
    conn = get_db_connection()
    try:
        if state:
            cursor = conn.execute(
                "SELECT * FROM market_stats_by_city WHERE city = ? AND state = ?",
                (city, state)
            )
        else:
            cursor = conn.execute(
                "SELECT * FROM market_stats_by_city WHERE city = ?",
                (city,)
            )
        
        stats = cursor.fetchone()
        if not stats:
            if state:
                raise HTTPException(status_code=404, detail=f"No statistics found for {city}, {state}")
            else:
                raise HTTPException(status_code=404, detail=f"No statistics found for {city}")
        
        return dict(stats)
    finally:
        conn.close()

@app.get("/market-stats/zipcode/{zipcode}", tags=["Market Statistics"], response_model=ZipCodeStats)
async def get_zipcode_stats(zipcode: int):
    """Get market statistics for a specific ZIP code"""
    conn = get_db_connection()
    try:
        # Get market stats
        cursor = conn.execute(
            "SELECT * FROM market_stats_by_zipcode WHERE zip_code = ?",
            (zipcode,)
        )
        
        stats = cursor.fetchone()
        if not stats:
            raise HTTPException(status_code=404, detail=f"No statistics found for ZIP code {zipcode}")
        
        stats_dict = dict(stats)
        
        # Try to get neighborhood quality score
        cursor = conn.execute(
            "SELECT quality_score FROM neighborhood_quality WHERE zip_code = ?",
            (str(zipcode),)
        )
        
        quality = cursor.fetchone()
        if quality:
            stats_dict["neighborhood_quality"] = quality["quality_score"]
        
        return stats_dict
    finally:
        conn.close()

@app.get("/market-stats/state/{state}", tags=["Market Statistics"], response_model=StateOverview)
async def get_state_overview(state: str):
    """Get market overview for a state, including top cities"""
    conn = get_db_connection()
    try:
        # Get state stats
        cursor = conn.execute(
            "SELECT * FROM stats_by_state WHERE state = ?",
            (state,)
        )
        
        stats = cursor.fetchone()
        if not stats:
            raise HTTPException(status_code=404, detail=f"No statistics found for state {state}")
        
        # Get top cities in the state by property count
        cursor = conn.execute(
            """
            SELECT city, property_count, avg_price, avg_cap_rate, avg_cash_yield, 
                   avg_irr, avg_total_return, avg_investment_ranking
            FROM market_stats_by_city 
            WHERE state = ? 
            ORDER BY property_count DESC
            LIMIT 10
            """,
            (state,)
        )
        
        top_cities = [dict(row) for row in cursor.fetchall()]
        
        # Create response
        result = dict(stats)
        result["top_cities"] = top_cities
        
        return result
    finally:
        conn.close()

@app.get("/market-stats/property-types", tags=["Market Statistics"])
async def get_property_type_stats():
    """Get investment statistics by property type"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT * FROM stats_by_property_type ORDER BY property_count DESC")
        stats = [dict(row) for row in cursor.fetchall()]
        return stats
    finally:
        conn.close()

@app.get("/market-stats/bedroom-counts", tags=["Market Statistics"])
async def get_bedroom_stats():
    """Get investment statistics by bedroom count"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT * FROM stats_by_bedroom_count ORDER BY beds")
        stats = [dict(row) for row in cursor.fetchall()]
        return stats
    finally:
        conn.close()

from typing import Dict, Any

@app.get("/audit/{property_id}", tags=["Audit"], response_model=Dict[str, Any])
async def get_property_audit(property_id: int):
    """
    API endpoint to get audit data for a property.
    Returns all calculation audit data from various audit tables for transparency.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Get all audit tables
        audit_tables = [
            'rental_income_audit',
            'cash_flow_audit',
            'mortgage_audit',
            'investment_returns_audit',
            'cash_flow_projections_audit'
        ]
        
        # Also check for enhanced tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%enhanced'")
        enhanced_tables = [row['name'] for row in cursor.fetchall()]
        audit_tables.extend(enhanced_tables)
        
        # Check for calculation audit log
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='calculation_audit_log'")
        if cursor.fetchone():
            audit_tables.append('calculation_audit_log')
        
        result = {
            'property_id': property_id,
            'audit_data': {}
        }
        
        # Get property details
        cursor.execute('SELECT * FROM properties WHERE property_id = ?', (property_id,))
        property_data = cursor.fetchone()
        if not property_data:
            raise HTTPException(status_code=404, detail=f"Property ID {property_id} not found")
            
        result['property'] = dict(property_data)
        
        # Get data from each audit table
        for table in audit_tables:
            try:
                cursor.execute(f'SELECT * FROM {table} WHERE property_id = ?', (property_id,))
                rows = cursor.fetchall()
                if rows:
                    result['audit_data'][table] = [dict(row) for row in rows]
            except Exception as e:
                # Table might not exist or have a different structure
                # Log the error but continue with other tables
                print(f"Error querying {table}: {str(e)}")
                continue
        
        return result
    finally:
        conn.close()
        
@app.get("/investment-analysis/top-ranked", tags=["Investment Analysis"], response_model=List[PropertySearchResult])
async def get_top_ranked_properties(
    limit: int = Query(20, ge=1, le=100, description="Number of properties to return"),
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    state: Optional[str] = None
):
    """Get properties with the highest investment ranking"""
    conn = get_db_connection()
    try:
        # Use the view_top_ranked_properties view for better performance
        query = "SELECT * FROM view_top_ranked_properties WHERE 1=1"
        params = []
        
        if min_price:
            query += " AND list_price >= ?"
            params.append(min_price)
            
        if max_price:
            query += " AND list_price <= ?"
            params.append(max_price)
            
        if state:
            query += " AND state = ?"
            params.append(state)
        
        query += " ORDER BY investment_ranking DESC, cap_rate DESC LIMIT ?"
        params.append(limit)
        
        cursor = conn.execute(query, params)
        properties = [map_to_search_result(dict(row)) for row in cursor.fetchall()]
        
        return properties
    finally:
        conn.close()

@app.get("/investment-analysis/top-cap-rate", tags=["Investment Analysis"], response_model=List[PropertySearchResult])
async def get_top_cap_rate_properties(
    limit: int = Query(20, ge=1, le=100, description="Number of properties to return"),
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    state: Optional[str] = None
):
    """Get properties with the highest cap rates"""
    conn = get_db_connection()
    try:
        # Use the view_top_cap_rate view for better performance
        query = "SELECT * FROM view_top_cap_rate WHERE 1=1"
        params = []
        
        if min_price:
            query += " AND list_price >= ?"
            params.append(min_price)
            
        if max_price:
            query += " AND list_price <= ?"
            params.append(max_price)
            
        if state:
            query += " AND state = ?"
            params.append(state)
        
        query += " ORDER BY cap_rate DESC LIMIT ?"
        params.append(limit)
        
        cursor = conn.execute(query, params)
        properties = [map_to_search_result(dict(row)) for row in cursor.fetchall()]
        
        return properties
    finally:
        conn.close()

@app.get("/investment-analysis/top-cash-flow", tags=["Investment Analysis"], response_model=List[PropertySearchResult])
async def get_top_cash_flow_properties(
    limit: int = Query(20, ge=1, le=100, description="Number of properties to return"),
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    state: Optional[str] = None
):
    """Get properties with the highest cash flow"""
    conn = get_db_connection()
    try:
        # Use the view_top_cash_flow view for better performance
        query = "SELECT * FROM view_top_cash_flow WHERE 1=1"
        params = []
        
        if min_price:
            query += " AND list_price >= ?"
            params.append(min_price)
            
        if max_price:
            query += " AND list_price <= ?"
            params.append(max_price)
            
        if state:
            query += " AND state = ?"
            params.append(state)
        
        query += " ORDER BY lcf_year1 DESC LIMIT ?"
        params.append(limit)
        
        cursor = conn.execute(query, params)
        properties = [map_to_search_result(dict(row)) for row in cursor.fetchall()]
        
        return properties
    finally:
        conn.close()

@app.get("/investment-analysis/top-cash-on-cash", tags=["Investment Analysis"], response_model=List[PropertySearchResult])
async def get_top_cash_on_cash_properties(
    limit: int = Query(20, ge=1, le=100, description="Number of properties to return"),
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    state: Optional[str] = None
):
    """Get properties with the highest cash-on-cash return"""
    conn = get_db_connection()
    try:
        # Use the view_top_cash_on_cash view for better performance
        query = "SELECT * FROM view_top_cash_on_cash WHERE 1=1"
        params = []
        
        if min_price:
            query += " AND list_price >= ?"
            params.append(min_price)
            
        if max_price:
            query += " AND list_price <= ?"
            params.append(max_price)
            
        if state:
            query += " AND state = ?"
            params.append(state)
        
        query += " ORDER BY cash_on_cash DESC LIMIT ?"
        params.append(limit)
        
        cursor = conn.execute(query, params)
        properties = [map_to_search_result(dict(row)) for row in cursor.fetchall()]
        
        return properties
    finally:
        conn.close()

@app.get("/investment-analysis/top-total-return", tags=["Investment Analysis"], response_model=List[PropertySearchResult])
async def get_top_total_return_properties(
    limit: int = Query(20, ge=1, le=100, description="Number of properties to return"),
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    state: Optional[str] = None
):
    """Get properties with the highest total return"""
    conn = get_db_connection()
    try:
        # Use the view_top_total_return view for better performance
        query = "SELECT * FROM view_top_total_return WHERE 1=1"
        params = []
        
        if min_price:
            query += " AND list_price >= ?"
            params.append(min_price)
            
        if max_price:
            query += " AND list_price <= ?"
            params.append(max_price)
            
        if state:
            query += " AND state = ?"
            params.append(state)
        
        query += " ORDER BY total_return DESC LIMIT ?"
        params.append(limit)
        
        cursor = conn.execute(query, params)
        properties = [map_to_search_result(dict(row)) for row in cursor.fetchall()]
        
        return properties
    finally:
        conn.close()

@app.get("/investment-analysis/compare", tags=["Investment Analysis"], response_model=List[InvestmentComparison])
async def compare_properties(property_ids: str = Query(..., description="Comma-separated list of property IDs")):
    """Compare investment metrics for multiple properties"""
    # Parse property IDs
    try:
        ids = [int(id.strip()) for id in property_ids.split(",")]
        if not ids:
            raise ValueError("No property IDs provided")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid property ID format")
    
    # Get property data
    conn = get_db_connection()
    try:
        placeholders = ", ".join("?" for _ in ids)
        query = f"""
        SELECT 
            property_id, full_street_line, city, state, zip_code, list_price,
            zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash, total_return,
            lcf_year1, equity_at_exit, monthly_payment, accumulated_cash_flow, 
            cash_equity, investment_ranking
        FROM properties 
        WHERE property_id IN ({placeholders})
        """
        
        cursor = conn.execute(query, ids)
        properties = [dict(row) for row in cursor.fetchall()]
        
        if not properties:
            raise HTTPException(status_code=404, detail="No properties found with the provided IDs")
        
        # Enhance with calculated fields
        for prop in properties:
            # Calculate 5-year return (accumulated cash flow plus equity at exit)
            five_year_return = (prop.get("accumulated_cash_flow") or 0) + (prop.get("equity_at_exit") or 0)
            prop["five_year_return"] = five_year_return
        
        # Sort by investment_ranking
        properties.sort(key=lambda x: (x.get("investment_ranking") or 0, x.get("cap_rate") or 0), reverse=True)
        
        return properties
    finally:
        conn.close()

@app.get("/neighborhood-quality/{zipcode}", tags=["Neighborhood Analysis"])
async def get_neighborhood_quality(zipcode: str):
    """Get neighborhood quality score for a ZIP code"""
    conn = get_db_connection()
    try:
        cursor = conn.execute(
            "SELECT quality_score FROM neighborhood_quality WHERE zip_code = ?",
            (zipcode,)
        )
        
        quality = cursor.fetchone()
        if not quality:
            # Check if we have any properties with this ZIP code
            cursor = conn.execute(
                "SELECT COUNT(*) FROM properties WHERE zip_code = ?",
                (int(zipcode),)
            )
            count = cursor.fetchone()[0]
            
            if count == 0:
                raise HTTPException(status_code=404, detail=f"ZIP code {zipcode} not found")
            
            # Return default quality
            return {"zip_code": zipcode, "quality_score": 0.75, "is_default": True}
        
        return {"zip_code": zipcode, "quality_score": quality["quality_score"], "is_default": False}
    finally:
        conn.close()

@app.get("/zori-data/{zipcode}", tags=["Neighborhood Analysis"])
async def get_zori_data(zipcode: str):
    """Get ZORI rent data for a ZIP code"""
    conn = get_db_connection()
    try:
        cursor = conn.execute(
            "SELECT * FROM zori_data WHERE RegionName = ?",
            (zipcode,)
        )
        
        data = cursor.fetchone()
        if not data:
            raise HTTPException(status_code=404, detail=f"ZORI data not found for ZIP code {zipcode}")
        
        return dict(data)
    finally:
        conn.close()

@app.get("/health", tags=["System"])
async def health_check():
    """Check API health"""
    try:
        conn = get_db_connection()
        cursor = conn.execute("SELECT COUNT(*) FROM properties")
        property_count = cursor.fetchone()[0]
        conn.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "property_count": property_count,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# Main
# Main
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    
    # Log startup information
    print(f"Starting server on port {port}")
    print(f"LOCAL_TESTING mode: {LOCAL_TESTING}")
    
    uvicorn.run(
        app, 
        host="0.0.0.0",  # Bind to all interfaces
        port=port,
        log_level="info",
        timeout_keep_alive=300,  # Keep-alive timeout
        # Lower backlog for Cloud Run (which has limited concurrent connections)
        backlog=128
    )