from fastapi import FastAPI, HTTPException, Query, Path, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict, Any
import sqlite3
from pydantic import BaseModel, Field, validator
import json
import math
from datetime import datetime

app = FastAPI(
    title="Real Estate Investment API",
    description="API for analyzing real estate investment properties with detailed metrics and neighborhood analysis",
    version="1.0.0"
)

# Enable CORS for frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development - restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection helper
def get_db_connection():
    conn = sqlite3.connect('investment_properties.db')
    conn.row_factory = sqlite3.Row  # This enables column access by name
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
        if v < 0:
            raise ValueError("Value cannot be negative")
        return v

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
    monthly_payment: Optional[float] = None
    lcf_year1: Optional[float] = None
    equity_at_exit: Optional[float] = None
    cash_equity: Optional[float] = None
    
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
                "sqft": 1200,
                "list_price": 750000,
                "style": "Condo",
                "year_built": 2005,
                "price_per_sqft": 625,
                "zori_monthly_rent": 4200,
                "cap_rate": 5.2,
                "cash_yield": 4.8,
                "irr": 12.5,
                "cash_on_cash": 1.8,
                "lcf_year1": 12000
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
    hoa_fee: Optional[float] = None
    parking_garage: Optional[int] = None
    lot_sqft: Optional[float] = None
    
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
    avg_price_per_sqft: float
    avg_annual_cash_flow: Optional[float] = None
    avg_rent_growth_rate: Optional[float] = None

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
    avg_price_per_sqft: float
    avg_annual_cash_flow: Optional[float] = None
    avg_rent_growth_rate: Optional[float] = None
    neighborhood_quality: Optional[float] = None

# Investment criteria model
class InvestmentCriteria(BaseModel):
    min_cap_rate: float = Field(default=0, ge=0, description="Minimum cap rate (percentage)")
    min_cash_yield: float = Field(default=0, ge=0, description="Minimum cash yield (percentage)")
    min_irr: float = Field(default=0, ge=0, description="Minimum IRR (percentage)")
    min_cash_on_cash: float = Field(default=0, ge=0, description="Minimum cash-on-cash return")
    min_monthly_cash_flow: float = Field(default=0, description="Minimum monthly cash flow")
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
                "min_monthly_cash_flow": 500,
                "max_price": 750000,
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
    lcf_year1: Optional[float] = None
    five_year_return: Optional[float] = None
    equity_at_exit: Optional[float] = None
    monthly_payment: Optional[float] = None
    
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
                "lcf_year1": 12000,
                "five_year_return": 240000,
                "equity_at_exit": 975000,
                "monthly_payment": 2800
            }
        }

# Pagination parameters
def pagination_params(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page")
):
    return {"page": page, "page_size": page_size}

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
        
    if criteria.min_monthly_cash_flow > 0:
        query += " AND lcf_year1 / 12 >= ?"
        params.append(criteria.min_monthly_cash_flow)
        
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
        "cap_rate", "cash_yield", "irr", "cash_on_cash", 
        "list_price", "lcf_year1", "zori_monthly_rent", 
        "sqft", "beds", "price_per_sqft"
    ]
    
    if sort_by not in valid_sort_fields:
        sort_by = "cap_rate"
    
    direction = "DESC" if sort_desc else "ASC"
    query += f" ORDER BY {sort_by} {direction}"
    
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

# API Endpoints

@app.get("/", tags=["General"])
async def root():
    """API root endpoint with basic information"""
    return {
        "name": "Real Estate Investment API",
        "version": "1.0.0",
        "description": "API for analyzing real estate investment properties with detailed metrics",
        "endpoints": {
            "properties": "/properties/",
            "cities": "/cities/",
            "zipcodes": "/zipcodes/",
            "market_stats": "/market-stats/",
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
    state: Optional[str] = None,
    city: Optional[str] = None,
    sort_by: str = "cap_rate",
    sort_desc: bool = True,
    pagination: Dict = Depends(pagination_params)
):
    """
    Get properties with filtering by investment criteria.
    Allows searching across all properties with various filters.
    """
    if criteria is None:
        criteria = InvestmentCriteria()
    
    conn = get_db_connection()
    try:
        # Base query with all requested fields
        query = """
        SELECT 
            property_id, text, style, full_street_line, street, unit, 
            city, state, zip_code, beds, full_baths, half_baths, 
            sqft, year_built, list_price, list_date, sold_price, 
            last_sold_date, assessed_value, estimated_value, 
            tax, tax_history, price_per_sqft, neighborhoods, 
            hoa_fee, primary_photo, alt_photos, 
            cap_rate, cash_equity, cash_yield,
            zori_monthly_rent, zori_annual_rent, irr, 
            cash_on_cash, lcf_year1, equity_at_exit, monthly_payment
        FROM properties 
        WHERE 1=1
        """
        params = []
        
        # Apply location filters
        if state:
            query += " AND state = ?"
            params.append(state)
            
        if city:
            query += " AND city = ?"
            params.append(city)
        
        # Apply investment criteria
        query, params = apply_investment_criteria(query, params, criteria)
        
        # Get total count before pagination
        count_query = f"SELECT COUNT(*) FROM ({query})"
        cursor = conn.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # Apply sorting
        query = apply_sorting(query, sort_by, sort_desc)
        
        # Apply pagination
        page = pagination["page"]
        page_size = pagination["page_size"]
        query, params = paginate_results(query, params, page, page_size)
        
        # Execute query
        cursor = conn.execute(query, params)
        properties = [dict(row) for row in cursor.fetchall()]
        
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
    sort_by: str = "cap_rate",
    sort_desc: bool = True,
    pagination: Dict = Depends(pagination_params)
):
    """Get properties by city name with filtering by investment criteria"""
    if criteria is None:
        criteria = InvestmentCriteria()
    
    conn = get_db_connection()
    try:
        # Base query with all requested fields
        query = """
        SELECT 
            property_id, text, style, full_street_line, street, unit, 
            city, state, zip_code, beds, full_baths, half_baths, 
            sqft, year_built, list_price, list_date, sold_price, 
            last_sold_date, assessed_value, estimated_value, 
            tax, tax_history, price_per_sqft, neighborhoods, 
            hoa_fee, primary_photo, alt_photos, 
            cap_rate, cash_equity, cash_yield,
            zori_monthly_rent, zori_annual_rent, irr, 
            cash_on_cash, lcf_year1, equity_at_exit, monthly_payment
        FROM properties 
        WHERE city = ?
        """
        params = [city]
        
        if state:
            query += " AND state = ?"
            params.append(state)
        
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
        query = apply_sorting(query, sort_by, sort_desc)
        
        # Apply pagination
        page = pagination["page"]
        page_size = pagination["page_size"]
        query, params = paginate_results(query, params, page, page_size)
        
        # Execute query
        cursor = conn.execute(query, params)
        properties = [dict(row) for row in cursor.fetchall()]
        
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
    sort_by: str = "cap_rate",
    sort_desc: bool = True,
    pagination: Dict = Depends(pagination_params)
):
    """Get properties by ZIP code with filtering by investment criteria"""
    if criteria is None:
        criteria = InvestmentCriteria()
    
    conn = get_db_connection()
    try:
        # Base query with all requested fields
        query = """
        SELECT 
            property_id, text, style, full_street_line, street, unit, 
            city, state, zip_code, beds, full_baths, half_baths, 
            sqft, year_built, list_price, list_date, sold_price, 
            last_sold_date, assessed_value, estimated_value, 
            tax, tax_history, price_per_sqft, neighborhoods, 
            hoa_fee, primary_photo, alt_photos, 
            cap_rate, cash_equity, cash_yield,
            zori_monthly_rent, zori_annual_rent, irr, 
            cash_on_cash, lcf_year1, equity_at_exit, monthly_payment
        FROM properties 
        WHERE zip_code = ?
        """
        params = [zipcode]
        
        # Apply investment criteria
        query, params = apply_investment_criteria(query, params, criteria)
        
        # Get total count before pagination
        count_query = f"SELECT COUNT(*) FROM ({query})"
        cursor = conn.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        if total_count == 0:
            raise HTTPException(status_code=404, detail=f"No properties found with ZIP code {zipcode}")
        
        # Apply sorting
        query = apply_sorting(query, sort_by, sort_desc)
        
        # Apply pagination
        page = pagination["page"]
        page_size = pagination["page_size"]
        query, params = paginate_results(query, params, page, page_size)
        
        # Execute query
        cursor = conn.execute(query, params)
        properties = [dict(row) for row in cursor.fetchall()]
        
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
    """Get detailed information about a specific property"""
    conn = get_db_connection()
    try:
        cursor = conn.execute(
            """
            SELECT 
                property_id, text, style, full_street_line, street, unit, 
                city, state, zip_code, beds, full_baths, half_baths, 
                sqft, year_built, list_price, list_date, sold_price, 
                last_sold_date, assessed_value, estimated_value, 
                tax, tax_history, price_per_sqft, neighborhoods, 
                hoa_fee, primary_photo, alt_photos, 
                cap_rate, cash_equity, cash_yield,
                zori_monthly_rent, zori_annual_rent, zori_growth_rate,
                irr, cash_on_cash, lcf_year1, equity_at_exit, monthly_payment,
                noi_year1, noi_year2, noi_year3, noi_year4, noi_year5,
                ucf_year1, ucf_year2, ucf_year3, ucf_year4, ucf_year5,
                lcf_year1, lcf_year2, lcf_year3, lcf_year4, lcf_year5,
                loan_amount, annual_debt_service, down_payment_pct, interest_rate, loan_term,
                exit_cap_rate, exit_value, accumulated_cash_flow, 
                days_on_mls, lot_sqft, parking_garage
            FROM properties 
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

@app.get("/properties/{property_id}/cash-flow-projection", tags=["Investment Analysis"])
async def get_property_cash_flow_projection(property_id: int):
    """Get cash flow projections for a specific property"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("""
        SELECT 
            property_id, full_street_line, city, state, zip_code,
            list_price, zori_monthly_rent, zori_annual_rent,
            noi_year1, noi_year2, noi_year3, noi_year4, noi_year5,
            ucf_year1, ucf_year2, ucf_year3, ucf_year4, ucf_year5,
            lcf_year1, lcf_year2, lcf_year3, lcf_year4, lcf_year5,
            monthly_payment, annual_debt_service, cash_equity,
            accumulated_cash_flow, total_principal_paid, exit_value,
            equity_at_exit, cash_on_cash, irr
        FROM properties 
        WHERE property_id = ?
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
            SELECT city, property_count, avg_price, avg_cap_rate, avg_cash_yield, avg_irr
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

@app.get("/market-stats/decades", tags=["Market Statistics"])
async def get_decade_stats():
    """Get investment statistics by decade built"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT * FROM stats_by_year_built ORDER BY decade")
        stats = [dict(row) for row in cursor.fetchall()]
        return stats
    finally:
        conn.close()

@app.get("/investment-analysis/top-cap-rate", tags=["Investment Analysis"], response_model=List[Property])
async def get_top_cap_rate_properties(
    limit: int = Query(20, ge=1, le=100, description="Number of properties to return"),
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    state: Optional[str] = None
):
    """Get properties with the highest cap rates"""
    conn = get_db_connection()
    try:
        query = """
        SELECT 
            property_id, text, style, full_street_line, street, unit, 
            city, state, zip_code, beds, full_baths, half_baths, 
            sqft, year_built, list_price, list_date, sold_price, 
            last_sold_date, assessed_value, estimated_value, 
            tax, tax_history, price_per_sqft, neighborhoods, 
            hoa_fee, primary_photo, alt_photos, 
            cap_rate, cash_equity, cash_yield,
            zori_monthly_rent, zori_annual_rent, irr, 
            cash_on_cash, lcf_year1, equity_at_exit, monthly_payment
        FROM properties 
        WHERE cap_rate > 0
        """
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
        properties = [dict(row) for row in cursor.fetchall()]
        
        return properties
    finally:
        conn.close()

@app.get("/investment-analysis/top-cash-flow", tags=["Investment Analysis"], response_model=List[Property])
async def get_top_cash_flow_properties(
    limit: int = Query(20, ge=1, le=100, description="Number of properties to return"),
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    state: Optional[str] = None
):
    """Get properties with the highest cash flow"""
    conn = get_db_connection()
    try:
        query = """
        SELECT 
            property_id, text, style, full_street_line, street, unit, 
            city, state, zip_code, beds, full_baths, half_baths, 
            sqft, year_built, list_price, list_date, sold_price, 
            last_sold_date, assessed_value, estimated_value, 
            tax, tax_history, price_per_sqft, neighborhoods, 
            hoa_fee, primary_photo, alt_photos, 
            cap_rate, cash_equity, cash_yield,
            zori_monthly_rent, zori_annual_rent, irr, 
            cash_on_cash, lcf_year1, equity_at_exit, monthly_payment
        FROM properties 
        WHERE lcf_year1 > 0
        """
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
        properties = [dict(row) for row in cursor.fetchall()]
        
        return properties
    finally:
        conn.close()

@app.get("/investment-analysis/top-irr", tags=["Investment Analysis"], response_model=List[Property])
async def get_top_irr_properties(
    limit: int = Query(20, ge=1, le=100, description="Number of properties to return"),
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    state: Optional[str] = None
):
    """Get properties with the highest IRR (Internal Rate of Return)"""
    conn = get_db_connection()
    try:
        query = """
        SELECT 
            property_id, text, style, full_street_line, street, unit, 
            city, state, zip_code, beds, full_baths, half_baths, 
            sqft, year_built, list_price, list_date, sold_price, 
            last_sold_date, assessed_value, estimated_value, 
            tax, tax_history, price_per_sqft, neighborhoods, 
            hoa_fee, primary_photo, alt_photos, 
            cap_rate, cash_equity, cash_yield,
            zori_monthly_rent, zori_annual_rent, irr, 
            cash_on_cash, lcf_year1, equity_at_exit, monthly_payment
        FROM properties 
        WHERE irr > 0
        """
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
        
        query += " ORDER BY irr DESC LIMIT ?"
        params.append(limit)
        
        cursor = conn.execute(query, params)
        properties = [dict(row) for row in cursor.fetchall()]
        
        return properties
    finally:
        conn.close()

@app.get("/investment-analysis/top-cash-on-cash", tags=["Investment Analysis"], response_model=List[Property])
async def get_top_cash_on_cash_properties(
    limit: int = Query(20, ge=1, le=100, description="Number of properties to return"),
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    state: Optional[str] = None
):
    """Get properties with the highest cash-on-cash return"""
    conn = get_db_connection()
    try:
        query = """
        SELECT 
            property_id, text, style, full_street_line, street, unit, 
            city, state, zip_code, beds, full_baths, half_baths, 
            sqft, year_built, list_price, list_date, sold_price, 
            last_sold_date, assessed_value, estimated_value, 
            tax, tax_history, price_per_sqft, neighborhoods, 
            hoa_fee, primary_photo, alt_photos, 
            cap_rate, cash_equity, cash_yield,
            zori_monthly_rent, zori_annual_rent, irr, 
            cash_on_cash, lcf_year1, equity_at_exit, monthly_payment
        FROM properties 
        WHERE cash_on_cash > 0
        """
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
        properties = [dict(row) for row in cursor.fetchall()]
        
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
            property_id, text, style, full_street_line, street, unit, 
            city, state, zip_code, list_price, list_date, sold_price,
            last_sold_date, assessed_value, estimated_value, tax, tax_history,
            zori_monthly_rent, cap_rate, cash_yield, irr, 
            cash_on_cash, lcf_year1, equity_at_exit, monthly_payment,
            accumulated_cash_flow, cash_equity
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
        
        # Sort by IRR
        properties.sort(key=lambda x: x.get("irr") or 0, reverse=True)
        
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
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)