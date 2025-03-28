import sqlite3
import pandas as pd
import os
import sys
import logging
import math
from datetime import datetime
import glob
import numpy as np
import re
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_setup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_FILE = 'final.db'
FILTERED_PROPERTIES_FILE = 'final.csv'
ZILLOW_RENT_DATA_FILE = 'zillow_rent_data.csv'

# Neighborhood quality factors - default values if ZORI calculation fails
NEIGHBORHOOD_QUALITY = {
    # Default for others (will be updated with ZORI data)
    'default': 0.75
}

def setup_database():
    """
    Set up the SQLite database with filtered investment property data from CSV.
    Creates tables, imports data, and adds necessary indices.
    """
    start_time = datetime.now()
    logger.info(f"Starting database setup at {start_time}")
    
    # Check if filtered properties file exists
    if not os.path.exists(FILTERED_PROPERTIES_FILE):
        logger.error(f"Filtered properties file not found: {FILTERED_PROPERTIES_FILE}")
        sys.exit(1)
    
    # Check for ZORI data
    has_zori_data = os.path.exists(ZILLOW_RENT_DATA_FILE)
    if not has_zori_data:
        logger.warning(f"ZORI data file not found: {ZILLOW_RENT_DATA_FILE}")
        logger.warning("Neighborhood quality metrics will use default values")
    
    # Connect to SQLite database (creates if it doesn't exist)
    try:
        conn = sqlite3.connect(DB_FILE)
        logger.info(f"Connected to database: {DB_FILE}")
    except sqlite3.Error as e:
        logger.error(f"SQLite connection error: {e}")
        sys.exit(1)
    
    try:
        cursor = conn.cursor()
        
        # Load filtered properties data
        logger.info(f"Loading filtered properties data: {FILTERED_PROPERTIES_FILE}")
        properties_df = pd.read_csv(FILTERED_PROPERTIES_FILE)
        
        # Process ZORI data if available
        zori_quality_factors = {}
        if has_zori_data:
            logger.info("Processing ZORI data...")
            zori_df, zori_quality_factors = process_zori_data(ZILLOW_RENT_DATA_FILE)
            
            # Create ZORI data table
            logger.info("Creating ZORI data table...")
            zori_df.to_sql('zori_data', conn, if_exists='replace', index=False)
            
            # Create index on RegionName (ZIP code)
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_zori_zipcode ON zori_data(RegionName)')
            
            # Create ZORI neighborhood quality table
            create_neighborhood_quality_table(conn, zori_quality_factors)
        
        # Prepare properties data for database import
        logger.info("Preparing properties data...")
        properties_df = prepare_filtered_properties_data(properties_df)
        
        # Create main properties table with all property and investment data
        logger.info("Creating properties table...")
        properties_df.to_sql('properties', conn, if_exists='replace', index=False)
        
        # Add any missing required fields
        ensure_required_fields(conn)
        
        # Create calculation audit tables
        create_calculation_audit_tables(conn, properties_df)
        
        # Create indices for faster querying
        create_database_indices(conn)
        
        # Create materialized views for common queries
        create_materialized_views(conn)
        
        # Create derived tables for analytics
        create_derived_tables(conn)
        
        # Create API-specific views for quick access
        create_api_views(conn)
        
        # Commit all changes
        conn.commit()
        
        # Verify data was inserted properly
        cursor.execute("SELECT COUNT(*) FROM properties")
        count = cursor.fetchone()[0]
        logger.info(f"Successfully imported {count} properties into the database")
        
        # Close connection
        conn.close()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Database setup completed successfully in {duration:.2f} seconds")
        
        # Return summary info
        return {
            "total_properties": count,
            "database_file": DB_FILE,
            "has_zori_data": has_zori_data,
            "zori_quality_zip_count": len(zori_quality_factors) if has_zori_data else 0,
            "setup_duration_seconds": duration
        }
        
    except Exception as e:
        logger.error(f"Error during database setup: {str(e)}")
        conn.close()
        raise

def prepare_filtered_properties_data(df):
    """
    Prepare filtered properties data for database import.
    
    Args:
        df: DataFrame with filtered properties data
        
    Returns:
        Prepared DataFrame ready for database import
    """
    logger.info(f"Preparing {len(df)} filtered investment properties")
    
    # Basic cleaning
    numeric_cols = df.select_dtypes(include=['number']).columns
    string_cols = df.select_dtypes(include=['object']).columns
    
    # Fill numeric columns with 0
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    # Fill string columns with empty string
    df[string_cols] = df[string_cols].fillna('')
    
    # Ensure zip_code is integer
    if 'zip_code' in df.columns:
        df['zip_code'] = df['zip_code'].astype(int)
    
    # Create combined bathroom count if not present
    if 'baths' not in df.columns and 'full_baths' in df.columns and 'half_baths' in df.columns:
        df['baths'] = df['full_baths'] + 0.5 * df['half_baths']
        
    # Ensure investment_ranking is available
    if 'investment_ranking' not in df.columns and 'investment_score' in df.columns:
        df['investment_ranking'] = df['investment_score'].astype(int)
        
    # Create complete address field if not present
    if 'full_address' not in df.columns:
        df['full_address'] = df.apply(
            lambda x: f"{x['full_street_line']}, {x['city']}, {x['state']} {x['zip_code']}",
            axis=1
        )
        
    # Remove any duplicate property_ids if they exist
    if 'property_id' in df.columns:
        initial_count = len(df)
        df = df.drop_duplicates(subset=['property_id'])
        if len(df) < initial_count:
            logger.info(f"Removed {initial_count - len(df)} duplicate properties")
    
    return df

def process_zori_data(zori_file):
    """
    Process ZORI data and calculate neighborhood quality factors.
    
    Args:
        zori_file: Path to the ZORI data CSV file
        
    Returns:
        Tuple of (zori_df, quality_factors_dict) where:
          - zori_df is a DataFrame with processed ZORI data
          - quality_factors_dict is a dictionary mapping ZIP codes to quality scores
    """
    try:
        # Read ZORI data
        zori_df = pd.read_csv(zori_file)
        logger.info(f"ZORI data contains {len(zori_df)} rows")
        
        # Get date columns (format: YYYY-MM-DD)
        date_columns = [col for col in zori_df.columns if re.match(r'^\d{4}-\d{2}-\d{2}$', col)]
        date_columns.sort()
        
        # Define analysis periods
        latest_date = date_columns[-1]
        one_year_ago_index = max(0, len(date_columns) - 13)  # 12 months back
        one_year_ago_date = date_columns[one_year_ago_index]
        five_years_ago_index = max(0, len(date_columns) - 61)  # 5 years back
        five_years_ago_date = date_columns[five_years_ago_index]
        
        logger.info(f"ZORI analysis periods: Latest={latest_date}, 1yr ago={one_year_ago_date}, 5yr ago={five_years_ago_date}")
        
        # Calculate growth rates and store in new columns
        zori_df['latest_rent'] = zori_df[latest_date]
        zori_df['one_year_ago_rent'] = zori_df[one_year_ago_date]
        zori_df['five_years_ago_rent'] = zori_df[five_years_ago_date]
        
        # Calculate one-year growth
        zori_df['one_year_growth'] = np.where(
            (zori_df['one_year_ago_rent'] > 0) & (~zori_df['one_year_ago_rent'].isna()) & (~zori_df['latest_rent'].isna()),
            ((zori_df['latest_rent'] / zori_df['one_year_ago_rent']) - 1) * 100,
            None
        )
        
        # Calculate five-year CAGR
        zori_df['five_year_cagr'] = np.where(
            (zori_df['five_years_ago_rent'] > 0) & (~zori_df['five_years_ago_rent'].isna()) & (~zori_df['latest_rent'].isna()),
            (np.power((zori_df['latest_rent'] / zori_df['five_years_ago_rent']), 1/5) - 1) * 100,
            None
        )
        
        # Keep only necessary columns for storage efficiency
        columns_to_keep = [
            'RegionID', 'RegionName', 'RegionType', 'StateName', 'State', 'City', 'Metro', 'CountyName',
            'latest_rent', 'one_year_ago_rent', 'five_years_ago_rent', 
            'one_year_growth', 'five_year_cagr'
        ]
        zori_df = zori_df[columns_to_keep]
        
        # Ensure RegionName is treated as string ZIP code
        zori_df['RegionName'] = zori_df['RegionName'].astype(str).str.zfill(5)
        
        # Calculate neighborhood quality factors
        quality_factors = calculate_neighborhood_quality(zori_df)
        
        return zori_df, quality_factors
        
    except Exception as e:
        logger.error(f"Error processing ZORI data: {e}")
        return pd.DataFrame(), {}

def calculate_neighborhood_quality(zori_df):
    """
    Calculate neighborhood quality factors based on ZORI data.
    
    Args:
        zori_df: DataFrame with processed ZORI data
        
    Returns:
        Dictionary mapping ZIP codes to quality scores (0.75-0.95 range)
    """
    quality_factors = {}
    
    try:
        # Group by state to calculate percentiles within each state
        state_groups = zori_df.groupby('State')
        
        for state, group in state_groups:
            if len(group) < 5:  # Skip states with too few data points
                continue
                
            # Calculate rent percentiles within state
            group = group.copy()
            group['rent_percentile'] = group['latest_rent'].rank(pct=True) * 100
            
            # Calculate growth percentiles within state (only for rows with valid growth data)
            growth_data = group[~group['five_year_cagr'].isna()].copy()
            if len(growth_data) > 5:
                growth_data['growth_percentile'] = growth_data['five_year_cagr'].rank(pct=True) * 100
                
                # Merge back to main group
                group = pd.merge(
                    group, 
                    growth_data[['RegionName', 'growth_percentile']], 
                    on='RegionName', 
                    how='left'
                )
            else:
                group['growth_percentile'] = 50  # Default middle percentile
                
            # Fill NaN percentiles with median value
            group['growth_percentile'] = group['growth_percentile'].fillna(50)
            
            # Calculate quality score (weighted formula)
            # 65% weight to rent level (current desirability) + 35% weight to growth (future potential)
            group['quality_score'] = (group['rent_percentile'] * 0.65 + group['growth_percentile'] * 0.35) / 100
            
            # Scale to 0.75-0.95 range
            group['final_score'] = 0.75 + (group['quality_score'] * 0.20)
            group['final_score'] = group['final_score'].clip(0.75, 0.95)
            
            # Add to quality factors dictionary
            for _, row in group.iterrows():
                zip_code = row['RegionName']
                quality_factors[zip_code] = row['final_score']
        
        logger.info(f"Calculated neighborhood quality factors for {len(quality_factors)} ZIP codes")
        
        # Log some stats about the distribution
        scores = list(quality_factors.values())
        if scores:
            logger.info(f"Quality score range: {min(scores):.4f} to {max(scores):.4f}")
            logger.info(f"Quality score mean: {sum(scores)/len(scores):.4f}")
        
        return quality_factors
    
    except Exception as e:
        logger.error(f"Error calculating neighborhood quality: {e}")
        return {}

def create_neighborhood_quality_table(conn, quality_factors):
    """
    Create a table with neighborhood quality factors.
    
    Args:
        conn: SQLite connection
        quality_factors: Dictionary mapping ZIP codes to quality scores
    """
    if not quality_factors:
        logger.warning("No neighborhood quality factors to store")
        return
    
    try:
        # Create DataFrame from quality factors
        quality_df = pd.DataFrame({
            'zip_code': list(quality_factors.keys()),
            'quality_score': list(quality_factors.values())
        })
        
        # Create table
        quality_df.to_sql('neighborhood_quality', conn, if_exists='replace', index=False)
        
        # Create index
        cursor = conn.cursor()
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_nq_zipcode ON neighborhood_quality(zip_code)')
        
        logger.info(f"Created neighborhood quality table with {len(quality_factors)} ZIP codes")
    
    except Exception as e:
        logger.error(f"Error creating neighborhood quality table: {e}")

def create_calculation_audit_tables(conn, properties_df):
    """
    Create tables to store calculation inputs and outputs for auditing purposes.
    
    Args:
        conn: SQLite connection
        properties_df: DataFrame with property data
    """
    logger.info("Creating calculation audit tables...")
    
    try:
        cursor = conn.cursor()
        
        # Create rental income calculation audit table
        columns = [
            'property_id', 'list_price', 'beds', 'full_baths', 'half_baths', 'sqft', 
            'year_built', 'style', 'zip_code', 'state', 'neighborhood_factor', 
            'bed_bath_factor', 'size_factor', 'condition_factor', 'property_type_factor',
            'zori_monthly_rent', 'zori_annual_rent', 'zori_growth_rate', 'gross_rent_multiplier'
        ]
        
        rental_columns = [col for col in columns if col in properties_df.columns]
        rental_df = properties_df[rental_columns].copy()
        
        # Add calculated fields for complete auditing
        if 'zori_monthly_rent' in rental_df.columns:
            rental_df['is_zori_based'] = True
        else:
            rental_df['is_zori_based'] = False
            
        rental_df.to_sql('rental_income_audit', conn, if_exists='replace', index=False)
        
        # Create cash flow calculation audit table
        cf_columns = [
            'property_id', 'list_price', 'zori_monthly_rent', 'zori_annual_rent',
            'tax', 'tax_used', 'hoa_fee', 'hoa_fee_used', 'annual_hoa_fee', 'down_payment_pct',
            'transaction_cost', 'cash_equity', 'noi_year1', 'cap_rate', 'ucf'
        ]
        
        cf_columns = [col for col in cf_columns if col in properties_df.columns]
        cf_df = properties_df[cf_columns].copy()
        cf_df.to_sql('cash_flow_audit', conn, if_exists='replace', index=False)
        
        # Create mortgage calculation audit table
        mortgage_columns = [
            'property_id', 'list_price', 'down_payment_pct', 'transaction_cost',
            'cash_equity', 'interest_rate', 'loan_term', 'loan_amount',
            'monthly_payment', 'annual_debt_service', 'principal_paid_year1',
            'loan_balance_year1', 'total_principal_paid', 'final_loan_balance',
            'accumulated_cash_flow'
        ]
        
        mortgage_columns = [col for col in mortgage_columns if col in properties_df.columns]
        mortgage_df = properties_df[mortgage_columns].copy()
        mortgage_df.to_sql('mortgage_audit', conn, if_exists='replace', index=False)
        
        # Create investment return audit table
        returns_columns = [
            'property_id', 'cap_rate', 'zori_growth_rate', 'exit_cap_rate',
            'exit_value', 'equity_at_exit', 'cash_on_cash', 'irr', 'total_return',
            'investment_score', 'investment_ranking'
        ]
        
        returns_columns = [col for col in returns_columns if col in properties_df.columns]
        returns_df = properties_df[returns_columns].copy()
        returns_df.to_sql('investment_returns_audit', conn, if_exists='replace', index=False)
        
        # Create cash flow projections audit table
        projections_columns = [
            'property_id', 'list_price', 'zori_monthly_rent', 
            'noi_year1', 'noi_year2', 'noi_year3', 'noi_year4', 'noi_year5',
            'ucf_year1', 'ucf_year2', 'ucf_year3', 'ucf_year4', 'ucf_year5',
            'lcf_year1', 'lcf_year2', 'lcf_year3', 'lcf_year4', 'lcf_year5'
        ]
        
        projections_columns = [col for col in projections_columns if col in properties_df.columns]
        proj_df = properties_df[projections_columns].copy()
        proj_df.to_sql('cash_flow_projections_audit', conn, if_exists='replace', index=False)
        
        # Create indices on property_id for all audit tables
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rental_audit_property_id ON rental_income_audit(property_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cashflow_audit_property_id ON cash_flow_audit(property_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_mortgage_audit_property_id ON mortgage_audit(property_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_returns_audit_property_id ON investment_returns_audit(property_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_projections_audit_property_id ON cash_flow_projections_audit(property_id)')
        
        logger.info("Created calculation audit tables successfully")
        
    except Exception as e:
        logger.error(f"Error creating calculation audit tables: {e}")

def create_database_indices(conn):
    """
    Create indices for faster querying.
    
    Args:
        conn: SQLite connection
    """
    logger.info("Creating database indices...")
    cursor = conn.cursor()
    
    # Create index on property_id for lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_property_id ON properties(property_id)')
    
    # Create indices for common search fields
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_city ON properties(city)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_zip_code ON properties(zip_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_state ON properties(state)')
    
    # Create indices for filtering by location combinations
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_city_state ON properties(city, state)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_zip_city_state ON properties(zip_code, city, state)')
    
    # Create indices for sorting by investment metrics
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cap_rate ON properties(cap_rate)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cash_yield ON properties(cash_yield)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_irr ON properties(irr)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cash_on_cash ON properties(cash_on_cash)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_total_return ON properties(total_return)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_investment_ranking ON properties(investment_ranking)')
    
    # Create index for price per square foot
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_per_sqft ON properties(price_per_sqft)')
    
    # Create index for monthly rent
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monthly_rent ON properties(zori_monthly_rent)')
    
    # Create index for cash flow
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_lcf ON properties(lcf_year1)')
    
    # Create spatial index for location-based queries
    cursor.execute("SELECT 1 FROM pragma_table_info('properties') WHERE name='latitude'")
    has_lat_long = cursor.fetchone() is not None
    
    if has_lat_long:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_location ON properties(latitude, longitude)')
    
    # Create index for price range filtering
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_list_price ON properties(list_price)')
    
    # Create indexes for property characteristics
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_beds ON properties(beds)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_full_baths ON properties(full_baths)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_half_baths ON properties(half_baths)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sqft ON properties(sqft)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_style ON properties(style)')
    
    # Composite indices for combined filtering and sorting
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_city_investment_ranking ON properties(city, investment_ranking)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_zip_investment_ranking ON properties(zip_code, investment_ranking)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_state_investment_ranking ON properties(state, investment_ranking)')
    
    # Composite indices for price range filtering with location
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_city_price ON properties(city, list_price)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_zip_price ON properties(zip_code, list_price)')
    
    # Commit changes
    conn.commit()
    logger.info("Database indices created successfully")

def create_materialized_views(conn):
    """
    Create materialized views for common queries.
    
    Args:
        conn: SQLite connection
    """
    logger.info("Creating materialized views...")
    cursor = conn.cursor()
    
    # View for top investment properties by investment_ranking
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS view_top_ranked_properties AS
    SELECT 
        property_id, full_street_line, city, state, zip_code,
        beds, full_baths, half_baths, sqft, list_price, 
        zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash,
        total_return, equity_at_exit, lcf_year1, investment_ranking,
        primary_photo, broker_name, broker_id
    FROM properties
    WHERE investment_ranking > 0
    ORDER BY investment_ranking DESC, cap_rate DESC
    LIMIT 1000
    ''')
    
    # View for top cap rate properties
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS view_top_cap_rate AS
    SELECT 
        property_id, full_street_line, city, state, zip_code,
        beds, full_baths, half_baths, sqft, list_price, 
        zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash,
        total_return, equity_at_exit, lcf_year1, investment_ranking,
        primary_photo, broker_name, broker_id
    FROM properties
    WHERE cap_rate > 0
    ORDER BY cap_rate DESC
    LIMIT 1000
    ''')
    
    # View for top cash flow properties
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS view_top_cash_flow AS
    SELECT 
        property_id, full_street_line, city, state, zip_code,
        beds, full_baths, half_baths, sqft, list_price,
        zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash,
        total_return, lcf_year1, lcf_year2, lcf_year3, lcf_year4, lcf_year5,
        accumulated_cash_flow, investment_ranking,
        primary_photo, broker_name, broker_id
    FROM properties
    WHERE lcf_year1 > 0
    ORDER BY lcf_year1 DESC
    LIMIT 1000
    ''')
    
    # View for top cash-on-cash properties
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS view_top_cash_on_cash AS
    SELECT 
        property_id, full_street_line, city, state, zip_code,
        beds, full_baths, half_baths, sqft, list_price,
        zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash,
        total_return, cash_equity, equity_at_exit, investment_ranking,
        primary_photo, broker_name, broker_id
    FROM properties
    WHERE cash_on_cash > 0
    ORDER BY cash_on_cash DESC
    LIMIT 1000
    ''')
    
    # View for top total return properties
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS view_top_total_return AS
    SELECT 
        property_id, full_street_line, city, state, zip_code,
        beds, full_baths, half_baths, sqft, list_price,
        zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash,
        total_return, cash_equity, equity_at_exit, investment_ranking,
        primary_photo, broker_name, broker_id
    FROM properties
    WHERE total_return > 0
    ORDER BY total_return DESC
    LIMIT 1000
    ''')
    
    # Create a lookup table for cities
    cursor.execute('''
    CREATE TABLE city_lookup AS
    SELECT DISTINCT city, state, COUNT(*) as property_count
    FROM properties
    GROUP BY city, state
    ORDER BY state, city
    ''')
    
    # Create a lookup table for zip codes
    cursor.execute('''
    CREATE TABLE zipcode_lookup AS
    SELECT DISTINCT zip_code, city, state, COUNT(*) as property_count
    FROM properties
    GROUP BY zip_code, city, state
    ORDER BY state, city, zip_code
    ''')
    
    # Create lookup table for property styles
    cursor.execute('''
    CREATE TABLE style_lookup AS
    SELECT DISTINCT style, COUNT(*) as property_count
    FROM properties
    WHERE style IS NOT NULL AND style != ''
    GROUP BY style
    ORDER BY property_count DESC
    ''')
    
    # Commit changes
    conn.commit()
    logger.info("Materialized views created successfully")

def create_derived_tables(conn):
    """
    Create derived tables with summarized data for analytics.
    
    Args:
        conn: SQLite connection
    """
    try:
        cursor = conn.cursor()
        logger.info("Creating derived tables for analytics...")
        
        # Create table with market statistics by city
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_stats_by_city AS
        SELECT 
            city, 
            state,
            COUNT(*) as property_count,
            AVG(list_price) as avg_price,
            MIN(list_price) as min_price,
            MAX(list_price) as max_price,
            AVG(zori_monthly_rent) as avg_rent,
            AVG(cap_rate) as avg_cap_rate,
            AVG(cash_yield) as avg_cash_yield,
            AVG(irr) as avg_irr,
            AVG(cash_on_cash) as avg_cash_on_cash,
            AVG(total_return) as avg_total_return,
            AVG(price_per_sqft) as avg_price_per_sqft,
            AVG(lcf_year1) as avg_annual_cash_flow,
            AVG(zori_growth_rate) as avg_rent_growth_rate,
            AVG(investment_ranking) as avg_investment_ranking
        FROM properties
        GROUP BY city, state
        HAVING COUNT(*) >= 5
        ORDER BY state, city
        ''')
        
        # Create table with market statistics by zip code
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_stats_by_zipcode AS
        SELECT 
            zip_code,
            city, 
            state,
            COUNT(*) as property_count,
            AVG(list_price) as avg_price,
            MIN(list_price) as min_price,
            MAX(list_price) as max_price,
            AVG(zori_monthly_rent) as avg_rent,
            AVG(cap_rate) as avg_cap_rate,
            AVG(cash_yield) as avg_cash_yield,
            AVG(irr) as avg_irr,
            AVG(cash_on_cash) as avg_cash_on_cash,
            AVG(total_return) as avg_total_return,
            AVG(price_per_sqft) as avg_price_per_sqft,
            AVG(lcf_year1) as avg_annual_cash_flow,
            AVG(zori_growth_rate) as avg_rent_growth_rate,
            AVG(investment_ranking) as avg_investment_ranking
        FROM properties
        GROUP BY zip_code, city, state
        HAVING COUNT(*) >= 3
        ORDER BY state, city, zip_code
        ''')
        
        # Create table with property type statistics
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stats_by_property_type AS
        SELECT 
            style as property_type,
            COUNT(*) as property_count,
            AVG(list_price) as avg_price,
            AVG(zori_monthly_rent) as avg_rent,
            AVG(cap_rate) as avg_cap_rate,
            AVG(cash_yield) as avg_cash_yield,
            AVG(irr) as avg_irr,
            AVG(cash_on_cash) as avg_cash_on_cash,
            AVG(total_return) as avg_total_return,
            AVG(lcf_year1) as avg_annual_cash_flow,
            AVG(investment_ranking) as avg_investment_ranking
        FROM properties
        WHERE style IS NOT NULL AND style != ''
        GROUP BY style
        HAVING COUNT(*) >= 5
        ORDER BY property_count DESC
        ''')
        
        # Create table with bedroom count statistics
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stats_by_bedroom_count AS
        SELECT 
            beds,
            COUNT(*) as property_count,
            AVG(list_price) as avg_price,
            AVG(zori_monthly_rent) as avg_rent,
            AVG(cap_rate) as avg_cap_rate,
            AVG(cash_yield) as avg_cash_yield,
            AVG(irr) as avg_irr,
            AVG(cash_on_cash) as avg_cash_on_cash,
            AVG(total_return) as avg_total_return,
            AVG(lcf_year1) as avg_annual_cash_flow,
            AVG(investment_ranking) as avg_investment_ranking
        FROM properties
        WHERE beds > 0
        GROUP BY beds
        HAVING COUNT(*) >= 5
        ORDER BY beds
        ''')
        
        # Create table with state-level statistics
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stats_by_state AS
        SELECT 
            state,
            COUNT(*) as property_count,
            COUNT(DISTINCT city) as city_count,
            COUNT(DISTINCT zip_code) as zipcode_count,
            AVG(list_price) as avg_price,
            AVG(zori_monthly_rent) as avg_rent,
            AVG(cap_rate) as avg_cap_rate,
            AVG(cash_yield) as avg_cash_yield,
            AVG(irr) as avg_irr,
            AVG(cash_on_cash) as avg_cash_on_cash,
            AVG(total_return) as avg_total_return,
            AVG(lcf_year1) as avg_annual_cash_flow,
            AVG(zori_growth_rate) as avg_rent_growth_rate,
            AVG(investment_ranking) as avg_investment_ranking
        FROM properties
        GROUP BY state
        ORDER BY property_count DESC
        ''')
        
        # Commit changes
        conn.commit()
        
        # Log results
        cursor.execute("SELECT COUNT(*) FROM market_stats_by_city")
        city_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM market_stats_by_zipcode")
        zip_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM stats_by_property_type")
        type_count = cursor.fetchone()[0]
        
        logger.info(f"Created analytics tables: {city_count} cities, {zip_count} zip codes, {type_count} property types")
        
        return {
            "cities_analyzed": city_count,
            "zipcodes_analyzed": zip_count,
            "property_types_analyzed": type_count
        }
        
    except Exception as e:
        logger.error(f"Error creating derived tables: {str(e)}")
        raise

def create_api_views(conn):
    """
    Create specific views optimized for API endpoints.
    
    Args:
        conn: SQLite connection
    """
    logger.info("Creating API-specific views...")
    
    try:
        cursor = conn.cursor()
        
        # Create a view for property search results (limited fields)
        cursor.execute('''
        CREATE VIEW IF NOT EXISTS api_property_search AS
        SELECT 
            property_id,
            full_street_line,
            city,
            state,
            zip_code,
            beds,
            CASE 
                WHEN baths IS NOT NULL THEN baths
                ELSE full_baths + (0.5 * half_baths)
            END as baths,
            sqft,
            list_price,
            price_per_sqft,
            zori_monthly_rent,
            cap_rate,
            cash_on_cash,
            total_return,
            irr,
            investment_ranking,
            primary_photo,
            style
        FROM properties
        ''')
        
        # Create view for property details (all required fields)
        cursor.execute('''
        CREATE VIEW IF NOT EXISTS api_property_details AS
        SELECT 
            property_id,
            full_street_line,
            city,
            state,
            zip_code,
            beds,
            CASE 
                WHEN baths IS NOT NULL THEN baths
                ELSE full_baths + (0.5 * half_baths)
            END as baths,
            sqft,
            year_built,
            list_price,
            price_per_sqft,
            zori_monthly_rent,
            zori_annual_rent,
            cap_rate,
            cash_on_cash,
            irr,
            total_return,
            down_payment_pct,
            interest_rate,
            monthly_payment,
            loan_amount,
            cash_equity,
            lcf_year1,
            investment_ranking,
            investment_score,
            primary_photo,
            alt_photos,
            broker_id,
            broker_name,
            broker_email,
            broker_phones,
            agent_id,
            agent_name,
            agent_email,
            agent_phones,
            office_name,
            office_phones
        FROM properties
        ''')
        
        # Create view for calculation audit details
        cursor.execute('''
        CREATE VIEW IF NOT EXISTS api_calculation_audit AS
        SELECT 
            p.property_id,
            p.full_street_line,
            p.city,
            p.state,
            p.zip_code,
            r.zori_monthly_rent,
            r.zori_annual_rent,
            r.zori_growth_rate,
            r.gross_rent_multiplier,
            cf.tax_used,
            cf.hoa_fee_used,
            cf.noi_year1,
            cf.cap_rate,
            cf.ucf,
            m.down_payment_pct,
            m.interest_rate,
            m.loan_term,
            m.loan_amount,
            m.monthly_payment,
            m.annual_debt_service,
            m.final_loan_balance,
            ret.exit_cap_rate,
            ret.exit_value,
            ret.equity_at_exit,
            ret.irr,
            ret.cash_on_cash,
            ret.total_return,
            ret.investment_ranking,
            pr.lcf_year1,
            pr.lcf_year2,
            pr.lcf_year3,
            pr.lcf_year4,
            pr.lcf_year5
        FROM properties p
        LEFT JOIN rental_income_audit r ON p.property_id = r.property_id
        LEFT JOIN cash_flow_audit cf ON p.property_id = cf.property_id
        LEFT JOIN mortgage_audit m ON p.property_id = m.property_id
        LEFT JOIN investment_returns_audit ret ON p.property_id = ret.property_id
        LEFT JOIN cash_flow_projections_audit pr ON p.property_id = pr.property_id
        ''')
        
        # Create view for quick location search
        cursor.execute('''
        CREATE VIEW IF NOT EXISTS api_location_lookup AS
        SELECT DISTINCT
            city,
            state,
            zip_code,
            COUNT(*) as property_count
        FROM properties
        GROUP BY city, state, zip_code
        ''')
        
        # Create view for property rankings by location
        cursor.execute('''
        CREATE VIEW IF NOT EXISTS api_location_rankings AS
        SELECT
            city,
            state,
            zip_code,
            AVG(investment_ranking) as avg_ranking,
            AVG(cap_rate) as avg_cap_rate,
            AVG(cash_on_cash) as avg_cash_on_cash,
            AVG(irr) as avg_irr,
            AVG(total_return) as avg_total_return,
            AVG(list_price) as avg_price,
            COUNT(*) as property_count
        FROM properties
        GROUP BY city, state, zip_code
        HAVING COUNT(*) >= 3
        ''')
        
        # Commit changes
        conn.commit()
        logger.info("API views created successfully")
        
    except Exception as e:
        logger.error(f"Error creating API views: {e}")
        raise

def ensure_required_fields(conn):
    """
    Ensure that all required fields exist in the database.
    Checks and adds any missing columns needed for API responses.
    """
    logger.info("Checking for required fields in the database...")
    
    cursor = conn.cursor()
    
    # Get current columns
    cursor.execute("PRAGMA table_info(properties)")
    existing_columns = {col[1] for col in cursor.fetchall()}
    
    # Define all required fields for the API
    required_fields = [
        ("property_id", "INTEGER"),
        ("text", "TEXT"),
        ("style", "TEXT"),
        ("full_street_line", "TEXT"),
        ("street", "TEXT"),
        ("unit", "TEXT"),
        ("city", "TEXT"),
        ("state", "TEXT"),
        ("zip_code", "INTEGER"),
        ("beds", "REAL"),
        ("full_baths", "REAL"),
        ("half_baths", "REAL"),
        ("baths", "REAL"),  # Added for convenience
        ("sqft", "REAL"),
        ("year_built", "INTEGER"),
        ("list_price", "REAL"),
        ("list_date", "TEXT"),
        ("sold_price", "REAL"),
        ("last_sold_date", "TEXT"),
        ("assessed_value", "REAL"),
        ("estimated_value", "REAL"),
        ("tax", "REAL"),
        ("tax_history", "TEXT"),
        ("price_per_sqft", "REAL"),
        ("neighborhoods", "TEXT"),
        ("hoa_fee", "REAL"),
        ("annual_hoa_fee", "REAL"),  # Add this new field
        ("primary_photo", "TEXT"),
        ("alt_photos", "TEXT"),
        ("cap_rate", "REAL"),
        ("cash_equity", "REAL"),
        ("cash_yield", "REAL"),
        ("zori_monthly_rent", "REAL"),
        ("zori_annual_rent", "REAL"),
        ("zori_growth_rate", "REAL"),
        ("irr", "REAL"),
        ("cash_on_cash", "REAL"),
        ("total_return", "REAL"),  # Added for total return metric
        ("monthly_payment", "REAL"),
        ("lcf_year1", "REAL"),
        ("equity_at_exit", "REAL"),
        ("investment_ranking", "INTEGER"),  # Added for investment ranking
        ("broker_id", "INTEGER"),
        ("broker_name", "TEXT"),
        ("broker_email", "TEXT"),
        ("broker_phones", "TEXT"),
        ("agent_id", "INTEGER"),
        ("agent_name", "TEXT"),
        ("agent_email", "TEXT"),
        ("agent_phones", "TEXT"),
        ("office_name", "TEXT"),
        ("office_phones", "TEXT")
    ]
    
    # Add any missing columns
    missing_columns = []
    for field_name, field_type in required_fields:
        if field_name not in existing_columns:
            missing_columns.append((field_name, field_type))
    
    if missing_columns:
        logger.info(f"Adding {len(missing_columns)} missing columns to properties table")
        for field_name, field_type in missing_columns:
            try:
                cursor.execute(f"ALTER TABLE properties ADD COLUMN {field_name} {field_type}")
                logger.info(f"Added column: {field_name} ({field_type})")
            except sqlite3.Error as e:
                logger.error(f"Error adding column {field_name}: {e}")
        conn.commit()
    else:
        logger.info("All required fields exist in the properties table")
    
    return len(missing_columns)

def validate_database():
    """
    Run validation checks on the database to ensure data integrity.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Check for null property IDs
        cursor.execute("SELECT COUNT(*) FROM properties WHERE property_id IS NULL OR property_id = 0")
        null_ids = cursor.fetchone()[0]
        
        # Check for required API fields
        cursor.execute("SELECT COUNT(*) FROM properties WHERE list_price = 0 OR city = '' OR state = ''")
        missing_required = cursor.fetchone()[0]
        
        # Check for negative metrics
        cursor.execute("SELECT COUNT(*) FROM properties WHERE cap_rate < 0 OR cash_yield < 0")
        negative_metrics = cursor.fetchone()[0]
        
        # Check for unrealistic values
        cursor.execute("SELECT COUNT(*) FROM properties WHERE cap_rate > 30") # Cap rates above 30% are suspicious
        high_cap_rates = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM properties WHERE irr > 50") # IRRs above 50% are suspicious
        high_irrs = cursor.fetchone()[0]
        
        # Check for properties missing broker info
        cursor.execute("SELECT COUNT(*) FROM properties WHERE broker_name = '' OR broker_name IS NULL")
        missing_broker = cursor.fetchone()[0]
        
        # Check for missing image data
        cursor.execute("SELECT COUNT(*) FROM properties WHERE primary_photo = '' OR primary_photo IS NULL")
        missing_images = cursor.fetchone()[0]
        
        # Check if investment_ranking is present
        cursor.execute("SELECT COUNT(*) FROM properties WHERE investment_ranking IS NULL OR investment_ranking = 0")
        missing_ranking = cursor.fetchone()[0]
        
        # Log validation results
        logger.info("=== Database Validation Results ===")
        logger.info(f"Properties with null IDs: {null_ids}")
        logger.info(f"Properties missing required API fields: {missing_required}")
        logger.info(f"Properties with negative metrics: {negative_metrics}")
        logger.info(f"Properties with suspiciously high cap rates (>30%): {high_cap_rates}")
        logger.info(f"Properties with suspiciously high IRRs (>50%): {high_irrs}")
        logger.info(f"Properties missing broker information: {missing_broker}")
        logger.info(f"Properties missing primary photos: {missing_images}")
        logger.info(f"Properties missing investment ranking: {missing_ranking}")
        
        # Check for data integrity
        validation_passed = (null_ids == 0 and missing_required == 0 and
                           negative_metrics < 20 and high_cap_rates < 10 and 
                           high_irrs < 10 and missing_ranking == 0)
        
        if validation_passed:
            logger.info("Database passed validation checks")
        else:
            logger.warning("Database contains potentially problematic data - review logs")
        
        conn.close()
        
        return {
            "null_ids": null_ids,
            "missing_required": missing_required,
            "negative_metrics": negative_metrics,
            "high_cap_rates": high_cap_rates,
            "high_irrs": high_irrs,
            "missing_broker": missing_broker,
            "missing_images": missing_images,
            "missing_ranking": missing_ranking,
            "validation_passed": validation_passed
        }
        
    except Exception as e:
        logger.error(f"Error during database validation: {str(e)}")
        raise

if __name__ == "__main__":
    # Check if database already exists
    db_exists = os.path.exists(DB_FILE)
    if db_exists:
        logger.warning(f"Database {DB_FILE} already exists. It will be overwritten.")
        confirm = input("Continue? (y/n): ")
        if confirm.lower() != 'y':
            logger.info("Database setup cancelled by user")
            sys.exit(0)
    
    try:
        # Step 1: Create and populate main database
        setup_results = setup_database()
        
        # Step 2: Validate database
        validation_results = validate_database()
        
        # Summarize results
        logger.info("=== Database Setup Summary ===")
        logger.info(f"Total filtered properties imported: {setup_results['total_properties']}")
        logger.info(f"Setup completed in {setup_results['setup_duration_seconds']:.2f} seconds")
        logger.info(f"ZORI data available: {'Yes' if setup_results['has_zori_data'] else 'No'}")
        if setup_results['has_zori_data']:
            logger.info(f"ZIP codes with quality factors: {setup_results['zori_quality_zip_count']}")
        logger.info(f"Database validation: {'PASSED' if validation_results['validation_passed'] else 'FAILED'}")
        
        print("\nDatabase setup complete!")
        print(f"- {setup_results['total_properties']} filtered investment properties imported")
        
        if hasattr(setup_results, 'cities_analyzed'):
            print(f"- {setup_results['cities_analyzed']} cities with detailed market stats")
            print(f"- {setup_results['zipcodes_analyzed']} zip codes with detailed market stats")
        
        print(f"\nDatabase file: {DB_FILE}")
        
    except Exception as e:
        logger.error(f"Database setup failed: {str(e)}")
        print(f"\nError: Database setup failed - {str(e)}")
        sys.exit(1)