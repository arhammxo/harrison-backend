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
DB_FILE = 'investment_properties.db'
PROPERTY_DATA_FILES = [
    'for_sale_20250227_0226.csv',
    'for_sale_20250227_0235.csv',
    'for_sale_20250227_0243.csv',
    'for_sale_20250227_0244.csv',
    'for_sale_20250227_0245.csv',
    'for_sale_20250227_0438.csv',
    'for_sale_20250227_0459.csv',
    'for_sale_20250227_0502.csv',
    'for_sale_20250227_0625.csv',
    'for_sale_20250227_0629.csv',
    'for_sale_20250227_0629.csv',
    'for_sale_20250227_0638.csv',
    'for_sale_20250227_0643.csv',
    'for_sale_20250227_0659.csv',
    'for_sale_20250227_0702.csv',
    'for_sale_20250227_0705.csv'
]
INVESTMENT_RESULTS_FILE = 'investment_analysis_results.csv'
ZILLOW_RENT_DATA_FILE = 'zillow_rent_data.csv'

# Neighborhood quality factors - default values if ZORI calculation fails
NEIGHBORHOOD_QUALITY = {
    # Default for others (will be updated with ZORI data)
    'default': 0.75
}

def setup_database():
    """
    Set up the SQLite database with investment property data from CSV.
    Creates tables, imports data, and adds necessary indices.
    """
    start_time = datetime.now()
    logger.info(f"Starting database setup at {start_time}")
    
    # Check if CSV files exist
    if not os.path.exists(INVESTMENT_RESULTS_FILE):
        logger.error(f"Investment results file not found: {INVESTMENT_RESULTS_FILE}")
        sys.exit(1)
    
    # Check property files
    existing_property_files = []
    for file in PROPERTY_DATA_FILES:
        if os.path.exists(file):
            existing_property_files.append(file)
        else:
            logger.warning(f"Property file not found: {file}")
    
    if not existing_property_files:
        logger.error("No property data files found.")
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
        
        # First, process property files
        logger.info("Processing property data files...")
        properties_df = process_property_files(existing_property_files)
        
        # Then process investment results
        logger.info(f"Processing investment results: {INVESTMENT_RESULTS_FILE}")
        investment_df = pd.read_csv(INVESTMENT_RESULTS_FILE)
        
        # Merge property data with investment results
        logger.info("Merging property and investment data...")
        merged_df = merge_property_and_investment_data(properties_df, investment_df)
        
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
        
        # Create main properties table with all property and investment data
        logger.info("Creating properties table...")
        merged_df.to_sql('properties', conn, if_exists='replace', index=False)
        
        # Add any missing required fields
        ensure_required_fields(conn)
        
        # Create indices for faster querying
        create_database_indices(conn)
        
        # Create materialized views for common queries
        create_materialized_views(conn)
        
        # Create derived tables for analytics
        create_derived_tables(conn)
        
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

def process_property_files(files):
    """
    Read multiple property CSV files and combine them into a single DataFrame.
    
    Args:
        files: List of property data CSV files
        
    Returns:
        Combined DataFrame with all property data
    """
    all_properties = []
    
    for file in files:
        logger.info(f"Reading property file: {file}")
        df = pd.read_csv(file)
        logger.info(f"  - File contains {len(df)} properties")
        all_properties.append(df)
    
    # Combine all DataFrames
    combined_df = pd.concat(all_properties, ignore_index=True)
    logger.info(f"Combined {len(combined_df)} properties from {len(files)} files")
    
    # Drop duplicates based on property_id
    original_count = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['property_id'])
    dupes_removed = original_count - len(combined_df)
    if dupes_removed > 0:
        logger.info(f"Removed {dupes_removed} duplicate properties")
    
    # Basic cleaning
    numeric_cols = combined_df.select_dtypes(include=['number']).columns
    string_cols = combined_df.select_dtypes(include=['object']).columns
    
    # Fill numeric columns with 0
    combined_df[numeric_cols] = combined_df[numeric_cols].fillna(0)
    
    # Fill string columns with empty string
    combined_df[string_cols] = combined_df[string_cols].fillna('')
    
    # Ensure zip_code is integer
    if 'zip_code' in combined_df.columns:
        combined_df['zip_code'] = combined_df['zip_code'].astype(int)
    
    return combined_df

def merge_property_and_investment_data(properties_df, investment_df):
    """
    Merge property data with investment metrics.
    
    Args:
        properties_df: DataFrame with property details
        investment_df: DataFrame with investment metrics
        
    Returns:
        Merged DataFrame with all data
    """
    # Identify key columns for merging
    merge_columns = ['property_id']
    
    # If property_id is not in investment_df, try other combinations
    if 'property_id' not in investment_df.columns:
        potential_keys = []
        
        if all(col in investment_df.columns for col in ['full_street_line', 'city', 'state', 'zip_code']):
            potential_keys = ['full_street_line', 'city', 'state', 'zip_code']
        elif all(col in investment_df.columns for col in ['street', 'city', 'state', 'zip_code']):
            potential_keys = ['street', 'city', 'state', 'zip_code']
        
        if potential_keys:
            merge_columns = potential_keys
            logger.info(f"Using alternative merge keys: {potential_keys}")
        else:
            logger.warning("Cannot identify reliable merge keys. Using property_id and some properties may be missed.")
    
    # Perform merge
    merged_df = pd.merge(properties_df, investment_df, on=merge_columns, how='left', suffixes=('', '_inv'))
    
    # Log merge statistics
    match_count = merged_df['list_price_inv'].notna().sum()
    logger.info(f"Merged {match_count} of {len(properties_df)} properties with investment data")
    
    # Clean up duplicate columns
    duplicate_cols = [col for col in merged_df.columns if col.endswith('_inv')]
    merge_cols_set = set(merge_columns)
    
    for col in duplicate_cols:
        base_col = col[:-4]  # Remove _inv suffix
        if base_col not in merge_cols_set:
            # Use _inv column value if base column is NaN or 0
            merged_df[base_col] = merged_df[base_col].fillna(merged_df[col])
            
            # If numeric, prefer non-zero values
            if pd.api.types.is_numeric_dtype(merged_df[base_col]):
                mask = (merged_df[base_col] == 0) & (merged_df[col] != 0)
                merged_df.loc[mask, base_col] = merged_df.loc[mask, col]
        
        # Drop the duplicate column
        merged_df = merged_df.drop(columns=[col])
    
    # Clean up NaN values
    numeric_cols = merged_df.select_dtypes(include=['number']).columns
    string_cols = merged_df.select_dtypes(include=['object']).columns
    
    merged_df[numeric_cols] = merged_df[numeric_cols].fillna(0)
    merged_df[string_cols] = merged_df[string_cols].fillna('')
    
    return merged_df

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
    
    # Create indices for sorting by investment metrics
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cap_rate ON properties(cap_rate)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cash_yield ON properties(cash_yield)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_irr ON properties(irr)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cash_on_cash ON properties(cash_on_cash)')
    
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
    
    # View for top investment properties by cap rate
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS view_top_cap_rate AS
    SELECT 
        property_id, full_street_line, city, state, zip_code,
        beds, full_baths, half_baths, sqft, list_price, 
        zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash,
        equity_at_exit, lcf_year1
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
        lcf_year1, lcf_year2, lcf_year3, lcf_year4, lcf_year5,
        accumulated_cash_flow
    FROM properties
    WHERE lcf_year1 > 0
    ORDER BY lcf_year1 DESC
    LIMIT 1000
    ''')
    
    # View for top IRR properties
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS view_top_irr AS
    SELECT 
        property_id, full_street_line, city, state, zip_code,
        beds, full_baths, half_baths, sqft, list_price,
        zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash,
        equity_at_exit
    FROM properties
    WHERE irr > 0
    ORDER BY irr DESC
    LIMIT 1000
    ''')
    
    # View for top cash-on-cash properties
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS view_top_cash_on_cash AS
    SELECT 
        property_id, full_street_line, city, state, zip_code,
        beds, full_baths, half_baths, sqft, list_price,
        zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash,
        cash_equity, equity_at_exit
    FROM properties
    WHERE cash_on_cash > 0
    ORDER BY cash_on_cash DESC
    LIMIT 1000
    ''')
    
    # View for luxury properties (high end of market)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS view_luxury_properties AS
    SELECT 
        property_id, full_street_line, city, state, zip_code,
        beds, full_baths, half_baths, sqft, list_price,
        zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash
    FROM properties
    WHERE list_price > (SELECT percentile FROM 
                        (SELECT DISTINCT list_price as value, 
                         PERCENT_RANK() OVER (ORDER BY list_price) AS percentile
                         FROM properties)
                        WHERE percentile >= 0.9
                        LIMIT 1)
    ORDER BY list_price DESC
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
            AVG(price_per_sqft) as avg_price_per_sqft,
            AVG(lcf_year1) as avg_annual_cash_flow,
            AVG(zori_growth_rate) as avg_rent_growth_rate
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
            AVG(price_per_sqft) as avg_price_per_sqft,
            AVG(lcf_year1) as avg_annual_cash_flow,
            AVG(zori_growth_rate) as avg_rent_growth_rate
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
            AVG(lcf_year1) as avg_annual_cash_flow
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
            AVG(lcf_year1) as avg_annual_cash_flow
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
            AVG(lcf_year1) as avg_annual_cash_flow,
            AVG(zori_growth_rate) as avg_rent_growth_rate
        FROM properties
        GROUP BY state
        ORDER BY property_count DESC
        ''')
        
        # Create table with year built statistics
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stats_by_year_built AS
        SELECT 
            CAST((year_built / 10) * 10 AS INTEGER) AS decade,
            COUNT(*) as property_count,
            AVG(list_price) as avg_price,
            AVG(zori_monthly_rent) as avg_rent,
            AVG(cap_rate) as avg_cap_rate,
            AVG(lcf_year1) as avg_annual_cash_flow
        FROM properties
        WHERE year_built > 1900 AND year_built < 2030
        GROUP BY decade
        ORDER BY decade
        ''')
        
        # Create a correlation matrix for key metrics
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS metric_correlations AS
        WITH data AS (
            SELECT 
                list_price, sqft, price_per_sqft, beds, full_baths,
                zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash,
                lcf_year1, equity_at_exit
            FROM properties
            WHERE list_price > 0 AND sqft > 0 AND zori_monthly_rent > 0
        ),
        variances AS (
            SELECT
                AVG((list_price - (SELECT AVG(list_price) FROM data)) * (list_price - (SELECT AVG(list_price) FROM data))) AS var_list_price,
                AVG((sqft - (SELECT AVG(sqft) FROM data)) * (sqft - (SELECT AVG(sqft) FROM data))) AS var_sqft,
                AVG((price_per_sqft - (SELECT AVG(price_per_sqft) FROM data)) * (price_per_sqft - (SELECT AVG(price_per_sqft) FROM data))) AS var_price_per_sqft,
                AVG((beds - (SELECT AVG(beds) FROM data)) * (beds - (SELECT AVG(beds) FROM data))) AS var_beds,
                AVG((full_baths - (SELECT AVG(full_baths) FROM data)) * (full_baths - (SELECT AVG(full_baths) FROM data))) AS var_full_baths,
                AVG((zori_monthly_rent - (SELECT AVG(zori_monthly_rent) FROM data)) * (zori_monthly_rent - (SELECT AVG(zori_monthly_rent) FROM data))) AS var_zori_monthly_rent,
                AVG((cap_rate - (SELECT AVG(cap_rate) FROM data)) * (cap_rate - (SELECT AVG(cap_rate) FROM data))) AS var_cap_rate,
                AVG((cash_yield - (SELECT AVG(cash_yield) FROM data)) * (cash_yield - (SELECT AVG(cash_yield) FROM data))) AS var_cash_yield,
                AVG((irr - (SELECT AVG(irr) FROM data)) * (irr - (SELECT AVG(irr) FROM data))) AS var_irr,
                AVG((cash_on_cash - (SELECT AVG(cash_on_cash) FROM data)) * (cash_on_cash - (SELECT AVG(cash_on_cash) FROM data))) AS var_cash_on_cash,
                AVG((lcf_year1 - (SELECT AVG(lcf_year1) FROM data)) * (lcf_year1 - (SELECT AVG(lcf_year1) FROM data))) AS var_lcf_year1,
                AVG((equity_at_exit - (SELECT AVG(equity_at_exit) FROM data)) * (equity_at_exit - (SELECT AVG(equity_at_exit) FROM data))) AS var_equity_at_exit
            FROM data
        )
        SELECT
            'list_price' AS metric1, 'list_price' AS metric2,
            1.0 AS correlation
        UNION ALL
        SELECT 
            'list_price' AS metric1, 'sqft' AS metric2,
            AVG((list_price - (SELECT AVG(list_price) FROM data)) * (sqft - (SELECT AVG(sqft) FROM data))) / 
            SQRT((SELECT var_list_price FROM variances) * (SELECT var_sqft FROM variances)) AS correlation
        FROM data
        
        -- Add all combinations manually for main metrics
        -- This would be much longer in practice, including all metric combinations
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
        
        # Check for negative metrics
        cursor.execute("SELECT COUNT(*) FROM properties WHERE cap_rate < 0 OR cash_yield < 0")
        negative_metrics = cursor.fetchone()[0]
        
        # Check for unrealistic values
        cursor.execute("SELECT COUNT(*) FROM properties WHERE cap_rate > 30") # Cap rates above 30% are suspicious
        high_cap_rates = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM properties WHERE irr > 50") # IRRs above 50% are suspicious
        high_irrs = cursor.fetchone()[0]
        
        # Check for incomplete data
        cursor.execute("SELECT COUNT(*) FROM properties WHERE list_price = 0 OR sqft = 0")
        incomplete_data = cursor.fetchone()[0]
        
        # Check for missing bedroom/bathroom data
        cursor.execute("SELECT COUNT(*) FROM properties WHERE beds = 0 OR full_baths = 0")
        missing_bed_bath = cursor.fetchone()[0]
        
        # Check if we have ZORI data for each ZIP code
        cursor.execute("""
        SELECT COUNT(DISTINCT p.zip_code) FROM properties p
        LEFT JOIN zori_data z ON p.zip_code = z.RegionName
        WHERE z.RegionName IS NULL
        """)
        missing_zori = cursor.fetchone()[0]
        
        # Validate investment metrics consistency
        cursor.execute("""
        SELECT COUNT(*) FROM properties
        WHERE cap_rate > 0 AND cash_yield = 0
        OR cap_rate = 0 AND cash_yield > 0
        """)
        inconsistent_metrics = cursor.fetchone()[0]
        
        # Log validation results
        logger.info("=== Database Validation Results ===")
        logger.info(f"Properties with null IDs: {null_ids}")
        logger.info(f"Properties with negative metrics: {negative_metrics}")
        logger.info(f"Properties with suspiciously high cap rates (>30%): {high_cap_rates}")
        logger.info(f"Properties with suspiciously high IRRs (>50%): {high_irrs}")
        logger.info(f"Properties with incomplete data (price or sqft = 0): {incomplete_data}")
        logger.info(f"Properties missing bed/bath data: {missing_bed_bath}")
        logger.info(f"ZIP codes without ZORI data: {missing_zori}")
        logger.info(f"Properties with inconsistent metrics: {inconsistent_metrics}")
        
        # Check for data integrity
        validation_passed = (null_ids == 0 and negative_metrics == 0 and 
                           high_cap_rates < 10 and high_irrs < 10 and
                           inconsistent_metrics < 20)
        
        if validation_passed:
            logger.info("Database passed validation checks")
        else:
            logger.warning("Database contains potentially problematic data - review logs")
        
        conn.close()
        
        return {
            "null_ids": null_ids,
            "negative_metrics": negative_metrics,
            "high_cap_rates": high_cap_rates,
            "high_irrs": high_irrs,
            "incomplete_data": incomplete_data,
            "missing_bed_bath": missing_bed_bath,
            "missing_zori": missing_zori,
            "inconsistent_metrics": inconsistent_metrics,
            "validation_passed": validation_passed
        }
        
    except Exception as e:
        logger.error(f"Error during database validation: {str(e)}")
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
        ("monthly_payment", "REAL"),
        ("lcf_year1", "REAL"),
        ("equity_at_exit", "REAL")
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
        logger.info(f"Total properties imported: {setup_results['total_properties']}")
        logger.info(f"Setup completed in {setup_results['setup_duration_seconds']:.2f} seconds")
        logger.info(f"ZORI data available: {'Yes' if setup_results['has_zori_data'] else 'No'}")
        if setup_results['has_zori_data']:
            logger.info(f"ZIP codes with quality factors: {setup_results['zori_quality_zip_count']}")
        logger.info(f"Database validation: {'PASSED' if validation_results['validation_passed'] else 'FAILED'}")
        
        print("\nDatabase setup complete!")
        print(f"- {setup_results['total_properties']} properties imported")
        
        if hasattr(setup_results, 'cities_analyzed'):
            print(f"- {setup_results['cities_analyzed']} cities with detailed market stats")
            print(f"- {setup_results['zipcodes_analyzed']} zip codes with detailed market stats")
        
        print(f"\nDatabase file: {DB_FILE}")
        
    except Exception as e:
        logger.error(f"Database setup failed: {str(e)}")
        print(f"\nError: Database setup failed - {str(e)}")
        sys.exit(1)