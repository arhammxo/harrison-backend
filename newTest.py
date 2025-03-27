import csv
import math
import numpy as np
import numpy_financial as npf
from datetime import datetime
import os.path
import sys
import glob
from concurrent.futures import ProcessPoolExecutor, as_completed
import ast

# =====================================================================
# UTILITY FUNCTIONS
# =====================================================================

def round_price(value):
    """
    Round price values to integers for dollar amounts.
    """
    try:
        if value is None or value == '':
            return None
        # Convert to float first, then round to integer
        return int(round(float(value)))
    except (ValueError, TypeError):
        return value

def format_phone_number(phone_data, extract_numbers_only=False):
    """
    Format phone numbers in a consistent way.
    
    Args:
        phone_data: List of dictionaries or string representation of phone data
        extract_numbers_only: If True, returns just a string of formatted numbers separated by semicolons
        
    Returns:
        Formatted phone data or string of formatted phone numbers
    """
    # Explicitly check for None
    if phone_data is None:
        return None if not extract_numbers_only else ""
        
    formatted_phones = []
    
    try:
        # If it's a string, try to convert to Python object
        if isinstance(phone_data, str):
            # Handle empty strings or 'None' strings
            if not phone_data.strip() or phone_data.lower() == 'none':
                return "" if extract_numbers_only else phone_data
                
            try:
                phone_data = ast.literal_eval(phone_data)
            except (ValueError, SyntaxError):
                # If conversion fails, return as is
                return phone_data
                
        # Process the phone data
        if isinstance(phone_data, list):
            for item in phone_data:
                if isinstance(item, dict) and 'number' in item and item['number']:
                    number = str(item['number'])
                    # Keep only digits
                    digits = ''.join(c for c in number if c.isdigit())
                    
                    # Format the number
                    formatted_number = number  # Default to original
                    if len(digits) == 10:  # Standard US number
                        formatted_number = f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
                    elif len(digits) == 11 and digits[0] == '1':  # With country code
                        formatted_number = f"({digits[1:4]}) {digits[4:7]}-{digits[7:11]}"
                    
                    # Update the item in the original structure
                    item['number'] = formatted_number
                    
                    # Also add to our formatted_phones list for possible extraction
                    formatted_phones.append(formatted_number)
        
        # Return based on extraction choice
        if extract_numbers_only:
            return "; ".join(formatted_phones)
        else:
            return phone_data
            
    except Exception as e:
        # If any error occurs, log and return empty string or original
        print(f"Error formatting phone: {e}")
        return "" if extract_numbers_only else phone_data

# =====================================================================
# GLOBAL CONSTANTS AND CONFIGURATION
# =====================================================================

# List of fields that should be rounded to whole dollars
DOLLAR_FIELDS = [
    # Base property data
    'list_price', 'list_price_min', 'list_price_max', 'sold_price', 
    'assessed_value', 'estimated_value', 'tax',
    
    # Rent estimates
    'zori_monthly_rent', 'zori_annual_rent', 'zori_rent_year1', 'zori_rent_year2', 
    'zori_rent_year3', 'zori_rent_year4', 'zori_rent_year5',
    
    # Cash flow metrics
    'monthly_rent', 'annual_rent', 'tax_used', 'hoa_fee_used', 
    'cash_equity', 'transaction_cost',
    'noi_year1', 'noi_year2', 'noi_year3', 'noi_year4', 'noi_year5',
    'ucf', 'ucf_year1', 'ucf_year2', 'ucf_year3', 'ucf_year4', 'ucf_year5',
    
    # Mortgage metrics
    'loan_amount', 'monthly_payment', 'annual_debt_service',
    'principal_paid_year1', 'principal_paid_year2', 'principal_paid_year3', 
    'principal_paid_year4', 'principal_paid_year5',
    'loan_balance_year1', 'loan_balance_year2', 'loan_balance_year3', 
    'loan_balance_year4', 'loan_balance_year5',
    'lcf_year1', 'lcf_year2', 'lcf_year3', 'lcf_year4', 'lcf_year5',
    'total_principal_paid', 'final_loan_balance', 'accumulated_cash_flow',
    
    # Return metrics
    'exit_value', 'equity_at_exit'
]

# Input and output file paths
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
    'for_sale_20250227_0638.csv',
    'for_sale_20250227_0643.csv',
    'for_sale_20250227_0659.csv',
    'for_sale_20250227_0702.csv',
    'for_sale_20250227_0705.csv',
    "for_sale_20250327_1703.csv",
    "for_sale_20250327_1707.csv",
    "for_sale_20250327_1708.csv",
    "for_sale_20250327_1709.csv",
    "for_sale_20250327_1712.csv",
    "for_sale_20250327_1713.csv",
    "for_sale_20250327_1714.csv",
    "for_sale_20250327_2024.csv",
    "for_sale_20250327_2029.csv",
    "for_sale_20250327_2031.csv"
]
ZILLOW_RENT_DATA_FILE = 'zillow_rent_data.csv'
OUTPUT_FINAL_FILE = 'final.csv'

# Temporary files for intermediate steps
TEMP_DIR = 'temp_files'
TEMP_ZORI_ESTIMATES_PATTERN = os.path.join(TEMP_DIR, 'temp_zori_estimates_{}.csv')
TEMP_CASH_FLOW_PATTERN = os.path.join(TEMP_DIR, 'temp_cash_flow_{}.csv')
TEMP_MERGED_ZORI = os.path.join(TEMP_DIR, 'merged_zori_estimates.csv')
TEMP_MERGED_CASH_FLOW = os.path.join(TEMP_DIR, 'merged_cash_flow.csv')

# Create temp directory if it doesn't exist
os.makedirs(TEMP_DIR, exist_ok=True)

# Neighborhood quality factors based on ZIP codes
# Higher scores = better neighborhoods = higher growth potential, lower risk
NEIGHBORHOOD_QUALITY = {
    # NYC Manhattan premium neighborhoods (sorted by median price)
    '10013': 0.95, '10012': 0.95, '10007': 0.95, '10103': 0.95, '10069': 0.94, '10018': 0.94, 
    '10021': 0.93, '10001': 0.93, '10014': 0.93, '10011': 0.93, '10028': 0.93, '10024': 0.92,
    '10023': 0.92, '10128': 0.92, '10019': 0.92, '10022': 0.92, '10065': 0.92, '10075': 0.92,
    '10016': 0.91, '10010': 0.91, '10038': 0.91, '10036': 0.90, '10280': 0.90, '10282': 0.90,
    '10025': 0.90, '10003': 0.90, '10009': 0.89, '10006': 0.89, '10002': 0.89, '10005': 0.88,
    '10027': 0.87, '10026': 0.87, '10029': 0.86, '10004': 0.86, '10044': 0.86, '10017': 0.86,
    '10035': 0.84, '10033': 0.84, '10031': 0.83, '10032': 0.83, '10030': 0.82, '10037': 0.82,
    '10039': 0.81, '10040': 0.81, '10034': 0.80,

    # Brooklyn neighborhoods (sorted by median price)
    '11231': 0.93, '11225': 0.92, '11249': 0.92, '11217': 0.91, '11216': 0.91, '11201': 0.91,
    '11222': 0.90, '11215': 0.90, '11204': 0.89, '11211': 0.89, '11233': 0.88, '11220': 0.88,
    '11221': 0.88, '11205': 0.87, '11237': 0.87, '11228': 0.87, '11232': 0.87, '11238': 0.87,
    '11223': 0.86, '11206': 0.86, '11214': 0.86, '11207': 0.85, '11212': 0.84, '11213': 0.84,
    '11218': 0.84, '11226': 0.84, '11236': 0.84, '11203': 0.83, '11208': 0.83, '11219': 0.83, 
    '11229': 0.83, '11210': 0.82, '11235': 0.82, '11224': 0.80, '11243': 0.80, '11209': 0.80,
    '11239': 0.78,

    # Queens neighborhoods (sorted by median price)
    '11109': 0.91, '11366': 0.90, '11211': 0.89, '11363': 0.89, '11357': 0.88, '11385': 0.88,
    '11356': 0.87, '11362': 0.87, '11103': 0.87, '11378': 0.87, '11358': 0.87, '11101': 0.87,
    '11361': 0.87, '11379': 0.87, '11432': 0.86, '11365': 0.86, '11416': 0.86, '11429': 0.86,
    '11355': 0.85, '11426': 0.85, '11422': 0.85, '11411': 0.85, '11417': 0.85, '11106': 0.85,
    '11433': 0.85, '11427': 0.85, '11419': 0.84, '11368': 0.84, '11420': 0.84, '11354': 0.84,
    '11418': 0.84, '11412': 0.84, '11434': 0.84, '11102': 0.83, '11423': 0.83, '11428': 0.83,
    '11413': 0.83, '11413': 0.83, '11436': 0.82, '11414': 0.82, '11435': 0.82, '11415': 0.82,
    '11374': 0.81, '11372': 0.81, '11364': 0.81, '11367': 0.81, '11370': 0.81, '11377': 0.81,
    '11375': 0.81, '11373': 0.80, '11105': 0.80, '11104': 0.80, '11369': 0.79, '11692': 0.78,
    '11694': 0.78, '11691': 0.77, '11693': 0.77, '11004': 0.77, '11005': 0.77, '11040': 0.77,
    '11756': 0.77,
    
    # Bronx neighborhoods (sorted by median price)
    '10454': 0.86, '10475': 0.86, '10453': 0.85, '10455': 0.85, '10458': 0.85, '10469': 0.84,
    '10465': 0.84, '10459': 0.84, '10461': 0.84, '10473': 0.83, '10473': 0.83, '10460': 0.83,
    '10457': 0.82, '10467': 0.82, '10466': 0.82, '10456': 0.81, '10472': 0.81, '10464': 0.81,
    '10301': 0.81, '10451': 0.80, '10462': 0.80, '10471': 0.80, '10463': 0.80, '10468': 0.78,
    '10474': 0.78, '10452': 0.78,

    # Staten Island neighborhoods (sorted by median price)
    '10307': 0.87, '10309': 0.87, '10314': 0.87, '10308': 0.86, '10304': 0.85, '10306': 0.85,
    '10305': 0.84, '10301': 0.83, '10310': 0.83, '10312': 0.82, '10302': 0.81, '10303': 0.80,

    # California neighborhoods
    '90001': 0.76, '90002': 0.77, '90003': 0.77, '90004': 0.85, '90005': 0.86, '90006': 0.77,
    '90007': 0.78, '90008': 0.82, '90010': 0.88, '90011': 0.75, '90012': 0.82, '90013': 0.80,
    '90014': 0.83, '90015': 0.84, '90016': 0.84, '90017': 0.83, '90018': 0.80, '90019': 0.85,
    '90020': 0.87, '90021': 0.78, '90022': 0.79, '90023': 0.76, '90024': 0.91, '90025': 0.90,
    '90026': 0.88, '90027': 0.89, '90028': 0.87, '90029': 0.84, '90031': 0.83, '90032': 0.82,
    '90033': 0.78, '90034': 0.89, '90035': 0.90, '90036': 0.90, '90037': 0.77, '90038': 0.82,
    '90039': 0.88, '90041': 0.88, '90042': 0.85, '90043': 0.84, '90044': 0.77, '90045': 0.87,
    '90046': 0.90, '90047': 0.79, '90048': 0.91, '90049': 0.92, '90056': 0.85, '90057': 0.83,
    '90059': 0.76, '90061': 0.77, '90062': 0.78, '90063': 0.79, '90064': 0.89, '90065': 0.85,
    '90066': 0.89, '90067': 0.92, '90068': 0.90, '90069': 0.92, '90077': 0.93, '90094': 0.91,
    '90210': 0.94, '90230': 0.87, '90232': 0.89, '90247': 0.83, '90272': 0.93, '90291': 0.91,
    '90292': 0.91, '90293': 0.89, '90402': 0.93, '90501': 0.82, '90710': 0.84, '90717': 0.83,
    '90731': 0.86, '90732': 0.87, '90744': 0.80,
    
    # Los Angeles Valley
    '91040': 0.88, '91042': 0.87, '91214': 0.89, '91303': 0.87, '91304': 0.85, '91306': 0.85,
    '91307': 0.88, '91311': 0.87, '91316': 0.89, '91324': 0.86, '91325': 0.85, '91326': 0.89,
    '91331': 0.83, '91335': 0.85, '91340': 0.82, '91342': 0.85, '91343': 0.84, '91344': 0.87,
    '91345': 0.84, '91352': 0.84, '91356': 0.90, '91364': 0.90, '91367': 0.90, '91401': 0.86,
    '91402': 0.82, '91403': 0.91, '91405': 0.83, '91406': 0.84, '91411': 0.87, '91423': 0.91,
    '91436': 0.93, '91504': 0.89, '91601': 0.87, '91602': 0.89, '91604': 0.91, '91605': 0.82,
    '91606': 0.84, '91607': 0.88,
    
    # San Diego area
    '91911': 0.83, '91913': 0.87, '91942': 0.87, '91950': 0.84, '92014': 0.93, '92027': 0.85,
    '92029': 0.88, '92037': 0.92, '92067': 0.94, '92101': 0.89, '92102': 0.82, '92103': 0.90,
    '92104': 0.87, '92105': 0.83, '92106': 0.89, '92107': 0.90, '92108': 0.87, '92109': 0.91,
    '92110': 0.88, '92111': 0.86, '92113': 0.80, '92114': 0.82, '92115': 0.85, '92116': 0.88,
    '92117': 0.87, '92119': 0.89, '92120': 0.90, '92121': 0.91, '92122': 0.90, '92123': 0.88,
    '92124': 0.89, '92126': 0.87, '92127': 0.92, '92128': 0.91, '92129': 0.90, '92130': 0.93,
    '92131': 0.91, '92139': 0.84, '92154': 0.82, '92173': 0.85,
    
    # San Francisco area
    '94102': 0.85, '94103': 0.87, '94104': 0.91, '94105': 0.92, '94107': 0.91, '94108': 0.91,
    '94109': 0.90, '94110': 0.90, '94111': 0.93, '94112': 0.86, '94114': 0.92, '94115': 0.93,
    '94116': 0.88, '94117': 0.91, '94118': 0.92, '94121': 0.90, '94122': 0.89, '94123': 0.93,
    '94124': 0.79, '94127': 0.90, '94131': 0.91, '94132': 0.87, '94133': 0.91, '94134': 0.83,
    '94158': 0.92,
    
    # Las Vegas area
    '89011': 0.90, '89031': 0.83, '89101': 0.78, '89102': 0.82, '89103': 0.85, '89104': 0.80,
    '89106': 0.79, '89107': 0.81, '89108': 0.82, '89109': 0.86, '89110': 0.80, '89113': 0.90,
    '89115': 0.77, '89117': 0.89, '89118': 0.86, '89119': 0.84, '89120': 0.85, '89121': 0.83,
    '89122': 0.80, '89123': 0.87, '89124': 0.88, '89128': 0.86, '89129': 0.88, '89130': 0.84,
    '89131': 0.86, '89134': 0.89, '89135': 0.91, '89138': 0.92, '89139': 0.87, '89141': 0.89,
    '89142': 0.81, '89143': 0.85, '89144': 0.89, '89145': 0.87, '89146': 0.85, '89147': 0.86,
    '89148': 0.88, '89149': 0.87, '89156': 0.82, '89158': 0.90, '89161': 0.91, '89166': 0.88,
    '89169': 0.83, '89178': 0.89, '89179': 0.90, '89183': 0.88,
    
    # Default for others
    'default': 0.75
}

# Property type characteristic modifiers
PROPERTY_TYPE_MODIFIERS = {
    # For rental adjustments
    'rent': {
        'Condo': 1.15,      # Premium for amenities and newer condition
        'Co-op': 0.95,      # Discount for restrictions and often older buildings
        'Single Family': 1.05,
        'Multi Family': 0.90, # Per unit discount for multi-family
        'Townhouse': 1.10,   # Premium for privacy and space
        'Luxury': 1.25,      # Premium segment
        'default': 1.0
    },
    # For growth rate adjustments
    'growth': {
        'Condo': 1.05,
        'Co-op': 0.95,
        'Single Family': 1.02,
        'Multi Family': 1.08,
        'Townhouse': 1.04,
        'default': 1.0
    }
}

# Base mortgage rates by price ranges
BASE_RATES = {
    'under_250k': 8.000,  # Higher rates for lower-priced properties (potentially higher risk)
    '250k_500k': 7.750,
    '500k_750k': 7.500,
    '750k_1m': 7.250,
    'over_1m': 7.000,   # Better rates for luxury properties
}

# Loan term options by price and property type
LOAN_TERMS = {
    'under_500k': 15,     # 15-year terms for lower values
    '500k_750k': 20,      # 20-year terms for mid-range
    'over_750k': 25,      # 25-year terms for higher values
}

# Maximum number of worker processes to use
MAX_WORKERS = 4

# =====================================================================
# ZORI DATA PROCESSING FUNCTIONS
# =====================================================================

def load_zori_data():
    """
    Load and process Zillow Observed Rent Index (ZORI) data.
    Returns dictionaries with rent values, growth rates, and seasonality data by zip code.
    """
    print(f"Loading ZORI data from {ZILLOW_RENT_DATA_FILE}...")
    zori_by_zip = {}
    growth_rates_by_zip = {}
    seasonality_patterns = {}
    state_avg_rents = {}
    
    with open(ZILLOW_RENT_DATA_FILE, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        date_columns = [col for col in reader.fieldnames if col.startswith('20')]
        sorted_date_columns = sorted(date_columns)
        
        # Get latest date and historical comparison dates
        latest_date = sorted_date_columns[-1]
        one_year_ago_date = sorted_date_columns[-13]  # 12 months back
        five_years_ago_date = sorted_date_columns[-61]  # 5 years back
        
        # Process each zip code row
        for row in reader:
            try:
                zip_code = str(int(float(row['RegionName'])))
                state = row['State']
                
                # Get latest rent value
                latest_rent = float(row[latest_date]) if row[latest_date] else None
                
                # Calculate 1-year growth rate
                one_year_ago_rent = float(row[one_year_ago_date]) if row[one_year_ago_date] else None
                one_year_growth = ((latest_rent / one_year_ago_rent) - 1) * 100 if latest_rent and one_year_ago_rent else None
                
                # Calculate 5-year CAGR
                five_years_ago_rent = float(row[five_years_ago_date]) if row[five_years_ago_date] else None
                five_year_cagr = (math.pow(latest_rent / five_years_ago_rent, 1/5) - 1) * 100 if latest_rent and five_years_ago_rent else None
                
                # Calculate seasonal patterns (month-over-month changes)
                monthly_patterns = {}
                last_24_months = sorted_date_columns[-24:]
                for i in range(1, len(last_24_months)):
                    current_month = last_24_months[i]
                    prev_month = last_24_months[i-1]
                    month_num = int(current_month.split('-')[1])
                    
                    current_rent = float(row[current_month]) if row[current_month] else None
                    prev_rent = float(row[prev_month]) if row[prev_month] else None
                    
                    if current_rent and prev_rent:
                        change_pct = ((current_rent / prev_rent) - 1) * 100
                        if month_num not in monthly_patterns:
                            monthly_patterns[month_num] = []
                        monthly_patterns[month_num].append(change_pct)
                
                # Store the data
                if latest_rent:
                    zori_by_zip[zip_code] = latest_rent
                    
                    growth_rates_by_zip[zip_code] = {
                        'one_year': one_year_growth if one_year_growth else 0,
                        'five_year_cagr': five_year_cagr if five_year_cagr else 0
                    }
                    
                    seasonality_patterns[zip_code] = monthly_patterns
                    
                    # Track state averages
                    if state not in state_avg_rents:
                        state_avg_rents[state] = {'sum': 0, 'count': 0}
                    state_avg_rents[state]['sum'] += latest_rent
                    state_avg_rents[state]['count'] += 1
                    
            except (ValueError, TypeError, ZeroDivisionError):
                continue
    
    # Calculate average monthly seasonality across all zip codes
    avg_seasonality = {}
    for month in range(1, 13):
        all_changes = []
        for zip_code, patterns in seasonality_patterns.items():
            if month in patterns:
                all_changes.extend(patterns[month])
        
        if all_changes:
            avg_seasonality[month] = sum(all_changes) / len(all_changes)
        else:
            avg_seasonality[month] = 0
            
    # Calculate state average rents
    state_averages = {}
    for state, data in state_avg_rents.items():
        if data['count'] > 0:
            state_averages[state] = data['sum'] / data['count']
    
    print(f"Loaded ZORI data for {len(zori_by_zip)} zip codes across {len(state_averages)} states")
    return zori_by_zip, growth_rates_by_zip, avg_seasonality, state_averages

def find_closest_zip_with_data(target_zip, zori_by_zip):
    """
    Find the closest zip code that has ZORI data.
    Returns the original zip if it exists in the data, otherwise finds closest available zip.
    """
    if target_zip in zori_by_zip:
        return target_zip
        
    # If the zip code isn't found, find the closest one
    try:
        target_zip_int = int(target_zip)
        closest_zip = None
        min_distance = float('inf')
        
        for zip_code in zori_by_zip.keys():
            try:
                zip_int = int(zip_code)
                distance = abs(zip_int - target_zip_int)
                
                if distance < min_distance:
                    min_distance = distance
                    closest_zip = zip_code
            except ValueError:
                continue
                
        return closest_zip
    except ValueError:
        # If we can't convert to int, return None
        return None

# =====================================================================
# PROPERTY CHARACTERISTIC ADJUSTMENT FUNCTIONS
# =====================================================================

def calculate_bed_bath_factor(beds, baths):
    """
    Calculate an adjustment factor based on bedroom and bathroom count.
    Returns a multiplier reflecting the rental premium/discount for the configuration.
    """
    bed_value = 0
    bath_value = 0
    
    # Non-linear bedroom value
    if beds is not None:
        if beds == 0:  # Studio
            bed_value = 0.85
        elif beds == 1:
            bed_value = 1.0  # Base case
        elif beds == 2:
            bed_value = 1.2
        elif beds == 3:
            bed_value = 1.35
        elif beds >= 4:
            bed_value = 1.45  # Diminishing returns after 4 bedrooms
        else:
            bed_value = 1.0  # Default
    else:
        bed_value = 1.0
        
    # Bathroom value
    if baths is not None:
        if baths < 1:
            bath_value = 0.9
        elif baths == 1:
            bath_value = 1.0  # Base case
        elif baths == 1.5:
            bath_value = 1.05
        elif baths == 2:
            bath_value = 1.1
        elif baths <= 3:
            bath_value = 1.2
        else:
            bath_value = 1.25
    else:
        bath_value = 1.0
    
    # Combine with premium for bed/bath ratio
    if beds and baths and beds > 0:
        ratio = baths / beds
        if ratio >= 1.5:  # Premium for high bath-to-bed ratio
            return (bed_value + bath_value) / 2 * 1.05
    
    return (bed_value + bath_value) / 2

def calculate_size_factor(sqft):
    """
    Calculate an adjustment factor based on property size.
    Returns a multiplier reflecting the rental premium/discount for the size.
    """
    if not sqft or sqft <= 0:
        return 1.0
        
    # Diminishing returns for larger units
    if sqft < 500:
        return 0.85
    elif sqft < 750:
        return 0.95
    elif sqft < 1000:
        return 1.0  # Base case
    elif sqft < 1500:
        return 1.1
    elif sqft < 2000:
        return 1.2
    elif sqft < 3000:
        return 1.3
    else:
        return 1.4

def calculate_condition_factor(year_built):
    """
    Calculate an adjustment factor based on property age and condition.
    Returns a multiplier reflecting the rental premium/discount for the age.
    """
    if not year_built or year_built <= 0:
        return 1.0
        
    current_year = datetime.now().year
    age = current_year - year_built
    
    if age < 3:
        return 1.15  # New construction premium
    elif age < 10:
        return 1.1
    elif age < 20:
        return 1.05
    elif age < 40:
        return 1.0  # Base case
    elif age < 75:
        return 0.95
    else:
        return 0.9

def calculate_amenity_score(row):
    """
    Calculate an amenity score based on property description and features.
    Returns a multiplier reflecting the rental premium for amenities.
    """
    score = 1.0
    
    # Check for doorman/luxury building indicators in text description
    description = str(row.get('text', '')).lower()
    luxury_keywords = ['doorman', 'concierge', 'luxury', 'high-end', 'renovated', 
                      'marble', 'stainless', 'premium', 'upscale', 'views', 'pool',
                      'gym', 'fitness', 'modern', 'updated', 'granite', 'new appliances']
    
    # Count luxury keywords in description
    keyword_count = sum(1 for keyword in luxury_keywords if keyword in description)
    score += min(0.2, 0.01 * keyword_count)  # Cap at 20% boost
    
    # Check for specific amenities
    if row.get('parking_garage') and float(row.get('parking_garage', 0) or 0) > 0:
        score += 0.05
        
    # Check HOA fee as proxy for amenities
    try:
        hoa_fee = float(row.get('hoa_fee', 0) or 0)
        if hoa_fee > 500:
            score += 0.05
        elif hoa_fee > 1000:
            score += 0.1
    except (ValueError, TypeError):
        pass
        
    return score

def get_neighborhood_factor(zip_code):
    """
    Get the neighborhood quality factor for a given zip code.
    Returns a factor between 0.75 and 0.95 representing neighborhood quality.
    """
    return NEIGHBORHOOD_QUALITY.get(zip_code, NEIGHBORHOOD_QUALITY['default'])

def calculate_down_payment_pct(list_price, neighborhood_factor):
    """
    Calculate the appropriate down payment percentage based on property price and neighborhood.
    Returns a percentage between 30% and 60%.
    """
    # Base down payment percentage by price range
    if list_price < 200000:
        down_payment_pct = 0.35  # 35% down for cheaper properties
    elif list_price < 500000:
        down_payment_pct = 0.40  # 40% down for lower mid-range
    elif list_price < 750000:
        down_payment_pct = 0.45  # 45% down for upper mid-range
    elif list_price < 1000000:
        down_payment_pct = 0.50  # 50% down for higher end
    else:
        down_payment_pct = 0.55  # 55% down for luxury properties
    
    # Additional adjustment based on neighborhood quality
    # Better neighborhoods can get more favorable financing
    down_payment_pct -= (neighborhood_factor - 0.75) * 0.05
    
    # Ensure down payment is between 30% and 60%
    return min(0.60, max(0.30, down_payment_pct))

def determine_mortgage_terms(list_price, neighborhood_factor):
    """
    Determine appropriate mortgage interest rate and term based on property characteristics.
    Returns interest rate and loan term.
    """
    # Base rate by price range
    if list_price < 250000:
        interest_rate = BASE_RATES['under_250k']
    elif list_price < 500000:
        interest_rate = BASE_RATES['250k_500k']
    elif list_price < 750000:
        interest_rate = BASE_RATES['500k_750k']
    elif list_price < 1000000:
        interest_rate = BASE_RATES['750k_1m']
    else:
        interest_rate = BASE_RATES['over_1m']
    
    # Adjustment based on neighborhood quality (up to 0.5% reduction for prime areas)
    neighborhood_adjustment = (neighborhood_factor - 0.75) * 1.0
    interest_rate -= neighborhood_adjustment
    
    # Cap the range of possible interest rates
    interest_rate = min(9.0, max(6.0, interest_rate))
    
    # Determine loan term based on property price
    if list_price < 500000:
        loan_term = LOAN_TERMS['under_500k']
    elif list_price < 750000:
        loan_term = LOAN_TERMS['500k_750k']
    else:
        loan_term = LOAN_TERMS['over_750k']
    
    return interest_rate, loan_term

def calculate_growth_rate(zip_code, neighborhood_factor, property_style, growth_rates_by_zip):
    """
    Calculate customized growth rate based on location, property type, and historical data.
    Returns annual growth rate percentage.
    """
    # Get growth data for this zip code or use default
    growth_data = growth_rates_by_zip.get(zip_code, {'one_year': 3.0, 'five_year_cagr': 3.0})
    five_year_cagr = growth_data['five_year_cagr']
    
    # Get property type modifier
    property_type_modifier = PROPERTY_TYPE_MODIFIERS['growth'].get(property_style, PROPERTY_TYPE_MODIFIERS['growth']['default'])
    
    # Base growth rate (between 2-6%)
    base_growth_rate = min(6.0, max(2.0, five_year_cagr))
    
    # Apply neighborhood and property type adjustments
    neighborhood_adjustment = neighborhood_factor * 0.02  # Up to 2% additional based on neighborhood
    property_adjustment = (property_type_modifier - 1.0) * 0.01  # Up to 1% additional based on property type
    
    # Final growth rate (capped between 2% and 10%)
    growth_rate = min(10.0, max(2.0, base_growth_rate + neighborhood_adjustment * 100 + property_adjustment * 100))
    
    return growth_rate / 100  # Return as decimal

def calculate_exit_cap_rate(entry_cap_rate, growth_rate, neighborhood_factor):
    """
    Calculate the exit cap rate, typically higher than entry cap rate to reflect future risk.
    Returns exit cap rate as a decimal.
    """
    # Convert entry cap rate to decimal if it's in percentage form
    if entry_cap_rate > 1:
        entry_cap_rate = entry_cap_rate / 100
    
    # Base cap rate expansion
    base_expansion = 0.01  # 1% base expansion
    
    # Reduce expansion for better neighborhoods and higher growth properties
    neighborhood_adjustment = (neighborhood_factor - 0.75) * 0.015
    growth_adjustment = (growth_rate - 0.03) * 0.5
    
    cap_rate_expansion = max(0, base_expansion - neighborhood_adjustment - growth_adjustment)
    exit_cap_rate = entry_cap_rate + cap_rate_expansion
    
    # Ensure exit cap rate is reasonable (min 4%, max 10%)
    return min(0.10, max(0.04, exit_cap_rate))

def calculate_property_ranking(row):
    """
    Calculate a comprehensive investment ranking (1-10) for a property.
    Returns a float score and integer ranking.
    """
    try:
        # Extract key metrics
        cap_rate = float(row.get('cap_rate', 0) or 0)
        cash_on_cash = float(row.get('cash_on_cash', 0) or 0)
        irr = float(row.get('irr', 0) or 0)
        grm = float(row.get('gross_rent_multiplier', 0) or 0)
        down_payment_pct = float(row.get('down_payment_pct', 0) or 0)
        interest_rate = float(row.get('interest_rate', 0) or 0)
        growth_rate = float(row.get('zori_growth_rate', 0) or 0)
        total_principal_paid = float(row.get('total_principal_paid', 0) or 0)
        list_price = float(row.get('list_price', 0) or 0)
        total_return = float(row.get('total_return', 0) or 0)
        zip_code = str(int(float(row.get('zip_code', 0) or 0)))
        
        # Get neighborhood quality factor
        neighborhood_factor = get_neighborhood_factor(zip_code)
        
        # 1. Financial Performance Score (40%)
        
        # Cap Rate Score (10%)
        if cap_rate < 3:
            cap_rate_score = 2 + (cap_rate / 3) * 2  # 2-4 points
        elif cap_rate < 5:
            cap_rate_score = 4 + ((cap_rate - 3) / 2) * 2  # 4-6 points
        elif cap_rate < 7:
            cap_rate_score = 6 + ((cap_rate - 5) / 2) * 2  # 6-8 points
        else:
            cap_rate_score = 8 + min(2, (cap_rate - 7) / 3)  # 8-10 points, capped at 10
        
        # Cash-on-Cash Return Score (15%)
        if cash_on_cash < 0:
            coc_score = 1 + (cash_on_cash + 15) / 15 * 2  # 1-3 points
        elif cash_on_cash < 3:
            coc_score = 3 + (cash_on_cash / 3) * 2  # 3-5 points
        elif cash_on_cash < 6:
            coc_score = 5 + ((cash_on_cash - 3) / 3) * 2  # 5-7 points
        elif cash_on_cash < 9:
            coc_score = 7 + ((cash_on_cash - 6) / 3) * 2  # 7-9 points
        else:
            coc_score = 9 + min(1, (cash_on_cash - 9) / 3)  # 9-10 points, capped at 10
        
        # IRR Score (15%)
        if irr < 0:
            irr_score = 1 + (irr + 25) / 25 * 2  # 1-3 points
        elif irr < 5:
            irr_score = 3 + (irr / 5) * 2  # 3-5 points
        elif irr < 10:
            irr_score = 5 + ((irr - 5) / 5) * 2  # 5-7 points
        elif irr < 15:
            irr_score = 7 + ((irr - 10) / 5) * 2  # 7-9 points
        else:
            irr_score = 9 + min(1, (irr - 15) / 10)  # 9-10 points, capped at 10
        
        # 2. Risk Assessment Score (30%)
        
        # Price-to-Rent Ratio Score (10%)
        if grm > 35:
            grm_score = 1 + min(2, (60 - grm) / 25)  # 1-3 points
        elif grm > 25:
            grm_score = 3 + ((35 - grm) / 10) * 2  # 3-5 points
        elif grm > 20:
            grm_score = 5 + ((25 - grm) / 5) * 2  # 5-7 points
        elif grm > 15:
            grm_score = 7 + ((20 - grm) / 5) * 2  # 7-9 points
        else:
            grm_score = 9 + min(1, (15 - grm) / 10)  # 9-10 points, capped at 10
        
        # Leverage Risk Score (10%)
        # Higher down payment (less leverage) = higher score
        leverage_risk_score = 5 + (down_payment_pct - 0.4) * 20  # 0.3=3, 0.4=5, 0.5=7, 0.6=9
        # Adjust for interest rate (lower is better)
        rate_adjustment = (8 - interest_rate) * 0.3  # +0.6 for 6%, 0 for 8%
        leverage_risk_score = min(10, max(1, leverage_risk_score + rate_adjustment))
        
        # Location Quality Score (10%)
        location_score = (neighborhood_factor - 0.75) * 40  # 0.75=0, 0.85=4, 0.95=8
        location_score = min(10, max(1, location_score + 1))  # Scale to 1-10
        
        # 3. Growth Potential Score (30%)
        
        # Appreciation Potential Score (10%)
        growth_score = 5 + (growth_rate - 3) * 0.7  # 3%=5, 7%=7.8
        growth_score = min(10, max(1, growth_score))
        
        # Equity Building Score (10%)
        # Calculate principal paydown as % of purchase price
        if list_price > 0:
            principal_pct = (total_principal_paid / list_price) * 100
            equity_score = 1 + principal_pct * 0.5  # 2%=2, 6%=4, 10%=6, 14%=8, 18%=10
            equity_score = min(10, max(1, equity_score))
        else:
            equity_score = 5  # Default
        
        # Total Return Score (10%)
        if total_return <= 1:
            return_score = 1 + total_return * 3  # 0=1, 0.33=2, 0.67=3, 1=4
        elif total_return <= 1.5:
            return_score = 4 + (total_return - 1) * 4  # 1=4, 1.25=5, 1.5=6
        elif total_return <= 2:
            return_score = 6 + (total_return - 1.5) * 4  # 1.5=6, 1.75=7, 2=8
        else:
            return_score = 8 + min(2, (total_return - 2))  # 2=8, 3=9, 4+=10
        
        # Calculate weighted total score
        financial_score = (cap_rate_score * 0.10) + (coc_score * 0.15) + (irr_score * 0.15)
        risk_score = (grm_score * 0.10) + (leverage_risk_score * 0.10) + (location_score * 0.10)
        growth_score = (growth_score * 0.10) + (equity_score * 0.10) + (return_score * 0.10)
        
        total_score = financial_score + risk_score + growth_score
        
        # Convert to 1-10 scale if needed (though it should already be close)
        total_score = min(10, max(1, total_score))
        
        # Round to one decimal place
        total_score = round(total_score, 1)
        
        # Convert to integer ranking 1-10
        ranking = min(10, max(1, round(total_score)))
        
        return total_score, ranking
        
    except Exception as e:
        print(f"Error calculating property ranking: {e}")
        return 5.0, 5  # Default middle ranking

# =====================================================================
# RENTAL INCOME ESTIMATION FUNCTIONS
# =====================================================================

def estimate_rental_income(row, zori_by_zip, growth_rates_by_zip, avg_seasonality, state_averages):
    """
    Estimate rental income using ZORI data and property characteristics.
    Returns monthly rent, annual rent, growth rate, 5-year projections, and gross rent multiplier.
    """
    try:
        # Get the property zip code
        zip_code = str(int(float(row.get('zip_code', 0) or 0)))
        state = row.get('state')
        
        # Find ZORI data for this zip code
        if zip_code not in zori_by_zip:
            zip_code = find_closest_zip_with_data(zip_code, zori_by_zip)
            
        if not zip_code:
            # Fall back to state average if available
            if state in state_averages:
                base_zori_rent = state_averages[state]
            else:
                return None, None, None, None, None
        else:
            # Get base ZORI rent for this zip
            base_zori_rent = zori_by_zip[zip_code]
        
        # Get property characteristics
        beds = float(row.get('beds', 0) or 0)
        full_baths = float(row.get('full_baths', 0) or 0)
        half_baths = float(row.get('half_baths', 0) or 0)
        baths = full_baths + (0.5 * half_baths)
        sqft = float(row.get('sqft', 0) or 0)
        year_built = float(row.get('year_built', 0) or 0)
        property_style = str(row.get('style', '')).strip() or 'default'
        
        # Calculate adjustment factors
        bed_bath_factor = calculate_bed_bath_factor(beds, baths)
        size_factor = calculate_size_factor(sqft)
        condition_factor = calculate_condition_factor(year_built)
        amenity_factor = calculate_amenity_score(row)
        property_type_factor = PROPERTY_TYPE_MODIFIERS['rent'].get(property_style, PROPERTY_TYPE_MODIFIERS['rent']['default'])
        
        # Current month for seasonality
        current_month = datetime.now().month
        seasonality_factor = 1 + (avg_seasonality.get(current_month, 0) / 100)
        
        # Special case for multi-family properties
        if property_style == 'Multi Family' and beds >= 4:
            # Assume it's a multi-unit building with separate rentable units
            units = max(2, beds // 2)  # Estimate number of units
            adjusted_rent = base_zori_rent * units * 0.85  # 15% discount for multi-unit
        else:
            # Weighted adjustment calculation
            adjustment_factor = (
                bed_bath_factor * 0.35 +  # 35% weight to beds/baths
                size_factor * 0.25 +      # 25% weight to size
                condition_factor * 0.15 +  # 15% weight to condition/age
                amenity_factor * 0.15 +   # 15% weight to amenities
                property_type_factor * 0.10  # 10% weight to property type
            )
            
            # Apply seasonality
            adjustment_factor *= seasonality_factor
            
            # Calculate adjusted rent
            adjusted_rent = base_zori_rent * adjustment_factor
        
        # Get neighborhood factor for growth rate calculation
        neighborhood_factor = get_neighborhood_factor(zip_code)
        
        # Calculate property-specific growth rate
        growth_rate = calculate_growth_rate(zip_code, neighborhood_factor, property_style, growth_rates_by_zip)
        
        # Calculate 5-year rent projections
        rent_projections = [adjusted_rent]
        for year in range(1, 5):
            projected_rent = rent_projections[-1] * (1 + growth_rate)
            rent_projections.append(projected_rent)
        
        # Calculate annual rent
        annual_rent = adjusted_rent * 12
        
        # Calculate gross rent multiplier (price to annual rent)
        try:
            list_price = float(row.get('list_price', 0) or 0)
            grm = list_price / annual_rent if annual_rent > 0 else 0
        except (ValueError, ZeroDivisionError):
            grm = 0
            
        return adjusted_rent, annual_rent, growth_rate * 100, rent_projections, grm
        
    except Exception as e:
        print(f"Error estimating rental income: {e}")
        return None, None, None, None, None

# =====================================================================
# INVESTMENT METRICS CALCULATION FUNCTIONS
# =====================================================================

def calculate_cash_flow_metrics(row, is_zori_based=True):
    """
    Calculate cash flow metrics based on rental income and property characteristics.
    Returns a dictionary with all calculated metrics.
    """
    metrics = {}
    
    try:
        # Get property values
        list_price = float(row.get('list_price', 0) or 0)
        if list_price <= 0:
            return metrics
            
        # Determine which rental income to use
        if is_zori_based and row.get('zori_monthly_rent'):
            monthly_rent = float(row.get('zori_monthly_rent', 0))
            annual_rent = float(row.get('zori_annual_rent', 0))
            growth_rate = float(row.get('zori_growth_rate', 3.0)) / 100  # Convert to decimal
        else:
            # Fallback to original PTR calculation if ZORI data isn't available
            ptr_value = float(row.get('PTR', 0) or 0)
            if ptr_value <= 0:
                return metrics
                
            sqft = float(row.get('sqft', 0) or 0)
            price_per_sqft = float(row.get('price_per_sqft', 0) or 0)
            
            # Calculate BRE
            if sqft > 0 and price_per_sqft > 0:
                bre = sqft * price_per_sqft
            else:
                bre = list_price
                
            # Calculate adjustment factor
            beds = float(row.get('beds', 0) or 0)
            full_baths = float(row.get('full_baths', 0) or 0)
            af = 1 + (0.05 + 0.02 * beds + 0.01 * full_baths + 0.05 * int(sqft > 2000))
            
            # Calculate FRE
            monthly_rent = ptr_value * bre * af
            annual_rent = monthly_rent * 12
            growth_rate = 0.03  # Default 3% growth rate
        
        metrics['monthly_rent'] = monthly_rent
        metrics['annual_rent'] = annual_rent
        
        # Get property characteristics for calculations
        zip_code = str(int(float(row.get('zip_code', 0) or 0)))
        neighborhood_factor = get_neighborhood_factor(zip_code)
        property_style = str(row.get('style', '')).strip() or 'default'
        
        # Calculate expenses
        tax = float(row.get('tax', 0) or 0)
        if tax == 0:
            tax = 0.01 * list_price  # Estimate tax at 1% of list price
        metrics['tax_used'] = tax
        
        hoa_fee = float(row.get('hoa_fee', 0) or 0)
        if hoa_fee == 0:
            hoa_fee = (0.0015 * list_price) / 12  # Estimate HOA at 0.15% of list price annually
        metrics['hoa_fee_used'] = hoa_fee
        
        # Calculate variable down payment percentage
        down_payment_pct = calculate_down_payment_pct(list_price, neighborhood_factor)
        metrics['down_payment_pct'] = down_payment_pct
        
        # Calculate mortgage terms
        interest_rate, loan_term = determine_mortgage_terms(list_price, neighborhood_factor)
        metrics['interest_rate'] = interest_rate
        metrics['loan_term'] = loan_term
        
        # Calculate transaction costs and cash equity
        transaction_cost = 0.01 * list_price
        cash_equity = down_payment_pct * (list_price + transaction_cost)
        metrics['transaction_cost'] = transaction_cost
        metrics['cash_equity'] = cash_equity
        
        # Calculate NOI for 5 years with all standard operating expenses
        current_rent = annual_rent
        for year in range(1, 6):
            # Calculate standard operating expenses
            vacancy = current_rent * 0.05  # 5% vacancy rate
            management = current_rent * 0.08  # 8% property management fee
            maintenance = current_rent * 0.05  # 5% for maintenance
            insurance = list_price * 0.005  # Annual insurance at 0.5% of property value
            
            # Total expenses
            total_expenses = (hoa_fee * 12) + vacancy + management + maintenance + insurance
            
            # Calculate NOI
            noi = current_rent - total_expenses
            metrics[f'noi_year{year}'] = noi
            current_rent *= (1 + growth_rate)  # Apply growth rate
        
        # Calculate cap rate with reasonable bounds
        if list_price > 0:
            raw_cap_rate = (metrics['noi_year1'] / list_price) * 100
            metrics['cap_rate'] = min(15, max(-5, raw_cap_rate))  # Limit to reasonable range
        else:
            metrics['cap_rate'] = 0
        
        # Calculate unlevered cash flow (UCF)
        for year in range(1, 6):
            metrics[f'ucf_year{year}'] = metrics[f'noi_year{year}'] - tax
        
        metrics['ucf'] = metrics['ucf_year1']  # First year UCF
        
        # Calculate cash yield
        metrics['cash_yield'] = (metrics['ucf'] / cash_equity) * 100 if cash_equity > 0 else 0
        
        return metrics
        
    except Exception as e:
        print(f"Error calculating cash flow metrics: {e}")
        return metrics

def calculate_mortgage_metrics(row, metrics):
    """
    Calculate mortgage-related metrics including principal payments, loan balance, and debt service.
    Updates the metrics dictionary with these values.
    """
    try:
        cash_equity = metrics.get('cash_equity', 0)
        list_price = float(row.get('list_price', 0) or 0)
        transaction_cost = metrics.get('transaction_cost', 0)
        down_payment_pct = metrics.get('down_payment_pct', 0.5)
        
        # Calculate loan amount
        total_cost = list_price + transaction_cost
        loan_amount = total_cost * (1 - down_payment_pct)
        metrics['loan_amount'] = loan_amount
        
        # Get interest rate and term
        interest_rate = metrics.get('interest_rate', 7.5) / 100  # Convert to decimal
        loan_term = metrics.get('loan_term', 15)
        
        # Calculate monthly payment
        monthly_rate = interest_rate / 12
        total_periods = loan_term * 12
        if monthly_rate > 0 and total_periods > 0:
            monthly_payment = npf.pmt(monthly_rate, total_periods, -loan_amount)
            metrics['monthly_payment'] = monthly_payment
            metrics['annual_debt_service'] = monthly_payment * 12
        else:
            metrics['monthly_payment'] = 0
            metrics['annual_debt_service'] = 0
            
        # Calculate principal payments for years 1-5
        total_principal = 0
        remaining_balance = loan_amount
        
        for year in range(1, 6):
            year_start_period = (year - 1) * 12 + 1
            year_end_period = year * 12
            
            # Calculate principal paid this year
            principal_paid = 0
            for period in range(year_start_period, year_end_period + 1):
                interest_payment = remaining_balance * monthly_rate
                principal_payment = monthly_payment - interest_payment
                principal_paid += principal_payment
                remaining_balance -= principal_payment
            
            metrics[f'principal_paid_year{year}'] = principal_paid
            total_principal += principal_paid
            
            # Store ending balance at each year
            metrics[f'loan_balance_year{year}'] = remaining_balance
            
            # Ensure ucf_year{year} exists before calculating lcf
            ucf_key = f'ucf_year{year}'
            if ucf_key in metrics:
                # Calculate levered cash flow (LCF)
                metrics[f'lcf_year{year}'] = metrics[ucf_key] - metrics['annual_debt_service']
            else:
                metrics[f'lcf_year{year}'] = 0  # Default if UCF is missing
        
        # Store total principal payments and final mortgage balance
        metrics['total_principal_paid'] = total_principal
        metrics['final_loan_balance'] = remaining_balance
        
        # Calculate accumulated cash flow (ensure lcf values exist)
        lcf_sum = 0
        for year in range(1, 6):
            lcf_key = f'lcf_year{year}'
            if lcf_key in metrics:
                lcf_sum += metrics[lcf_key]
        
        metrics['accumulated_cash_flow'] = lcf_sum
        
        return metrics
    
    except Exception as e:
        print(f"Error calculating mortgage metrics: {e}")
        return metrics

def calculate_investment_returns(row, metrics):
    """
    Calculate investment return metrics including exit value, cash-on-cash return, and IRR.
    Updates the metrics dictionary with these values.
    """
    try:
        # Calculate exit cap rate
        cap_rate = metrics.get('cap_rate', 0) / 100  # Convert to decimal
        growth_rate = float(row.get('zori_growth_rate', 3.0)) / 100
        
        # Get neighborhood factor
        zip_code = str(int(float(row.get('zip_code', 0) or 0)))
        neighborhood_factor = get_neighborhood_factor(zip_code)
        
        # Calculate exit cap rate
        exit_cap_rate = calculate_exit_cap_rate(cap_rate, growth_rate, neighborhood_factor)
        metrics['exit_cap_rate'] = exit_cap_rate * 100  # Store as percentage
        
        # Calculate exit value using year 5 NOI and exit cap rate
        noi_year5 = metrics.get('noi_year5', 0)
        if exit_cap_rate > 0:
            exit_value = noi_year5 / exit_cap_rate
            metrics['exit_value'] = exit_value
        else:
            metrics['exit_value'] = 0
        
        # Calculate equity at exit
        final_loan_balance = metrics.get('final_loan_balance', 0)
        accumulated_cash_flow = metrics.get('accumulated_cash_flow', 0)
        equity_at_exit = metrics['exit_value'] - final_loan_balance + accumulated_cash_flow
        metrics['equity_at_exit'] = equity_at_exit
        
        # Calculate annual cash-on-cash return (first year)
        cash_equity = metrics.get('cash_equity', 0)
        if cash_equity > 0:
            # Calculate first-year cash-on-cash return (standard definition)
            first_year_coc = (metrics.get('lcf_year1', 0) / cash_equity) * 100
            metrics['cash_on_cash'] = min(25, max(-15, first_year_coc))  # Apply reasonable bounds
            
            # Keep the total return (renamed for clarity)
            total_return = equity_at_exit / cash_equity
            metrics['total_return'] = total_return
            
            # Calculate IRR using proper discounted cash flow
            try:
                # Create cash flow array (negative initial investment, followed by annual cash flows, plus terminal value)
                cash_flows = [-cash_equity]  # Initial investment (negative)
                
                # Add annual cash flows
                for year in range(1, 6):
                    lcf_key = f'lcf_year{year}'
                    if lcf_key in metrics:
                        cash_flows.append(metrics[lcf_key])
                
                # Add exit proceed to final year cash flow
                if len(cash_flows) >= 6:  # Make sure we have enough years
                    cash_flows[5] += metrics['exit_value'] - metrics['final_loan_balance']
                
                # Calculate IRR
                if any(cf > 0 for cf in cash_flows[1:]):  # At least one positive cash flow
                    irr = npf.irr(cash_flows)
                    # Apply reasonable bounds
                    metrics['irr'] = min(35, max(-25, irr * 100))  # Store as percentage with bounds
                else:
                    metrics['irr'] = -25  # Minimum IRR for clearly poor investments
            except:
                metrics['irr'] = 0  # Default for calculation errors
        else:
            metrics['cash_on_cash'] = 0
            metrics['irr'] = 0
            metrics['total_return'] = 0
        
        return metrics
    
    except Exception as e:
        print(f"Error calculating investment returns: {e}")
        return metrics

# =====================================================================
# MAIN PROCESSING FUNCTIONS
# =====================================================================

def process_row_values(row):
    """
    Process a CSV row to format phone numbers and round dollar values.
    Updates the row in place.
    """
    # Format phone numbers
    if 'agent_phones' in row:
        row['agent_phones'] = format_phone_number(row['agent_phones'], extract_numbers_only=True)
    if 'office_phones' in row:
        row['office_phones'] = format_phone_number(row['office_phones'], extract_numbers_only=True)
    
    # Round all dollar values
    for field in DOLLAR_FIELDS:
        if field in row and row[field]:
            row[field] = round_price(row[field])
            
    return row

def process_rental_estimates_for_file(input_file, output_file, zori_data):
    """
    Process properties in a specific file and calculate ZORI-based rental estimates.
    Saves results to a temporary file.
    """
    zori_by_zip, growth_rates_by_zip, avg_seasonality, state_averages = zori_data
    
    print(f"Processing rental income for {input_file}...")
    
    with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        
        # Add new fields for ZORI estimates
        fieldnames = reader.fieldnames + [
            'zori_monthly_rent',
            'zori_annual_rent',
            'zori_growth_rate',
            'zori_rent_year1',
            'zori_rent_year2',
            'zori_rent_year3',
            'zori_rent_year4',
            'zori_rent_year5',
            'gross_rent_multiplier'
        ]
        
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        count = 0
        for row in reader:
            row = process_row_values(row)
            # Replace the phone formatting code in each function with:
            if 'agent_phones' in row:
                row['agent_phones'] = format_phone_number(row['agent_phones'], extract_numbers_only=True)
            if 'office_phones' in row:
                row['office_phones'] = format_phone_number(row['office_phones'], extract_numbers_only=True)
                
            # Ensure list_price is an integer
            if 'list_price' in row and row['list_price']:
                try:
                    row['list_price'] = round_price(row['list_price'])
                except (ValueError, TypeError):
                    pass
                    
            # Round other price fields
            for field in ['list_price_min', 'list_price_max', 'sold_price', 
                          'assessed_value', 'estimated_value']:
                if field in row and row[field]:
                    row[field] = round_price(row[field])
                    
            # Calculate ZORI-based rental estimate
            monthly_rent, annual_rent, growth_rate, projections, grm = estimate_rental_income(
                row, zori_by_zip, growth_rates_by_zip, avg_seasonality, state_averages)
            
            # Add the values to the row
            if monthly_rent:
                row['zori_monthly_rent'] = round_price(monthly_rent)
                row['zori_annual_rent'] = round_price(annual_rent)
                row['zori_growth_rate'] = round(growth_rate, 2)
                row['gross_rent_multiplier'] = round(grm, 2) if grm else None
                
                # Add 5-year projections
                for i, proj in enumerate(projections):
                    row[f'zori_rent_year{i+1}'] = round_price(proj)
            else:
                row['zori_monthly_rent'] = None
                row['zori_annual_rent'] = None
                row['zori_growth_rate'] = None
                row['zori_rent_year1'] = None
                row['zori_rent_year2'] = None
                row['zori_rent_year3'] = None
                row['zori_rent_year4'] = None
                row['zori_rent_year5'] = None
                row['gross_rent_multiplier'] = None
                
            writer.writerow(row)
            count += 1
            if count % 1000 == 0:
                print(f"  - Processed {count} properties in {input_file}...")
    
    print(f"Completed rental estimation for {count} properties in {input_file}")
    return count

# Updated function - modify this function in the file
def merge_csv_files(input_files, output_file):
    """
    Merge multiple CSV files into a single file, preserving headers.
    """
    print(f"Merging {len(input_files)} files into {output_file}...")
    
    # Make sure there are files to merge
    if not input_files:
        print("No files to merge!")
        return
    
    # Read the first file to get the fieldnames
    with open(input_files[0], 'r', newline='', encoding='utf-8') as first_file:
        reader = csv.DictReader(first_file)
        fieldnames = reader.fieldnames
    
    # Write all files to the output file
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        total_rows = 0
        for file_path in input_files:
            with open(file_path, 'r', newline='', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    row = process_row_values(row)
                    # Replace the phone formatting code in each function with:
                    if 'agent_phones' in row:
                        row['agent_phones'] = format_phone_number(row['agent_phones'], extract_numbers_only=True)
                    if 'office_phones' in row:
                        row['office_phones'] = format_phone_number(row['office_phones'], extract_numbers_only=True)
                    
                    # Round price-related fields
                    for field in ['list_price', 'list_price_min', 'list_price_max', 'sold_price', 
                                  'assessed_value', 'estimated_value', 'zori_monthly_rent', 'zori_annual_rent']:
                        if field in row and row[field]:
                            row[field] = round_price(row[field])
                    
                    writer.writerow(row)
                    total_rows += 1
    
    print(f"Merged {total_rows} total rows into {output_file}")
    return total_rows

# Updated function - modify this function in the file
def process_investment_metrics_for_file(input_file, output_file):
    """
    Process properties with rental estimates and calculate investment metrics.
    Saves results to a temporary file.
    """
    print(f"Processing investment metrics for {input_file}...")
    
    with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        
        # Add new fields for investment metrics
        additional_fields = [
            'monthly_rent', 'annual_rent', 'tax_used', 'hoa_fee_used',
            'down_payment_pct', 'interest_rate', 'loan_term',
            'transaction_cost', 'cash_equity', 
            'noi_year1', 'noi_year2', 'noi_year3', 'noi_year4', 'noi_year5',
            'cap_rate', 'ucf', 'cash_yield',
            'ucf_year1', 'ucf_year2', 'ucf_year3', 'ucf_year4', 'ucf_year5'
        ]
        
        fieldnames = reader.fieldnames + additional_fields
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        count = 0
        for row in reader:
            row = process_row_values(row)
            # Replace the phone formatting code in each function with:
            if 'agent_phones' in row:
                row['agent_phones'] = format_phone_number(row['agent_phones'], extract_numbers_only=True)
            if 'office_phones' in row:
                row['office_phones'] = format_phone_number(row['office_phones'], extract_numbers_only=True)
                
            # Round price-related fields
            for field in ['list_price', 'list_price_min', 'list_price_max', 'sold_price', 
                          'assessed_value', 'estimated_value', 'zori_monthly_rent', 'zori_annual_rent']:
                if field in row and row[field]:
                    row[field] = round_price(row[field])
                    
            # Calculate cash flow metrics
            metrics = calculate_cash_flow_metrics(row, is_zori_based=True)
            
            # Add metrics to the row
            for key, value in metrics.items():
                if key in ['monthly_rent', 'annual_rent', 'cash_equity', 'transaction_cost']:
                    row[key] = round_price(value) if isinstance(value, (int, float)) else value
                else:
                    row[key] = round(value, 2) if isinstance(value, (int, float)) else value
                
            writer.writerow(row)
            count += 1
            if count % 1000 == 0:
                print(f"  - Processed {count} properties in {input_file}...")
    
    print(f"Completed cash flow metrics for {count} properties in {input_file}")
    return count

# Updated function - modify this function in the file
def process_final_metrics_for_file(input_file, output_file):
    """
    Process properties with cash flow metrics and calculate final investment returns.
    Saves results to the output file.
    """
    print(f"Processing final investment metrics for {input_file}...")
    
    with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        
        # Add new fields for final metrics
        additional_fields = [
            'loan_amount', 'monthly_payment', 'annual_debt_service',
            'principal_paid_year1', 'principal_paid_year2', 'principal_paid_year3', 
            'principal_paid_year4', 'principal_paid_year5',
            'loan_balance_year1', 'loan_balance_year2', 'loan_balance_year3', 
            'loan_balance_year4', 'loan_balance_year5',
            'lcf_year1', 'lcf_year2', 'lcf_year3', 'lcf_year4', 'lcf_year5',
            'total_principal_paid', 'final_loan_balance', 'accumulated_cash_flow',
            'exit_cap_rate', 'exit_value', 'equity_at_exit', 'cash_on_cash', 'irr',
            'total_return'
        ]
        
        fieldnames = reader.fieldnames + additional_fields
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        count = 0
        for row in reader:
            row = process_row_values(row)
            # Replace the phone formatting code in each function with:
            if 'agent_phones' in row:
                row['agent_phones'] = format_phone_number(row['agent_phones'], extract_numbers_only=True)
            if 'office_phones' in row:
                row['office_phones'] = format_phone_number(row['office_phones'], extract_numbers_only=True)
                
            # Round price-related fields
            for field in ['list_price', 'list_price_min', 'list_price_max', 'sold_price', 
                          'assessed_value', 'estimated_value', 'monthly_rent', 'annual_rent',
                          'cash_equity', 'transaction_cost']:
                if field in row and row[field]:
                    row[field] = round_price(row[field])
                    
            # Extract metrics to build the metrics dictionary
            metrics = {}
            # First, get all the UCF values
            for i in range(1, 6):
                key = f'ucf_year{i}'
                if key in row and row[key]:
                    try:
                        metrics[key] = float(row[key])
                    except (ValueError, TypeError):
                        metrics[key] = 0
                else:
                    metrics[key] = 0
                    
            # Now get all other metrics
            for field in reader.fieldnames:
                if field in ['monthly_rent', 'annual_rent', 'tax_used', 'hoa_fee_used', 
                           'down_payment_pct', 'interest_rate', 'loan_term', 
                           'transaction_cost', 'cash_equity', 'cap_rate', 'ucf', 'cash_yield',
                           'noi_year1', 'noi_year2', 'noi_year3', 'noi_year4', 'noi_year5']:
                    if field in row and row[field]:
                        try:
                            metrics[field] = float(row[field])
                        except (ValueError, TypeError):
                            metrics[field] = 0
                    else:
                        metrics[field] = 0
            
            # Calculate mortgage metrics
            metrics = calculate_mortgage_metrics(row, metrics)
            
            # Calculate investment returns
            metrics = calculate_investment_returns(row, metrics)
            
            # Add metrics to the row
            for key, value in metrics.items():
                if key in ['loan_amount', 'monthly_payment', 'annual_debt_service', 
                          'exit_value', 'equity_at_exit']:
                    row[key] = round_price(value) if isinstance(value, (int, float)) else value
                else:
                    row[key] = round(value, 2) if isinstance(value, (int, float)) else value
                
            writer.writerow(row)
            count += 1
            if count % 1000 == 0:
                print(f"  - Processed {count} properties in {input_file}...")
    
    print(f"Completed final investment metrics for {count} properties")
    return count

def filter_investment_outliers(input_file, output_file):
    """
    Filter properties with unrealistic investment metrics and add property ranking.
    Creates a new CSV with only valid properties.
    """
    print(f"Filtering investment outliers from {input_file}...")
    
    with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        
        # Add ranking fields to fieldnames
        fieldnames = reader.fieldnames + ['investment_score', 'investment_ranking']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        total_count = 0
        filtered_count = 0
        
        for row in reader:
            row = process_row_values(row)
            total_count += 1
            
            # Ensure list_price is an integer
            if 'list_price' in row and row['list_price']:
                try:
                    row['list_price'] = int(float(row['list_price']))
                except (ValueError, TypeError):
                    pass
            
            # Check if property has unrealistic metrics
            try:
                cap_rate = float(row.get('cap_rate', 0) or 0)
                irr = float(row.get('irr', 0) or 0)
                cash_on_cash = float(row.get('cash_on_cash', 0) or 0)
                grm = float(row.get('gross_rent_multiplier', 0) or 0)
                
                # Skip properties with clearly problematic metrics
                if (abs(cap_rate) > 15 or 
                    abs(irr) > 35 or 
                    abs(cash_on_cash) > 25 or
                    grm <= 5 or grm > 60):
                    filtered_count += 1
                    continue
                    
                # Additional filters for reasonable metrics
                # Skip properties with extremely negative cash flows
                lcf_year1 = float(row.get('lcf_year1', 0) or 0)
                ucf_year1 = float(row.get('ucf_year1', 0) or 0)
                if lcf_year1 < -50000 or ucf_year1 < -30000:
                    filtered_count += 1
                    continue
                
                # Skip properties with unrealistic price-to-rent ratios
                list_price = float(row.get('list_price', 0) or 0)
                annual_rent = float(row.get('annual_rent', 0) or 0)
                if annual_rent > 0 and (list_price / annual_rent > 60 or list_price / annual_rent < 5):
                    filtered_count += 1
                    continue
                
                # Calculate property ranking
                investment_score, investment_ranking = calculate_property_ranking(row)
                row['investment_score'] = investment_score
                row['investment_ranking'] = investment_ranking
                
                # Property passed all filters - include it
                writer.writerow(row)
                
            except (ValueError, TypeError, ZeroDivisionError):
                # Skip rows with calculation errors
                filtered_count += 1
                continue
    
    print(f"Filtered {filtered_count} properties out of {total_count} total")
    print(f"Saved {total_count - filtered_count} valid properties to {output_file}")
    return total_count - filtered_count

def process_rental_estimates():
    """
    Process all property data files and calculate ZORI-based rental estimates.
    Uses parallel processing for efficiency.
    """
    print("Starting rental income estimation for all files...")
    
    # Load ZORI data (shared among all workers)
    zori_data = load_zori_data()
    
    # Create a list to store paths of generated files
    temp_zori_files = []
    
    # Process each property data file in parallel
    with ProcessPoolExecutor(max_workers=min(MAX_WORKERS, len(PROPERTY_DATA_FILES))) as executor:
        futures = []
        
        for i, property_file in enumerate(PROPERTY_DATA_FILES):
            temp_file = TEMP_ZORI_ESTIMATES_PATTERN.format(i)
            temp_zori_files.append(temp_file)
            
            # Submit task to executor
            future = executor.submit(
                process_rental_estimates_for_file,
                property_file,
                temp_file,
                zori_data
            )
            futures.append(future)
        
        # Wait for all tasks to complete and get total count
        total_count = sum(future.result() for future in as_completed(futures))
    
    # Merge all temporary ZORI estimate files
    merge_csv_files(temp_zori_files, TEMP_MERGED_ZORI)
    
    print(f"Completed rental estimation for {total_count} properties across all files")
    return total_count

def process_investment_metrics():
    """
    Process the merged ZORI estimates and calculate investment metrics.
    """
    print("Starting investment metrics calculation...")
    
    # Process the merged property data with rental estimates
    process_investment_metrics_for_file(TEMP_MERGED_ZORI, TEMP_MERGED_CASH_FLOW)
    
    print("Completed cash flow metrics calculation")

def clean_up_temp_files():
    """Remove temporary files and directory."""
    print("Cleaning up temporary files...")
    try:
        for pattern in [TEMP_ZORI_ESTIMATES_PATTERN, TEMP_CASH_FLOW_PATTERN]:
            for file_path in glob.glob(pattern.replace('{}', '*')):
                os.remove(file_path)
                
        if os.path.exists(TEMP_MERGED_ZORI):
            os.remove(TEMP_MERGED_ZORI)
        if os.path.exists(TEMP_MERGED_CASH_FLOW):
            os.remove(TEMP_MERGED_CASH_FLOW)
            
        # Remove temp directory if it's empty
        try:
            os.rmdir(TEMP_DIR)
        except OSError:
            # Directory not empty or other error, ignore
            pass
            
    except Exception as e:
        print(f"Error cleaning up temporary files: {e}")

def main():
    """Main function to run the complete investment analysis workflow."""
    start_time = datetime.now()
    
    print("\n======== REAL ESTATE INVESTMENT ANALYSIS WORKFLOW ========\n")
    print(f"Input property data files:")
    for i, file in enumerate(PROPERTY_DATA_FILES):
        print(f"  {i+1}. {file}")
    print(f"Input ZORI data: {ZILLOW_RENT_DATA_FILE}")
    print(f"Output file: {OUTPUT_FINAL_FILE}\n")
    
    try:
        # Step 1: Calculate rental income estimates for all files
        process_rental_estimates()
        
        # Step 2: Calculate cash flow metrics
        process_investment_metrics()
        
        # Step 3: Calculate final investment returns
        temp_final_file = OUTPUT_FINAL_FILE + '.temp'
        process_final_metrics_for_file(TEMP_MERGED_CASH_FLOW, temp_final_file)
        
        # Step 4: Filter out properties with unrealistic metrics
        filter_investment_outliers(temp_final_file, OUTPUT_FINAL_FILE)
        
        # Clean up
        if os.path.exists(temp_final_file):
            os.remove(temp_final_file)
        clean_up_temp_files()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n========= ANALYSIS COMPLETE =========")
        print(f"Results saved to {OUTPUT_FINAL_FILE}")
        print(f"Total processing time: {duration}")
        
    except Exception as e:
        print(f"Error in main workflow: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()