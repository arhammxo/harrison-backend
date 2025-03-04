import csv
import os

# Input and output file paths
INPUT_FILE = 'investment_analysis_resultsUPDATED.csv'
OUTPUT_FILE = 'filtered_investment_propertiesUPDATED.csv'

# Define essential fields that must have non-zero, non-null values
ESSENTIAL_FIELDS = [
    'list_price',
    'beds',
    'full_baths',
    'sqft',
    'zori_monthly_rent',
    'annual_rent',
    'cap_rate',
    'cash_on_cash',
    'irr',
    'total_return'
]

# Define reasonable ranges for key investment metrics
METRIC_RANGES = {
    'cap_rate': {'min': 1, 'max': 15},           # At least 1% cap rate, max 15%
    'cash_on_cash': {'min': -10, 'max': 20},     # Allow slightly negative cash flow, max 20%
    'irr': {'min': -10, 'max': 35},              # Allow slightly negative IRR, max 35%
    'gross_rent_multiplier': {'min': 6, 'max': 30}  # Reasonable GRM range
}

def is_valid_property(row):
    """Check if a property meets all our criteria."""
    # Check essential fields are present and non-zero
    for field in ESSENTIAL_FIELDS:
        if field not in row or not row[field] or row[field] == '0':
            return False
        
        try:
            # Make sure the value is numeric and not zero
            value = float(row[field])
            if value == 0:
                return False
        except (ValueError, TypeError):
            return False
    
    # Check key metrics are within reasonable ranges
    for metric, range_values in METRIC_RANGES.items():
        if metric in row and row[metric]:
            try:
                value = float(row[metric])
                if value < range_values['min'] or value > range_values['max']:
                    return False
            except (ValueError, TypeError):
                return False
    
    # Check for reasonable price per sqft
    try:
        list_price = float(row['list_price'])
        sqft = float(row['sqft'])
        if sqft > 0:
            ppsqft = list_price / sqft
            if ppsqft < 50 or ppsqft > 2000:
                return False
    except (ValueError, TypeError, ZeroDivisionError):
        pass
    
    # Check that annual debt service doesn't exceed annual rent by too much
    try:
        annual_debt_service = float(row.get('annual_debt_service', 0) or 0)
        annual_rent = float(row.get('annual_rent', 0) or 0)
        if annual_debt_service > annual_rent * 1.5:
            return False
    except (ValueError, TypeError):
        pass
    
    # Additional checks for other criteria could be added here
    
    # If we made it here, the property passes all filters
    return True

def main():
    """Main function to filter the CSV file."""
    print(f"Reading input file: {INPUT_FILE}")
    
    # Count rows for reporting
    total_rows = 0
    filtered_rows = 0
    
    with open(INPUT_FILE, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        
        # Open output file and write header
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            
            # Process each row
            for row in reader:
                total_rows += 1
                
                if is_valid_property(row):
                    writer.writerow(row)
                    filtered_rows += 1
    
    # Print summary
    print(f"Total properties processed: {total_rows}")
    print(f"Properties meeting all criteria: {filtered_rows}")
    print(f"Filtered out: {total_rows - filtered_rows} properties")
    print(f"Filtered data saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()