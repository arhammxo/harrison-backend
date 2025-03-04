from flask import Flask, request, jsonify, send_from_directory
import sqlite3
import os
import logging
from functools import wraps
import json
from flask_swagger_ui import get_swaggerui_blueprint
from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_FILE = 'new_investment_properties.db'

# Initialize Flask app
app = Flask(__name__)

# Configure Swagger
SWAGGER_URL = '/docs'  # URL for exposing Swagger UI
API_URL = '/static/swagger.json'  # Our API url (can be a local file or url)

# Call factory function to create our blueprint
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={  # Swagger UI config overrides
        'app_name': "Investment Properties API",
        'layout': 'BaseLayout',
        'deepLinking': True,
        'defaultModelsExpandDepth': 1,
        'defaultModelExpandDepth': 1,
        'docExpansion': 'list'  # show all endpoints expanded by default
    }
)

# Register blueprint at URL
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# Create API spec
spec = APISpec(
    title="Investment Properties API",
    version="1.0.0",
    openapi_version="3.0.2",
    plugins=[FlaskPlugin(), MarshmallowPlugin()],
)

# Check if database exists
if not os.path.exists(DB_FILE):
    logger.error(f"Database file {DB_FILE} not found. Please run database.py first.")
    exit(1)

# Database connection helper
def get_db_connection():
    """Create a connection to the database"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # Return rows as dictionaries
    return conn

# Error handler for database issues
def db_exception_handler(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except sqlite3.Error as e:
            logger.error(f"Database error: {str(e)}")
            return jsonify({
                'success': False,
                'error': 'Database error occurred',
                'message': str(e)
            }), 500
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return jsonify({
                'success': False,
                'error': 'An unexpected error occurred',
                'message': str(e)
            }), 500
    return decorated_function

# Helper to parse sorting parameters
def parse_sort_params(sort_by, allowed_fields):
    """Parse and validate sorting parameters"""
    if not sort_by or sort_by not in allowed_fields:
        # Default to investment_ranking
        return "investment_ranking DESC"
    
    # Define the direction (descending for most metrics)
    direction = "DESC"
    
    # For price_per_sqft, we might want ascending order
    if sort_by == "price_per_sqft":
        direction = "ASC"
    
    return f"{sort_by} {direction}"

# Helper to parse price range parameters
def parse_price_range(min_price, max_price):
    """Parse and validate price range parameters"""
    conditions = []
    params = []
    
    if min_price is not None and min_price.isdigit():
        conditions.append("list_price >= ?")
        params.append(int(min_price))
    
    if max_price is not None and max_price.isdigit():
        conditions.append("list_price <= ?")
        params.append(int(max_price))
    
    return conditions, params

# API Root
@app.route('/', methods=['GET'])
def index():
    """API root endpoint with basic info"""
    return jsonify({
        'name': 'Investment Properties API',
        'version': '1.0.0',
        'endpoints': {
            'Search Properties': '/api/properties',
            'Property Detail': '/api/properties/<property_id>',
            'Property Calculation Audit': '/api/properties/<property_id>/audit',
            'Cities': '/api/cities',
            'Zip Codes': '/api/zipcodes',
            'States': '/api/states',
            'Documentation': '/docs'
        }
    })

# Properties Search Endpoint
@app.route('/api/properties', methods=['GET'])
@db_exception_handler
def search_properties():
    """
    Search properties with filters
    Query params:
    - zip_code: Filter by zip code
    - city: Filter by city
    - state: Filter by state
    - min_price: Minimum price
    - max_price: Maximum price
    - sort_by: Field to sort by (investment_ranking, price_per_sqft, cap_rate, cash_on_cash, total_return)
    - page: Page number (default: 1)
    - limit: Results per page (default: 20, max: 100)
    """
    # Get query parameters
    zip_code = request.args.get('zip_code')
    city = request.args.get('city')
    state = request.args.get('state')
    min_price = request.args.get('min_price')
    max_price = request.args.get('max_price')
    sort_by = request.args.get('sort_by', 'investment_ranking')
    page = max(1, int(request.args.get('page', 1)))
    limit = min(100, int(request.args.get('limit', 20)))
    offset = (page - 1) * limit
    
    # Allowed sorting fields
    allowed_sort_fields = [
        'investment_ranking', 'price_per_sqft', 'cap_rate', 
        'cash_on_cash', 'irr', 'total_return', 'list_price'
    ]
    
    # Build the query
    base_query = """
    SELECT 
        property_id, full_street_line, city, state, zip_code,
        beds, baths, sqft, list_price, price_per_sqft,
        zori_monthly_rent, cap_rate, cash_on_cash, irr, total_return,
        investment_ranking, primary_photo
    FROM api_property_search
    """
    
    count_query = "SELECT COUNT(*) as count FROM api_property_search"
    
    # Build where clauses
    conditions = []
    params = []
    
    # Add location filters
    if zip_code:
        conditions.append("zip_code = ?")
        params.append(zip_code)
    
    if city:
        conditions.append("LOWER(city) = LOWER(?)")
        params.append(city)
    
    if state:
        conditions.append("LOWER(state) = LOWER(?)")
        params.append(state)
    
    # Add price range filters
    price_conditions, price_params = parse_price_range(min_price, max_price)
    conditions.extend(price_conditions)
    params.extend(price_params)
    
    # Combine all conditions
    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)
        base_query += where_clause
        count_query += where_clause
    
    # Add sorting
    sort_clause = parse_sort_params(sort_by, allowed_sort_fields)
    base_query += f" ORDER BY {sort_clause}"
    
    # Add pagination
    base_query += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    # Execute queries
    conn = get_db_connection()
    
    # Get total count for pagination
    count_result = conn.execute(count_query, params[:-2] if params else []).fetchone()
    total_count = count_result['count'] if count_result else 0
    
    # Get paginated results
    results = conn.execute(base_query, params).fetchall()
    
    # Convert results to list of dictionaries
    properties = [dict(row) for row in results]
    
    # Calculate pagination info
    total_pages = (total_count + limit - 1) // limit
    
    # Close connection
    conn.close()
    
    # Return response
    return jsonify({
        'success': True,
        'data': {
            'properties': properties,
            'pagination': {
                'page': page,
                'limit': limit,
                'total_count': total_count,
                'total_pages': total_pages
            },
            'filters': {
                'zip_code': zip_code,
                'city': city,
                'state': state,
                'min_price': min_price,
                'max_price': max_price,
                'sort_by': sort_by
            }
        }
    })

# Property Detail Endpoint
@app.route('/api/properties/<int:property_id>', methods=['GET'])
@db_exception_handler
def get_property_detail(property_id):
    """Get detailed information for a specific property"""
    conn = get_db_connection()
    
    # Query property details
    result = conn.execute(
        "SELECT * FROM api_property_details WHERE property_id = ?", 
        (property_id,)
    ).fetchone()
    
    if not result:
        conn.close()
        return jsonify({
            'success': False,
            'error': 'Property not found',
            'message': f'No property found with ID {property_id}'
        }), 404
    
    # Convert to dictionary and separate photo URLs
    property_data = dict(result)
    
    # Parse alt_photos if it's a string
    alt_photos = property_data.get('alt_photos', '')
    if alt_photos and isinstance(alt_photos, str):
        try:
            # Try to parse as JSON
            photo_list = json.loads(alt_photos)
            property_data['alt_photos'] = photo_list
        except json.JSONDecodeError:
            # If not valid JSON, split by commas
            property_data['alt_photos'] = [p.strip() for p in alt_photos.split(',') if p.strip()]
    
    # Create broker info object
    property_data['broker_info'] = {
        'broker_id': property_data.get('broker_id'),
        'broker_name': property_data.get('broker_name'),
        'broker_email': property_data.get('broker_email'),
        'broker_phones': property_data.get('broker_phones'),
        'agent_id': property_data.get('agent_id'),
        'agent_name': property_data.get('agent_name'),
        'agent_email': property_data.get('agent_email'),
        'agent_phones': property_data.get('agent_phones'),
        'office_name': property_data.get('office_name'),
        'office_phones': property_data.get('office_phones')
    }
    
    # Remove the broker fields from the main object to avoid duplication
    for key in list(property_data['broker_info'].keys()):
        if key in property_data:
            del property_data[key]
    
    conn.close()
    
    return jsonify({
        'success': True,
        'data': property_data
    })

# Property Calculation Audit Endpoint
@app.route('/api/properties/<int:property_id>/audit', methods=['GET'])
@db_exception_handler
def get_property_audit(property_id):
    """Get calculation audit data for a specific property"""
    conn = get_db_connection()
    
    # Check if property exists
    property_check = conn.execute(
        "SELECT property_id FROM properties WHERE property_id = ?", 
        (property_id,)
    ).fetchone()
    
    if not property_check:
        conn.close()
        return jsonify({
            'success': False,
            'error': 'Property not found',
            'message': f'No property found with ID {property_id}'
        }), 404
    
    # Query audit data
    result = conn.execute(
        "SELECT * FROM api_calculation_audit WHERE property_id = ?", 
        (property_id,)
    ).fetchone()
    
    if not result:
        conn.close()
        return jsonify({
            'success': False,
            'error': 'Audit data not found',
            'message': f'No audit data available for property ID {property_id}'
        }), 404
    
    # Convert sqlite3.Row to a regular dictionary
    result_dict = dict(result)
    
    # Get cash flow projections
    projections = {}
    for year in range(1, 6):
        year_data = {
            'noi': result_dict.get(f'noi_year{year}'),
            'ucf': result_dict.get(f'ucf_year{year}'),
            'lcf': result_dict.get(f'lcf_year{year}')
        }
        projections[f'year{year}'] = year_data
    
    # Create structured response
    audit_data = {
        'property_id': result_dict['property_id'],
        'property_info': {
            'full_street_line': result_dict.get('full_street_line', ''),
            'city': result_dict.get('city', ''),
            'state': result_dict.get('state', ''),
            'zip_code': result_dict.get('zip_code', 0)
        },
        'rental_income': {
            'zori_monthly_rent': result_dict.get('zori_monthly_rent', 0),
            'zori_annual_rent': result_dict.get('zori_annual_rent', 0),
            'zori_growth_rate': result_dict.get('zori_growth_rate', 0),
            'gross_rent_multiplier': result_dict.get('gross_rent_multiplier', 0)
        },
        'expenses': {
            'tax_used': result_dict.get('tax_used', 0),
            'hoa_fee_used': result_dict.get('hoa_fee_used', 0)
        },
        'mortgage': {
            'down_payment_pct': result_dict.get('down_payment_pct', 0),
            'interest_rate': result_dict.get('interest_rate', 0),
            'loan_term': result_dict.get('loan_term', 0),
            'loan_amount': result_dict.get('loan_amount', 0),
            'monthly_payment': result_dict.get('monthly_payment', 0),
            'annual_debt_service': result_dict.get('annual_debt_service', 0),
            'final_loan_balance': result_dict.get('final_loan_balance', 0)
        },
        'returns': {
            'cap_rate': result_dict.get('cap_rate', 0),
            'exit_cap_rate': result_dict.get('exit_cap_rate', 0),
            'exit_value': result_dict.get('exit_value', 0),
            'equity_at_exit': result_dict.get('equity_at_exit', 0),
            'irr': result_dict.get('irr', 0),
            'cash_on_cash': result_dict.get('cash_on_cash', 0),
            'total_return': result_dict.get('total_return', 0),
            'investment_ranking': result_dict.get('investment_ranking', 0)
        },
        'projections': projections
    }
    
    conn.close()
    
    return jsonify({
        'success': True,
        'data': audit_data
    })

# Cities Lookup Endpoint
@app.route('/api/cities', methods=['GET'])
@db_exception_handler
def get_cities():
    """Get a list of available cities, optionally filtered by state"""
    state = request.args.get('state')
    
    conn = get_db_connection()
    
    query = "SELECT city, state, property_count FROM city_lookup"
    params = []
    
    if state:
        query += " WHERE LOWER(state) = LOWER(?)"
        params.append(state)
    
    query += " ORDER BY state, city"
    
    results = conn.execute(query, params).fetchall()
    cities = [dict(row) for row in results]
    
    conn.close()
    
    return jsonify({
        'success': True,
        'data': {
            'cities': cities,
            'count': len(cities)
        }
    })

# Zip Codes Lookup Endpoint
@app.route('/api/zipcodes', methods=['GET'])
@db_exception_handler
def get_zipcodes():
    """Get a list of available zip codes, optionally filtered by city and/or state"""
    city = request.args.get('city')
    state = request.args.get('state')
    
    conn = get_db_connection()
    
    query = "SELECT zip_code, city, state, property_count FROM zipcode_lookup"
    conditions = []
    params = []
    
    if city:
        conditions.append("LOWER(city) = LOWER(?)")
        params.append(city)
    
    if state:
        conditions.append("LOWER(state) = LOWER(?)")
        params.append(state)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " ORDER BY state, city, zip_code"
    
    results = conn.execute(query, params).fetchall()
    zipcodes = [dict(row) for row in results]
    
    conn.close()
    
    return jsonify({
        'success': True,
        'data': {
            'zipcodes': zipcodes,
            'count': len(zipcodes)
        }
    })

# States Lookup Endpoint
@app.route('/api/states', methods=['GET'])
@db_exception_handler
def get_states():
    """Get a list of available states with property counts"""
    conn = get_db_connection()
    
    results = conn.execute("""
        SELECT state, COUNT(*) as property_count 
        FROM properties 
        GROUP BY state 
        ORDER BY state
    """).fetchall()
    
    states = [dict(row) for row in results]
    
    conn.close()
    
    return jsonify({
        'success': True,
        'data': {
            'states': states,
            'count': len(states)
        }
    })

# Market Statistics Endpoint
@app.route('/api/stats/market', methods=['GET'])
@db_exception_handler
def get_market_stats():
    """Get market statistics by location"""
    location_type = request.args.get('type', 'city') # city, zipcode, or state
    
    allowed_types = ['city', 'zipcode', 'state']
    if location_type not in allowed_types:
        return jsonify({
            'success': False,
            'error': 'Invalid location type',
            'message': f'Location type must be one of: {", ".join(allowed_types)}'
        }), 400
    
    # Map location type to table name
    table_mapping = {
        'city': 'market_stats_by_city',
        'zipcode': 'market_stats_by_zipcode',
        'state': 'stats_by_state'
    }
    
    table_name = table_mapping[location_type]
    
    conn = get_db_connection()
    
    results = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    stats = [dict(row) for row in results]
    
    conn.close()
    
    return jsonify({
        'success': True,
        'data': {
            'stats': stats,
            'count': len(stats),
            'type': location_type
        }
    })

# Error Handlers
@app.errorhandler(404)
def not_found(e):
    return jsonify({
        'success': False,
        'error': 'Not found',
        'message': 'The requested resource does not exist'
    }), 404

@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({
        'success': False,
        'error': 'Method not allowed',
        'message': 'The method is not allowed for the requested URL'
    }), 405

@app.errorhandler(500)
def server_error(e):
    return jsonify({
        'success': False,
        'error': 'Server error',
        'message': 'An internal server error occurred'
    }), 500

# Endpoint to serve the swagger specification
@app.route('/static/swagger.json')
def get_swagger():
    """Serve the swagger.json specification file."""
    # If we need to dynamically generate parts of the swagger spec, we could do it here
    return send_from_directory(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static'),
        'swagger.json'
    )
    
# Simple endpoint to redirect from docs to Swagger UI
@app.route('/docs/swagger')
def redirect_to_swagger():
    """Redirect to Swagger UI"""
    return app.redirect(SWAGGER_URL)

if __name__ == '__main__':
    logger.info("Starting Investment Properties API...")
    app.run(debug=True, host='0.0.0.0', port=5000)