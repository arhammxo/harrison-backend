import sqlite3
from flask import Flask, render_template, request, jsonify, g
import json
import os
import logging
import io
import base64
from datetime import datetime
import warnings
import matplotlib.pyplot as plt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("audit_dashboard.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("audit_dashboard")

# Suppress Matplotlib warnings
warnings.filterwarnings("ignore", category=UserWarning)

app = Flask(__name__)

# Configuration
DB_FILE = 'final.db'
# Force Flask to use UTF-8 for template loading
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.jinja_env.auto_reload = True
app.config['TEMPLATE_ENCODING'] = 'utf-8'

def get_db_connection():
    """Create a database connection"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

# Pre-render the cash flow data as HTML table instead of using matplotlib
def generate_cash_flow_table(property_data):
    """Generate an HTML table for cash flow data instead of a chart"""
    try:
        # Check if property_data is valid
        if not property_data:
            logger.error("Property data is empty or None")
            return None
            
        # Extract cash flow data safely
        years = list(range(1, 6))
        ucf_data = []
        lcf_data = []
        
        for year in years:
            # Use safe extraction with conversion to float
            try:
                ucf_value = float(property_data.get(f'ucf_year{year}', 0) or 0)
                lcf_value = float(property_data.get(f'lcf_year{year}', 0) or 0)
            except (ValueError, TypeError) as e:
                logger.error(f"Error converting cash flow data for year {year}: {e}")
                ucf_value = 0
                lcf_value = 0
                
            ucf_data.append(ucf_value)
            lcf_data.append(lcf_value)
        
        # Calculate totals
        ucf_total = sum(ucf_data)
        lcf_total = sum(lcf_data)
        
        # Generate HTML table instead of chart
        html = """
        <table class="table table-striped">
            <thead>
                <tr>
                    <th>Year</th>
                    <th>Unlevered Cash Flow</th>
                    <th>Levered Cash Flow</th>
                </tr>
            </thead>
            <tbody>
        """
        
        # Add rows for each year
        for i, year in enumerate(years):
            html += f"""
                <tr>
                    <td>Year {year}</td>
                    <td>${ucf_data[i]:,.0f}</td>
                    <td>${lcf_data[i]:,.0f}</td>
                </tr>
            """
        
        # Add totals row
        html += f"""
                <tr class="table-info">
                    <td><strong>Total</strong></td>
                    <td><strong>${ucf_total:,.0f}</strong></td>
                    <td><strong>${lcf_total:,.0f}</strong></td>
                </tr>
            </tbody>
        </table>
        """
        
        return html
        
    except Exception as e:
        logger.error(f"Error generating cash flow table: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return "<div class='alert alert-danger'>Error generating cash flow data</div>"

@app.route('/')
def index():
    """Homepage with property search"""
    return render_template('index.html')

@app.route('/search')
def search_properties():
    """Search for properties by various criteria"""
    query = request.args.get('query', '')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Search by property ID, address, city, zip, etc.
    cursor.execute('''
    SELECT property_id, full_street_line, city, state, zip_code, list_price
    FROM properties
    WHERE property_id LIKE ? 
    OR full_street_line LIKE ? 
    OR city LIKE ? 
    OR zip_code LIKE ?
    LIMIT 100
    ''', (f'%{query}%', f'%{query}%', f'%{query}%', f'%{query}%'))
    
    results = cursor.fetchall()
    conn.close()
    
    return jsonify([dict(row) for row in results])

@app.route('/property/<int:property_id>')
def property_detail(property_id):
    """Show property details with audit trail and robust error handling"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get property details
        cursor.execute('SELECT * FROM properties WHERE property_id = ?', (property_id,))
        property_data = cursor.fetchone()
        
        if not property_data:
            conn.close()
            return "Property not found", 404
        
        # Convert SQLite Row object to dict for safer handling
        if property_data:
            property_dict = dict(property_data)
        else:
            property_dict = {}
        
        # Get audit trail with error handling
        audit_trail = {}
        
        # Get audit data with safe error handling
        tables_to_query = [
            ('rental_income_audit', 'rental'),
            ('cash_flow_audit', 'cashflow'),
            ('mortgage_audit', 'mortgage'),
            ('investment_returns_audit', 'returns'),
            ('cash_flow_projections_audit', 'projections')
        ]
        
        for table_name, key in tables_to_query:
            try:
                cursor.execute(f'SELECT * FROM {table_name} WHERE property_id = ?', (property_id,))
                result = cursor.fetchone()
                if result:
                    audit_trail[key] = dict(result)
                else:
                    audit_trail[key] = {}
            except Exception as e:
                logger.error(f"Error querying {table_name}: {e}")
                audit_trail[key] = {}
        
        # Try enhanced audit tables
        enhanced_tables = [
            ('rental_income_audit_enhanced', 'rental_enhanced'),
            ('cash_flow_audit_enhanced', 'cashflow_enhanced'),
            ('mortgage_audit_enhanced', 'mortgage_enhanced'),
            ('investment_returns_audit_enhanced', 'returns_enhanced'),
            ('ranking_audit_enhanced', 'ranking_enhanced')
        ]
        
        for table_name, key in enhanced_tables:
            try:
                cursor.execute(f'SELECT * FROM {table_name} WHERE property_id = ?', (property_id,))
                result = cursor.fetchone()
                if result:
                    audit_trail[key] = dict(result)
            except:
                # Table might not exist - that's okay
                pass
        
        # Get detailed audit trail if available
        try:
            cursor.execute('SELECT * FROM calculation_audit_log WHERE property_id = ? ORDER BY calculation_timestamp', (property_id,))
            detailed_results = cursor.fetchall()
            if detailed_results:
                audit_trail['detailed'] = [dict(row) for row in detailed_results]
            else:
                audit_trail['detailed'] = []
        except:
            audit_trail['detailed'] = []
        
        # Close database connection before generating chart
        conn.close()
        
        # Generate HTML table instead of matplotlib chart
        cash_flow_table = generate_cash_flow_table(property_dict)
        
        return render_template(
            'property_detail.html',
            property=property_dict,
            audit_trail=audit_trail,
            cash_flow_table=cash_flow_table
        )
        
    except Exception as e:
        logger.error(f"Unexpected error in property_detail: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return f"An error occurred while loading property details: {str(e)}", 500

@app.route('/api/audit/<int:property_id>')
def get_property_audit(property_id):
    """API endpoint to get audit data for a property"""
    conn = get_db_connection()
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
    if property_data:
        result['property'] = dict(property_data)
    
    # Get data from each audit table
    for table in audit_tables:
        try:
            cursor.execute(f'SELECT * FROM {table} WHERE property_id = ?', (property_id,))
            rows = cursor.fetchall()
            if rows:
                result['audit_data'][table] = [dict(row) for row in rows]
        except:
            # Table might not exist or have a different structure
            pass
    
    conn.close()
    return jsonify(result)

@app.route('/calculations/<int:property_id>')
def property_calculations(property_id):
    """Show detailed calculations for a property"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get property details
    cursor.execute('SELECT * FROM properties WHERE property_id = ?', (property_id,))
    property_data = cursor.fetchone()
    
    if not property_data:
        conn.close()
        return "Property not found", 404
    
    # Get metrics with formulas
    metrics = {}
    
    # Cap rate calculation
    metrics['cap_rate'] = {
        'formula': 'noi_year1 / list_price * 100',
        'description': 'Capitalization Rate (%) - First year NOI divided by purchase price',
        'values': {
            'noi_year1': property_data.get('noi_year1'),
            'list_price': property_data.get('list_price'),
            'result': property_data.get('cap_rate')
        }
    }
    
    # Cash on cash calculation
    metrics['cash_on_cash'] = {
        'formula': 'lcf_year1 / cash_equity * 100',
        'description': 'Cash on Cash Return (%) - First year cash flow divided by cash invested',
        'values': {
            'lcf_year1': property_data.get('lcf_year1'),
            'cash_equity': property_data.get('cash_equity'),
            'result': property_data.get('cash_on_cash')
        }
    }
    
    # IRR calculation
    metrics['irr'] = {
        'formula': 'Internal Rate of Return based on 5-year cash flows + exit value',
        'description': 'Internal Rate of Return (%) - Annualized return rate',
        'values': {
            'result': property_data.get('irr')
        }
    }
    
    conn.close()
    
    return render_template(
        'property_calculations.html',
        property=property_data,
        metrics=metrics
    )

def generate_cash_flow_chart(property_data, audit_trail):
    """Generate a chart of projected cash flows with robust error handling"""
    try:
        # Check if property_data is valid
        if not property_data:
            print("Error: property_data is empty or None")
            return None
            
        # Extract cash flow data safely
        years = [1, 2, 3, 4, 5]
        ucf_data = []
        lcf_data = []
        
        for year in years:
            # Use safe extraction with conversion to float
            try:
                ucf_value = float(property_data.get(f'ucf_year{year}', 0) or 0)
                lcf_value = float(property_data.get(f'lcf_year{year}', 0) or 0)
            except (ValueError, TypeError) as e:
                print(f"Error converting cash flow data for year {year}: {e}")
                ucf_value = 0
                lcf_value = 0
                
            ucf_data.append(ucf_value)
            lcf_data.append(lcf_value)
        
        # Create the chart
        plt.figure(figsize=(10, 6))
        plt.bar(years, ucf_data, width=0.4, label='Unlevered Cash Flow', color='blue', alpha=0.7)
        plt.bar([y + 0.4 for y in years], lcf_data, width=0.4, label='Levered Cash Flow', color='green', alpha=0.7)
        
        plt.xlabel('Year')
        plt.ylabel('Cash Flow ($)')
        plt.title('Projected Cash Flows')
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Convert plot to base64 encoded image for HTML with error handling
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        
        # Use error handling during base64 encoding
        try:
            # Get the binary data from buffer
            binary_data = buffer.getvalue()
            # Encode to base64
            base64_data = base64.b64encode(binary_data)
            # Only decode the base64 data (which should be ASCII-compatible)
            plot_data = base64_data.decode('ascii')
            plt.close()
            return plot_data
        except UnicodeDecodeError as e:
            print(f"Error decoding chart data: {e}")
            plt.close()
            # Return a simple placeholder instead
            return None
            
    except Exception as e:
        print(f"Error generating chart: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# Create flask templates directory if it doesn't exist
os.makedirs('templates', exist_ok=True)

# Create index.html template
with open('templates/index.html', 'w') as f:
    f.write('''
<!DOCTYPE html>
<html>
<head>
    <title>Real Estate Investment Audit Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { padding-top: 20px; }
        .container { max-width: 1200px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Real Estate Investment Audit Dashboard</h1>
        
        <div class="row mt-4">
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h5>Property Search</h5>
                    </div>
                    <div class="card-body">
                        <div class="input-group mb-3">
                            <input type="text" id="searchInput" class="form-control" placeholder="Enter property ID, address, city, or ZIP code">
                            <button class="btn btn-primary" id="searchButton">Search</button>
                        </div>
                        <div id="searchResults" class="mt-3"></div>
                    </div>
                </div>
            </div>
            
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h5>Recently Viewed Properties</h5>
                    </div>
                    <div class="card-body">
                        <ul id="recentProperties" class="list-group">
                            <!-- Recent properties will appear here -->
                        </ul>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        document.getElementById('searchButton').addEventListener('click', function() {
            const query = document.getElementById('searchInput').value;
            if (query) {
                fetch(`/search?query=${encodeURIComponent(query)}`)
                    .then(response => response.json())
                    .then(data => {
                        const resultsDiv = document.getElementById('searchResults');
                        if (data.length === 0) {
                            resultsDiv.innerHTML = '<p>No properties found.</p>';
                            return;
                        }
                        
                        let html = '<ul class="list-group">';
                        data.forEach(property => {
                            html += `<li class="list-group-item">
                                <a href="/property/${property.property_id}">
                                    ${property.full_street_line}, ${property.city}, ${property.state} ${property.zip_code}
                                </a>
                                <br>
                                <small>Property ID: ${property.property_id} | Price: $${property.list_price.toLocaleString()}</small>
                            </li>`;
                        });
                        html += '</ul>';
                        resultsDiv.innerHTML = html;
                        
                        // Store in recently viewed
                        if (data.length > 0) {
                            const recentList = document.getElementById('recentProperties');
                            const property = data[0];
                            const li = document.createElement('li');
                            li.className = 'list-group-item';
                            li.innerHTML = `<a href="/property/${property.property_id}">
                                ${property.full_street_line}, ${property.city}, ${property.state}
                            </a>`;
                            
                            // Add to top of list
                            if (recentList.firstChild) {
                                recentList.insertBefore(li, recentList.firstChild);
                            } else {
                                recentList.appendChild(li);
                            }
                        }
                    })
                    .catch(error => {
                        console.error('Error:', error);
                    });
            }
        });
    </script>
</body>
</html>
    ''')

# Create property_detail.html template
with open('templates/property_detail.html', 'w') as f:
    f.write('''
<!DOCTYPE html>
<html>
<head>
    <title>Property Audit Details</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { padding-top: 20px; }
        .container { max-width: 1200px; }
        .audit-section { margin-bottom: 30px; }
        .formula { font-family: monospace; background-color: #f8f9fa; padding: 5px; border-radius: 3px; }
    </style>
</head>
<body>
    <div class="container">
        <nav aria-label="breadcrumb">
            <ol class="breadcrumb">
                <li class="breadcrumb-item"><a href="/">Home</a></li>
                <li class="breadcrumb-item active">Property {{ property.property_id }}</li>
            </ol>
        </nav>
        
        <div class="row">
            <div class="col-md-12">
                <h1>{{ property.full_street_line }}</h1>
                <h4>{{ property.city }}, {{ property.state }} {{ property.zip_code }}</h4>
                
                <div class="card mt-4">
                    <div class="card-header">
                        <h5>Cash Flow Projections</h5>
                    </div>
                    <div class="card-body">
                        {% if cash_flow_chart %}
                            <div class="text-center">
                                <img src="data:image/png;base64,{{ cash_flow_chart }}" class="img-fluid" alt="Cash Flow Chart">
                            </div>
                        {% else %}
                            <div class="text-center p-4">
                                <div class="alert alert-warning">
                                    <h5>Chart unavailable</h5>
                                    <p>The cash flow chart could not be generated. Below is a table of the cash flow data instead.</p>
                                </div>
                                
                                <table class="table table-striped mt-3">
                                    <thead>
                                        <tr>
                                            <th>Year</th>
                                            <th>Unlevered Cash Flow</th>
                                            <th>Levered Cash Flow</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {% for year in range(1, 6) %}
                                        <tr>
                                            <td>Year {{ year }}</td>
                                            <td>${{ '{:,.0f}'.format(property.get('ucf_year' + year|string, 0) or 0) }}</td>
                                            <td>${{ '{:,.0f}'.format(property.get('lcf_year' + year|string, 0) or 0) }}</td>
                                        </tr>
                                        {% endfor %}
                                        <tr class="table-info">
                                            <td><strong>Total</strong></td>
                                            <td>${{ '{:,.0f}'.format((property.get('ucf_year1', 0) or 0) + 
                                                                    (property.get('ucf_year2', 0) or 0) + 
                                                                    (property.get('ucf_year3', 0) or 0) + 
                                                                    (property.get('ucf_year4', 0) or 0) + 
                                                                    (property.get('ucf_year5', 0) or 0)) }}</td>
                                            <td>${{ '{:,.0f}'.format(property.get('accumulated_cash_flow', 0) or 0) }}</td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        {% endif %}
                    </div>
                </div>
                
                <!-- Cash Flow Chart -->
                {% if cash_flow_chart %}
                <div class="card mt-4">
                    <div class="card-header">
                        <h5>Cash Flow Projections</h5>
                    </div>
                    <div class="card-body">
                        {% if cash_flow_table %}
                            <div class="table-responsive">
                                {{ cash_flow_table|safe }}
                            </div>
                        {% else %}
                            <div class="alert alert-warning">
                                <p>Cash flow data is unavailable for this property.</p>
                            </div>
                        {% endif %}
                    </div>
                </div>
                {% endif %}
                
                <div class="row mt-4">
                    <div class="col-md-12">
                        <ul class="nav nav-tabs" id="auditTabs" role="tablist">
                            <li class="nav-item" role="presentation">
                                <button class="nav-link active" id="rental-tab" data-bs-toggle="tab" data-bs-target="#rental" type="button" role="tab">Rental Estimate</button>
                            </li>
                            <li class="nav-item" role="presentation">
                                <button class="nav-link" id="cashflow-tab" data-bs-toggle="tab" data-bs-target="#cashflow" type="button" role="tab">Cash Flow</button>
                            </li>
                            <li class="nav-item" role="presentation">
                                <button class="nav-link" id="mortgage-tab" data-bs-toggle="tab" data-bs-target="#mortgage" type="button" role="tab">Mortgage</button>
                            </li>
                            <li class="nav-item" role="presentation">
                                <button class="nav-link" id="returns-tab" data-bs-toggle="tab" data-bs-target="#returns" type="button" role="tab">Returns</button>
                            </li>
                            <li class="nav-item" role="presentation">
                                <button class="nav-link" id="audit-tab" data-bs-toggle="tab" data-bs-target="#audit" type="button" role="tab">Detailed Audit</button>
                            </li>
                        </ul>
                        
                        <div class="tab-content p-3 border border-top-0 rounded-bottom">
                            <!-- Rental Estimate Audit -->
                            <div class="tab-pane fade show active" id="rental" role="tabpanel">
                                <h4>Rental Income Estimation Audit</h4>
                                {% if audit_trail.rental %}
                                    <div class="row">
                                        <div class="col-md-6">
                                            <h5>Input Values</h5>
                                            <table class="table table-striped">
                                                <tr>
                                                    <th>Beds</th>
                                                    <td>{{ property.beds }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Baths</th>
                                                    <td>{{ property.full_baths + (property.half_baths * 0.5) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Sqft</th>
                                                    <td>{{ '{:,.0f}'.format(property.sqft) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Year Built</th>
                                                    <td>{{ property.year_built }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Property Style</th>
                                                    <td>{{ property.style }}</td>
                                                </tr>
                                                <tr>
                                                    <th>ZIP Code</th>
                                                    <td>{{ property.zip_code }}</td>
                                                </tr>
                                            </table>
                                        </div>
                                        <div class="col-md-6">
                                            <h5>Calculated Values</h5>
                                            <table class="table table-striped">
                                                <tr>
                                                    <th>Monthly Rent</th>
                                                    <td>${{ '{:,.0f}'.format(property.zori_monthly_rent) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Annual Rent</th>
                                                    <td>${{ '{:,.0f}'.format(property.zori_annual_rent) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Growth Rate</th>
                                                    <td>{{ '{:.2f}'.format(property.zori_growth_rate) }}%</td>
                                                </tr>
                                                <tr>
                                                    <th>Gross Rent Multiplier</th>
                                                    <td>{{ '{:.2f}'.format(property.gross_rent_multiplier) }}</td>
                                                </tr>
                                            </table>
                                            
                                            <h5 class="mt-4">Formulas</h5>
                                            <p><strong>Monthly Rent:</strong> <span class="formula">Base ZORI Rent × Bed/Bath Factor × Size Factor × Condition Factor × Property Type Factor</span></p>
                                            <p><strong>Annual Rent:</strong> <span class="formula">Monthly Rent × 12</span></p>
                                            <p><strong>Gross Rent Multiplier:</strong> <span class="formula">List Price ÷ Annual Rent</span></p>
                                        </div>
                                    </div>
                                {% else %}
                                    <p>No rental audit data available</p>
                                {% endif %}
                            </div>
                            
                            <!-- Cash Flow Audit -->
                            <div class="tab-pane fade" id="cashflow" role="tabpanel">
                                <h4>Cash Flow Calculation Audit</h4>
                                {% if audit_trail.cashflow %}
                                    <div class="row">
                                        <div class="col-md-6">
                                            <h5>Input Values</h5>
                                            <table class="table table-striped">
                                                <tr>
                                                    <th>Annual Rent</th>
                                                    <td>${{ '{:,.0f}'.format(property.zori_annual_rent) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Property Tax</th>
                                                    <td>${{ '{:,.0f}'.format(property.tax_used) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>HOA Fee (Annual)</th>
                                                    <td>${{ '{:,.0f}'.format(property.hoa_fee_used * 12) }}</td>
                                                </tr>
                                            </table>
                                        </div>
                                        <div class="col-md-6">
                                            <h5>NOI Calculation</h5>
                                            <table class="table table-striped">
                                                <tr>
                                                    <th>Annual Rent</th>
                                                    <td>${{ '{:,.0f}'.format(property.zori_annual_rent) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Vacancy (5%)</th>
                                                    <td>-${{ '{:,.0f}'.format(property.zori_annual_rent * 0.05) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Management (8%)</th>
                                                    <td>-${{ '{:,.0f}'.format(property.zori_annual_rent * 0.08) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Maintenance (5%)</th>
                                                    <td>-${{ '{:,.0f}'.format(property.zori_annual_rent * 0.05) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Insurance (0.5% of value)</th>
                                                    <td>-${{ '{:,.0f}'.format(property.list_price * 0.005) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>HOA Fee</th>
                                                    <td>-${{ '{:,.0f}'.format(property.hoa_fee_used * 12) }}</td>
                                                </tr>
                                                <tr class="table-success">
                                                    <th>NOI Year 1</th>
                                                    <td>${{ '{:,.0f}'.format(property.noi_year1) }}</td>
                                                </tr>
                                            </table>
                                            
                                            <h5 class="mt-4">Other Metrics</h5>
                                            <table class="table table-striped">
                                                <tr>
                                                    <th>Cap Rate</th>
                                                    <td>{{ '{:.2f}'.format(property.cap_rate) }}%</td>
                                                </tr>
                                                <tr>
                                                    <th>Unlevered Cash Flow</th>
                                                    <td>${{ '{:,.0f}'.format(property.ucf) }}</td>
                                                </tr>
                                            </table>
                                            
                                            <h5 class="mt-4">Formulas</h5>
                                            <p><strong>NOI:</strong> <span class="formula">Annual Rent - Vacancy - Management - Maintenance - Insurance - HOA</span></p>
                                            <p><strong>Cap Rate:</strong> <span class="formula">(NOI ÷ List Price) × 100</span></p>
                                            <p><strong>Unlevered CF:</strong> <span class="formula">NOI - Property Tax</span></p>
                                        </div>
                                    </div>
                                {% else %}
                                    <p>No cash flow audit data available</p>
                                {% endif %}
                            </div>
                            
                            <!-- Mortgage Audit -->
                            <div class="tab-pane fade" id="mortgage" role="tabpanel">
                                <h4>Mortgage Calculation Audit</h4>
                                {% if audit_trail.mortgage %}
                                    <div class="row">
                                        <div class="col-md-6">
                                            <h5>Input Values</h5>
                                            <table class="table table-striped">
                                                <tr>
                                                    <th>List Price</th>
                                                    <td>${{ '{:,.0f}'.format(property.list_price) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Down Payment %</th>
                                                    <td>{{ '{:.0f}'.format(property.down_payment_pct * 100) }}%</td>
                                                </tr>
                                                <tr>
                                                    <th>Interest Rate</th>
                                                    <td>{{ '{:.3f}'.format(property.interest_rate) }}%</td>
                                                </tr>
                                                <tr>
                                                    <th>Loan Term</th>
                                                    <td>{{ property.loan_term }} years</td>
                                                </tr>
                                            </table>
                                        </div>
                                        <div class="col-md-6">
                                            <h5>Mortgage Details</h5>
                                            <table class="table table-striped">
                                                <tr>
                                                    <th>Loan Amount</th>
                                                    <td>${{ '{:,.0f}'.format(property.loan_amount) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Monthly Payment</th>
                                                    <td>${{ '{:,.0f}'.format(property.monthly_payment) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Annual Debt Service</th>
                                                    <td>${{ '{:,.0f}'.format(property.annual_debt_service) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Total Principal Paid (5 years)</th>
                                                    <td>${{ '{:,.0f}'.format(property.total_principal_paid) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Loan Balance After 5 Years</th>
                                                    <td>${{ '{:,.0f}'.format(property.final_loan_balance) }}</td>
                                                </tr>
                                            </table>
                                            
                                            <h5 class="mt-4">Levered Cash Flow</h5>
                                            <table class="table table-striped">
                                                <tr>
                                                    <th>Year 1</th>
                                                    <td>${{ '{:,.0f}'.format(property.lcf_year1) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Year 2</th>
                                                    <td>${{ '{:,.0f}'.format(property.lcf_year2) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Year 3</th>
                                                    <td>${{ '{:,.0f}'.format(property.lcf_year3) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Year 4</th>
                                                    <td>${{ '{:,.0f}'.format(property.lcf_year4) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Year 5</th>
                                                    <td>${{ '{:,.0f}'.format(property.lcf_year5) }}</td>
                                                </tr>
                                                <tr class="table-success">
                                                    <th>Accumulated Cash Flow</th>
                                                    <td>${{ '{:,.0f}'.format(property.accumulated_cash_flow) }}</td>
                                                </tr>
                                            </table>
                                        </div>
                                    </div>
                                {% else %}
                                    <p>No mortgage audit data available</p>
                                {% endif %}
                            </div>
                            
                            <!-- Returns Audit -->
                            <div class="tab-pane fade" id="returns" role="tabpanel">
                                <h4>Investment Returns Audit</h4>
                                {% if audit_trail.returns %}
                                    <div class="row">
                                        <div class="col-md-6">
                                            <h5>Exit Value Calculation</h5>
                                            <table class="table table-striped">
                                                <tr>
                                                    <th>Year 5 NOI</th>
                                                    <td>${{ '{:,.0f}'.format(property.noi_year5) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Exit Cap Rate</th>
                                                    <td>{{ '{:.2f}'.format(property.exit_cap_rate) }}%</td>
                                                </tr>
                                                <tr class="table-success">
                                                    <th>Exit Value</th>
                                                    <td>${{ '{:,.0f}'.format(property.exit_value) }}</td>
                                                </tr>
                                            </table>
                                            
                                            <h5 class="mt-4">Final Equity Position</h5>
                                            <table class="table table-striped">
                                                <tr>
                                                    <th>Exit Value</th>
                                                    <td>${{ '{:,.0f}'.format(property.exit_value) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Final Loan Balance</th>
                                                    <td>-${{ '{:,.0f}'.format(property.final_loan_balance) }}</td>
                                                </tr>
                                                <tr>
                                                    <th>Accumulated Cash Flow</th>
                                                    <td>+${{ '{:,.0f}'.format(property.accumulated_cash_flow) }}</td>
                                                </tr>
                                                <tr class="table-success">
                                                    <th>Equity at Exit</th>
                                                    <td>${{ '{:,.0f}'.format(property.equity_at_exit) }}</td>
                                                </tr>
                                            </table>
                                        </div>
                                        <div class="col-md-6">
                                            <h5>Return Metrics</h5>
                                            <table class="table table-striped">
                                                <tr>
                                                    <th>Cash-on-Cash Return (Year 1)</th>
                                                    <td>{{ '{:.2f}'.format(property.cash_on_cash) }}%</td>
                                                </tr>
                                                <tr>
                                                    <th>Internal Rate of Return (IRR)</th>
                                                    <td>{{ '{:.2f}'.format(property.irr) }}%</td>
                                                </tr>
                                                <tr>
                                                    <th>Total Return Multiple</th>
                                                    <td>{{ '{:.2f}'.format(property.total_return) }}x</td>
                                                </tr>
                                            </table>
                                            
                                            <h5 class="mt-4">Formulas</h5>
                                            <p><strong>Exit Value:</strong> <span class="formula">Year 5 NOI ÷ Exit Cap Rate</span></p>
                                            <p><strong>Equity at Exit:</strong> <span class="formula">Exit Value - Final Loan Balance + Accumulated Cash Flow</span></p>
                                            <p><strong>Cash-on-Cash Return:</strong> <span class="formula">(Year 1 Levered CF ÷ Cash Equity) × 100</span></p>
                                            <p><strong>IRR:</strong> <span class="formula">IRR of all cash flows including initial investment and exit proceeds</span></p>
                                            <p><strong>Total Return:</strong> <span class="formula">Equity at Exit ÷ Cash Equity</span></p>
                                        </div>
                                    </div>
                                {% else %}
                                    <p>No returns audit data available</p>
                                {% endif %}
                            </div>
                            
                            <!-- Detailed Audit Trail -->
                            <div class="tab-pane fade" id="audit" role="tabpanel">
                                <h4>Detailed Calculation Audit Trail</h4>
                                {% if audit_trail.detailed %}
                                    <div class="table-responsive">
                                        <table class="table table-striped table-sm">
                                            <thead>
                                                <tr>
                                                    <th>Timestamp</th>
                                                    <th>Metric</th>
                                                    <th>Inputs</th>
                                                    <th>Result</th>
                                                    <th>Formula</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {% for audit in audit_trail.detailed %}
                                                <tr>
                                                    <td>{{ audit.calculation_timestamp }}</td>
                                                    <td>{{ audit.metric_name }}</td>
                                                    <td><code>{{ audit.inputs }}</code></td>
                                                    <td>{{ audit.result }}</td>
                                                    <td><code>{{ audit.formula }}</code></td>
                                                </tr>
                                                {% endfor %}
                                            </tbody>
                                        </table>
                                    </div>
                                {% else %}
                                    <p>No detailed audit trail available</p>
                                {% endif %}
                            </div>
                        </div>
                    </div>
                </div>
                
                <div class="mt-4">
                    <a href="/calculations/{{ property.property_id }}" class="btn btn-primary">View Detailed Calculations</a>
                    <a href="/api/audit/{{ property.property_id }}" class="btn btn-outline-secondary" target="_blank">View API Data</a>
                </div>
            </div>
        </div>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
    ''')

# Create property_calculations.html template
with open('templates/property_calculations.html', 'w') as f:
    f.write('''
<!DOCTYPE html>
<html>
<head>
    <title>Property Calculations</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { padding-top: 20px; }
        .container { max-width: 1200px; }
        .formula { font-family: monospace; background-color: #f8f9fa; padding: 5px; border-radius: 3px; }
        .calculation-card { margin-bottom: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <nav aria-label="breadcrumb">
            <ol class="breadcrumb">
                <li class="breadcrumb-item"><a href="/">Home</a></li>
                <li class="breadcrumb-item"><a href="/property/{{ property.property_id }}">Property {{ property.property_id }}</a></li>
                <li class="breadcrumb-item active">Detailed Calculations</li>
            </ol>
        </nav>
        
        <h1>Detailed Calculations for {{ property.full_street_line }}</h1>
        
        <div class="row mt-4">
            <div class="col-md-12">
                {% for metric_name, metric in metrics.items() %}
                <div class="card calculation-card">
                    <div class="card-header">
                        <h4>{{ metric.description }}</h4>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-6">
                                <h5>Formula</h5>
                                <div class="formula p-3 mb-4">{{ metric.formula }}</div>
                                
                                <h5>Input Values</h5>
                                <table class="table table-striped">
                                    {% for name, value in metric.values.items() %}
                                    {% if name != 'result' %}
                                    <tr>
                                        <th>{{ name }}</th>
                                        <td>{% if value is number and value > 100 %}${{ '{:,.0f}'.format(value) }}{% else %}{{ value }}{% endif %}</td>
                                    </tr>
                                    {% endif %}
                                    {% endfor %}
                                </table>
                            </div>
                            <div class="col-md-6">
                                <h5>Calculation</h5>
                                <div class="p-3 bg-light">
                                    <p>Substituting values into the formula:</p>
                                    {% if metric_name == 'cap_rate' %}
                                    <p class="formula">
                                        cap_rate = (noi_year1 / list_price) * 100<br>
                                        cap_rate = (${{ '{:,.0f}'.format(metric.values.noi_year1) }} / ${{ '{:,.0f}'.format(metric.values.list_price) }}) * 100<br>
                                        cap_rate = {{ '{:.2f}'.format(metric.values.result) }}%
                                    </p>
                                    {% elif metric_name == 'cash_on_cash' %}
                                    <p class="formula">
                                        cash_on_cash = (lcf_year1 / cash_equity) * 100<br>
                                        cash_on_cash = (${{ '{:,.0f}'.format(metric.values.lcf_year1) }} / ${{ '{:,.0f}'.format(metric.values.cash_equity) }}) * 100<br>
                                        cash_on_cash = {{ '{:.2f}'.format(metric.values.result) }}%
                                    </p>
                                    {% else %}
                                    <p class="formula">
                                        {{ metric_name }} = {{ metric.formula }}<br>
                                        {{ metric_name }} = {{ metric.values.result }}
                                    </p>
                                    {% endif %}
                                </div>
                                
                                <h5 class="mt-4">Result</h5>
                                <div class="p-3 bg-success text-white">
                                    <h3>{{ metric.description.split(' (')[0] }}: {% if metric.values.result is number and metric.values.result > 100 %}${{ '{:,.0f}'.format(metric.values.result) }}{% else %}{{ metric.values.result }}{% endif %}</h3>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
    ''')

import os
import codecs

def ensure_utf8_templates():
    """
    Ensures all templates are properly saved with UTF-8 encoding.
    This function will convert any non-UTF-8 templates to UTF-8.
    """
    template_dir = os.path.join(os.path.dirname(__file__), 'templates')
    
    if not os.path.exists(template_dir):
        os.makedirs(template_dir)
        print(f"Created templates directory: {template_dir}")
    
    templates = [
        ('index.html', get_index_template()),
        ('property_detail.html', get_property_detail_template()),
        ('property_calculations.html', get_property_calculations_template())
    ]
    
    for filename, content in templates:
        filepath = os.path.join(template_dir, filename)
        
        # If file exists, check if it's valid UTF-8
        if os.path.exists(filepath):
            try:
                # Try to read with UTF-8
                with codecs.open(filepath, 'r', encoding='utf-8') as f:
                    f.read()
                print(f"Template {filename} is valid UTF-8")
            except UnicodeDecodeError:
                print(f"Template {filename} is not valid UTF-8, converting...")
                try:
                    # Try to read with other encodings
                    for enc in ['latin-1', 'cp1252', 'iso-8859-1']:
                        try:
                            with codecs.open(filepath, 'r', encoding=enc) as f:
                                content = f.read()
                            # Write back as UTF-8
                            with codecs.open(filepath, 'w', encoding='utf-8') as f:
                                f.write(content)
                            print(f"Successfully converted {filename} from {enc} to UTF-8")
                            break
                        except UnicodeDecodeError:
                            continue
                except Exception as e:
                    print(f"Error converting {filename}: {e}")
                    # Backup and rewrite
                    os.rename(filepath, f"{filepath}.bak")
                    with codecs.open(filepath, 'w', encoding='utf-8') as f:
                        f.write(content)
                    print(f"Backed up original to {filepath}.bak and wrote new UTF-8 version")
        else:
            # Write new file with UTF-8 encoding
            with codecs.open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Created new template {filename} with UTF-8 encoding")

# Template content functions
def get_index_template():
    # Return the content for index.html
    return """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Real Estate Investment Audit Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { padding-top: 20px; }
        .container { max-width: 1200px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Real Estate Investment Audit Dashboard</h1>
        
        <div class="row mt-4">
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h5>Property Search</h5>
                    </div>
                    <div class="card-body">
                        <div class="input-group mb-3">
                            <input type="text" id="searchInput" class="form-control" placeholder="Enter property ID, address, city, or ZIP code">
                            <button class="btn btn-primary" id="searchButton">Search</button>
                        </div>
                        <div id="searchResults" class="mt-3"></div>
                    </div>
                </div>
            </div>
            
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h5>Recently Viewed Properties</h5>
                    </div>
                    <div class="card-body">
                        <ul id="recentProperties" class="list-group">
                            <!-- Recent properties will appear here -->
                        </ul>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        document.getElementById('searchButton').addEventListener('click', function() {
            const query = document.getElementById('searchInput').value;
            if (query) {
                fetch(`/search?query=${encodeURIComponent(query)}`)
                    .then(response => response.json())
                    .then(data => {
                        const resultsDiv = document.getElementById('searchResults');
                        if (data.length === 0) {
                            resultsDiv.innerHTML = '<p>No properties found.</p>';
                            return;
                        }
                        
                        let html = '<ul class="list-group">';
                        data.forEach(property => {
                            html += `<li class="list-group-item">
                                <a href="/property/${property.property_id}">
                                    ${property.full_street_line}, ${property.city}, ${property.state} ${property.zip_code}
                                </a>
                                <br>
                                <small>Property ID: ${property.property_id} | Price: $${property.list_price.toLocaleString()}</small>
                            </li>`;
                        });
                        html += '</ul>';
                        resultsDiv.innerHTML = html;
                        
                        // Store in recently viewed
                        if (data.length > 0) {
                            const recentList = document.getElementById('recentProperties');
                            const property = data[0];
                            const li = document.createElement('li');
                            li.className = 'list-group-item';
                            li.innerHTML = `<a href="/property/${property.property_id}">
                                ${property.full_street_line}, ${property.city}, ${property.state}
                            </a>`;
                            
                            // Add to top of list
                            if (recentList.firstChild) {
                                recentList.insertBefore(li, recentList.firstChild);
                            } else {
                                recentList.appendChild(li);
                            }
                        }
                    })
                    .catch(error => {
                        console.error('Error:', error);
                    });
            }
        });
    </script>
</body>
</html>"""

def get_property_detail_template():
    # Return a simplified version of property_detail.html with proper UTF-8 encoding
    return """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Property Audit Details</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { padding-top: 20px; }
        .container { max-width: 1200px; }
        .audit-section { margin-bottom: 30px; }
        .formula { font-family: monospace; background-color: #f8f9fa; padding: 5px; border-radius: 3px; }
    </style>
</head>
<body>
    <div class="container">
        <nav aria-label="breadcrumb">
            <ol class="breadcrumb">
                <li class="breadcrumb-item"><a href="/">Home</a></li>
                <li class="breadcrumb-item active">Property {{ property.property_id }}</li>
            </ol>
        </nav>
        
        <div class="row">
            <div class="col-md-12">
                <h1>{{ property.full_street_line }}</h1>
                <h4>{{ property.city }}, {{ property.state }} {{ property.zip_code }}</h4>
                
                <div class="card mt-4">
                    <div class="card-header">
                        <h5>Property Details</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-4">
                                <p><strong>Property ID:</strong> {{ property.property_id }}</p>
                                <p><strong>List Price:</strong> ${{ '{:,.0f}'.format(property.list_price) }}</p>
                                <p><strong>Beds:</strong> {{ property.beds }}</p>
                                <p><strong>Baths:</strong> {{ property.full_baths + (property.half_baths * 0.5) }}</p>
                                <p><strong>Sqft:</strong> {{ '{:,.0f}'.format(property.sqft) }}</p>
                            </div>
                            <div class="col-md-4">
                                <p><strong>Cap Rate:</strong> {{ '{:.2f}'.format(property.cap_rate) }}%</p>
                                <p><strong>Cash on Cash:</strong> {{ '{:.2f}'.format(property.cash_on_cash) }}%</p>
                                <p><strong>IRR:</strong> {{ '{:.2f}'.format(property.irr) }}%</p>
                                <p><strong>Total Return:</strong> {{ '{:.2f}'.format(property.total_return) }}x</p>
                                <p><strong>Investment Ranking:</strong> {{ property.investment_ranking }}/10</p>
                            </div>
                            <div class="col-md-4">
                                <p><strong>Monthly Rent:</strong> ${{ '{:,.0f}'.format(property.zori_monthly_rent) }}</p>
                                <p><strong>Annual NOI:</strong> ${{ '{:,.0f}'.format(property.noi_year1) }}</p>
                                <p><strong>Cash Flow Year 1:</strong> ${{ '{:,.0f}'.format(property.lcf_year1) }}</p>
                                <p><strong>Down Payment:</strong> {{ '{:.0f}'.format(property.down_payment_pct * 100) }}%</p>
                                <p><strong>Cash Equity:</strong> ${{ '{:,.0f}'.format(property.cash_equity) }}</p>
                            </div>
                        </div>
                    </div>
                </div>
                
                <!-- Cash Flow Table -->
                <div class="card mt-4">
                    <div class="card-header">
                        <h5>Cash Flow Projections</h5>
                    </div>
                    <div class="card-body">
                        {% if cash_flow_table %}
                            <div class="table-responsive">
                                {{ cash_flow_table|safe }}
                            </div>
                        {% else %}
                            <div class="alert alert-warning">
                                <p>Cash flow data is unavailable for this property.</p>
                            </div>
                        {% endif %}
                    </div>
                </div>
                
                <!-- Rest of template content truncated for brevity -->
                <div class="mt-4">
                    <a href="/calculations/{{ property.property_id }}" class="btn btn-primary">View Detailed Calculations</a>
                    <a href="/api/audit/{{ property.property_id }}" class="btn btn-outline-secondary" target="_blank">View API Data</a>
                </div>
            </div>
        </div>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>"""

def get_property_calculations_template():
    # Return the content for property_calculations.html
    return """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Property Calculations</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { padding-top: 20px; }
        .container { max-width: 1200px; }
        .formula { font-family: monospace; background-color: #f8f9fa; padding: 5px; border-radius: 3px; }
        .calculation-card { margin-bottom: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <nav aria-label="breadcrumb">
            <ol class="breadcrumb">
                <li class="breadcrumb-item"><a href="/">Home</a></li>
                <li class="breadcrumb-item"><a href="/property/{{ property.property_id }}">Property {{ property.property_id }}</a></li>
                <li class="breadcrumb-item active">Detailed Calculations</li>
            </ol>
        </nav>
        
        <h1>Detailed Calculations for {{ property.full_street_line }}</h1>
        
        <div class="row mt-4">
            <div class="col-md-12">
                {% for metric_name, metric in metrics.items() %}
                <div class="card calculation-card">
                    <div class="card-header">
                        <h4>{{ metric.description }}</h4>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-6">
                                <h5>Formula</h5>
                                <div class="formula p-3 mb-4">{{ metric.formula }}</div>
                                
                                <h5>Input Values</h5>
                                <table class="table table-striped">
                                    {% for name, value in metric.values.items() %}
                                    {% if name != 'result' %}
                                    <tr>
                                        <th>{{ name }}</th>
                                        <td>{% if value is number and value > 100 %}${{ '{:,.0f}'.format(value) }}{% else %}{{ value }}{% endif %}</td>
                                    </tr>
                                    {% endif %}
                                    {% endfor %}
                                </table>
                            </div>
                            <div class="col-md-6">
                                <h5>Calculation</h5>
                                <div class="p-3 bg-light">
                                    <p>Substituting values into the formula:</p>
                                    {% if metric_name == 'cap_rate' %}
                                    <p class="formula">
                                        cap_rate = (noi_year1 / list_price) * 100<br>
                                        cap_rate = (${{ '{:,.0f}'.format(metric.values.noi_year1) }} / ${{ '{:,.0f}'.format(metric.values.list_price) }}) * 100<br>
                                        cap_rate = {{ '{:.2f}'.format(metric.values.result) }}%
                                    </p>
                                    {% elif metric_name == 'cash_on_cash' %}
                                    <p class="formula">
                                        cash_on_cash = (lcf_year1 / cash_equity) * 100<br>
                                        cash_on_cash = (${{ '{:,.0f}'.format(metric.values.lcf_year1) }} / ${{ '{:,.0f}'.format(metric.values.cash_equity) }}) * 100<br>
                                        cash_on_cash = {{ '{:.2f}'.format(metric.values.result) }}%
                                    </p>
                                    {% else %}
                                    <p class="formula">
                                        {{ metric_name }} = {{ metric.formula }}<br>
                                        {{ metric_name }} = {{ metric.values.result }}
                                    </p>
                                    {% endif %}
                                </div>
                                
                                <h5 class="mt-4">Result</h5>
                                <div class="p-3 bg-success text-white">
                                    <h3>{{ metric.description.split(' (')[0] }}: {% if metric.values.result is number and metric.values.result > 100 %}${{ '{:,.0f}'.format(metric.values.result) }}{% else %}{{ metric.values.result }}{% endif %}</h3>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>"""

# Add this function call before app.run()
# ensure_utf8_templates()

if __name__ == '__main__':
    import os
    import argparse
    import logging
    from waitress import serve
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("audit_dashboard.log"),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger("audit_dashboard")
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Real Estate Investment Audit Dashboard')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind the server to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind the server to')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    parser.add_argument('--db-file', default='final.db', help='Path to SQLite database file')
    parser.add_argument('--production', action='store_true', help='Run in production mode with Waitress')
    parser.add_argument('--fix-templates', action='store_true', help='Force UTF-8 encoding for all templates')
    args = parser.parse_args()
    
    # Override with environment variables if present
    host = os.environ.get('AUDIT_DASHBOARD_HOST', args.host)
    port = int(os.environ.get('AUDIT_DASHBOARD_PORT', args.port))
    debug = os.environ.get('AUDIT_DASHBOARD_DEBUG', '').lower() in ('true', '1', 't') or args.debug
    db_file = os.environ.get('AUDIT_DASHBOARD_DB_FILE', args.db_file)
    production = os.environ.get('AUDIT_DASHBOARD_PRODUCTION', '').lower() in ('true', '1', 't') or args.production
    fix_templates = os.environ.get('AUDIT_DASHBOARD_FIX_TEMPLATES', '').lower() in ('true', '1', 't') or args.fix_templates
    
    # Set the database file globally
    DB_FILE = db_file
    logger.info(f"Using database file: {DB_FILE}")
    
    # Verify database exists and is accessible
    if not os.path.exists(DB_FILE):
        logger.error(f"Database file not found: {DB_FILE}")
        print(f"Error: Database file not found: {DB_FILE}")
        print("Please run the database setup script first.")
        exit(1)
    
    try:
        conn = get_db_connection()
        conn.close()
        logger.info("Successfully connected to database")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        print(f"Error: Failed to connect to database: {e}")
        exit(1)
    
    # Fix template encoding if requested
    if fix_templates:
        logger.info("Fixing template encoding...")
        print("Ensuring all templates use UTF-8 encoding...")
        ensure_utf8_templates()
    
    # Run the application
    logger.info(f"Starting server on {host}:{port} (debug={debug}, production={production})")
    print(f"Starting Real Estate Investment Audit Dashboard on http://{host}:{port}")
    
    if production:
        print("Running in production mode with Waitress server")
        serve(app, host=host, port=port)
    else:
        print(f"Running in {'debug' if debug else 'development'} mode with Flask's built-in server")
        print("WARNING: This is not suitable for production use")
        print("TIP: If you encounter encoding errors, restart with --fix-templates")
        app.run(host=host, port=port, debug=debug)