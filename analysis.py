import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import os
import scipy.stats as stats

# Set up better visualization defaults
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 12

# File paths
NOZORI_FILE = 'investment_nozori.csv'
ZORI_FILE = 'investment_zori.csv'
OUTPUT_DIR = 'comparison_results'

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_data():
    """Load both CSV files and return as pandas DataFrames."""
    print(f"Loading data from {NOZORI_FILE} and {ZORI_FILE}...")
    
    # Load the data
    df_nozori = pd.read_csv(NOZORI_FILE)
    df_zori = pd.read_csv(ZORI_FILE)
    
    # Print basic info
    print(f"No ZORI data: {df_nozori.shape[0]} properties with {df_nozori.shape[1]} columns")
    print(f"ZORI data: {df_zori.shape[0]} properties with {df_zori.shape[1]} columns")
    
    # Check if property IDs match between the files
    common_ids = set(df_nozori['property_id']) & set(df_zori['property_id'])
    print(f"Properties in both datasets: {len(common_ids)}")
    
    # Filter to common properties for fair comparison
    df_nozori = df_nozori[df_nozori['property_id'].isin(common_ids)]
    df_zori = df_zori[df_zori['property_id'].isin(common_ids)]
    
    # Sort by property_id to ensure alignment
    df_nozori = df_nozori.sort_values('property_id').reset_index(drop=True)
    df_zori = df_zori.sort_values('property_id').reset_index(drop=True)
    
    return df_nozori, df_zori

def identify_key_metrics():
    """Identify key investment metrics to compare."""
    return [
        # Rental income metrics
        'monthly_rent', 'annual_rent', 'gross_rent_multiplier',
        
        # Property valuation metrics
        'cap_rate', 'exit_cap_rate',
        
        # Cash flow metrics
        'ucf', 'cash_yield',
        'lcf_year1', 'lcf_year5',
        'accumulated_cash_flow',
        
        # Mortgage metrics
        'down_payment_pct', 'interest_rate', 'loan_term',
        'loan_amount', 'monthly_payment',
        
        # Growth and return metrics
        'zori_growth_rate', 
        'exit_value', 'equity_at_exit',
        'irr', 'cash_on_cash'
    ]

def calculate_differences(df_nozori, df_zori, metrics):
    """Calculate differences between the two datasets for key metrics."""
    # Initialize a results DataFrame
    diff_df = pd.DataFrame({
        'property_id': df_zori['property_id'],
        'zip_code': df_zori['zip_code'],
        'list_price': df_zori['list_price'],
        'beds': df_zori['beds'],
        'sqft': df_zori['sqft'],
        'style': df_zori['style'],
        'year_built': df_zori['year_built']
    })
    
    # Calculate absolute and percentage differences
    for metric in metrics:
        # Skip metrics that might not be in both datasets
        if metric not in df_nozori.columns or metric not in df_zori.columns:
            print(f"Skipping {metric} - not found in both datasets")
            continue
            
        # Calculate differences
        abs_diff = df_zori[metric] - df_nozori[metric]
        
        # For percentage, avoid division by zero
        pct_diff = np.zeros(len(df_zori))
        mask = df_nozori[metric] != 0
        pct_diff[mask] = (abs_diff[mask] / df_nozori[metric][mask]) * 100
        
        # Store in the results DataFrame
        diff_df[f'{metric}_nozori'] = df_nozori[metric]
        diff_df[f'{metric}_zori'] = df_zori[metric]
        diff_df[f'{metric}_abs_diff'] = abs_diff
        diff_df[f'{metric}_pct_diff'] = pct_diff
    
    return diff_df

def statistical_comparison(diff_df, metrics):
    """Perform statistical comparison of the differences."""
    stats_df = pd.DataFrame(columns=[
        'Metric', 'NoZORI Mean', 'ZORI Mean', 'Mean Diff', 'Mean % Diff',
        'NoZORI Median', 'ZORI Median', 'Median Diff', 'Median % Diff',
        'NoZORI StdDev', 'ZORI StdDev', 't-statistic', 'p-value'
    ])
    
    for i, metric in enumerate(metrics):
        if f'{metric}_nozori' not in diff_df.columns:
            continue
            
        # Calculate basic statistics
        nozori_mean = diff_df[f'{metric}_nozori'].mean()
        zori_mean = diff_df[f'{metric}_zori'].mean()
        mean_diff = diff_df[f'{metric}_abs_diff'].mean()
        mean_pct_diff = diff_df[f'{metric}_pct_diff'].mean()
        
        nozori_median = diff_df[f'{metric}_nozori'].median()
        zori_median = diff_df[f'{metric}_zori'].median()
        median_diff = diff_df[f'{metric}_abs_diff'].median()
        median_pct_diff = diff_df[f'{metric}_pct_diff'].median()
        
        nozori_std = diff_df[f'{metric}_nozori'].std()
        zori_std = diff_df[f'{metric}_zori'].std()
        
        # Paired t-test (are the differences statistically significant?)
        t_stat, p_val = stats.ttest_rel(
            diff_df[f'{metric}_zori'].values, 
            diff_df[f'{metric}_nozori'].values
        )
        
        # Add to stats DataFrame
        stats_df.loc[i] = [
            metric, nozori_mean, zori_mean, mean_diff, mean_pct_diff,
            nozori_median, zori_median, median_diff, median_pct_diff,
            nozori_std, zori_std, t_stat, p_val
        ]
    
    # Save statistical results
    stats_df.to_csv(os.path.join(OUTPUT_DIR, 'statistical_comparison.csv'), index=False)
    
    # Create a summary of significant differences
    sig_diff = stats_df[stats_df['p-value'] < 0.05].copy()
    sig_diff = sig_diff.sort_values('Mean % Diff', key=abs, ascending=False)
    
    return stats_df, sig_diff

def create_visualizations(diff_df, metrics):
    """Create visualizations to compare the two datasets."""
    # Directory for plots
    plots_dir = os.path.join(OUTPUT_DIR, 'plots')
    os.makedirs(plots_dir, exist_ok=True)
    
    for metric in metrics:
        if f'{metric}_nozori' not in diff_df.columns:
            continue
            
        # Create distribution comparison plot
        plt.figure(figsize=(14, 10))
        
        # Setup a 2x2 grid
        plt.subplot(2, 2, 1)
        sns.histplot(diff_df[f'{metric}_nozori'], kde=True, bins=30, color='blue', label='No ZORI')
        sns.histplot(diff_df[f'{metric}_zori'], kde=True, bins=30, color='red', alpha=0.6, label='ZORI')
        plt.title(f'Distribution of {metric}')
        plt.xlabel(metric)
        plt.ylabel('Count')
        plt.legend()
        
        # Scatter plot to compare values
        plt.subplot(2, 2, 2)
        plt.scatter(diff_df[f'{metric}_nozori'], diff_df[f'{metric}_zori'], alpha=0.3)
        # Add a 45-degree line
        min_val = min(diff_df[f'{metric}_nozori'].min(), diff_df[f'{metric}_zori'].min())
        max_val = max(diff_df[f'{metric}_nozori'].max(), diff_df[f'{metric}_zori'].max())
        plt.plot([min_val, max_val], [min_val, max_val], 'r--')
        plt.title(f'{metric}: ZORI vs No ZORI')
        plt.xlabel(f'No ZORI {metric}')
        plt.ylabel(f'ZORI {metric}')
        
        # Histogram of differences
        plt.subplot(2, 2, 3)
        sns.histplot(diff_df[f'{metric}_abs_diff'], kde=True, bins=30, color='green')
        plt.axvline(x=0, color='r', linestyle='--')
        plt.title(f'Distribution of Differences in {metric}')
        plt.xlabel(f'Difference (ZORI - No ZORI)')
        plt.ylabel('Count')
        
        # Distribution of percent differences (capped to avoid extreme outliers)
        plt.subplot(2, 2, 4)
        # Cap percent differences for visualization
        pct_diff_capped = diff_df[f'{metric}_pct_diff'].clip(-100, 100)
        sns.histplot(pct_diff_capped, kde=True, bins=30, color='purple')
        plt.axvline(x=0, color='r', linestyle='--')
        plt.title(f'Distribution of % Differences in {metric}')
        plt.xlabel(f'% Difference (ZORI - No ZORI)')
        plt.ylabel('Count')
        
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, f'{metric}_comparison.png'), dpi=300)
        plt.close()
        
        # Create property type comparison boxplots
        if 'style' in diff_df.columns:
            plt.figure(figsize=(16, 8))
            styles = diff_df['style'].dropna().unique()
            
            if len(styles) <= 10:  # Only do this for a reasonable number of property styles
                plt.subplot(1, 2, 1)
                sns.boxplot(x='style', y=f'{metric}_pct_diff', data=diff_df)
                plt.title(f'% Difference in {metric} by Property Type')
                plt.xticks(rotation=45)
                plt.axhline(y=0, color='r', linestyle='--')
                
                plt.subplot(1, 2, 2)
                prop_means = diff_df.groupby('style')[f'{metric}_pct_diff'].mean().sort_values()
                sns.barplot(x=prop_means.index, y=prop_means.values)
                plt.title(f'Mean % Difference in {metric} by Property Type')
                plt.xticks(rotation=45)
                plt.axhline(y=0, color='r', linestyle='--')
                
                plt.tight_layout()
                plt.savefig(os.path.join(plots_dir, f'{metric}_by_property_type.png'), dpi=300)
                plt.close()
    
    # Create correlation heatmap of differences
    plt.figure(figsize=(16, 14))
    diff_cols = [col for col in diff_df.columns if '_pct_diff' in col]
    diff_cols = diff_cols[:20] if len(diff_cols) > 20 else diff_cols  # Limit to top 20 for readability
    
    if diff_cols:
        corr_matrix = diff_df[diff_cols].corr()
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
        sns.heatmap(corr_matrix, mask=mask, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
        plt.title('Correlation Between % Differences in Key Metrics')
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'difference_correlation_heatmap.png'), dpi=300)
        plt.close()

def segment_analysis(diff_df, metrics):
    """Analyze differences by property segments (price range, size, age, etc.)."""
    results = {}
    
    # Define segments
    # Price segments
    price_segments = [
        ('Under $250K', lambda df: df['list_price'] < 250000),
        ('$250K-$500K', lambda df: (df['list_price'] >= 250000) & (df['list_price'] < 500000)),
        ('$500K-$750K', lambda df: (df['list_price'] >= 500000) & (df['list_price'] < 750000)),
        ('$750K-$1M', lambda df: (df['list_price'] >= 750000) & (df['list_price'] < 1000000)),
        ('$1M-$2M', lambda df: (df['list_price'] >= 1000000) & (df['list_price'] < 2000000)),
        ('Over $2M', lambda df: df['list_price'] >= 2000000),
    ]
    
    # Property size segments (sqft)
    size_segments = [
        ('Under 1000 sqft', lambda df: df['sqft'] < 1000),
        ('1000-1500 sqft', lambda df: (df['sqft'] >= 1000) & (df['sqft'] < 1500)),
        ('1500-2000 sqft', lambda df: (df['sqft'] >= 1500) & (df['sqft'] < 2000)),
        ('2000-3000 sqft', lambda df: (df['sqft'] >= 2000) & (df['sqft'] < 3000)),
        ('Over 3000 sqft', lambda df: df['sqft'] >= 3000),
    ]
    
    # Property age segments
    current_year = 2025  # As per your scenario
    age_segments = [
        ('New (0-5 years)', lambda df: (current_year - df['year_built']) <= 5),
        ('5-10 years', lambda df: (current_year - df['year_built'] > 5) & (current_year - df['year_built'] <= 10)),
        ('10-20 years', lambda df: (current_year - df['year_built'] > 10) & (current_year - df['year_built'] <= 20)),
        ('20-30 years', lambda df: (current_year - df['year_built'] > 20) & (current_year - df['year_built'] <= 30)),
        ('30-50 years', lambda df: (current_year - df['year_built'] > 30) & (current_year - df['year_built'] <= 50)),
        ('Over 50 years', lambda df: (current_year - df['year_built'] > 50)),
    ]
    
    # Bedroom segments
    bedroom_segments = [
        ('Studio/1 BR', lambda df: df['beds'] <= 1),
        ('2 BR', lambda df: df['beds'] == 2),
        ('3 BR', lambda df: df['beds'] == 3),
        ('4 BR', lambda df: df['beds'] == 4),
        ('5+ BR', lambda df: df['beds'] >= 5),
    ]
    
    # Analyze all segments
    segment_types = {
        'price': price_segments,
        'size': size_segments,
        'age': age_segments,
        'bedrooms': bedroom_segments
    }
    
    segment_results = []
    
    for segment_type, segments in segment_types.items():
        for metric in metrics:
            if f'{metric}_pct_diff' not in diff_df.columns:
                continue
                
            for segment_name, segment_filter in segments:
                try:
                    # Filter data
                    segment_data = diff_df[segment_filter(diff_df)]
                    
                    # If not enough data in segment, skip
                    if len(segment_data) < 10:
                        continue
                    
                    # Calculate metrics for segment
                    mean_diff = segment_data[f'{metric}_pct_diff'].mean()
                    median_diff = segment_data[f'{metric}_pct_diff'].median()
                    count = len(segment_data)
                    
                    # Add to results
                    segment_results.append({
                        'Segment Type': segment_type,
                        'Segment': segment_name,
                        'Metric': metric,
                        'Mean % Diff': mean_diff,
                        'Median % Diff': median_diff,
                        'Count': count,
                        'Pct of Total': count / len(diff_df) * 100
                    })
                except Exception as e:
                    print(f"Error analyzing {segment_name} for {metric}: {e}")
    
    # Convert to DataFrame and save
    segment_df = pd.DataFrame(segment_results)
    segment_df.to_csv(os.path.join(OUTPUT_DIR, 'segment_analysis.csv'), index=False)
    
    # Create visualizations for segment analysis
    plots_dir = os.path.join(OUTPUT_DIR, 'plots')
    
    # Focus on key metrics
    key_segment_metrics = ['monthly_rent', 'cap_rate', 'irr', 'cash_on_cash']
    key_segment_metrics = [m for m in key_segment_metrics if m in metrics]
    
    for segment_type in segment_types:
        for metric in key_segment_metrics:
            metric_segments = segment_df[(segment_df['Segment Type'] == segment_type) & 
                                         (segment_df['Metric'] == metric)]
            
            if len(metric_segments) > 0:
                plt.figure(figsize=(12, 6))
                sns.barplot(x='Segment', y='Mean % Diff', data=metric_segments)
                plt.title(f'Mean % Difference in {metric} by {segment_type.capitalize()} Segment')
                plt.axhline(y=0, color='r', linestyle='--')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.savefig(os.path.join(plots_dir, f'{metric}_by_{segment_type}.png'), dpi=300)
                plt.close()
    
    return segment_df

def geographic_analysis(diff_df, metrics):
    """Analyze differences by geographic location (ZIP code)."""
    # For each ZIP code with sufficient data, calculate average differences
    zip_results = []
    
    for metric in metrics:
        if f'{metric}_pct_diff' not in diff_df.columns:
            continue
            
        # Group by ZIP code
        zip_groups = diff_df.groupby('zip_code')
        
        for zip_code, group in zip_groups:
            # Only analyze ZIPs with enough data
            if len(group) < 5:
                continue
                
            # Calculate metrics
            mean_diff = group[f'{metric}_pct_diff'].mean()
            median_diff = group[f'{metric}_pct_diff'].median()
            count = len(group)
            
            # Add to results
            zip_results.append({
                'ZIP Code': zip_code,
                'Metric': metric,
                'Mean % Diff': mean_diff,
                'Median % Diff': median_diff,
                'Count': count
            })
    
    # Convert to DataFrame and save
    if zip_results:
        zip_df = pd.DataFrame(zip_results)
        zip_df.to_csv(os.path.join(OUTPUT_DIR, 'geographic_analysis.csv'), index=False)
        
        # Create visualizations for top/bottom ZIPs
        plots_dir = os.path.join(OUTPUT_DIR, 'plots')
        
        # Focus on key metrics
        key_geo_metrics = ['monthly_rent', 'cap_rate', 'irr']
        key_geo_metrics = [m for m in key_geo_metrics if m in metrics]
        
        for metric in key_geo_metrics:
            metric_zips = zip_df[zip_df['Metric'] == metric].copy()
            
            if len(metric_zips) > 0:
                # Get top and bottom ZIPs by difference
                metric_zips = metric_zips.sort_values('Mean % Diff')
                top_zips = metric_zips.tail(10).sort_values('Mean % Diff')
                bottom_zips = metric_zips.head(10)
                
                # Create visualization
                plt.figure(figsize=(16, 8))
                
                plt.subplot(1, 2, 1)
                sns.barplot(x='ZIP Code', y='Mean % Diff', data=bottom_zips)
                plt.title(f'Bottom 10 ZIP Codes: {metric} (ZORI vs. No ZORI)')
                plt.axhline(y=0, color='r', linestyle='--')
                plt.xticks(rotation=90)
                
                plt.subplot(1, 2, 2)
                sns.barplot(x='ZIP Code', y='Mean % Diff', data=top_zips)
                plt.title(f'Top 10 ZIP Codes: {metric} (ZORI vs. No ZORI)')
                plt.axhline(y=0, color='r', linestyle='--')
                plt.xticks(rotation=90)
                
                plt.tight_layout()
                plt.savefig(os.path.join(plots_dir, f'{metric}_by_zip.png'), dpi=300)
                plt.close()
        
        return zip_df
    
    return None

def outlier_analysis(diff_df, metrics):
    """Identify outliers where models differ significantly."""
    outlier_results = []
    
    for metric in metrics:
        if f'{metric}_pct_diff' not in diff_df.columns:
            continue
            
        # Define outliers as properties where % difference exceeds 3 standard deviations
        pct_diff = diff_df[f'{metric}_pct_diff']
        mean = pct_diff.mean()
        std = pct_diff.std()
        
        threshold = 3 * std
        outliers = diff_df[(pct_diff > mean + threshold) | (pct_diff < mean - threshold)].copy()
        
        # If we have outliers, analyze them
        if len(outliers) > 0:
            outliers['metric'] = metric
            outliers['pct_diff'] = outliers[f'{metric}_pct_diff']
            outliers['abs_diff'] = outliers[f'{metric}_abs_diff']
            outliers['z_score'] = (outliers['pct_diff'] - mean) / std
            
            # Keep just the columns we need
            keep_cols = ['property_id', 'metric', 'list_price', 'zip_code', 'beds', 'sqft', 
                       'style', 'year_built', 'pct_diff', 'abs_diff', 'z_score']
            outliers = outliers[keep_cols]
            
            outlier_results.append(outliers)
    
    # Combine all outliers
    if outlier_results:
        all_outliers = pd.concat(outlier_results)
        all_outliers = all_outliers.sort_values('z_score', key=abs, ascending=False)
        
        # Save the results
        all_outliers.to_csv(os.path.join(OUTPUT_DIR, 'outlier_analysis.csv'), index=False)
        
        # Count outliers by property type
        if 'style' in all_outliers.columns:
            style_counts = all_outliers.groupby('style').size().reset_index(name='count')
            style_counts = style_counts.sort_values('count', ascending=False)
            
            plt.figure(figsize=(12, 6))
            sns.barplot(x='style', y='count', data=style_counts)
            plt.title('Count of Outliers by Property Type')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'plots', 'outliers_by_property_type.png'), dpi=300)
            plt.close()
        
        return all_outliers
    
    return None

def investment_recommendation_analysis(df_nozori, df_zori, diff_df):
    """Analyze how investment recommendations might differ between the two approaches."""
    # We'll define a "good investment" by certain criteria
    # Create a recommendation function
    def get_recommendation(row, source='zori'):
        suffix = '_zori' if source == 'zori' else '_nozori'
        
        # Criteria for good investment
        cap_rate_min = 5.0  # Minimum cap rate
        cash_yield_min = 6.0  # Minimum cash yield
        irr_min = 10.0  # Minimum IRR
        cash_on_cash_min = 1.5  # Minimum cash-on-cash (1.5 = 50% return)
        
        # Check criteria
        metrics = {}
        
        # Get the metrics if they exist
        try:
            metrics['cap_rate'] = row.get(f'cap_rate{suffix}')
            metrics['cash_yield'] = row.get(f'cash_yield{suffix}')
            metrics['irr'] = row.get(f'irr{suffix}')
            metrics['cash_on_cash'] = row.get(f'cash_on_cash{suffix}')
            
            # Count how many criteria are met
            score = 0
            if metrics['cap_rate'] and metrics['cap_rate'] >= cap_rate_min:
                score += 1
            if metrics['cash_yield'] and metrics['cash_yield'] >= cash_yield_min:
                score += 1
            if metrics['irr'] and metrics['irr'] >= irr_min:
                score += 1
            if metrics['cash_on_cash'] and metrics['cash_on_cash'] >= cash_on_cash_min:
                score += 1
                
            # Classify based on score
            if score >= 3:
                return 'Excellent'
            elif score == 2:
                return 'Good'
            elif score == 1:
                return 'Fair'
            else:
                return 'Poor'
        except:
            return 'Unknown'
    
    # Apply the recommendation function
    diff_df['recommendation_nozori'] = diff_df.apply(lambda row: get_recommendation(row, 'nozori'), axis=1)
    diff_df['recommendation_zori'] = diff_df.apply(lambda row: get_recommendation(row, 'zori'), axis=1)
    
    # Create a confusion matrix of recommendations
    rec_cross = pd.crosstab(
        diff_df['recommendation_nozori'], 
        diff_df['recommendation_zori'],
        normalize=False
    )
    
    rec_cross_pct = pd.crosstab(
        diff_df['recommendation_nozori'], 
        diff_df['recommendation_zori'],
        normalize='all'
    ) * 100
    
    # Save the results
    rec_cross.to_csv(os.path.join(OUTPUT_DIR, 'recommendation_matrix.csv'))
    rec_cross_pct.to_csv(os.path.join(OUTPUT_DIR, 'recommendation_matrix_pct.csv'))
    
    # Create a visualization
    plt.figure(figsize=(12, 10))
    
    plt.subplot(2, 1, 1)
    sns.heatmap(rec_cross, annot=True, fmt='d', cmap='Blues')
    plt.title('Investment Recommendation Comparison (Counts)')
    plt.xlabel('ZORI Recommendation')
    plt.ylabel('No ZORI Recommendation')
    
    plt.subplot(2, 1, 2)
    sns.heatmap(rec_cross_pct, annot=True, fmt='.1f', cmap='Blues')
    plt.title('Investment Recommendation Comparison (% of Total)')
    plt.xlabel('ZORI Recommendation')
    plt.ylabel('No ZORI Recommendation')
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'plots', 'recommendation_comparison.png'), dpi=300)
    plt.close()
    
    # Identify properties with different recommendations
    diff_df['recommendation_change'] = diff_df.apply(
        lambda row: 'Same' if row['recommendation_nozori'] == row['recommendation_zori'] 
                   else f"{row['recommendation_nozori']} → {row['recommendation_zori']}", 
        axis=1
    )
    
    # Calculate the percentage of properties with changed recommendations
    pct_changed = (diff_df['recommendation_nozori'] != diff_df['recommendation_zori']).mean() * 100
    
    # Count recommendation changes by type
    rec_changes = diff_df[diff_df['recommendation_nozori'] != diff_df['recommendation_zori']]
    change_counts = rec_changes['recommendation_change'].value_counts().reset_index()
    change_counts.columns = ['Change', 'Count']
    
    # Save changes
    change_counts.to_csv(os.path.join(OUTPUT_DIR, 'recommendation_changes.csv'), index=False)
    
    # Create visualization of changes
    if len(change_counts) > 0:
        plt.figure(figsize=(12, 6))
        sns.barplot(x='Change', y='Count', data=change_counts)
        plt.title(f'Investment Recommendation Changes ({pct_changed:.1f}% of properties changed)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'plots', 'recommendation_changes.png'), dpi=300)
        plt.close()
    
    return rec_cross, pct_changed, change_counts

def generate_summary_report(stats_df, sig_diff, segment_df, change_pct):
    """Generate a summary report of the analysis."""
    with open(os.path.join(OUTPUT_DIR, 'summary_report.md'), 'w') as f:
        f.write("# ZORI vs. No ZORI Investment Analysis Comparison\n\n")
        
        f.write("## Overview\n\n")
        f.write("This report compares real estate investment analysis results with and without ZORI (Zillow Observed Rent Index) data integration.\n\n")
        
        f.write("## Key Findings\n\n")
        
        # Recommendation changes
        f.write(f"### Investment Recommendation Impact\n\n")
        f.write(f"- **{change_pct:.1f}%** of properties had different investment recommendations when using ZORI data.\n")
        f.write(f"- This indicates that incorporating ZORI data has a meaningful impact on investment decision-making.\n\n")
        
        # Most significant metric differences
        f.write("### Most Significant Metric Differences\n\n")
        f.write("The following metrics showed the most significant differences between the two approaches:\n\n")
        
        # Format the significant differences table
        f.write("| Metric | Mean % Difference | p-value | Interpretation |\n")
        f.write("|--------|-------------------|---------|----------------|\n")
        
        for _, row in sig_diff.head(10).iterrows():
            metric = row['Metric']
            mean_pct_diff = row['Mean % Diff']
            p_value = row['p-value']
            
            # Determine an interpretation
            if abs(mean_pct_diff) > 20:
                interpretation = "Major difference"
            elif abs(mean_pct_diff) > 10:
                interpretation = "Significant difference"
            elif abs(mean_pct_diff) > 5:
                interpretation = "Moderate difference"
            else:
                interpretation = "Minor difference"
                
            # Add direction
            if mean_pct_diff > 0:
                interpretation += " (ZORI higher)"
            else:
                interpretation += " (ZORI lower)"
                
            f.write(f"| {metric} | {mean_pct_diff:.2f}% | {p_value:.5f} | {interpretation} |\n")
        
        f.write("\n")
        
        # Segment analysis insights
        f.write("### Property Segment Insights\n\n")
        
        # Group by segment type
        segment_types = segment_df['Segment Type'].unique()
        
        for segment_type in segment_types:
            f.write(f"#### {segment_type.capitalize()} Segments\n\n")
            
            segment_metrics = segment_df[segment_df['Segment Type'] == segment_type]
            
            # Get unique metrics
            metrics = segment_metrics['Metric'].unique()
            
            for metric in metrics[:3]:  # Limit to 3 metrics per segment type
                f.write(f"**{metric}**:\n\n")
                
                # Get data for this metric
                metric_data = segment_metrics[segment_metrics['Metric'] == metric].copy()
                
                # Sort by absolute mean difference
                metric_data['Abs Mean % Diff'] = metric_data['Mean % Diff'].abs()
                metric_data = metric_data.sort_values('Abs Mean % Diff', ascending=False).head(5)
                
                for _, row in metric_data.iterrows():
                    segment = row['Segment']
                    mean_diff = row['Mean % Diff']
                    count = row['Count']
                    
                    direction = "higher" if mean_diff > 0 else "lower"
                    f.write(f"- **{segment}**: ZORI estimates are {abs(mean_diff):.2f}% {direction} ({count} properties)\n")
                
                f.write("\n")
        
        # Conclusion
        f.write("## Conclusion and Recommendations\n\n")
        
        # Count metrics where ZORI is higher/lower
        higher_count = (stats_df['Mean % Diff'] > 0).sum()
        lower_count = (stats_df['Mean % Diff'] < 0).sum()
        significant_count = (stats_df['p-value'] < 0.05).sum()
        
        f.write(f"Based on the analysis of {len(stats_df)} key investment metrics:\n\n")
        
        f.write(f"- ZORI-based estimates are higher for {higher_count} metrics and lower for {lower_count} metrics.\n")
        f.write(f"- {significant_count} metrics show statistically significant differences (p < 0.05).\n\n")
        
        # Generate overall recommendation
        if change_pct > 30:
            recommendation = "**Strongly recommend** using ZORI data for investment analysis as it leads to substantially different investment decisions."
        elif change_pct > 15:
            recommendation = "**Recommend** using ZORI data as it provides meaningful improvements to investment analysis accuracy."
        elif change_pct > 5:
            recommendation = "ZORI data provides some benefit, though the impact on investment decisions is moderate."
        else:
            recommendation = "ZORI data has minimal impact on investment decisions for this property set."
            
        f.write(f"### Final Recommendation\n\n")
        f.write(recommendation + "\n\n")
        
        # Additional insights based on metrics
        rent_diff = stats_df[stats_df['Metric'] == 'monthly_rent']['Mean % Diff'].values[0] if 'monthly_rent' in stats_df['Metric'].values else 0
        cap_diff = stats_df[stats_df['Metric'] == 'cap_rate']['Mean % Diff'].values[0] if 'cap_rate' in stats_df['Metric'].values else 0
        irr_diff = stats_df[stats_df['Metric'] == 'irr']['Mean % Diff'].values[0] if 'irr' in stats_df['Metric'].values else 0
        
        if abs(rent_diff) > 10:
            rent_direction = "higher" if rent_diff > 0 else "lower"
            f.write(f"- ZORI-based rental estimates are significantly {rent_direction} ({abs(rent_diff):.1f}%), suggesting {'more optimistic' if rent_diff > 0 else 'more conservative'} income projections.\n")
        
        if abs(cap_diff) > 10:
            cap_direction = "higher" if cap_diff > 0 else "lower"
            f.write(f"- Cap rates are {cap_direction} with ZORI data ({abs(cap_diff):.1f}%), indicating {'better' if cap_diff > 0 else 'poorer'} property performance estimates.\n")
        
        if abs(irr_diff) > 10:
            irr_direction = "higher" if irr_diff > 0 else "lower"
            f.write(f"- IRR projections are {irr_direction} with ZORI data ({abs(irr_diff):.1f}%), suggesting {'more favorable' if irr_diff > 0 else 'less favorable'} long-term return expectations.\n")
        
        f.write("\n")
        
        f.write("### Next Steps\n\n")
        f.write("1. Review the detailed analysis files for specific insights on property segments.\n")
        f.write("2. Examine the properties where recommendations changed significantly.\n")
        f.write("3. Consider implementing a validation process against actual rental data where possible.\n")

def main():
    """Main function to run the analysis."""
    print("Starting comparison of ZORI vs. No ZORI investment analysis...")
    
    # Load data
    df_nozori, df_zori = load_data()
    
    # Identify key metrics to compare
    metrics = identify_key_metrics()
    print(f"Comparing {len(metrics)} key investment metrics")
    
    # Calculate differences
    diff_df = calculate_differences(df_nozori, df_zori, metrics)
    
    # Statistical comparison
    stats_df, sig_diff = statistical_comparison(diff_df, metrics)
    print(f"Found {len(sig_diff)} metrics with statistically significant differences")
    
    # Create visualizations
    create_visualizations(diff_df, metrics)
    print("Created visualizations for metric comparisons")
    
    # Segment analysis
    segment_df = segment_analysis(diff_df, metrics)
    print("Completed segment analysis")
    
    # Geographic analysis
    geo_df = geographic_analysis(diff_df, metrics)
    if geo_df is not None:
        print(f"Completed geographic analysis for {len(geo_df) // len(metrics)} ZIP codes")
    
    # Outlier analysis
    outlier_df = outlier_analysis(diff_df, metrics)
    if outlier_df is not None:
        print(f"Identified {len(outlier_df)} outlier cases")
    
    # Investment recommendation analysis
    _, change_pct, _ = investment_recommendation_analysis(df_nozori, df_zori, diff_df)
    print(f"Investment recommendations changed for {change_pct:.1f}% of properties")
    
    # Generate summary report
    generate_summary_report(stats_df, sig_diff, segment_df, change_pct)
    print("Generated summary report")
    
    print(f"\nAnalysis complete. Results saved to {OUTPUT_DIR} directory.")

if __name__ == "__main__":
    main()