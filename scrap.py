from homeharvest import scrape_property
from datetime import datetime
import time

# Generate filename based on listing type and current timestamp
listing_type = "for_sale"
current_timestamp = datetime.now().strftime("%Y%m%d_%H%M")
filename = f"{listing_type}_{current_timestamp}.csv"

# Start timing
start_time = time.time()

properties = scrape_property(
  location="Denver, CO",
  listing_type=listing_type,  # or (for_sale, for_rent, pending)
  past_days=30,  # sold in last 30 days - listed in last 30 days if (for_sale, for_rent)

  # property_type=['single_family','multi_family'],
  # date_from="2023-05-01", # alternative to past_days
  # date_to="2023-05-28",
  # foreclosure=True
  # mls_only=True,  # only fetch MLS listings
)

# Calculate and print elapsed time
elapsed_time = time.time() - start_time
print(f"Scraped {len(properties)} properties in {elapsed_time:.2f} seconds")

# Export to csv
properties.to_csv(filename, index=False)
print(properties.head())