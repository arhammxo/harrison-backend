#!/bin/bash
set -e

echo "Downloading database from Google Drive..."

# Replace GOOGLE_DRIVE_URL with your actual Google Drive link
# If using a direct download link
if [ -z "$GOOGLE_DRIVE_URL" ]; then
  echo "Error: GOOGLE_DRIVE_URL environment variable is not set"
  exit 1
fi

# Check if the file ID is provided instead of the full URL
if [[ $GOOGLE_DRIVE_URL == *"drive.google.com"* ]]; then
  # Extract file ID from the Google Drive URL
  if [[ $GOOGLE_DRIVE_URL == *"/d/"* ]]; then
    FILE_ID=$(echo $GOOGLE_DRIVE_URL | sed -n 's/.*\/d\/\([^\/]*\).*/\1/p')
  elif [[ $GOOGLE_DRIVE_URL == *"id="* ]]; then
    FILE_ID=$(echo $GOOGLE_DRIVE_URL | sed -n 's/.*id=\([^&]*\).*/\1/p')
  else
    echo "Unable to extract file ID from Google Drive URL"
    exit 1
  fi
  
  echo "Extracted file ID: $FILE_ID"
  
  # Use gdown to download from Google Drive
  gdown --id $FILE_ID -O investment_properties.db
else
  # Direct download link provided
  wget -O investment_properties.db $GOOGLE_DRIVE_URL
fi

# Verify the download was successful
if [ ! -f investment_properties.db ]; then
  echo "Error: Failed to download the database"
  exit 1
fi

echo "Database downloaded successfully!"

# Set proper permissions
chmod 644 investment_properties.db

# Print database info
echo "Database size: $(du -h investment_properties.db | cut -f1)"

# Update the database name in code if needed
if [ "$DATABASE_NAME" != "" ] && [ "$DATABASE_NAME" != "investment_properties.db" ]; then
  echo "Renaming database from investment_properties.db to $DATABASE_NAME"
  mv investment_properties.db $DATABASE_NAME
fi

echo "Database setup complete!"