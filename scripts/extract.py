import requests
import json
from datetime import datetime, time 
import os
import pandas as pd

# constants
time_format = '%Y-%m-%d %H:%M:%S'
now = datetime.now()
current_date = now.strftime(time_format)
max_retries = 3
retry_delay = 5

# directories
base_dir = '/Users/user/Documents/CDE-Capstone-Project/'
log_dir = os.path.join(base_dir, 'logs/log.txt')
data_dir = os.path.join(base_dir, 'data')

# Log initialization
with open(log_dir, 'w') as f:
    f.write(f'{current_date} - Log file cleared for new logs \n')

# Log function
def log(message):
    current_date = now.strftime(time_format)
    with open(log_dir, 'a') as f:
        f.write(f'{current_date} - {message}\n')

def extract_data(url):
    for retry in range(max_retries):
        try:
            log(f"Attempting to fetch data (attempt {retry + 1}/{max_retries})")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            log(f"Successfully retrieved data for {len(data)} countries")
            return data
        except requests.exceptions.RequestException as e:
            log(f"Error fetching data: {str(e)}")
            if retry < max_retries - 1:
                log(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                log("Max retries exceeded")
    return None

  

def process_data(data):
    try:
        log("Processing country data...")

        processed_records = []
        for country in data:
            flat_country = {}

            # Flatten nested dictionaries with a customized function
            def flatten_dict(d, parent_key=''):
                for k, v in d.items():
                    new_key = f"{parent_key}_{k}" if parent_key else k
                    if isinstance(v, dict):
                        flatten_dict(v, new_key)
                    elif isinstance(v, list):
                        flat_country[new_key] = json.dumps(v) if v is not None else None
                    else:
                        flat_country[new_key] = v

            flatten_dict(country)

            # Handle languages
            languages = country.get('languages', {})
            flat_country['languages'] = ', '.join([v for v in languages.values()])

            # Handle currencies specifically with standardized keys
            currencies = country.get('currencies', {})
            for code, currency in currencies.items():
                flat_country['currency_code'] = code
                flat_country['currency_name'] = currency.get('name', 'N/A')
                flat_country['currency_symbol'] = currency.get('symbol', 'N/A')

            processed_records.append(flat_country)

        # Create DataFrame while avoiding unnecessary nested structures
        df = pd.DataFrame(processed_records)
        
        # Save files with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        parquet_file = os.path.join(data_dir, f'countries_{timestamp}.parquet')
        csv_file = os.path.join(data_dir, f'countries_{timestamp}.csv')
        
        # Save both formats
        df.to_parquet(parquet_file, index=False)
        # df.to_csv(csv_file, index=False)
        
        log(f"Data saved successfully:")
        log(f"Parquet: {parquet_file}")
        # log(f"CSV: {csv_file}")
        
        # Log dataset information
        log(f"\nDataset dimensions: {df.shape[0]} rows x {df.shape[1]} columns")
        log("\nColumns in dataset:")
        for col in df.columns:
            log(f"{col}")
        
        return parquet_file
    
    except Exception as e:
        log(f"Error processing data: {str(e)}")
        return None
        
