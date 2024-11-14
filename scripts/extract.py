import json
import os
from datetime import datetime, time

import requests

# constants
time_format = "%Y-%m-%d %H:%M:%S"
now = datetime.now()
current_date = now.strftime(time_format)
max_retries = 3
retry_delay = 5

# directories
base_dir = os.getcwd() + "/"
log_dir = os.path.join(base_dir, "logs/log.txt")
data_dir = os.path.join(base_dir, "data/raw/")

# Log initialization
with open(log_dir, "w") as f:
    f.write(f"{current_date} - Log file cleared for new logs \n")


# Log function
def log(message):
    """
    Logs a message to the specified log file.

    Parameters:
    message (str): The message to be logged.

    Returns:
    None
    """
    current_date = now.strftime(time_format)
    with open(log_dir, "a") as f:
        f.write(f"{current_date} - {message}\n")


def extract_data(url):
    for retry in range(max_retries):
        try:
            log(
                f"Attempting to fetch data"
                f"(attempt {retry + 1}/{max_retries})"
            )
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            log(f"Successfully retrieved data for {len(data)} countries")
            # Save the data as a JSON file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_file = f"countries_{timestamp}.json"
            json_path = os.path.join(data_dir, json_file)
            with open(json_path, "w") as f:
                json.dump(data, f, indent=2)
            log(f"Data saved as: {json_file}")
            log(f"File path: {json_path}")
            return json_file, json_path
        except requests.exceptions.RequestException as e:
            log(f"Error fetching data: {str(e)}", level="error")
            if retry < max_retries - 1:
                log(f"Retrying in {retry_delay} seconds...", level="warning")
                time.sleep(retry_delay)
            else:
                log("Max retries exceeded", level="error")
    return None, None
