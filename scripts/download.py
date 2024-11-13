import configparser
import json
import os
from datetime import datetime

import boto3
import pandas as pd
from .extract import log

base_dir = os.getcwd() + "/"
config_dir = os.path.join(base_dir, "credentials/config.ini")
cleaned_dir = os.path.join(base_dir, "data/cleaned/")

# Load AWS credentials from configuration file
config = configparser.ConfigParser()
config.read(config_dir)

ACCESS_KEY_ID = config["AWS"]["ACCESS_KEY_ID"]
SECRET_ACCESS_KEY = config["AWS"]["SECRET_ACCESS_KEY"]


def get_json_data_from_s3(bucket_name, object_s3_path):
    try:
        # Initialize S3 client
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY,
        )
        s3_client = session.client("s3")

        # Fetch the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_s3_path)
        # Read the content of the file
        content = response["Body"].read().decode("utf-8")
        # Parse the content as JSON
        json_data = json.loads(content)
        print("JSON data loaded successfully")
        return json_data
    except Exception as e:
        print(f"Error retrieving or parsing JSON data from S3: {e}")
        return None


def extract_country_data(json_data):
    country_data = []
    log("Starting data extraction for countries")

    for country in json_data:
        try:
            # Get the first key in the nativeName dictionary
            native_name_key = next(iter(country["name"]["nativeName"]), None)
            # Get the common native name, or use an empty string if not found
            common_native_name = (
                country["name"]["nativeName"]
                .get(native_name_key, {})
                .get("common", "")
            )
            data = {
                "Country_Name": country["name"]["common"],
                "independence": country.get("independent", None),
                "united_nation_members": country.get("unMember", None),
                "start_of_week": country.get("startOfWeek", ""),
                "official_name": country["name"].get("official", ""),
                "common_native_name": common_native_name,
                "currency_code": (
                    list(country["currencies"])[0]
                    if country.get("currencies")
                    else ""
                ),
                "currency_name": (
                    country["currencies"]
                    .get(list(country["currencies"])[0], {})
                    .get("name", "")
                    if country.get("currencies")
                    else ""
                ),
                "currency_symbol": (
                    country["currencies"]
                    .get(list(country["currencies"])[0], {})
                    .get("symbol", "")
                    if country.get("currencies")
                    else ""
                ),
                "country_code": (
                    f"{country['idd']['root']}{country['idd']['suffixes'][0]}"
                    if country.get("idd")
                    else ""
                ),
                "capital": (
                    country["capital"][0] if country.get("capital") else ""
                ),
                "region": country.get("region", ""),
                "subregion": country.get("subregion", ""),
                "languages": (
                    ", ".join(country["languages"].values())
                    if country.get("languages")
                    else ""
                ),
                "area": country.get("area", ""),
                "population": country.get("population", ""),
                "continents": (
                    ", ".join(country["continents"])
                    if country.get("continents")
                    else ""
                ),
            }
            country_data.append(data)
            log(f"Data extracted for {country['name']['common']}")
        except Exception as e:
            log(
                f"Error extracting data for country "
                f"{country.get('name', {}).get('common', 'Unknown')}: {e}"
            )

    if country_data:
        try:
            df = pd.DataFrame(country_data)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            cleaned_file = f"countries_{timestamp}.parquet"
            cleaned_file_path = os.path.join(cleaned_dir, cleaned_file)
            df.to_parquet(cleaned_file_path, index=False)
            log(f"Data successfully saved to {cleaned_file_path}")
            return cleaned_file, cleaned_file_path
        except Exception as e:
            log(f"Error saving data as parquet: {e}")
            return None, None
    else:
        log("No data was extracted, returning None.")
        return None, None
