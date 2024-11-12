import boto3
import pandas as pd
import configparser
import os
from extract import log
import json
from datetime import datetime


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
            aws_secret_access_key=SECRET_ACCESS_KEY
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
                country["name"]["nativeName"].get(native_name_key, {}).get("common", "")
            )
            data = {
                "Country_Name": country["name"]["common"],
                "independence": (
                    country["independent"] if country["independent"] else ""
                ),
                "united_nation_members": (
                    country["unMember"] if country["unMember"] else ""
                ),
                "start_of_week": (
                    country["startOfWeek"] if country["startOfWeek"] else ""
                ),
                "official_name": (
                    country["name"]["official"] if country["name"]["official"] else ""
                ),
                "common_native_name": common_native_name,
                "currency_code": (
                    list(country["currencies"])[0] if country["currencies"] else ""
                ),
                "currency_name": (
                    country["currencies"][list(country["currencies"])[0]]["name"]
                    if country["currencies"]
                    else ""
                ),
                "currency_symbol": (
                    country["currencies"][list(country["currencies"])[0]]["symbol"]
                    if country["currencies"]
                    else ""
                ),
                "country_code": f"+{country['idd']['root']}{country['idd']['suffixes'][0]}",
                "capital": country["capital"][0] if country["capital"] else "",
                "region": country["region"] if country["region"] else "",
                "subregion": country["subregion"] if country["subregion"] else "",
                "languages": (
                    ", ".join(country["languages"].values())
                    if country["languages"]
                    else ""
                ),
                "area": country["area"] if country["area"] else "",
                "population": country["population"] if country["population"] else "",
                "continents": (
                    ", ".join(country["continents"]) if country["continents"] else ""
                ),
            }
            country_data.append(data)
            log(f"Data extracted for {country['name']['common']}")
        except Exception as e:
            log(
                f"""
                Error extracting data for country
                  {country['name']['common']}: {e}
                """
            )
    if country_data:
        try:
            df = pd.DataFrame(country_data)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            cleaned_file = f"countries_{timestamp}.csv"
            cleaned_file_path = os.path.join(cleaned_dir, cleaned_file)
            df.to_csv(cleaned_file_path, index=False)
            return cleaned_file, cleaned_file_path
        except Exception as e:
            log(f"Error saving data as CSV: {e}")
            return None, None
    else:
        log("No data was extracted, returning None.")
        return None, None
