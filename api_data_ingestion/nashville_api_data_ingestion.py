import os
import json
import requests
import boto3

from datetime import datetime
from dotenv import load_dotenv


load_dotenv()


def create_s3_client():
    aws_profile = os.getenv("AWS_PROFILE")
    aws_region = os.getenv("AWS_DEFAULT_REGION")

    if aws_profile:
        boto3_session = boto3.Session(
            profile_name=aws_profile,
            region_name=aws_region
        )
        return boto3_session.client("s3")

    return boto3.client("s3", region_name=aws_region)


def upload_file_to_s3(local_file_path, s3_folder, file_name):
    s3_client = create_s3_client()

    bucket_name = os.getenv("S3_BUCKET_NAME")
    base_folder = os.getenv("S3_BASE_FOLDER")

    s3_file_path = f"{base_folder}/{s3_folder}/{file_name}"

    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_file_path)
        print(f"Uploaded to S3: {s3_file_path}")
    except Exception as upload_error:
        print("S3 upload failed")
        print(upload_error)


def save_data_to_json(data, folder_name, dataset_name):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    base_path = "local_data/raw_data"
    folder_path = os.path.join(base_path, folder_name)

    os.makedirs(folder_path, exist_ok=True)

    file_name = f"{dataset_name}_{timestamp}.json"
    file_path = os.path.join(folder_path, file_name)

    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=4)

    print(f"{dataset_name} saved locally at {file_path}")

    s3_folder = f"raw_data/{folder_name}"
    upload_file_to_s3(file_path, s3_folder, file_name)


def fetch_api_data(api_url, dataset_name, folder_name):
    if not api_url:
        print(f"{dataset_name} API URL is missing in .env")
        return None

    print(f"\nFetching {dataset_name}")

    api_response = requests.get(api_url, timeout=30)

    if api_response.status_code != 200:
        print(f"Failed to fetch {dataset_name}")
        print(f"Status code: {api_response.status_code}")
        print(api_response.text[:300])
        return None

    try:
        api_data = api_response.json()
    except ValueError:
        print(f"{dataset_name} did not return valid JSON")
        print(api_response.text[:300])
        return None

    if "error" in api_data:
        print(f"{dataset_name} API error:")
        print(api_data["error"])
        return None

    if "features" in api_data:
        record_count = len(api_data["features"])
    else:
        record_count = len(api_data)

    print(f"{dataset_name} fetched successfully")
    print(f"Number of records: {record_count}")

    save_data_to_json(api_data, folder_name, dataset_name)

    return api_data


def fetch_nashville_311_service_requests():
    return fetch_api_data(
        os.getenv("NASHVILLE_311_API_URL"),
        "nashville_311_service_requests",
        "nashville_311_service_requests"
    )


def fetch_nashville_housing_property_data():
    return fetch_api_data(
        os.getenv("NASHVILLE_HOUSING_API_URL"),
        "nashville_housing_property_data",
        "nashville_housing_property_data"
    )


def fetch_nashville_property_standards_data():
    return fetch_api_data(
        os.getenv("NASHVILLE_PROPERTY_STANDARDS_API_URL"),
        "nashville_property_standards_data",
        "nashville_property_standards_data"
    )


if __name__ == "__main__":
    fetch_nashville_311_service_requests()
    fetch_nashville_housing_property_data()
    fetch_nashville_property_standards_data()