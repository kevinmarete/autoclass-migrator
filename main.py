import argparse
import pandas as pd
import concurrent.futures
import logging
import os
import time
import random

from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError, TooManyRequests, InternalServerError, ServiceUnavailable
from tqdm import tqdm


# Initialize Google Cloud Storage client for each thread
def get_storage_client(project_id):
    return storage.Client(project=project_id)


# Retry decorator with exponential backoff
def retry_with_backoff(retries=5, backoff_in_seconds=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    return func(*args, **kwargs)
                except (TooManyRequests, InternalServerError, ServiceUnavailable) as e:
                    logging.warning(f"Retrying due to error: {e}")
                    sleep_time = backoff_in_seconds * (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(sleep_time)
                    attempt += 1
            raise Exception(f"Failed after {retries} retries")

        return wrapper

    return decorator


# Enable Autoclass and set ARCHIVE storage class for the bucket
@retry_with_backoff(retries=5)
def process_bucket(project_id, bucket_name):
    base_response = {
        "project_id": project_id,
        "bucket_name": bucket_name
    }
    try:
        client = get_storage_client(project_id)  # Thread-safe, each thread gets its own client
        bucket = client.get_bucket(bucket_name)
        migrated = False

        response = {
            **base_response,
            "storage_class": bucket.storage_class,
            "location": bucket.location,
            "location_type": bucket.location_type,
            "autoclass_enabled": bucket.autoclass_enabled,
            "autoclass_terminal_storage_class": bucket.autoclass_terminal_storage_class,
            "requester_pays": bucket.requester_pays,
        }

        # Check if Autoclass is enabled
        if not bucket.autoclass_enabled:
            bucket.autoclass_enabled = True
            logging.info(f"Autoclass enabled for bucket: {bucket_name} in project {project_id}")
            migrated = True

        # Check and update the terminal storage class for the bucket
        if not bucket.autoclass_terminal_storage_class == 'ARCHIVE':
            bucket.autoclass_terminal_storage_class = 'ARCHIVE'
            logging.info(f"Migrated bucket '{bucket_name}' in project '{project_id}' to ARCHIVE.")
            migrated = True

        if not migrated:
            logging.info(f"Skipped bucket '{bucket_name}' in project '{project_id}': already in Autoclass/ARCHIVE.")
            return {**response, "migration_status": "Skipped"}

        bucket.patch()  # Save the change
        return {**response, "migration_status": "Migrated"}

    except GoogleAPIError as e:
        logging.error(f"Error processing bucket '{bucket_name}' in project '{project_id}': {str(e)}")
        return {
            **base_response,
            "storage_class": "",
            "location": "",
            "location_type": "",
            "autoclass_enabled": False,
            "autoclass_terminal_storage_class": "",
            "requester_pays": False,
            "migration_status": f"Error: {str(e)}"
        }


# Process the CSV and create tasks for each bucket
def process_csv(file_path, output_path):
    df = pd.read_csv(file_path)
    results = []

    # Thread pool for concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [
            executor.submit(process_bucket, row['GOOGLE_PROJECT_ID'], row['BUCKET_NAME'])
            for _, row in df.iterrows()
        ]

        # Show progress bar using tqdm
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            try:
                result = future.result()  # Get the result from the thread
                results.append(result)
            except Exception as e:
                logging.error(f"Error in processing thread: {str(e)}")

    # Convert the results to a DataFrame and save to CSV
    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path, index=False)
    return results_df


if __name__ == "__main__":
    # Set up command-line arguments
    parser = argparse.ArgumentParser(description="GCP bucket Migrator: Autoclass and Storage Terminal Class")
    parser.add_argument('-f', '--input_file', required=True,
                        help='Path to the input CSV file containing project_id and bucket_name.')
    input_args = parser.parse_args()

    # Path to the input CSV file
    input_csv_file = input_args.input_file
    input_csv_file_name, input_csv_file_extension = os.path.splitext(input_csv_file)

    # Path to the output CSV file
    output_csv_file = f"{input_csv_file_name}_output{input_csv_file_extension}"

    # Configure logging
    logging.basicConfig(filename=f"{input_csv_file_name}_output.log", level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Process the CSV and generate output
    process_csv(input_csv_file, output_csv_file)

    print(f"Migration log saved to {output_csv_file}")
