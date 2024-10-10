import json
import boto3
import time
import csv
import requests
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load credentials from JSON file
with open('credentials.json', 'r') as config_file:
    config = json.load(config_file)
AWS_ACCESS_KEY = config['AWS_ACCESS_KEY']
AWS_SECRET_KEY = config['AWS_SECRET_KEY']
S3_BUCKET_NAME_1 = config['S3_BUCKET_NAME_1']
S3_BUCKET_NAME_2 = config['S3_BUCKET_NAME_2']
NEW_RELIC_API_KEY = config['NEW_RELIC_API_KEY']
NEW_RELIC_ACCOUNT_ID = config['NEW_RELIC_ACCOUNT_ID']

# Paths for S3 folders in the NCR bucket
INPUT_FOLDER_1 = 'Testing NCR/Input/'
ARCHIVE_FOLDER_1 = 'Testing NCR/Archive/'
ERROR_FOLDER_1 = 'Testing NCR/Error/'

# Paths for S3 folders in the TIll AUS bucket
INPUT_FOLDER_2 = 'transactions/Input1/'
ARCHIVE_FOLDER_2 = 'transactions/Archive/'
ERROR_FOLDER_2 = 'transactions/Error/'

# CSV file with the expected file list
EXPECTED_FILE_CHECKLIST = r'C:\Users\y.raj\Downloads\sncrCheckTest - Copy.csv'
# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

def send_event_to_new_relic(client_name, status, file_name, category, expected_time, event_type):
    url = f"https://insights-collector.newrelic.com/v1/accounts/{NEW_RELIC_ACCOUNT_ID}/events"
    headers = {
        'Api-Key': NEW_RELIC_API_KEY,
        'Content-Type': 'application/json'
    }
    payload = {
        'eventType': event_type,
        'clientName': client_name,
        'status': status,
        'fileName': file_name,
        'category': category,
        'expectedTime': expected_time.strftime('%Y-%m-%d %H:%M:%S'),
    }
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        logging.info(f"Event sent to New Relic: {status} for file {file_name} (Category: {category}, Expected Time: {expected_time})")
    except requests.exceptions.HTTPError as err:
        logging.error(f"Failed to send event to New Relic: {err}")
    time.sleep(2)

# Read the expected files from the CSV checklist
def read_expected_files():
    expected_files = []
    try:
        with open(EXPECTED_FILE_CHECKLIST, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                expected_files.append(row)
    except Exception as e:
        logging.error(f"Error reading expected files: {e}")
    return expected_files

TIME_BEFORE = timedelta(minutes=3)
TIME_AFTER = timedelta(minutes=3)
IN_PROGRESS_CHECK_INTERVAL = 90

def monitor_s3_folder():
    expected_files = read_expected_files()
    received_files = set()
    in_progress_files = {}
    file_timestamps = {}
    found_files = set()
    missing_files_reported = set()
    while True:
        try:
            current_time = datetime.now()
            logging.info(f"Current time: {current_time}")
            current_day = current_time.weekday()
            skip_files = current_day in [0, 6]  # 0 for Monday, 6 for Sunday
            # Fetch files for the NCR bucket
            input_files_1, archive_files_1, error_files_1 = fetch_s3_files(S3_BUCKET_NAME_1, INPUT_FOLDER_1, ARCHIVE_FOLDER_1, ERROR_FOLDER_1)
            # Fetch files for the TIL AUS bucket
            input_files_2, archive_files_2, error_files_2 = fetch_s3_files(S3_BUCKET_NAME_2, INPUT_FOLDER_2, ARCHIVE_FOLDER_2, ERROR_FOLDER_2)
            # Combine files from both buckets
            input_files = {**input_files_1, **input_files_2}
            archive_files = set(archive_files_1).union(set(archive_files_2))
            error_files = set(error_files_1).union(set(error_files_2))
            for expected in expected_files:
                expected_file_name = expected['fileName'].replace('<dateToken>', current_time.strftime('%Y%m%d'))
                if skip_files and expected_file_name in ['moare-ach-', 'moare-merchant-']:
                    continue
                expected_time = datetime.strptime(expected['expectedTime'], '%H:%M').replace(year=current_time.year, month=current_time.month, day=current_time.day)
                category = expected.get('category', 'General')
                time_before = expected_time - TIME_BEFORE
                time_after = expected_time + TIME_AFTER
                if time_before <= current_time <= time_after:
                    if expected_file_name in input_files:
                        if expected_file_name not in received_files:
                            send_event_to_new_relic(expected['client'], "Received", expected_file_name, category, expected_time, "Total Received file")
                            received_files.add(expected_file_name)
                            found_files.add(expected_file_name)
                            file_timestamps[expected_file_name] = input_files[expected_file_name]
                    else:
                        missing_files_reported.add(expected_file_name)
                # Handle missing file checks only after time_after
                if current_time > time_after and expected_file_name in missing_files_reported:
                    if (expected_file_name not in input_files and
                        expected_file_name not in archive_files and
                        expected_file_name not in error_files):
                        send_event_to_new_relic(expected['client'], "Missing", expected_file_name, category, expected_time, "Total Missing Files ")
                    missing_files_reported.discard(expected_file_name)
            # Handle in-progress and completion logic (added in-progress check)
            handle_in_progress_and_completion(expected_files, current_time, input_files, expected_time, archive_files, error_files, received_files, found_files, in_progress_files)
            time.sleep(10)
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user.")
            break
        except Exception as e:
            logging.error(f"Error in monitoring process: {e}")
def fetch_s3_files(bucket_name, input_folder, archive_folder, error_folder):
    input_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_folder)
    input_files = {obj['Key'].split('/')[-1]: obj['LastModified'] for obj in input_response.get('Contents', [])}
    archive_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=archive_folder)
    archive_files = {obj['Key'].split('/')[-1] for obj in archive_response.get('Contents', [])}
    error_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=error_folder)
    error_files = {obj['Key'].split('/')[-1] for obj in error_response.get('Contents', [])}
    return input_files, archive_files, error_files
def handle_in_progress_and_completion(expected_files, current_time, input_files,expected_time, archive_files, error_files, received_files, found_files, in_progress_files):
    # Check for "In Progress" files
    for file_name in received_files:
        if file_name in input_files:
            if file_name not in in_progress_files or (current_time - in_progress_files[file_name]).seconds >= IN_PROGRESS_CHECK_INTERVAL:
                expected_file_details = next((exp for exp in expected_files if exp['fileName'].replace('<dateToken>', current_time.strftime('%Y%m%d')) == file_name), None)
                if expected_file_details:
                    category = expected_file_details.get('category', 'General')
                    send_event_to_new_relic(expected_file_details['client'], "In Progress", file_name, category, expected_time, "Total In Progress Files")
                    in_progress_files[file_name] = current_time
    # Check for files moved to archive or error
    for file_name in list(received_files):
        if file_name in archive_files:
            expected_file_details = next((exp for exp in expected_files if exp['fileName'].replace('<dateToken>', current_time.strftime('%Y%m%d')) == file_name), None)
            if expected_file_details:
                category = expected_file_details.get('category', 'General')
                send_event_to_new_relic(expected_file_details['client'], "Completely Parsed", file_name, category, expected_time, "Total Parsed Files")
            received_files.remove(file_name)
            found_files.discard(file_name)
        elif file_name in error_files:
            expected_file_details = next((exp for exp in expected_files if exp['fileName'].replace('<dateToken>', current_time.strftime('%Y%m%d')) == file_name), None)
            if expected_file_details:
                category = expected_file_details.get('category', 'General')
                send_event_to_new_relic(expected_file_details['client'], "Error while Parsing", file_name, category, expected_time, "Total Files Error")
            received_files.remove(file_name)
            found_files.discard(file_name)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    monitor_s3_folder()