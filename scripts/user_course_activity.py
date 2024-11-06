import os
import time
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
import logging

# Set up logging to include the time, log level, and message in each log entry
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def load_environment_variables():
    # Load environment variables from a .env file located in the parent directory
    # If loading fails, log a warning and terminate the program
    if not load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env')):
        logging.warning("Environment variables could not be loaded.")
        exit(1)  # Exit with a non-zero status to indicate an error


def get_db_config():
    # Retrieve database configuration from environment variables
    # Return a dictionary with database connection parameters
    return {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }


def get_api_credentials():
    # Retrieve API credentials from environment variables
    # Return a dictionary with API client key and secret
    return {
        'client_key': os.getenv('CLIENT_KEY'),
        'client_secret': os.getenv('CLIENT_SECRET')
    }


def get_account_details():
    # Retrieve account details from environment variables
    # Return a dictionary with account name and ID
    return {
        'name': os.getenv('ACCOUNT_NAME'),
        'id': os.getenv('ACCOUNT_ID')
    }


def initialize_db_connection(db_config):
    # Establish a connection to the database using the provided configuration
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        return conn, cur
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        exit(1)  # Exit with a non-zero status to indicate an error


def force_sleep(sleep_timer):
    # Log a message indicating a pause in execution and sleep for the specified duration
    logging.info(
        f"Taking a break for {sleep_timer/60} minutes... [Current time: {time.ctime()}]")
    time.sleep(sleep_timer)


def insert_course_data(cursor, course):
    # Insert or update course data in the database
    cursor.execute('''
        INSERT INTO user_course_data (
            user_id, user_name, user_surname, user_email, user_role, user_external_id,
            course_id, course_title, course_category, course_duration, completion_ratio,
            num_video_consumed_minutes, course_enroll_date, course_start_date,
            course_completion_date, course_first_completion_date, course_last_accessed_date,
            last_activity_date, is_assigned, assigned_by, user_is_deactivated, lms_user_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id, course_id)
        DO UPDATE SET
            user_name = EXCLUDED.user_name,
            user_surname = EXCLUDED.user_surname,
            user_email = EXCLUDED.user_email,
            user_role = EXCLUDED.user_role,
            user_external_id = EXCLUDED.user_external_id,
            course_title = EXCLUDED.course_title,
            course_category = EXCLUDED.course_category,
            course_duration = EXCLUDED.course_duration,
            completion_ratio = EXCLUDED.completion_ratio,
            num_video_consumed_minutes = EXCLUDED.num_video_consumed_minutes,
            course_enroll_date = EXCLUDED.course_enroll_date,
            course_start_date = EXCLUDED.course_start_date,
            course_completion_date = EXCLUDED.course_completion_date,
            course_first_completion_date = EXCLUDED.course_first_completion_date,
            course_last_accessed_date = EXCLUDED.course_last_accessed_date,
            last_activity_date = EXCLUDED.last_activity_date,
            is_assigned = EXCLUDED.is_assigned,
            assigned_by = EXCLUDED.assigned_by,
            user_is_deactivated = EXCLUDED.user_is_deactivated,
            lms_user_id = EXCLUDED.lms_user_id;
    ''', (
        course.get('user_id'),
        course.get('user_name'),
        course.get('user_surname'),
        course.get('user_email'),
        course.get('user_role'),
        course.get('user_external_id'),
        course.get('course_id'),
        course.get('course_title'),
        course.get('course_category'),
        course.get('course_duration'),
        course.get('completion_ratio'),
        course.get('num_video_consumed_minutes'),
        parse_timestamp(course.get('course_enroll_date')),
        parse_timestamp(course.get('course_start_date')),
        parse_timestamp(course.get('course_completion_date')),
        parse_timestamp(course.get('course_first_completion_date')),
        parse_timestamp(course.get('course_last_accessed_date')),
        course.get('last_activity_date'),
        course.get('is_assigned'),
        course.get('assigned_by'),
        course.get('user_is_deactivated'),
        course.get('lms_user_id')
    ))


def fetch_and_store_data(cur, conn, api_url, api_credentials):
    # Fetch data from the API and store it in the database
    total_inserted = 0
    while api_url:
        response = requests.get(api_url, auth=HTTPBasicAuth(
            api_credentials['client_key'], api_credentials['client_secret']))
        try:
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.JSONDecodeError as e:
            logging.error(f"JSONDecodeError: {e}")
            force_sleep(1800)
            continue
        except requests.exceptions.HTTPError as e:
            handle_http_error(response, e)
            continue

        api_url = data.get('next')  # Get the URL for the next page of results
        logging.info(f"Next page link: {api_url}")

        for course in data['results']:
            insert_course_data(cur, course)
            total_inserted += 1

        conn.commit()  # Commit the transaction to the database
        logging.info(f"Current total records inserted: {total_inserted}")

        # Implement sleep logic based on the number of records inserted
        if total_inserted % 10000 == 0:
            force_sleep(1800)
        elif total_inserted % 1000 == 0:
            force_sleep(300)

    logging.info(f"Total records inserted: {total_inserted}")


def handle_http_error(response, error):
    # Handle specific HTTP errors by logging and sleeping for a specified duration
    if response.status_code == 503:
        logging.warning("HTTPError 503: Service Unavailable")
        force_sleep(3600)
    elif response.status_code == 524:
        logging.warning("HTTPError 524: A timeout occurred")
        force_sleep(1800)
    elif response.status_code == 429:
        logging.warning("HTTPError 429: Rate limit exceeded")
        force_sleep(1800)
    else:
        logging.error(f"HTTPError: {error}")
        raise error  # Raise the error for unhandled status codes


def parse_timestamp(timestamp_str):
    # Convert a timestamp string to a datetime object, handling UTC format
    if timestamp_str:
        return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    return None


def main():
    # Main function to load environment variables, initialize DB connection, and fetch data
    load_environment_variables()  # Load environment variables from the .env file
    db_config = get_db_config()  # Get the database configuration
    api_credentials = get_api_credentials()  # Get the API credentials
    account_details = get_account_details()  # Get the account details

    # Initialize the database connection and fetch data
    with initialize_db_connection(db_config) as (conn, cur):
        # Construct the initial API URL using account details
        initial_url = f"https://{account_details['name']}.udemy.com/api-2.0/organizations/{account_details['id']}/analytics/user-course-activity/"
        # Fetch and store data from the API
        fetch_and_store_data(cur, conn, initial_url, api_credentials)


if __name__ == "__main__":
    main()  # Execute the main function if the script is run directly
