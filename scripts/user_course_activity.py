import os
import time
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def load_environment_variables():
    if not load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env')):
        logging.warning("Environment variables could not be loaded.")
        exit(1)  # Exit the program with a non-zero status to indicate an error


def get_db_config():
    return {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }


def get_api_credentials():
    return {
        'client_key': os.getenv('CLIENT_KEY'),
        'client_secret': os.getenv('CLIENT_SECRET')
    }


def get_account_details():
    return {
        'name': os.getenv('ACCOUNT_NAME'),
        'id': os.getenv('ACCOUNT_ID')
    }


def initialize_db_connection(db_config):
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        return conn, cur
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        exit(1)


def force_sleep(sleep_timer):
    logging.info(
        f"Taking a break for {sleep_timer/60} minutes... [Current time: {time.ctime()}]")
    time.sleep(sleep_timer)


def insert_course_data(cursor, course):
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

        api_url = data.get('next')
        logging.info(f"Next page link: {api_url}")

        for course in data['results']:
            insert_course_data(cur, course)
            total_inserted += 1

        conn.commit()
        logging.info(f"Current total records inserted: {total_inserted}")

        if total_inserted % 10000 == 0:
            force_sleep(1800)
        elif total_inserted % 1000 == 0:
            force_sleep(300)

    logging.info(f"Total records inserted: {total_inserted}")


def handle_http_error(response, error):
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
        raise error


def parse_timestamp(timestamp_str):
    if timestamp_str:
        return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    return None


def main():
    load_environment_variables()
    db_config = get_db_config()
    api_credentials = get_api_credentials()
    account_details = get_account_details()

    with initialize_db_connection(db_config) as (conn, cur):
        initial_url = f"https://{account_details['name']}.udemy.com/api-2.0/organizations/{account_details['id']}/analytics/user-course-activity/"
        fetch_and_store_data(cur, conn, initial_url, api_credentials)


if __name__ == "__main__":
    main()
