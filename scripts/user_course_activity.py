import os
from dotenv import load_dotenv
import time
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import psycopg2

# Construct the path to the .env file relative to the current file
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')

# Load the .env file
load_dotenv(dotenv_path=env_path)

# Database connection details from .env
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# API credentials from .env
CLIENT_KEY = os.getenv('CLIENT_KEY')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

PAGE_SIZE = 20  # max 20
PAGE_NUMBER = 1

ACCOUNT_NAME = os.getenv('ACCOUNT_NAME')
ACCOUNT_ID = os.getenv('ACCOUNT_ID')

SHORT_SLEEP_TIMER = 300  # 5 minutes
LONG_SLEEP_TIMER = 1800  # 30 minutes

# Initialize database connection
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
cur = conn.cursor()


def force_sleep(sleep_timer):
    print(
        f"Taking a break for {sleep_timer/60} minutes... [Current time: ", time.ctime(), "]")
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


def fetch_and_store_data(api_url):
    total_inserted = 0

    while api_url:
        response = requests.get(
            api_url, auth=HTTPBasicAuth(CLIENT_KEY, CLIENT_SECRET))

        try:
            response.raise_for_status()
            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError as e:
                print(f"JSONDecodeError: {e}")
                force_sleep(LONG_SLEEP_TIMER)
                continue  # Skip the current iteration and retry with the next iteration of the loop
        except requests.exceptions.HTTPError as e:
            if response.status_code == 503:
                print("HTTPError 503: Service Unavailable")
                force_sleep(LONG_SLEEP_TIMER * 2)
                continue
            elif response.status_code == 524:
                print("HTTPError 524: A timeout occurred")
                force_sleep(LONG_SLEEP_TIMER)
                continue  # Retry after sleeping
            elif response.status_code == 429:
                print("HTTPError 429: Rate limit exceeded")
                force_sleep(LONG_SLEEP_TIMER)
                continue
            else:
                print(f"HTTPError: {e}")
                break  # Exit the loop for other HTTP errors

        # Check for next page
        api_url = data.get('next')
        print(f"Next page link: {api_url}")

        for course in data['results']:
            insert_course_data(cur, course)
            total_inserted += 1

        # Commit after inserting the batch
        conn.commit()

        print(f"Current total records inserted: {total_inserted}")

        # Sleep logic based on the number of records inserted
        if total_inserted % 10000 == 0:
            force_sleep(LONG_SLEEP_TIMER)
        elif total_inserted % 1000 == 0:
            force_sleep(SHORT_SLEEP_TIMER)

    print(f"Total records inserted: {total_inserted}")


def parse_timestamp(timestamp_str):
    if timestamp_str:
        return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    return None


# Start with the initial URL
initial_url = f"https://{ACCOUNT_NAME}.udemy.com/api-2.0/organizations/{ACCOUNT_ID}/analytics/user-course-activity/"
fetch_and_store_data(initial_url)

# Close the database connection
cur.close()
conn.close()
