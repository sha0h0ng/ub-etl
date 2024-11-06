import os
import time
import logging
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
from psycopg2 import sql

# Configure logging to display messages with time, level, and message
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def load_environment_variables():
    # Load environment variables from a .env file
    # If the .env file cannot be loaded, log a warning and exit the program
    if not load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env')):
        logging.warning("Environment variables could not be loaded.")
        exit(1)  # Exit the program with a non-zero status to indicate an error


def get_db_config():
    # Retrieve database configuration from environment variables
    # Return a dictionary containing database connection parameters
    return {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }


def get_api_credentials():
    # Retrieve API credentials from environment variables
    # Return a dictionary containing API client key and secret
    return {
        'client_key': os.getenv('CLIENT_KEY'),
        'client_secret': os.getenv('CLIENT_SECRET')
    }


def get_account_details():
    # Retrieve account details from environment variables
    # Return a dictionary containing account name and ID
    return {
        'name': os.getenv('ACCOUNT_NAME'),
        'id': os.getenv('ACCOUNT_ID')
    }


def initialize_db_connection(db_config):
    # Initialize a connection to the database using the provided configuration
    try:
        conn = psycopg2.connect(**db_config)  # Establish the connection
        cur = conn.cursor()  # Create a cursor object to interact with the database
        return conn, cur  # Return the connection and cursor
    except psycopg2.Error as e:
        # Log an error message if the connection fails and exit the program
        logging.error(f"Database connection error: {e}")
        exit(1)  # Exit the program with a non-zero status to indicate an error


def get_or_insert(cur, conn, table, title, url):
    # Get or insert a record in the specified table
    try:
        # Execute a SQL query to check if the record already exists
        cur.execute(sql.SQL("SELECT id FROM {} WHERE title = %s").format(
            sql.Identifier(table)), (title,))
        record = cur.fetchone()  # Fetch the result
        if record is None:
            # If the record does not exist, insert it into the table
            cur.execute(
                sql.SQL("INSERT INTO {} (title, url) VALUES (%s, %s) RETURNING id").format(
                    sql.Identifier(table)),
                (title, url))
            # Get the ID of the newly inserted record
            record_id = cur.fetchone()[0]
            conn.commit()  # Commit the transaction
            return record_id  # Return the ID of the inserted record
        return record[0]  # Return the ID of the existing record
    except psycopg2.Error as e:
        # Log an error message if the operation fails and rollback the transaction
        logging.error(f"Error in get_or_insert: {e}")
        conn.rollback()
        raise  # Re-raise the exception to be handled by the caller


def insert_course_data(cur, conn, course):
    # Insert course data into the database
    try:
        # Insert the main course data into the courses table
        cur.execute("""
            INSERT INTO courses (id, title, description, url, estimated_content_length, num_lectures, num_videos,
                                 mobile_native_deeplink, is_practice_test_course, num_quizzes, num_practice_tests,
                                 has_closed_caption, last_update_date, xapi_activity_id, is_custom, is_imported, headline, level, locale)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (
            course['id'], course['title'], course['description'], course['url'], course['estimated_content_length'],
            course['num_lectures'], course['num_videos'], course.get(
                'mobile_native_deeplink'),
            course['is_practice_test_course'], course['num_quizzes'], course['num_practice_tests'],
            course['has_closed_caption'], course.get(
                'last_update_date'), course['xapi_activity_id'],
            course['is_custom'], course['is_imported'], course['headline'], course['level'], course['locale']['locale']
        ))

        # Insert category data
        category_id = get_or_insert(
            cur, conn, 'categories', course['primary_category']['title'], course['primary_category']['url'])
        cur.execute("INSERT INTO course_categories (course_id, category_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (course['id'], category_id))

        # Insert subcategory data
        subcategory_id = get_or_insert(
            cur, conn, 'subcategories', course['primary_subcategory']['title'], course['primary_subcategory']['url'])
        cur.execute("INSERT INTO course_subcategories (course_id, subcategory_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (course['id'], subcategory_id))

        # Insert topics data
        for topic in course['topics']:
            cur.execute("INSERT INTO topics (course_id, topic_id, title, url) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], topic['id'], topic['title'], topic['url']))

        # Insert promo videos data
        for video in course['promo_video_url']:
            cur.execute("INSERT INTO promo_videos (course_id, type, label, file) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], video['type'], video['label'], video['file']))

        # Insert instructors data
        for instructor in course['instructors']:
            cur.execute("INSERT INTO instructors (course_id, instructor_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], instructor))

        # Insert requirements data if available
        if 'requirements' in course and isinstance(course['requirements'].get('list'), list):
            for requirement in course['requirements']['list']:
                cur.execute("INSERT INTO requirements (course_id, requirement) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (course['id'], requirement))

        # Insert what you will learn data
        for item in course['what_you_will_learn']['list']:
            cur.execute("INSERT INTO what_you_will_learn (course_id, item) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], item))

        # Insert images data
        for size, url in course['images'].items():
            cur.execute("INSERT INTO images (course_id, size, url) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], size, url))

        # Insert caption languages data
        for language in course['caption_languages']:
            cur.execute("INSERT INTO caption_languages (course_id, language) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], language))

        # Insert caption locales data
        for locale in course['caption_locales']:
            cur.execute("INSERT INTO caption_locales (course_id, locale, title, english_title) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], locale['locale'], locale['title'], locale['english_title']))

    except psycopg2.Error as e:
        # Log an error message if the operation fails and rollback the transaction
        logging.error(f"Error in insert_course_data: {e}")
        conn.rollback()
        raise  # Re-raise the exception to be handled by the caller


def force_sleep(sleep_timer=300):
    # Pause execution for a specified amount of time
    # Log a message indicating the duration of the sleep and the current time
    logging.info(
        f"Taking a break for {sleep_timer/60} minutes... [Current time: {time.ctime()}]")
    time.sleep(sleep_timer)  # Sleep for the specified duration


def fetch_and_store_data(cur, conn, api_url, api_credentials):
    # Fetch data from the API and store it in the database
    total_inserted = 0  # Initialize a counter for the total number of records inserted
    while api_url:
        try:
            # Make a GET request to the API using the provided URL and credentials
            response = requests.get(api_url, auth=HTTPBasicAuth(
                api_credentials['client_key'], api_credentials['client_secret']))
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()  # Parse the response JSON data
        except requests.exceptions.JSONDecodeError as e:
            # Log an error message if the JSON decoding fails and sleep before retrying
            logging.error(f"JSONDecodeError: {e}")
            force_sleep(1800)
            continue
        except requests.exceptions.HTTPError as e:
            # Handle specific HTTP errors
            if response.status_code == 524:
                logging.warning("HTTPError 524: A timeout occurred")
                force_sleep(1800)
                continue  # Retry after sleeping
            else:
                logging.error(f"HTTPError: {e}")
                break  # Exit the loop for other HTTP errors

        # Check for next page
        api_url = data.get('next')  # Get the URL for the next page of results
        logging.info(f"Next page link: {api_url}")

        # Insert each course data into the database
        for course in data['results']:
            insert_course_data(cur, conn, course)  # Insert the course data
            total_inserted += 1  # Increment the counter

        conn.commit()  # Commit the transaction
        logging.info(f"Current total records inserted: {total_inserted}")

        # Sleep logic based on the number of records inserted
        if total_inserted % 10000 == 0:
            # Sleep for a longer duration after every 10,000 records
            force_sleep(1800)
        elif total_inserted % 1000 == 0:
            # Sleep for a shorter duration after every 1,000 records
            force_sleep(300)

    # Log the total number of records inserted
    logging.info(f"Total records inserted: {total_inserted}")


def main():
    # Main function to load environment variables, initialize DB connection, and fetch data
    load_environment_variables()  # Load environment variables from the .env file
    db_config = get_db_config()  # Get the database configuration
    api_credentials = get_api_credentials()  # Get the API credentials
    account_details = get_account_details()  # Get the account details

    # Initialize the database connection and fetch data
    with initialize_db_connection(db_config) as (conn, cur):
        # Construct the initial API URL using account details
        initial_url = f"https://{account_details['name']}.udemy.com/api-2.0/organizations/{account_details['id']}/courses/list/?page_size=20&page=1"
        # Fetch and store data from the API
        fetch_and_store_data(cur, conn, initial_url, api_credentials)


if __name__ == "__main__":
    main()  # Execute the main function if the script is run directly
