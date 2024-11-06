import os
import time
import logging
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
from psycopg2 import sql

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
        exit(1)  # Exit the program with a non-zero status to indicate an error


def get_or_insert(cur, conn, table, title, url):
    try:
        cur.execute(sql.SQL("SELECT id FROM {} WHERE title = %s").format(
            sql.Identifier(table)), (title,))
        record = cur.fetchone()
        if record is None:
            cur.execute(
                sql.SQL("INSERT INTO {} (title, url) VALUES (%s, %s) RETURNING id").format(
                    sql.Identifier(table)),
                (title, url))
            record_id = cur.fetchone()[0]
            conn.commit()
            return record_id
        return record[0]
    except psycopg2.Error as e:
        logging.error(f"Error in get_or_insert: {e}")
        conn.rollback()
        raise


def insert_course_data(cur, conn, course):
    try:
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

        category_id = get_or_insert(
            cur, conn, 'categories', course['primary_category']['title'], course['primary_category']['url'])
        cur.execute("INSERT INTO course_categories (course_id, category_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (course['id'], category_id))

        subcategory_id = get_or_insert(
            cur, conn, 'subcategories', course['primary_subcategory']['title'], course['primary_subcategory']['url'])
        cur.execute("INSERT INTO course_subcategories (course_id, subcategory_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (course['id'], subcategory_id))

        for topic in course['topics']:
            cur.execute("INSERT INTO topics (course_id, topic_id, title, url) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], topic['id'], topic['title'], topic['url']))

        for video in course['promo_video_url']:
            cur.execute("INSERT INTO promo_videos (course_id, type, label, file) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], video['type'], video['label'], video['file']))

        for instructor in course['instructors']:
            cur.execute("INSERT INTO instructors (course_id, instructor_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], instructor))

        if 'requirements' in course and isinstance(course['requirements'].get('list'), list):
            for requirement in course['requirements']['list']:
                cur.execute("INSERT INTO requirements (course_id, requirement) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (course['id'], requirement))

        for item in course['what_you_will_learn']['list']:
            cur.execute("INSERT INTO what_you_will_learn (course_id, item) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], item))

        for size, url in course['images'].items():
            cur.execute("INSERT INTO images (course_id, size, url) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], size, url))

        for language in course['caption_languages']:
            cur.execute("INSERT INTO caption_languages (course_id, language) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], language))

        for locale in course['caption_locales']:
            cur.execute("INSERT INTO caption_locales (course_id, locale, title, english_title) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        (course['id'], locale['locale'], locale['title'], locale['english_title']))

    except psycopg2.Error as e:
        logging.error(f"Error in insert_course_data: {e}")
        conn.rollback()
        raise


def force_sleep(sleep_timer=300):
    logging.info(
        f"Taking a break for {sleep_timer/60} minutes... [Current time: {time.ctime()}]")
    time.sleep(sleep_timer)


def fetch_and_store_data(cur, conn, api_url, api_credentials):
    total_inserted = 0
    while api_url:
        try:
            response = requests.get(api_url, auth=HTTPBasicAuth(
                api_credentials['client_key'], api_credentials['client_secret']))
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.JSONDecodeError as e:
            logging.error(f"JSONDecodeError: {e}")
            force_sleep(1800)
            continue
        except requests.exceptions.HTTPError as e:
            if response.status_code == 524:
                logging.warning("HTTPError 524: A timeout occurred")
                force_sleep(1800)
                continue  # Retry after sleeping
            else:
                logging.error(f"HTTPError: {e}")
                break  # Exit the loop for other HTTP errors

        # Check for next page
        api_url = data.get('next')
        logging.info(f"Next page link: {api_url}")

        for course in data['results']:
            insert_course_data(cur, conn, course)
            total_inserted += 1

        conn.commit()
        logging.info(f"Current total records inserted: {total_inserted}")

        # Sleep logic based on the number of records inserted
        if total_inserted % 10000 == 0:
            force_sleep(1800)
        elif total_inserted % 1000 == 0:
            force_sleep(300)

    logging.info(f"Total records inserted: {total_inserted}")


def main():
    load_environment_variables()
    db_config = get_db_config()
    api_credentials = get_api_credentials()
    account_details = get_account_details()

    with initialize_db_connection(db_config) as (conn, cur):
        initial_url = f"https://{account_details['name']}.udemy.com/api-2.0/organizations/{account_details['id']}/courses/list/?page_size=20&page=1"
        fetch_and_store_data(cur, conn, initial_url, api_credentials)


if __name__ == "__main__":
    main()
