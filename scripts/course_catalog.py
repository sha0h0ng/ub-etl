import os
import time
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth
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


def get_or_insert_category(title, url):
    cur.execute("SELECT id FROM categories WHERE title = %s", (title,))
    category = cur.fetchone()
    if category is None:
        cur.execute(
            "INSERT INTO categories (title, url) VALUES (%s, %s) RETURNING id", (title, url))
        category_id = cur.fetchone()[0]
        conn.commit()
        return category_id
    return category[0]


def get_or_insert_subcategory(title, url):
    cur.execute("SELECT id FROM subcategories WHERE title = %s", (title,))
    subcategory = cur.fetchone()
    if subcategory is None:
        cur.execute(
            "INSERT INTO subcategories (title, url) VALUES (%s, %s) RETURNING id", (title, url))
        subcategory_id = cur.fetchone()[0]
        conn.commit()
        return subcategory_id
    return subcategory[0]


def insert_course_data(course):
    # Insert into courses table
    cur.execute("""
        INSERT INTO courses (id, title, description, url, estimated_content_length, num_lectures, num_videos,
                             mobile_native_deeplink, is_practice_test_course, num_quizzes, num_practice_tests,
                             has_closed_caption, last_update_date, xapi_activity_id, is_custom, is_imported, headline, level, locale)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, (
        course['id'], course['title'], course['description'], course['url'], course['estimated_content_length'],
        course['num_lectures'], course['num_videos'], course.get(
            'mobile_native_deeplink'), course['is_practice_test_course'],
        course['num_quizzes'], course['num_practice_tests'], course['has_closed_caption'],
        course.get(
            'last_update_date'), course['xapi_activity_id'], course['is_custom'], course['is_imported'],
        course['headline'], course['level'], course['locale']['locale']
    ))

    # Insert primary category
    category_id = get_or_insert_category(
        course['primary_category']['title'], course['primary_category']['url'])
    cur.execute("""
        INSERT INTO course_categories (course_id, category_id) VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """, (course['id'], category_id))

    # Insert primary subcategory
    subcategory_id = get_or_insert_subcategory(
        course['primary_subcategory']['title'], course['primary_subcategory']['url'])
    cur.execute("""
        INSERT INTO course_subcategories (course_id, subcategory_id) VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """, (course['id'], subcategory_id))

    # Insert topics
    for topic in course['topics']:
        cur.execute("""
            INSERT INTO topics (course_id, topic_id, title, url) VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (course['id'], topic['id'], topic['title'], topic['url']))

    # Insert promo videos
    for video in course['promo_video_url']:
        cur.execute("""
            INSERT INTO promo_videos (course_id, type, label, file) VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (course['id'], video['type'], video['label'], video['file']))

    # Insert instructors
    for instructor in course['instructors']:
        cur.execute("""
            INSERT INTO instructors (course_id, instructor_name) VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (course['id'], instructor))

    # Insert requirements
    if 'requirements' in course and 'list' in course['requirements'] and isinstance(course['requirements']['list'], list):
        for requirement in course['requirements']['list']:
            cur.execute("""
                INSERT INTO requirements (course_id, requirement) VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (course['id'], requirement))

    # Insert what you will learn
    for item in course['what_you_will_learn']['list']:
        cur.execute("""
            INSERT INTO what_you_will_learn (course_id, item) VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (course['id'], item))

    # Insert images
    for size, url in course['images'].items():
        cur.execute("""
            INSERT INTO images (course_id, size, url) VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (course['id'], size, url))

    # Insert caption languages
    for language in course['caption_languages']:
        cur.execute("""
            INSERT INTO caption_languages (course_id, language) VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (course['id'], language))

    # Insert caption locales
    for locale in course['caption_locales']:
        cur.execute("""
            INSERT INTO caption_locales (course_id, locale, title, english_title) VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (course['id'], locale['locale'], locale['title'], locale['english_title']))


def force_sleep(sleep_timer=300):
    print(
        f"Taking a break for {sleep_timer/60} minutes... [Current time: ", time.ctime(), "]")
    time.sleep(sleep_timer)


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
            if response.status_code == 524:
                print("HTTPError 524: A timeout occurred")
                force_sleep(LONG_SLEEP_TIMER)
                continue  # Retry after sleeping
            else:
                print(f"HTTPError: {e}")
                break  # Exit the loop for other HTTP errors

        # Check for next page
        api_url = data.get('next')
        print(f"Next page link: {api_url}")

        for course in data['results']:
            insert_course_data(course)
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


# Start with the initial URL
initial_url = f"https://{ACCOUNT_NAME}.udemy.com/api-2.0/organizations/{ACCOUNT_ID}/courses/list/?page_size={PAGE_SIZE}&page={PAGE_NUMBER}"
fetch_and_store_data(initial_url)

# Close the database connection
cur.close()
conn.close()
