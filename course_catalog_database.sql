CREATE TABLE courses (
    id INT PRIMARY KEY,
    title TEXT,
    description TEXT,
    url TEXT,
    estimated_content_length INT,
    num_lectures INT,
    num_videos INT,
    mobile_native_deeplink TEXT,
    is_practice_test_course BOOLEAN,
    num_quizzes INT,
    num_practice_tests INT,
    has_closed_caption BOOLEAN,
    last_update_date DATE,
    xapi_activity_id TEXT,
    is_custom BOOLEAN,
    is_imported BOOLEAN,
    headline TEXT,
    level TEXT,
    locale TEXT
);

CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    course_id INT,
    category_name TEXT
);

CREATE TABLE topics (
    id SERIAL PRIMARY KEY,
    course_id INT,
    topic_id INT,
    title TEXT,
    url TEXT
);

CREATE TABLE promo_videos (
    id SERIAL PRIMARY KEY,
    course_id INT,
    type TEXT,
    label TEXT,
    file TEXT
);

CREATE TABLE instructors (
    id SERIAL PRIMARY KEY,
    course_id INT,
    instructor_name TEXT
);

CREATE TABLE requirements (
    id SERIAL PRIMARY KEY,
    course_id INT,
    requirement TEXT
);

CREATE TABLE what_you_will_learn (
    id SERIAL PRIMARY KEY,
    course_id INT,
    item TEXT
);

CREATE TABLE images (
    id SERIAL PRIMARY KEY,
    course_id INT,
    size TEXT,
    url TEXT
);

CREATE TABLE caption_languages (
    id SERIAL PRIMARY KEY,
    course_id INT,
    language TEXT
);

CREATE TABLE caption_locales (
    id SERIAL PRIMARY KEY,
    course_id INT,
    locale TEXT,
    title TEXT,
    english_title TEXT
);

CREATE TABLE categories (
    id INT PRIMARY KEY,
    title TEXT,
    url TEXT
);

CREATE TABLE course_categories (
    id SERIAL PRIMARY KEY,
    course_id INT,
    category_id INT
);

CREATE TABLE subcategories (
    id INT PRIMARY KEY,
    title TEXT,
    url TEXT
);

CREATE TABLE course_subcategories (
    id SERIAL PRIMARY KEY,
    course_id INT,
    subcategory_id INT
);

