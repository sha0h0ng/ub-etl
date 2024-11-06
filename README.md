# Udemy Business API ETL 

A Python-based ETL pipeline for fetching and storing Udemy course catalog and user activity data from the Udemy Business API into a PostgreSQL database.

## Overview

This project consists of two main components:
1. Course Catalog Data Pipeline: Fetches detailed course information including categories, topics, instructors, and course metadata
2. User Course Activity Pipeline: Retrieves user engagement and progress data for all courses

## Prerequisites

- Python 3.x
- PostgreSQL database
- Udemy Business API credentials
- Required Python packages:
  - `requests`
  - `psycopg2`
  - `python-dotenv`

## Setup

1. Clone the repository
2. Create a `.env` file in the project root with the following variables:
```
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_HOST=your_database_host
DB_PORT=your_database_port
CLIENT_KEY=your_udemy_api_key
CLIENT_SECRET=your_udemy_api_secret
ACCOUNT_NAME=your_udemy_business_account_name
ACCOUNT_ID=your_udemy_business_account_id
```

3. Set up the database schema by running the SQL scripts:
   - `course_catalog_database.sql`
   - `user_course_activity_database.sql`

## Database Schema

### Course Catalog Tables
- `courses`: Main course information
- `categories`: Course categories
- `topics`: Course topics
- `promo_videos`: Course promotional videos
- `instructors`: Course instructors
- `requirements`: Course prerequisites
- `what_you_will_learn`: Course learning objectives
- `images`: Course images
- `caption_languages`: Available caption languages
- `caption_locales`: Caption locale information
- `course_categories`: Course-category relationships
- `course_subcategories`: Course-subcategory relationships

### User Activity Table
- `user_course_data`: Comprehensive user engagement and progress data

## Scripts

### course_catalog.py
- Fetches detailed course information from Udemy Business API
- Handles pagination and rate limiting
- Implements error handling and retry logic
- Features configurable sleep timers to prevent API throttling

### user_course_activity.py
- Retrieves user activity data for all courses
- Implements upsert logic to handle data updates
- Includes robust error handling for API timeouts and rate limits
- Manages incremental data loading

## Rate Limiting and Error Handling

The scripts implement two types of sleep timers:
- Short sleep (5 minutes) every 1,000 records
- Long sleep (30 minutes) every 10,000 records

Error handling includes:
- HTTP 524 (Timeout)
- HTTP 503 (Service Unavailable)
- HTTP 429 (Rate Limit)
- JSON decode errors

## Usage

1. Run the course catalog pipeline:
```bash
python course_catalog.py
```

2. Run the user activity pipeline:
```bash
python user_course_activity.py
```

## Data Refresh Strategy

The pipelines are designed to:
- Handle incremental updates
- Avoid duplicate entries
- Update existing records with the latest information
- Maintain data consistency through proper transaction management

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Add your chosen license here]