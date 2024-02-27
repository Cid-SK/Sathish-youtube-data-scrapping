# YouTube Data Harvesting and Warehousing

This project is a web application built with Streamlit that collects and stores data from multiple YouTube channels. It includes functionality for retrieving channel details, video details, and comment details, as well as querying and displaying this data.

## Features

- Collects and stores channel details, video details, and comment details in MongoDB.
- Creates tables in MySQL and inserts data into these tables.
- Provides options to query and display data from MySQL tables.

## Getting Started

To run the application locally, follow these steps:

1. Install the required Python packages listed in `requirements.txt`.
2. Set up your Google API key for accessing the YouTube Data API.
3. Configure your MongoDB and MySQL databases.
4. Run the Streamlit application using `streamlit run main.py`.

## Usage

- Use the "Get Data" section to collect and store data from a YouTube channel.
- Use the "Channel Details" section to view channel, video, and comment information.
- Use the "Query" section to perform SQL queries on the stored data.

## Dependencies

- Python 3.2
- Streamlit
- pymongo
- mysql-connector-python
- google-api-python-client
- pandas


