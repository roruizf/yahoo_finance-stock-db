import logging
import time
from db_funcs import *


def run(json_file_path: str, db_file_path: str) -> None:
    """
    Run the main processing flow.

    Args:
        json_file_path (str): Path to the JSON file.
        db_file_path (str): Path to the SQLite database file.

    Returns:
        None
    """
    # Set the maximum number of retry attempts
    max_retries = 15

    # Set the interval (in seconds) between retry attempts
    retry_interval = 15

    # Loop through retry attempts
    for attempt in range(1, max_retries + 1):
        try:
            # Fetch stock tickers and intervals from the JSON file
            stocks_tickers_intervals_list = get_stocks_tickers_and_intervals(
                json_file_path)

            # Create or connect to the SQLite database
            create_or_connect_to_database(db_file_path)

            # Create stock tables in the database based on intervals
            create_stock_tables(
                db_file_path, tables=stocks_tickers_intervals_list)

            # Update stock data in the database
            update_stock_data(db_file_path)

            # Successful execution, exit the loop
            break
        except Exception as e:
            # Log an error message for the current attempt
            logging.error(
                f"Attempt {attempt}/{max_retries} failed. Error: {str(e)}")

            # If it's not the last attempt, log a message and wait before retrying
            if attempt < max_retries:
                logging.info(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                # Max retries reached, log an error and exit the loop
                logging.error("Max retries reached. Exiting.")
                break


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(level=logging.INFO)  # Set the logging level to INFO

    # Define file paths
    # Path to the JSON file containing stock data
    json_file_path = './stocks_intervals.json'
    db_file_path = './yahoo_finance_stocks.db'  # Path to the SQLite database file

    # Run the main function with specified file paths
    run(json_file_path, db_file_path)
