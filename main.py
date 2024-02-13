import logging
import time
from db_funcs import *


def run(json_file_path: str, db_file_path: str, check_available_tickers: bool = False) -> None:
    """
    Run the main processing flow.

    Args:
        json_file_path (str): Path to the JSON file.
        db_file_path (str): Path to the SQLite database file.
        check_available_tickers (bool): Filter a list of stock tickers to include only those available on Yahoo Finance. Default value: False

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

            # Filter a list of stock tickers to include only those available on Yahoo Finance.
            if check_available_tickers:
                stocks_tickers_intervals_available_list = filter_available_tickers(
                    stocks_tickers_intervals_list)
            else:
                stocks_tickers_intervals_available_list = stocks_tickers_intervals_list

            # Create or connect to the SQLite database
            create_or_connect_to_database(db_file_path)

            # Create stock tables in the database based on intervals
            tables_list = [table.replace(
                "-", "$") for table in stocks_tickers_intervals_available_list]
            create_stock_tables(
                db_file_path, tables=tables_list)

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
    # json_file_path = './stocks_intervals_new.json'
    # json_file_path = './stocks_intervals_sp500.json'
    # json_file_path = './stocks_intervals_to_explore.json'
    # Path to the SQLite database file
    db_file_path = './yahoo_finance_stocks_1.db'
    # db_file_path = './yahoo_finance_stocks_new.db'
    # db_file_path = './yahoo_finance_stocks_sp500.db'
    # db_file_path = './yahoo_finance_stocks_to_explore.db'  # Path to the SQLite database file

    # Run the main function with specified file paths
    run(json_file_path, db_file_path, False)
