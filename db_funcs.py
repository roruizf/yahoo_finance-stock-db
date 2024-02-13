import dateutil.parser
from datetime import datetime, timedelta
import yfinance as yf
from typing import List, Dict, Union
import json
import os
import sqlite3
import pandas as pd
import re


def get_stocks_tickers_and_intervals(json_file_path: str) -> List[str]:
    """
    Generate combinations of stock tickers and intervals from a JSON file.

    Parameters:
    - json_file_path (str): The path to the JSON file containing stock data.

    Returns:
    - List[str]: A list of combinations in the format ["TICKER_INTERVAL", ...].
    """

    try:
        # Try to open and read the JSON file
        with open(json_file_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        # If the file is not found, return an empty list
        return []

    # Initialize an empty list to store combinations
    result = []

    # Iterate through each entry in the JSON data
    for entry in data:
        # Extract intervals and stocks from the entry
        intervals = entry.get("intervals", [])
        stocks = entry.get("stocks", [])

        # Generate combinations of stock tickers and intervals
        for stock in stocks:
            for interval in intervals:
                result.append(f"{stock}_{interval}")

    # Return the list of combinations
    return result


def is_ticker_available(ticker: str) -> bool:
    """
    Verify if a stock ticker is available on Yahoo Finance.

    Parameters:
    - ticker (str): Stock ticker symbol.

    Returns:
    - bool: True if the ticker is available, False otherwise.
    """
    try:
        # Create a Ticker object
        stock = yf.Ticker(ticker)

        # Download minimal data (1 month) to quickly check if the ticker is available
        data = stock.history(period='1mo')

        # Check if data is not empty
        return not data.empty
    except Exception as e:
        # Handle exceptions, print the error, and return False
        print(f"Error: {e}")
        return False


def filter_available_tickers(ticker_intervals: List[str]) -> List[str]:
    """
    Filter a list of stock ticker intervals to include only those available on Yahoo Finance.

    Parameters:
    - ticker_intervals (List[str]): List of stock ticker intervals (e.g., 'AAPL_1h').

    Returns:
    - List[str]: Filtered list of ticker intervals that are available on Yahoo Finance.
    """

    tickers = [item.split('_')[0] for item in ticker_intervals]

    # Removes all items from a list that contain any character that is not a letter or digit.
    tickers = [item for item in tickers if re.match(r'^[a-zA-Z0-9]+$', item)]

    # Removes all items from a list that start with a number
    tickers = [item for item in tickers if not item[0].isdigit()]

    # Inline the get_ticker_from_interval logic in the list comprehension
    available_ticker_intervals = [
        ticker for ticker in tickers if is_ticker_available(ticker)]
    return available_ticker_intervals


def create_or_connect_to_database(db_file_path: str) -> None:
    """
    Check if the SQLite database file exists, and create it if it doesn't.

    Parameters:
    - db_file_path (str): The path to the SQLite database file.
    """

    # Check if the database file exists
    if not os.path.exists(db_file_path):
        # If the file doesn't exist, create it
        # Create a connection to the database
        conn = sqlite3.connect(db_file_path)
        conn.close()  # Close the connection
        print(f"Database file '{db_file_path}' created successfully.")
    else:
        # If the file already exists, print a message
        print(f"Database file '{db_file_path}' already exists.")


def create_stock_tables(db_file_path: str, tables: list) -> None:
    """
    Check if specific tables exist in the SQLite database file and create them if needed.

    Parameters:
    - db_file_path (str): The path to the SQLite database file.
    - tables (list): A list of table names to check and create if needed.
    """

    # Dictionary to map intervals to column definitions
    interval_columns = {
        '1m': 'Datetime TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER',
        '2m': 'Datetime TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER',
        '5m': 'Datetime TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER',
        '15m': 'Datetime TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER',
        '30m': 'Datetime TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER',
        '60m': 'Datetime TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER',
        '90m': 'Datetime TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER',
        '1h': 'Datetime TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER',
        '1d': 'Date TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER',
        '5d': 'Date TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER',
        '1wk': 'Date TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER',
        '1mo': 'Date TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER',
        '3mo': 'Date TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, "Adj Close" REAL, Volume INTEGER'
    }

    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()

        # Iterate through each specified table
        for table in tables:
            # Check if the table exists
            cursor.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
            existing_table = cursor.fetchone()

            if existing_table is None:
                # Table does not exist, create it
                if table.endswith("_1d") or table.endswith("_5d") or table.endswith("_1wk") or table.endswith("_1mo") or table.endswith("_3mo"):
                    interval = table.split("_")[1]
                    if interval in interval_columns:
                        column_definition = interval_columns[interval]
                        query = f"CREATE TABLE {table} ({column_definition})"
                        cursor.execute(query)
                        print(f"Table '{table}' created successfully.")
                    else:
                        print(
                            f"Error: Invalid interval '{interval}' for table '{table}'.")
                elif table.endswith("_1m") or table.endswith("_2m") or table.endswith("_5m") or table.endswith("_15m") or table.endswith("_30m") or table.endswith("_60m") or table.endswith("_90m") or table.endswith("_1h"):
                    interval = table.split("_")[1]
                    if interval in interval_columns:
                        column_definition = interval_columns[interval]
                        query = f"CREATE TABLE {table} ({column_definition})"
                        cursor.execute(query)
                        print(f"Table '{table}' created successfully.")
                    else:
                        print(
                            f"Error: Invalid interval '{interval}' for table '{table}'.")
                else:
                    print(f"Error: Invalid table name '{table}'.")
            else:
                # Table already exists, print a message
                print(f"Table '{table}' already exists.")

    except sqlite3.Error as e:
        # Handle SQLite errors
        print(f"SQLite error: {e}")

    finally:
        # Close the database connection
        if conn:
            conn.close()


def update_stock_data(db_file_path: str) -> None:
    """
    Update stock data in SQLite tables based on intervals of existing tables.

    Parameters:
    - db_file_path (str): The path to the SQLite database file.
    """
    
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()

        # Get existing tables in the database
        existing_tables = [table[0] for table in cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

        # Determine intervals to update based on existing tables
        intervals_to_update = set()
        for table_name in existing_tables:
            _, interval = table_name.split("_")
            intervals_to_update.add(interval)

        # Check for valid intervals
        valid_intervals = {'1m', '2m', '5m', '15m', '30m',
                           '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo'}
        intervals_to_update = list(
            valid_intervals.intersection(intervals_to_update))

        # Proceed with updating data for each existing table
        for table_name in existing_tables:
            if any(table_name.endswith(interval) for interval in intervals_to_update):
                ticker, interval = table_name.split("_")
                ticker = ticker.replace('$', '-')
                column_name = 'Datetime' if interval in [
                    '1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h'] else 'Date'
                
                    # Define the maximum allowed period for each interval
                max_nbr_days_dict = {
                    '1m': 7,
                    '2m': 60,
                    '5m': 60,
                    '15m': 60,
                    '30m': 60,
                    '60m': 730,
                    '90m': 60,
                    '1h': 730,
                    '1d': None,
                    '5d': None,
                    '1wk': None,
                    '1mo': None,
                    '3mo': None
                }

                # Calculate start_date based on current date and maximum allowed period
                max_period = max_nbr_days_dict.get(interval, None)

                # Check if the table is empty
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]

                if count == 0:
                    # Table is empty, download data from '2022-01-01' until now
                    if max_period is not None:
                        start_date = (datetime.now() - timedelta(days=max_period-1)).strftime("%Y-%m-%d")
                    else:
                        start_date = datetime.now() - pd.DateOffset(365 * 10)
                else:
                    # Table is not empty, get the last date or datetime
                    cursor.execute(
                        f"SELECT MAX({column_name}) FROM {table_name}")
                    last_record = cursor.fetchone()[0]

                    if last_record is not None:
                        last_date_or_datetime = dateutil.parser.parse(
                            last_record)
                        last_date_or_datetime = last_date_or_datetime.replace(
                            tzinfo=None)  # Remove timezone info
                        start_date = (last_date_or_datetime + timedelta(days=0)).strftime(
                            "%Y-%m-%d")
                    else:
                        if max_period is not None:
                            start_date = (datetime.now() - timedelta(days=max_period-1)).strftime("%Y-%m-%d")
                        else:
                            start_date = start_date = datetime.now() - pd.DateOffset(365 * 10)

                # Download data from start_date until end_date using Yahoo Finance API
                if start_date is not None:
                    df = yf.download(ticker, interval=interval, start=start_date)
                else:
                    df = yf.download(ticker, interval=interval, end=datetime.now().strftime("%Y-%m-%d"))

                # Format the index based on the column name
                df.index = df.index.strftime(
                    "%Y-%m-%d" if column_name == 'Date' else "%Y-%m-%d %H:%M:%S")

                # Remove the last record from the table
                cursor.execute(
                    f"DELETE FROM {table_name} WHERE ROWID = (SELECT MAX(ROWID) FROM {table_name})")
                conn.commit()

                # Check if the data already exists in the table
                existing_data = pd.read_sql(
                    f"SELECT {column_name} FROM {table_name}", conn)

                existing_dates = set(existing_data[column_name])

                # Filter out existing dates from the new data
                df_to_append = df[~df.index.isin(existing_dates)]

                # Append the new data to the SQLite table, if there is any new data
                if not df_to_append.empty:
                    df_to_append.to_sql(
                        table_name, conn, if_exists='append', index=True, index_label=column_name)

                print(f"Data for table '{table_name}' updated successfully.")

    except sqlite3.Error as e:
        # Handle SQLite errors
        print(f"SQLite error: {e}")

    finally:
        # Close the database connection
        if conn:
            conn.close()

# def update_stock_data(db_file_path: str) -> None:
#     """
#     Update stock data in SQLite tables based on intervals of existing tables.

#     Parameters:
#     - db_file_path (str): The path to the SQLite database file.
#     """

#     try:
#         # Connect to the SQLite database
#         conn = sqlite3.connect(db_file_path)
#         cursor = conn.cursor()

#         # Get existing tables in the database
#         existing_tables = [table[0] for table in cursor.execute(
#             "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

#         # Determine intervals to update based on existing tables
#         intervals_to_update = set()
#         for table_name in existing_tables:
#             _, interval = table_name.split("_")
#             intervals_to_update.add(interval)

#         # Check for valid intervals
#         valid_intervals = {'1m', '2m', '5m', '15m', '30m',
#                            '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo'}
#         intervals_to_update = list(
#             valid_intervals.intersection(intervals_to_update))

#         # Proceed with updating data for each existing table
#         for table_name in existing_tables:
#             if any(table_name.endswith(interval) for interval in intervals_to_update):
#                 ticker, interval = table_name.split("_")
#                 column_name = 'Datetime' if interval in [
#                     '1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h'] else 'Date'

#                 # Check if the table is empty
#                 cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
#                 count = cursor.fetchone()[0]

#                 if count == 0:
#                     # Table is empty, download data from '2022-01-01' until now
#                     start_date = '2022-01-01'
#                 else:
#                     # Table is not empty, get the last date or datetime
#                     cursor.execute(
#                         f"SELECT MAX({column_name}) FROM {table_name}")
#                     last_record = cursor.fetchone()[0]

#                     if last_record is not None:
#                         last_date_or_datetime = dateutil.parser.parse(
#                             last_record)
#                         last_date_or_datetime = last_date_or_datetime.replace(
#                             tzinfo=None)  # Remove timezone info
#                         start_date = (last_date_or_datetime + timedelta(days=0)).strftime(
#                             "%Y-%m-%d")
#                     else:
#                         start_date = '2022-01-01'

#                 # Download data from start_date until now using Yahoo Finance API
#                 df = yf.download(ticker, interval=interval, start=start_date)

#                 # Format the index based on the column name
#                 df.index = df.index.strftime(
#                     "%Y-%m-%d" if column_name == 'Date' else "%Y-%m-%d %H:%M:%S")

#                 # Remove the last record from the table
#                 cursor.execute(
#                     f"DELETE FROM {table_name} WHERE ROWID = (SELECT MAX(ROWID) FROM {table_name})")
#                 conn.commit()

#                 # Check if the data already exists in the table
#                 existing_data = pd.read_sql(
#                     f"SELECT {column_name} FROM {table_name}", conn)

#                 existing_dates = set(existing_data[column_name])

#                 # Filter out existing dates from the new data
#                 df_to_append = df[~df.index.isin(existing_dates)]

#                 # Append the new data to the SQLite table, if there is any new data
#                 if not df_to_append.empty:
#                     df_to_append.to_sql(
#                         table_name, conn, if_exists='append', index=True, index_label=column_name)

#                 print(f"Data for table '{table_name}' updated successfully.")

#     except sqlite3.Error as e:
#         # Handle SQLite errors
#         print(f"SQLite error: {e}")

#     finally:
#         # Close the database connection
#         if conn:
#             conn.close()
