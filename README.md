# Yahoo Finance Stock Database & Updater

A Python project for updating stock data using Yahoo Finance API and storing it in an SQLite database.

## Overview

This project is designed to fetch stock data from Yahoo Finance using stock tickers and intervals specified in a JSON file. The retrieved data is then stored in an SQLite database for further analysis and processing.

## Project Structure

The project consists of the following files:

1. **main.py**: The main script that orchestrates the entire processing flow.
2. **db_funcs.py**: Module containing functions for database operations.

## Prerequisites

Make sure you have the following installed:

- Python 3.x
- Required Python packages (install using `pip install -r requirements.txt`)

## How to Run

1. Clone the repository to your local machine:

   ```git clone https://github.com/roruizf/yahoo_finance-stock-db.git```

2. Navigate to the project directory:
   ```cd yahoo_finance_stock_db```
3. Modify the `stocks_intervals.json` file to include the desired stock tickers and intervals.
4. Run the project using the following command:
   ```python3 main.py```

## Configuration
- `stocks_intervals.json`: JSON file containing stock data configurations.
- `yahoo_finance_stocks.db`: SQLite database file where stock data is stored.

## Logging
The project uses logging to capture information and errors during execution. Log messages are configured to be displayed at the INFO level.

## Retry Mechanism
The main processing flow includes a retry mechanism to handle temporary failures, such as network issues or API restrictions. The script will attempt to fetch and update stock data with a specified number of retries and intervals between attempts.

## Note
- Ensure that the requirements.txt file is used to install the required Python packages.
- Feel free to customize the project to suit your specific needs or integrate it into a larger financial data processing pipeline.