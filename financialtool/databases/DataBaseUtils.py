import sqlite3
import configparser
from pathlib import Path
from sqlite3 import Connection


def get_company_ticker_data():
    print("Fetching company ticker data from database")
    conn = connect_to_db()
    read_query = "Select * from Company_List"
    data = read_data_from_db(conn, read_query)
    company_names = []
    for (company, ticker) in data:
        company_names.append(ticker)
    disconnect_from_db(conn)
    return company_names

def add_new_company (company_data):
    conn = connect_to_db()
    read_query = "Select * from Company_List"
    data = read_data_from_db(conn, read_query)
    for companyInfo in company_data:
        company_name = companyInfo["CompanyName"]
        company_ticker = companyInfo["Ticker"]
        check_and_add_company(conn, company_name, company_ticker, data)
    disconnect_from_db(conn)

def check_and_add_company(conn, company_name, company_ticker, data):
    is_company_in_db = False
    for (company, ticker) in data:
        if (company_name == str(company)) and (company_ticker == str(ticker)):
            is_company_in_db = True
            break
    if not is_company_in_db:
        add_data_in_company_list_query = "Insert INTO CompanyList (CompanyName, Ticker) Values (\"" + company_name +  "\" , \"" + company_ticker + "\")"
        add_data_in_db(conn, add_data_in_company_list_query)
        print("Company " + company_name + " added to the Database with ticker : " + company_ticker)
    conn.commit()


def db_connect(db_file):
    print("Connecting to database : " + db_file)
    conn: Connection = sqlite3.connect(db_file)
    return conn

def read_config(section_name, option_name):
    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the configuration file
    config.read('../databaseConfig.ini')

    if config.has_section(section_name) and config.has_option(section_name, option_name):
        value = config.get(section_name, option_name)
        return value
    else:
        return ValueError

def connect_to_db():
    home_directory = Path.home()
    db_name = read_config("PATHS", "db_file")
    if db_name != ValueError:
        db_path = str(home_directory) + "/" + db_name
        conn = db_connect(db_path)
        return conn
    else:
        print("Error reading database configuration.")
        return None

def disconnect_from_db(conn):
    if conn:
        conn.close()
        print("Disconnected from SQLite database successfully!")

def read_data_from_db(conn, db_query):
    cursor = conn.cursor()
    data = cursor.execute(db_query).fetchall()
    return data

def add_data_in_db(conn, db_query):
    cursor = conn.cursor()
    data = cursor.execute(db_query).fetchall()
    cursor.execute("COMMIT")
    return data

def update_stock_data_batch(data_list, ticker):
    conn = connect_to_db()
    table_name = "Historical_Data_" + ticker
    if not table_exists(conn, table_name):
        print(f"Error: Table {table_name} does not exist.... creating it now")

    create_historical_data_table_for_company(conn, table_name)
    cursor = conn.cursor()
    print("adding batch of data to table " + table_name)
    cursor.executemany(f"""
        INSERT INTO {table_name} (
            Ticker, Date, Open, High, Low, Close, Volume, Dividends, Stock_Splits
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data_list)
    conn.commit()
    disconnect_from_db(conn)



def update_stock_data(ticker, date, open, high, low, close, volume, dividends, stock_splits):

    conn = connect_to_db()
    table_name = "Historical_Data_" + ticker
    if not table_exists(conn, table_name):
        print(f"Error: Table {table_name} does not exist.... creating it now")
        create_historical_data_table_for_company(conn, table_name)

    cursor = conn.cursor()
    cursor.execute(f"""
            INSERT INTO {table_name} (
                Ticker, Date, Open, High, Low, Close, Volume, Dividends, Stock_Splits
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ticker, date, open, high, low, close, volume, dividends, stock_splits))
    conn.commit()
    disconnect_from_db(conn)


def table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master WHERE type='table' AND name=?
    """, (table_name,))
    return cursor.fetchone() is not None


def create_historical_data_table_for_company(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Ticker TEXT,
            Date TEXT,
            Open REAL,
            High REAL,
            Low REAL,
            Close REAL,
            Volume INTEGER,
            Dividends REAL,
            Stock_Splits REAL
        )
    """)
    conn.commit()

