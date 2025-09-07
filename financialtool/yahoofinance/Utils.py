from financialtool.databases.DataBaseUtils import *


def add_stock_data (company_data):
    conn = connect_to_db()
    read_query = "Select * from CompanyList"
    data = read_data_from_db(conn, read_query)
    for companyInfo in company_data:
        company_name = companyInfo["CompanyName"]
        company_ticker = companyInfo["Ticker"]
        check_and_add_company(conn, company_name, company_ticker, data)
    disconnect_from_db(conn)