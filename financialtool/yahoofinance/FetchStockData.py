import yfinance as yf
from financialtool.databases.DataBaseUtils import *

data = get_company_ticker_data()
for company in data:
    print("Fetching data for company : " + company)
    stock = yf.Ticker(company)
    hist = stock.history(period="5d")
    for index, row in hist.iterrows():
        print(f" Date: {index}, Open: {row['Open']}, High: {row['High']}, Low: {row['Low']}, Close: {row['Close']}, Volume: {row['Volume']}")
    print("--------------------------------------------------")
