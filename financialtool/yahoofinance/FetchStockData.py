import yfinance as yf
from financialtool.databases.DataBaseUtils import *
import datetime

data = get_company_ticker_data()
for ticker in data:
    stock = yf.Ticker(ticker)
    date = get_last_date_for_company(ticker)
    if date and date != datetime.date.today().strftime('%Y-%m-%d'):
        print(f"Fetching data for {ticker} since {date}...")
        hist = stock.history(start=date, interval="1d")
        hist = hist[1:]
    elif date == datetime.date.today().strftime('%Y-%m-%d'):
        print(f"Data for {ticker} is already up to date.")
        continue
    else:
        hist = stock.history(period="max")
    batch = []
    for index, row in hist.iterrows():
        batch.append((
            ticker,
            index.to_pydatetime().strftime('%Y-%m-%d'),
            round(row['Open'], 3),
            round(row['High'], 3),
            round(row['Low'], 3),
            round(row['Close'], 3),
            row['Volume'],
            round(row['Dividends'], 3),
            row['Stock Splits']
        ))
    update_stock_data_batch(batch, ticker)

