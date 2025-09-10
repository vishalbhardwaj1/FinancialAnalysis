import yfinance as yf
from financialtool.databases.DataBaseUtils import *

data = get_company_ticker_data()
for ticker in data:
    stock = yf.Ticker(ticker)
    hist = stock.history(period="5d")
    batch = []
    for index, row in hist.iterrows():
        batch.append((
            ticker,
            index.to_pydatetime().strftime('%Y-%m-%d'),
            round(row['Open'], 1),
            round(row['High'], 1),
            round(row['Low'], 1),
            round(row['Close'], 1),
            row['Volume'],
            round(row['Dividends'], 1),
            row['Stock Splits']
        ))
    update_stock_data_batch(batch, ticker)

