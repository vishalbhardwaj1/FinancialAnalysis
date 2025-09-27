import schedule
import time
import yfinance as yf
from financialtool.databases.DataBaseUtils import *



def fetch_and_update():
    TICKERS = get_company_ticker_data()
    for ticker in TICKERS:
        stock = yf.Ticker(ticker)
        print("Ticker:", ticker)
        hist = stock.history(period="1y", interval="1wk")
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
        update_stock_data_batch(batch,ticker)

schedule.every().week.do(fetch_and_update)

if __name__ == "__main__":
    fetch_and_update()
    while True:
        schedule.run_pending()
        time.sleep(60)
