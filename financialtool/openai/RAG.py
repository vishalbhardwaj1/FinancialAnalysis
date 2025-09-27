from datetime import datetime, timedelta

import ollama
import yfinance as yf
from langchain.chains import create_history_aware_retriever
from langchain_community.document_loaders import DataFrameLoader
from langchain_ollama import OllamaLLM
import json

# Retrieve historical stock data using yfinance
def get_stock_data(ticker, start_date, end_date):
    """Fetches historical stock data for a given ticker."""
    stock = yf.Ticker(ticker)
    df = stock.history(start=start_date, end=end_date)
    return df

# --- Data Retrieval ---
def get_recent_news(ticker):
    stock = yf.Ticker(ticker)
    return stock.news

# --- Function to generate analysis ---
def get_financial_summary(ticker, num_days=30):
    today = datetime.now()
    start_date = today - timedelta(days=num_days)
    end_date = today
    stock_df_local = get_stock_data(ticker, start_date, end_date)
    news_articles = get_recent_news(ticker) or []

    print(f"Fetched {len(news_articles)} news articles for {ticker}.")

    for article in news_articles:
        article = json.dumps(article)
        data = json.loads(article)
        print(data['content']['title'])
        title = data['content']['title']
        summary = data['content']['summary']
        print(f"Headline: {title}\nSummary: {summary}\n")

    if news_articles:
        print(news_articles[0])
    else:
        print("No news articles found for", ticker)


    if stock_df_local is not None and not stock_df_local.empty:
        # Ensure 'Close' column is string type for Document validation
        stock_df_local['Close'] = stock_df_local['Close'].apply(lambda x: str(x))

        print("Ollama LLM initialized:", ollama.list)

        local_llm = OllamaLLM(model="llama3")
        loader = DataFrameLoader(stock_df_local, page_content_column='Close')

        stock_documents = loader.load()
        price_history = stock_df_local[['Close', 'Volume']].to_string()

    else:
        stock_documents = []
        price_history = "No price history available."

    # Format news articles
    print("Stock Documents Loaded:", len(stock_documents))
    recent_news = "\n".join([
        f"Headline: {n.get('title', 'No Title')}\nSummary: {n.get('link', 'No Summary')}"
        for n in news_articles
    ])

    print("Recent News:\n", recent_news)
    prompt = f"""
    You are a financial analyst. Based on the following stock data and news, provide a summary of the stock's performance and key events.

    Stock: {ticker}
    Recent Price and Volume History:
    {price_history}

    Recent News Headlines and Summaries:
    {recent_news}

    Analysis:
    """

    try:
        response = local_llm.invoke(prompt)
    except Exception as e:
        print("Error connecting to Ollama server:", e)
        response = "Could not generate analysis due to LLM connection error."

    return response


# Get and print the analysis for a stock
summary = get_financial_summary('GOOG')
print(f"Financial Analysis for GOOG:\n{summary}")
