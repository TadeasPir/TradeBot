import yfinance as yf
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import time


def fetch_stock_data(ticker_symbols, period="2y", interval="1d"):
    """
    Fetch historical stock data for the given ticker symbols.

    Args:
        ticker_symbols: List of stock ticker symbols (e.g., ["AAPL", "MSFT"])
        period: Time period to fetch (default: "2y" for 2 years)
        interval: Data interval (default: "1d" for daily)

    Returns:
        Dictionary with ticker symbols as keys and pandas DataFrames as values
    """
    data_dict = {}
    fundamental_data = {}

    for ticker in ticker_symbols:
        try:
            print(f"Fetching data for {ticker}...")
            stock = yf.Ticker(ticker)
            data = stock.history(period=period, interval=interval)

            if data.empty:
                print(f"No data available for {ticker}")
                continue

            data_dict[ticker] = data

            # Fetch fundamental parameters
            info = stock.info
            fundamental_data[ticker] = {
                'marketCap': info.get('marketCap', np.nan),
                'peRatio': info.get('trailingPE', np.nan),
                'dividendYield': info.get('dividendYield', np.nan) * 100 if info.get('dividendYield') else np.nan,
                'eps': info.get('trailingEps', np.nan),
                'beta': info.get('beta', np.nan)
            }

            print(f"Fundamental data for {ticker}: {fundamental_data[ticker]}")

            # Avoid hitting rate limits
            time.sleep(1)

        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

    return data_dict, fundamental_data


def add_technical_indicators(df):
    """Add common technical indicators to the dataframe."""

    # Make a copy to avoid warnings
    df = df.copy()

    # Calculate moving averages
    df['MA5'] = df['Close'].rolling(window=5).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA50'] = df['Close'].rolling(window=50).mean()

    # Calculate Relative Strength Index (RSI)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))

    # Calculate Bollinger Bands
    df['BB_Middle'] = df['Close'].rolling(window=20).mean()
    df['BB_Std'] = df['Close'].rolling(window=20).std()
    df['BB_Upper'] = df['BB_Middle'] + 2 * df['BB_Std']
    df['BB_Lower'] = df['BB_Middle'] - 2 * df['BB_Std']

    # Calculate MACD (Moving Average Convergence Divergence)
    df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = df['EMA12'] - df['EMA26']
    df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()

    # Calculate daily returns
    df['Daily_Return'] = df['Close'].pct_change()

    # Calculate volatility (standard deviation of returns)
    df['Volatility_14d'] = df['Daily_Return'].rolling(window=14).std()

    # Add day of week (0-4, Monday-Friday)
    df['DayOfWeek'] = pd.to_datetime(df.index).dayofweek

    # Drop rows with NaN values
    df = df.dropna()

    return df


def add_fundamental_data(df, ticker, fundamental_data):
    """Add fundamental data to the time series dataframe."""
    if ticker in fundamental_data:
        # Add fundamental parameters as constant columns
        for param, value in fundamental_data[ticker].items():
            df[param] = value
    return df


def prepare_ml_dataset(data_dict, fundamental_data, output_dir="stock_data"):
    """
    Prepare and save the data for machine learning.

    Args:
        data_dict: Dictionary with ticker symbols as keys and DataFrames as values
        fundamental_data: Dictionary with ticker symbols as keys and fundamental parameters as values
        output_dir: Directory to save the processed data
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Create a combined dataset with all stocks
    all_stocks_data = []

    for ticker, df in data_dict.items():
        # Add technical indicators
        ml_df = add_technical_indicators(df)

        # Add fundamental data
        ml_df = add_fundamental_data(ml_df, ticker, fundamental_data)

        # Add ticker column
        ml_df['Ticker'] = ticker

        # Save individual stock data
        output_path = os.path.join(output_dir, f"{ticker}_ml_data.csv")
        ml_df.to_csv(output_path)
        print(f"Data for {ticker} saved to {output_path}")

        # Add to combined dataset
        all_stocks_data.append(ml_df)

        # Create a data dictionary for reference
        info = {
            "ticker": ticker,
            "start_date": ml_df.index.min().strftime('%Y-%m-%d'),
            "end_date": ml_df.index.max().strftime('%Y-%m-%d'),
            "trading_days": len(ml_df),
            "features": list(ml_df.columns),
            "fundamental_data": fundamental_data.get(ticker, {})
        }

        # Save dataset info
        info_path = os.path.join(output_dir, f"{ticker}_info.txt")
        with open(info_path, 'w') as f:
            for key, value in info.items():
                f.write(f"{key}: {value}\n")

    # Combine all stocks data and save
    if all_stocks_data:
        combined_df = pd.concat(all_stocks_data)
        combined_output_path = os.path.join(output_dir, "all_stocks_ml_data.csv")
        combined_df.to_csv(combined_output_path)
        print(f"Combined data for all stocks saved to {combined_output_path}")


def main():
    # Define list of stocks to scrape (customize as needed)
    tickers = ["AAPL","WFSPX"]

    # Fetch the data
    stock_data, fundamental_data = fetch_stock_data(
        ticker_symbols=tickers,
        period="7y",  # 5 years of data
        interval="1d"  # Daily data
    )

    # Save fundamental data separately
    fundamental_df = pd.DataFrame.from_dict(fundamental_data, orient='index')
    output_dir = "stock_data_for_ml"
    os.makedirs(output_dir, exist_ok=True)
    fundamental_df.to_csv(os.path.join(output_dir, "fundamental_data.csv"))

    # Prepare and save the data for machine learning
    prepare_ml_dataset(stock_data, fundamental_data, output_dir=output_dir)

    print("\nData preparation complete. Your datasets are ready for machine learning!")


if __name__ == "__main__":
    main()
