import pandas as pd
import datetime


# Load sentiment data
def load_sentiment_data(file_path):
    sentiment_df = pd.read_csv(file_path)
    # Convert date to datetime format for matching, but keep it simple without timezone info
    sentiment_df['date'] = pd.to_datetime(sentiment_df['date'], errors='coerce')
    return sentiment_df


# Load stock data
def load_stock_data(file_path):
    stock_df = pd.read_csv(file_path)
    # Convert Date to datetime format with proper handling of timezone issues
    stock_df['Date'] = pd.to_datetime(stock_df['Date'], utc=True, errors='coerce')
    return stock_df


# Main function to merge data
def merge_data():
    try:
        # Load both datasets
        sentiment_df = load_sentiment_data('sentiment_scores.csv')
        stock_df = load_stock_data('AAPL_ml_data.csv')  # Assuming this is the name of your stock data file

        # Check if date conversion was successful
        if sentiment_df['date'].isna().any():
            print("Warning: Some sentiment dates could not be parsed. Check your data format.")
        if stock_df['Date'].isna().any():
            print("Warning: Some stock dates could not be parsed. Check your data format.")

        # Create date strings in consistent format for matching
        stock_df['DateStr'] = stock_df['Date'].dt.strftime('%Y-%m-%d') if hasattr(stock_df['Date'],
                                                                                  'dt') else pd.Series(
            ['' for _ in range(len(stock_df))])
        sentiment_df['DateStr'] = sentiment_df['date'].dt.strftime('%Y-%m-%d') if hasattr(sentiment_df['date'],
                                                                                          'dt') else pd.Series(
            ['' for _ in range(len(sentiment_df))])

        # Print sample data for debugging
        print("Stock date samples:", stock_df['DateStr'].head())
        print("Sentiment date samples:", sentiment_df['DateStr'].head())

        # Merge the dataframes on the date string
        merged_df = pd.merge(
            stock_df,
            sentiment_df[['DateStr', 'neg', 'neu', 'pos', 'compound']],
            left_on='DateStr',
            right_on='DateStr',
            how='left'  # Keep all stock data rows even if no sentiment data exists
        )

        # Remove the temporary DateStr column
        merged_df = merged_df.drop(columns=['DateStr'])

        # Save the merged data
        output_file = 'merged_stock_sentiment_data.csv'
        merged_df.to_csv(output_file, index=False)
        print(f"Merged data saved to {output_file}")

        return merged_df

    except Exception as e:
        print(f"An error occurred during merging: {str(e)}")
        # Print more details for debugging
        import traceback
        traceback.print_exc()
        return None


# Execute the merge
if __name__ == "__main__":
    merged_data = merge_data()

    # Display sample of merged data
    if merged_data is not None and not merged_data.empty:
        print("\nSample of merged data:")
        print(merged_data.head())
