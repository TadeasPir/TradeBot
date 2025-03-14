import json
import csv
from statistics import mean


def aggregate_sentiments(json_filename):
    """
    Reads the JSON news articles and aggregates sentiment scores
    by date (using the "query_date" field). For each date the average
    compound score is computed and classified into Positive, Negative,
    or Neutral.

    Returns a dictionary keyed by date (YYYY-MM-DD) with a structure like:
      {
        "2019-04-15": {"avg_compound": 0.0, "sentiment": "Neutral"},
        "2019-04-13": {"avg_compound": -0.1531, "sentiment": "Negative"},
         ...
      }
    """
    with open(json_filename, 'r', encoding='utf-8') as f:
        articles = json.load(f)

    sentiments_by_date = {}
    for article in articles:
        # Use "query_date" field (e.g., "2019-04-15") as key.
        date_key = article.get("query_date", "").strip()
        if not date_key:
            # fallback: use first 10 characters of "seendate"
            date_key = article.get("seendate", "")[:10]
        compound = article.get("sentiment_scores", {}).get("compound")
        if compound is None:
            continue
        sentiments_by_date.setdefault(date_key, []).append(compound)

    aggregated = {}
    for date, compounds in sentiments_by_date.items():
        avg_compound = mean(compounds)
        if avg_compound >= 0.01:
            sentiment = "Positive"
        elif avg_compound <= -0.00:
            sentiment = "Negative"
        else:
            sentiment = "Neutral"
        aggregated[date] = {"avg_compound": avg_compound, "sentiment": sentiment}

    return aggregated


def merge_sentiments_to_csv(csv_input, csv_output, sentiments):
    """
    Reads the CSV file (which contains the stock data) and
    adds two new columns based on matching the date.

    The new columns are:
      - News_Sentiment (e.g., "Positive", "Negative", "Neutral")
      - News_Compound (the average compound score from aggregated news)

    The match is done by comparing the first 10 characters of the CSV "Date"
    field (assumed to be in the format "YYYY-MM-DD ...") with the aggregated
    JSON date.
    """
    with open(csv_input, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        # add two new columns to the fieldnames:
        fieldnames = reader.fieldnames + ["News_Sentiment", "News_Compound"]
        merged_rows = []
        for row in reader:
            date_str = row.get("Date", "")
            date_key = date_str[:10]  # extract YYYY-MM-DD
            if date_key in sentiments:
                row["News_Sentiment"] = sentiments[date_key]["sentiment"]
                row["News_Compound"] = sentiments[date_key]["avg_compound"]
            else:
                row["News_Sentiment"] = ""
                row["News_Compound"] = ""
            merged_rows.append(row)

    with open(csv_output, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged_rows)


def main():
    # Define your file names (adjust as necessary)
    json_filename = "daily_english_articles_final_sentiments.json"
    csv_input = "AAPL_ml_data.csv"  # your input CSV file
    csv_output = "aapl_stock_data_with_sentiment.csv"  # output CSV file

    print(f"Aggregating sentiment data from {json_filename}...")
    aggregated_sentiments = aggregate_sentiments(json_filename)
    print("Aggregation complete.")

    print(f"Merging sentiment data into CSV from {csv_input}...")
    merge_sentiments_to_csv(csv_input, csv_output, aggregated_sentiments)
    print(f"Done! See the output file: {csv_output}")


if __name__ == "__main__":
    main()
