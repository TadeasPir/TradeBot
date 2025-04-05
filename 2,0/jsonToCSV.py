import json
import csv
import pandas as pd

# Load JSON data from file
json_file_path = 'daily_english_articles_final_sentiments.json'

try:
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Create CSV file with sentiment scores
    output_csv = 'sentiment_scores.csv'

    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['date', 'neg', 'neu', 'pos', 'compound']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for item in data:
            # Check if the item has sentiment_scores to handle potential missing data
            if 'sentiment_scores' in item:
                writer.writerow({
                    'date': item.get('query_date', ''),
                    'neg': item['sentiment_scores'].get('neg', ''),
                    'neu': item['sentiment_scores'].get('neu', ''),
                    'pos': item['sentiment_scores'].get('pos', ''),
                    'compound': item['sentiment_scores'].get('compound', '')
                })

    print(f"CSV file '{output_csv}' has been created successfully.")

    # Alternative pandas approach (commented out)
    # rows = []
    # for item in data:
    #     if 'sentiment_scores' in item:
    #         rows.append({
    #             'date': item.get('query_date', ''),
    #             'neg': item['sentiment_scores'].get('neg', ''),
    #             'neu': item['sentiment_scores'].get('neu', ''),
    #             'pos': item['sentiment_scores'].get('pos', ''),
    #             'compound': item['sentiment_scores'].get('compound', '')
    #         })
    #
    # df = pd.DataFrame(rows)
    # df.to_csv('sentiment_scores.csv', index=False)

except FileNotFoundError:
    print(f"Error: The file '{json_file_path}' was not found.")
except json.JSONDecodeError:
    print(f"Error: The file '{json_file_path}' contains invalid JSON.")
except Exception as e:
    print(f"An error occurred: {str(e)}")
