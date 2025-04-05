import pandas as pd

# Load the CSV file
df = pd.read_csv("aapl_stock_data_with_sentiment.csv")

# Filter rows where 'News_Compound' is exactly 0 or is null.
filtered_df = df[(df["News_Compound"] == 0) | (df["News_Compound"].isnull())]

# Extract the 'Date' column
filtered_dates = filtered_df["Date"]

# Save the filtered dates to a new CSV file
filtered_dates.to_csv("filtered_dates.csv", index=False)

print("Saved filtered dates:")
print(filtered_dates.tolist())
