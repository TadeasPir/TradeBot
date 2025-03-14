import sys
sys.set_int_max_str_digits(1000000)  # Increase maximum allowed integer string digits

from datetime import datetime, timedelta
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from gdeltdoc import GdeltDoc, Filters


def construct_gdelt_api_url(filters):
    """
    Build an approximate API request URL from the Filters object.
    We try to retrieve the filter values using both public and private
    attribute names.
    """
    base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
    # Try multiple attribute names
    keyword = (getattr(filters, "keyword", None) or
               getattr(filters, "_keyword", None))
    start_date = (getattr(filters, "start_date", None) or
                  getattr(filters, "_start_date", None))
    end_date = (getattr(filters, "end_date", None) or
                getattr(filters, "_end_date", None))
    country = (getattr(filters, "country", None) or
               getattr(filters, "_country", None))

    params = {}
    if keyword:
        params["query"] = keyword
    if start_date:
        params["startdatetime"] = start_date
    if end_date:
        params["enddatetime"] = end_date
    if country:
        params["country"] = country

    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    return f"{base_url}?{query_string}"


def save_articles(articles, filename="daily_english_articles save.json"):
    """Saves all articles into a JSON file."""
    with open(filename, "w") as outfile:
        json.dump(articles, outfile, indent=4)
    print(f"Saved {len(articles)} articles to {filename}")


def process_day(current_date):
    """
    Process a single day's query.
    Returns an article dictionary (with the query date added) if one is found,
    otherwise returns None.
    """
    day_str = current_date.strftime("%Y-%m-%d")
    next_day_str = (current_date + timedelta(days=1)).strftime("%Y-%m-%d")

    # Create filters for the given day.
    f = Filters(
        keyword="apple",
        start_date=day_str,
        end_date=next_day_str,
        country="US",
    )
    # Debug: Print filters for this day.
    print(f"Filters for {day_str}:", vars(f))

    # Print the constructed GDELT API URL for debugging.
    api_url = construct_gdelt_api_url(f)
    print(f"Processing {day_str}: {api_url}")

    # Initialize a GDELT document search client.
    # If GdeltDoc is thread-safe, you could use a shared, global instance.
    gd = GdeltDoc()

    try:
        articles = gd.article_search(f)
    except Exception as err:
        print(f"Error fetching {day_str}: {err}")
        return None

    found_article = None
    if not articles.empty:
        for _, article in articles.iterrows():
            if article.get("language") == "English":
                found_article = article.to_dict()
                break

    if found_article:
        found_article["query_date"] = day_str
        print(
            f"Article found for {day_str}: "
            f"{found_article.get('url')}\n"
        )
        return found_article
    else:
        print(f"No English article found for {day_str}.\n")
        return None


def main():
    # Set our date range.
    start_date = datetime.strptime("2019-04-17", "%Y-%m-%d")
    end_date = datetime.strptime("2025-03-07", "%Y-%m-%d")

    # Create a complete list of dates.
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)

    daily_articles = []
    max_workers = 10  # Adjust based on available resources.

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Map each day to a worker (future).
            future_to_date = {
                executor.submit(process_day, day): day for day in dates
            }
            for future in as_completed(future_to_date):
                day = future_to_date[future].strftime("%Y-%m-%d")
                try:
                    result = future.result()
                    if result:
                        daily_articles.append(result)
                        # Save progress to a temporary file after each successful fetch.
                        save_articles(
                            daily_articles,
                            filename="daily_english_articles_partial.json",
                        )
                except Exception as exc:
                    print(f"Day {day} generated an exception: {exc}")
    except KeyboardInterrupt:
        print("Process interrupted by user. Saving current progress...")
    except Exception as e:
        print("An error occurred during processing:", e)
    finally:
        # Save the final (or partial) results.
        save_articles(daily_articles)
        print("Finished processing all dates.")


if __name__ == "__main__":
    main()
