import datetime
import time
from urllib.parse import quote

from bs4 import BeautifulSoup
from newspaper import Article
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def parse_date_from_string(date_str):
    """
    Attempts to parse a date from a string.

    It first checks for an ISO-like format.
    Then it tries abbreviated (e.g. "Mar 12, 2018")
    and full month names (e.g. "March 12, 2018").
    """
    try:
        if "T" in date_str:
            date_str = date_str.replace("Z", "+00:00")
            dt = datetime.datetime.fromisoformat(date_str)
            return dt.date()
    except Exception:
        pass

    try:
        dt = datetime.datetime.strptime(date_str, "%b %d, %Y")
        return dt.date()
    except Exception:
        pass

    try:
        dt = datetime.datetime.strptime(date_str, "%B %d, %Y")
        return dt.date()
    except Exception:
        pass

    return None


def get_google_news_url(date, keyword="apple"):
    """
    Constructs a Google News search URL using a query of the form:
       "dd.mm.yyyy keyword"

    For example, for March 11, 2018 the query becomes "11.03.2018 apple".
    """
    date_str = date.strftime("%d.%m.%Y")
    query = f"{date_str} {keyword}"
    query_encoded = quote(query)
    url = (
        f"https://news.google.com/search?q={query_encoded}"
        f"&hl=en-US&gl=US&ceid=US:en"
    )
    return url, query


def get_candidate_articles(driver, search_url, count=6):
    """
    Retrieves candidate articles from the Google News search results
    using Selenium. It waits for <article> elements to appear, then it uses
    BeautifulSoup to parse the HTML for article elements. For each article,
    it extracts the first available anchor with an href attribute and a
    publication date if one exists.
    """
    candidates = []
    try:
        driver.get(search_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "article"))
        )
        # Additional sleep can help if content loads in stages.
        time.sleep(2)
        page_source = driver.page_source
    except Exception as e:
        print(f"Error loading page {search_url}: {e}")
        return candidates

    soup = BeautifulSoup(page_source, "html.parser")
    articles = soup.find_all("article")
    for article in articles:
        candidate_date = None
        time_tag = article.find("time")
        if time_tag:
            candidate_date = parse_date_from_string(time_tag.get_text(strip=True))

        # Look for an anchor with an href attribute.
        anchor = article.find("a", href=True)
        if anchor:
            href = anchor.get("href")
            if href.startswith("./"):
                href = "https://news.google.com" + href[1:]
            candidate = {"url": href, "candidate_date": candidate_date}
            if candidate not in candidates:
                candidates.append(candidate)
        if len(candidates) >= count:
            break
    return candidates


def get_article_data(url):
    """
    Downloads and parses an article using newspaper3k.
    Returns a tuple: (title, content, publish_date) where publish_date
    might be None.
    """
    try:
        article = Article(url)
        article.download()
        article.parse()
        return article.title, article.text, article.publish_date
    except Exception as e:
        print(f"Failed to process article {url}: {e}")
        return None, None, None


def choose_best_article(candidates, target_date):
    """
    Chooses the candidate article whose publication date is closest
    to the target_date. It first uses the candidate date extracted from
    the page, and if unavailable, falls back to the newspaper3k publish date.
    """
    best_diff = float("inf")
    best_data = None

    for candidate in candidates:
        url = candidate["url"]
        title, content, pub_date = get_article_data(url)
        if not title or not content:
            continue

        candidate_date = candidate["candidate_date"]
        if candidate_date is None and pub_date is not None:
            if isinstance(pub_date, datetime.datetime):
                candidate_date = pub_date.date()
            elif isinstance(pub_date, datetime.date):
                candidate_date = pub_date

        if candidate_date is not None:
            diff = abs((candidate_date - target_date).days)
        else:
            diff = float("inf")

        print(f"Candidate URL: {url}")
        print(f"Candidate date: {candidate_date}, Diff: {diff} days")
        if diff == 0:
            return title, content, url, candidate_date
        if diff < best_diff:
            best_diff = diff
            best_data = (title, content, url, candidate_date)

    if best_data:
        print(f"No exact match found; using candidate with diff {best_diff} days.")
        return best_data
    return None, None, None, None


def main():
    # Set up headless Chrome using Selenium.
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    # Ensure ChromeDriver is in your PATH or provide its executable path here.
    driver = webdriver.Chrome(options=options)

    # Define the date range: from 7 years ago until today.
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=7 * 365)
    results = []

    current_date = start_date
    while current_date < end_date:
        print(f"\nProcessing date: {current_date}")
        search_url, query = get_google_news_url(current_date, keyword="apple")
        print(f"Searching for query: '{query}' using URL: {search_url}")

        candidate_list = get_candidate_articles(driver, search_url, count=6)
        if not candidate_list:
            print("No candidate articles found for this date.")
            current_date += datetime.timedelta(days=1)
            time.sleep(2)
            continue

        title, content, selected_url, candidate_date = choose_best_article(
            candidate_list, current_date
        )
        if content:
            results.append(
                {
                    "date": current_date.isoformat(),
                    "search_query": query,
                    "title": title,
                    "url": selected_url,
                    "publish_date": candidate_date.isoformat()
                    if candidate_date
                    else None,
                    "content": content,
                }
            )
            print(f"Selected article: '{title}' on {candidate_date}")
        else:
            print("No article content could be extracted for this date.")

        # Pause between requests.
        time.sleep(2)
        current_date += datetime.timedelta(days=1)

    driver.quit()

    print("\nScraping complete. Total articles scraped:", len(results))
    # Optionally, save results to a file (JSON, CSV, etc.).

if __name__ == "__main__":
    main()
