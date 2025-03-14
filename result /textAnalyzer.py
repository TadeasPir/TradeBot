import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


def clean_text(text):
    """
    Cleans the article text by removing extraneous advertising or boilerplate lines.

    This function splits the text into lines and skips any line that contains
    known ad phrases (case insensitive) or is very short (< 20 characters).
    """
    # List of phrases (in lowercase) that indicate unwanted content.
    ad_phrases = [
        "unlock stock picks",
        "broker-level newsfeed",
        "upgrade now",
        "don’t miss the move",
        "sign up",
        "free daily newsletter",
        "try now>>",
        "read more on",  # sometimes used in headlines
        "view comments",
    ]

    lines = text.splitlines()
    cleaned_lines = []
    for line in lines:
        line_stripped = line.strip()
        if len(line_stripped) < 20:
            continue
        line_lower = line_stripped.lower()
        if any(phrase in line_lower for phrase in ad_phrases):
            continue
        cleaned_lines.append(line_stripped)
    return "\n".join(cleaned_lines)


def analyze_sentiment(text):
    """
    Uses VADER (Valence Aware Dictionary and sEntiment Reasoner) to analyze
    the sentiment of the provided text.

    The text is cleaned first so that extraneous advertising/boilerplate does
    not distort the compound score.

    Returns:
        tuple: (overall sentiment as a string, detailed scores dictionary)
    """
    analyzer = SentimentIntensityAnalyzer()
    cleaned = clean_text(text)
    scores = analyzer.polarity_scores(cleaned)
    compound = scores["compound"]

    if compound >= 0.01:
        sentiment = "Positive"
    elif compound <= -0.00:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"

    return sentiment, scores


def add_sentiment_to_articles(data):
    """
    Iterates through the list of articles and adds a 'sentiment'
    field (and a 'sentiment_scores' field) to each article based on its text.

    It uses the 'content' key if available; otherwise, it falls back to 'title'.
    """
    for article in data:
        text = article.get("content") or article.get("title", "")
        sentiment, scores = analyze_sentiment(text)
        article["sentiment"] = sentiment
        article["sentiment_scores"] = scores
    return data


def main():
    # Using relative paths since our current working directory is:
    # /Users/tade/PycharmProjects/PythonProject9/result
    input_filename = "daily_english_articles_final.json"
    output_filename = "daily_english_articles_final_sentiments.json"

    print(f"Loading articles from {input_filename}...")
    try:
        with open(input_filename, "r", encoding="utf-8") as f:
            data = json.load(f)
        print("Articles loaded successfully.")
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return

    print("Analyzing sentiment using VADER for each article...")
    updated_data = add_sentiment_to_articles(data)

    print(f"Saving updated articles to {output_filename}...")
    try:
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(updated_data, f, ensure_ascii=False, indent=4)
        print(f"Saved updated articles to {output_filename}.")
    except Exception as e:
        print(f"Error saving JSON file: {e}")


if __name__ == "__main__":
    main()
