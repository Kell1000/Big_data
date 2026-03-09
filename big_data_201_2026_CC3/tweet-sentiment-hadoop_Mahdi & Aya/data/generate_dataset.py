"""
Script to generate a sample tweets dataset for testing.
In production, replace with a real dataset from Kaggle:
  - Sentiment140: https://www.kaggle.com/datasets/kazanova/sentiment140
  - Twitter Airline Sentiment: https://www.kaggle.com/datasets/crowdflower/twitter-airline-sentiment
"""

import csv
import random

POSITIVE_TWEETS = [
    "I love this product, absolutely amazing experience!",
    "Great service today, very happy with the results",
    "This is the best day ever, feeling fantastic!",
    "Just had the most wonderful meal, highly recommend",
    "Thrilled with my new purchase, exceeded expectations",
    "Excellent customer support, resolved my issue immediately",
    "So grateful for all the support from my friends",
    "Beautiful weather today, perfect for a walk",
    "Just finished an incredible book, highly recommend",
    "The new update is fantastic, so much better now",
    "Happy to announce I got the job! Super excited",
    "Loving the new features in this app",
    "Outstanding performance today by the whole team",
    "Best coffee I have ever had this morning",
    "Feeling motivated and ready to take on the world",
]

NEGATIVE_TWEETS = [
    "Terrible experience with customer service, very disappointed",
    "This product is a complete waste of money",
    "Worst flight I have ever taken, never again",
    "Very frustrated with the constant delays",
    "Cannot believe how bad this service has gotten",
    "Disgusted by the lack of response from support",
    "This update ruined everything, reverting immediately",
    "So angry about the hidden fees, total scam",
    "Horrible quality, broke after just two days",
    "Extremely disappointed, not what was advertised at all",
    "Never buying from this brand again, awful experience",
    "The app crashes constantly, completely unusable",
    "Waited two hours and still no help, unacceptable",
    "Such a letdown, expected so much more",
    "This is the worst decision I have ever made",
]

NEUTRAL_TWEETS = [
    "Just checked the weather forecast for tomorrow",
    "The new model was released yesterday",
    "I am going to the store later today",
    "The meeting is scheduled for 3pm",
    "Currently reading about machine learning",
    "The package arrived this afternoon",
    "Watched a documentary about space last night",
    "The restaurant opens at noon on weekdays",
    "My flight departs at 6am tomorrow morning",
    "The report will be ready by end of day",
    "The conference starts next Monday in Paris",
    "Updated my profile information on the platform",
    "The team is working on the new feature",
    "Downloaded the latest version of the software",
    "The event was attended by over 500 people",
]

def generate_dataset(filename="tweets.csv", num_rows=3000):
    all_tweets = (
        [(t, "positive") for t in POSITIVE_TWEETS] * (num_rows // 45 + 1) +
        [(t, "negative") for t in NEGATIVE_TWEETS] * (num_rows // 45 + 1) +
        [(t, "neutral")  for t in NEUTRAL_TWEETS]  * (num_rows // 45 + 1)
    )
    random.shuffle(all_tweets)
    all_tweets = all_tweets[:num_rows]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "text", "sentiment"])
        for i, (text, sentiment) in enumerate(all_tweets, start=1):
            writer.writerow([i, text, sentiment])

    print(f"Dataset generated: {filename} ({num_rows} rows)")

if __name__ == "__main__":
    generate_dataset("tweets.csv", num_rows=3000)
