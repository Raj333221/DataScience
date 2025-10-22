import pandas as pd
from faker import Faker
import random

fake = Faker()
# Define number of records and topics
num_records = 3000000 # 3 million records
topics = ["Gadgets", "Electronics", "Sam-pple"]
sentiments = {"positive": ["I love {0}! It's amazing.", "The best {0} I've ever used!", "Highly recommend {0}, it's fantastic."],
    "negative": ["{0} is so disappointing.", "I regret buying {0}.", "Avoid {0} at all costs."],
    "neutral": ["Just saw an ad for {0}.", "What are your thoughts on {0}?", "Thinking of trying {0}."]}
# Generate data
data = []
for _ in range(num_records):
	topic = random.choice(topics)
	sentiment_type = random.choice(list(sentiments.keys()))
	tweet_text = random.choice(sentiments[sentiment_type]).format(topic)
	data.append([fake.uuid4(),topic,tweet_text,fake.date_time_between(start_date="-1y", end_date="now")])

# Create a DataFrame and save to a CSV file
columns = ["tweet_id", "topic", "tweet_text", "creation_date"]
df = pd.DataFrame(data, columns=columns)
df.to_csv("social-media-data.csv", index=False)
print("Generated 3 million tweets and saved to social-media-data.csv")
