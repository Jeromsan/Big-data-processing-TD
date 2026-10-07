# %%
import re
import string
from pathlib import Path

from confluent_kafka import Consumer

# %%
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'book_reader_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

# %%
TOPIC = 'new_topic'
OUTPUT_PATH = Path(__file__).resolve().parent / 'cleaned_messages.txt'
STOPWORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'of', 'to', 'in', 'on', 'at',
    'for', 'with', 'by', 'from', 'as', 'is', 'was', 'were', 'be', 'been',
    'are', 'it', 'its', 'he', 'she', 'they', 'him', 'her', 'his', 'them',
    'their', 'i', 'you', 'we', 'me', 'my', 'our', 'us', 'this', 'that',
    'these', 'those', 'into', 'over', 'under', 'after', 'before', 'when',
    'while', 'if', 'then', 'than', 'so', 'not', 'no', 'yes', 'would', 'could',
    'should', 'did', 'do', 'does', 'have', 'has', 'had', 'all', 'any', 'some',
    'one', 'two', 'three', 'more', 'most', 'much', 'many'
}


def clean_message(raw_message: str):
    """Applique les étapes de nettoyage inspirées du TP2 sans modifier le notebook."""
    lower = raw_message.lower()
    table = str.maketrans('', '', string.punctuation)
    without_punct = lower.translate(table)
    tokens = re.split(r'\s+', without_punct.strip())
    cleaned_tokens = [token for token in tokens if token and token not in STOPWORDS]
    return cleaned_tokens


consumer.subscribe([TOPIC])
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH.write_text('', encoding='utf-8')

# %%
# Configuration
MAX_EMPTY_POLLS = 10
MAX_ERRORS = 5
empty_polls = 0
error_count = 0

while True:
    msg = consumer.poll(1.0)

    # 1. Handle no message (timeout)
    if msg is None:
        empty_polls += 1
        if empty_polls >= MAX_EMPTY_POLLS:
            print("Closing: No new messages received.")
            break
        continue

    # 2. Handle Kafka errors
    if msg.error():
        error_count += 1
        print(f"Consumer error: {msg.error()}")
        if error_count >= MAX_ERRORS:
            print("Closing: Too many consecutive errors.")
            break
        continue

    # 3. Handle successful message
    empty_polls = 0
    error_count = 0
    raw_text = msg.value().decode('utf-8')
    cleaned_tokens = clean_message(raw_text)

    if cleaned_tokens:
        with OUTPUT_PATH.open('a', encoding='utf-8') as output_file:
            output_file.write(' '.join(cleaned_tokens) + '\n')
        print(f"Original: {raw_text}")
        print(f"Cleaned: {' '.join(cleaned_tokens)}")

# Clean up
consumer.close()
