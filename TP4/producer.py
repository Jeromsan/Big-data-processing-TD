# %%
import socket
from pathlib import Path
from confluent_kafka import Producer

# %%
config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname(),
}

producer = Producer(config)

# %%
TOPIC = 'new_topic'
BOOK_PATH = Path(__file__).resolve().with_name('Myself and others.txt')

# %%
try:
    with BOOK_PATH.open('r', encoding='utf-8', errors='replace') as book_file:
        for line_number, line in enumerate(book_file, start=1):
            message = line.rstrip('\n')
            if not message:
                continue

            producer.produce(
                topic=TOPIC,
                value=message.encode('utf-8')
            )
            print(f"[{line_number}] {message}")

    producer.flush()
    print(f"Finished sending all lines from {BOOK_PATH.name} to topic '{TOPIC}'.")
except FileNotFoundError:
    print(f"File not found: {BOOK_PATH}")
finally:
    producer.close()
