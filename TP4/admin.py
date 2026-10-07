# %%
from confluent_kafka.admin import AdminClient, NewTopic, TopicAlreadyExistsError

# %%
config = {
    'bootstrap.servers': 'localhost:9092',
}

admin_client = AdminClient(config)

# %%
# Create a new Kafka topic for this exercise
new_topic = 'new_topic'

try:
    admin_client.create_topics(
        [NewTopic(new_topic, num_partitions=1, replication_factor=1)]
    )
    print(f"Topic '{new_topic}' created successfully.")
except TopicAlreadyExistsError:
    print(f"Topic '{new_topic}' already exists.")

# %%
# List all current topics
metadata = admin_client.list_topics()
for topic_name in metadata.topics.keys():
    print(topic_name)

# %%
# Uncomment to delete the topic when needed
# admin_client.delete_topics([new_topic])
