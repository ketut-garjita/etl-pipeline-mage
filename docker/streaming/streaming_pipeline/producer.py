from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from config import BOOTSTRAP_SERVERS, TOPIC
import json
import csv

# Read CSV as generator
def read_csv(file_path):
    with open(file_path, 'r', encoding='utf-8-sig', errors='replace') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row

# Delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to topic: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()}')

# Create Kafka topic if it doesn't exist
def create_topic(bootstrap_servers, topic_name, num_partitions=6, replication_factor=1):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    topics_metadata = admin_client.list_topics(timeout=5)

    if topic_name in topics_metadata.topics:
        print(f"Topic '{topic_name}' already exists.")
        return

    new_topic = NewTopic(topic=topic_name,
                         num_partitions=num_partitions,
                         replication_factor=replication_factor)
    fs = admin_client.create_topics([new_topic])

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created with {num_partitions} partitions and replication factor {replication_factor}.")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")

# Produce data to Kafka
def produce_data(bootstrap_servers, topic, file_path):
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    try:
        for row in read_csv(file_path):
            json_row = json.dumps(row)
            producer.produce(topic, value=json_row, callback=delivery_report)
            producer.poll(0)  # non-blocking flush of delivery reports
        producer.flush()
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        producer.flush()

if __name__ == '__main__':
    bootstrap_servers = BOOTSTRAP_SERVERS
    topic = TOPIC
    csv_file_path = './data/DataCoSupplyChainDataset.csv'

    # Create topic with 6 partitions & replication factor 1
    create_topic(bootstrap_servers, topic, num_partitions=6, replication_factor=1)

    # Send CSV data to Kafka
    produce_data(bootstrap_servers, topic, csv_file_path)

