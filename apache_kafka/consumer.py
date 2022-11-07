from kafka import KafkaConsumer

topic_name = "contoh-topic"

consumer = KafkaConsumer(topic_name)

for message in consumer:
    print(message)
