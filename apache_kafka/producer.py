from kafka import KafkaProducer

topic_name = "contoh-topic"
kafka_server = "localhost:9092"

producer = KafkaProducer(bootstrap_servers=kafka_server)

producer.send(topic_name,
              b'Test Message new!!!!!!!!!!!!!!!!!!')  # b = binary message
producer.flush()