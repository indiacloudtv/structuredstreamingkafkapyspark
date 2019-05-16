from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random

KAFKA_TOPIC_NAME_CONS = "testtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = '34.73.102.250:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    transaction_card_type_list = ["Visa", "MasterCard", "Maestro"]

    message = None
    for i in range(5):
        i = i + 1
        message = {}
        print("Sending message to Kafka topic: " + str(i))
        event_datetime = datetime.now()

        message["transaction_id"] = str(i)
        message["transaction_card_type"] = random.choice(transaction_card_type_list)
        message["transaction_amount"] = round(random.uniform(5.5,555.5), 2)
        message["transaction_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")

        print("Message to be sent: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)