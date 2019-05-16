from kafka import KafkaConsumer
from json import loads
import time
import pandas as pd

KAFKA_CONSUMER_GROUP_NAME_CONS = "test_consumer_group"
KAFKA_TOPIC_NAME_CONS = "testtopic"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = '34.73.102.250:9092'

if __name__ == "__main__":

    print("Kafka Consumer Application Started ... ")
    # auto_offset_reset='latest'
    # auto_offset_reset='earliest'
    consumer = KafkaConsumer(
    KAFKA_TOPIC_NAME_CONS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
    value_deserializer=lambda x: loads(x.decode('utf-8')))

    def get_message_df():
        print("Reading Messages from Kafka Topic about to Start ... ")
        message_list = []
        counter = 0
        df = pd.DataFrame()
        for message in consumer:
            #print(dir(message))
            #print(type(message))
            print("Key: ", message.key)
            output_message = message.value
            #print(type(message.value))
            #print("Message received: ", output_message)
            #message_list.append(output_message)
            df.append(output_message, [counter])
            counter += 1
            print("Counter in for loop: ", counter)
            if counter == 10:
                print("Counter in if loop: ", counter)
                yield df
                #print(message_list)
                #message_list = [{'transaction_amount': 524.62, 'transaction_datetime': '2019-05-14 01:30:32', 'transaction_card_type': 'Maestro', 'transaction_id': '1'}]
                #print(message_list)
                #print("Before Creating DataFrame ...")
                #df = pd.DataFrame(message_list)
                #df.head()
                #print("After Creating DataFrame.")
                message_list = []
                #df = None
                counter = 0
                time.sleep(5)

    for df in get_message_df():
        print("Before DataFrame Head ...")
        df.head()
        print("After DataFrame Head.")