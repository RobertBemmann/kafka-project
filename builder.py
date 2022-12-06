from kafka import KafkaConsumer, KafkaClient, KafkaProducer
from kafka.admin import NewTopic
from datetime import datetime
from zoneinfo import ZoneInfo
import time

BOOTSTRAP_SERVER = "localhost:29092" #"kafka:29092"

input_topic_name = 'input_topic'
output_topic_name = 'output_topic'
topic_names = [input_topic_name, output_topic_name]

producer_input = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, client_id="input")

datapoints = [
        {"myKey": 1, "myTimestamp": "2022-03-01T09:11:04+01:00"},
        {"myKey": 2, "myTimestamp": "2022-03-01T09:12:08+01:00"},
        {"myKey": 3, "myTimestamp": "2022-03-01T09:13:12+01:00"},
        {"myKey": 4, "myTimestamp": ""},
        {"myKey": 5, "myTimestamp": "2022-03-01T09:14:05+01:00"}
    ]

def create_topics(admin_client):
    existing_topic_list = []
    topic_list = []
    for topic in topic_names:
        if topic not in existing_topic_list:
            topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
            print('Topic added - {}'.format(topic))
        else:
            print('Topic already exists - {topic}')

    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print(f"Topics created successfully: {str(topic_names)}")

def delete_topics(admin_client,topic_names=topic_names):
    try:
        admin_client.delete_topics(topics=topic_names)
        time.sleep(5)
        print(f"Topic deleted successfully - {topic_names}")
    except Exception as e:
        print(e)
    time.sleep(15)

def populate_input_topic(topic_name = input_topic_name, data=datapoints, producer=producer_input):
    for msg in datapoints:
        producer.send(topic_name, key=str.encode(str(msg['myKey'])), value=str.encode((msg['myTimestamp'])))
    producer.flush(1)
    print(f"Sent all the messages to topic - {topic_name}.")

def transform_timestamp(ts_string, source_tz='Europe/Berlin', target_tz='UTC'):
    utc = ZoneInfo(target_tz)
    localtz = ZoneInfo(source_tz)
    localtime = datetime.fromisoformat(ts_string).astimezone(localtz)
    utctime = localtime.replace(tzinfo=utc)
    print(f"Converted timestamp to UTC: {utctime}.")
    return str(utctime)

def transfomer_input_to_output(input_topic=input_topic_name, output_topic=output_topic_name, host=BOOTSTRAP_SERVER):
    consumer = KafkaConsumer(input_topic,
                             group_id='my-group',
                             bootstrap_servers=host,
                             auto_offset_reset='earliest',
                             enable_auto_commit=False)

    producer_output = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, client_id="output")

    print("Starting to consume...")
    for msg in consumer:
        key = msg.key.decode('utf-8'),
        value = msg.value.decode('utf-8')
        print("%s:%d:%d: key=%s value=%s" % (
            msg.topic,
            msg.partition,
            msg.offset,
            key,
            value
        )
              )
        if value=='':
            print("Empty timestamp - you may wanna check your producer.")
            pass
        else:
            value_transformed = transform_timestamp(value)
            producer_output.send(output_topic, key=str.encode(key[0]), value=str.encode(value_transformed))
            print(f"Sent messages to topic - {output_topic}: K - {key[0]}, V - {value_transformed}.")