import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
import socket



# 1. Setup a local Kafka cluster for development.
BOOTSTRAP_SERVER = "localhost:29092"
admin_client = AdminClient({
    "bootstrap.servers": BOOTSTRAP_SERVER
})

# 2. Prepare two Kafka topics.
# One topic is called "input_topic".
# The other topic is called "output_topic".

input_topic = NewTopic("input_topic", 1, 1)
output_topic = NewTopic("output_topic", 1, 1)
topic_list = [input_topic, output_topic]

admin_client.create_topics(topic_list)
time.sleep(5)
print(admin_client.list_topics().topics)


# 3. Populate the "input_topic" with five Kafka messages in JSON format. The Kafka messages are
# provided below.

conf = {'bootstrap.servers': BOOTSTRAP_SERVER,
        'client.id': socket.gethostname()}

producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

datapoints = [
    {"myKey": 1, "myTimestamp": "2022-03-01T09:11:04+01:00"},
    {"myKey": 2, "myTimestamp": "2022-03-01T09:12:08+01:00"},
    {"myKey": 3, "myTimestamp": "2022-03-01T09:13:12+01:00"},
    {"myKey": 4, "myTimestamp": ""},
    {"myKey": 5, "myTimestamp": "2022-03-01T09:14:05+01:00"}
]

for msg in datapoints:
    print(msg)
    producer.produce("input_topic", key=str(msg['myKey']), value=msg['myTimestamp'], callback=acked)
    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is acknowledged.
    producer.poll(1)


# Unfortunately, all the Kafka messages are malformed, the field "myTimestamp" is written in
# the wrong time zone Europe/Berlin. However, all timestamps (if available) should be by
# default in UTC.
# Example:
# Wrong: 2022-03-01T09:11:04+01:00 -> Europe/Berlin
# Correct: 2022-03-01T08:11:04+00:00 -> UTC
# 4. Write a simple transformer application in python to consume all the messages from
# "input_topic" and write the corrected messages to "output_topic".
# 5. Dockerize your application, so it can be run with Docker.
https://webme.ie/how-to-run-a-python-app-with-docker-compose/