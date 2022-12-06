import time
from kafka.admin import KafkaAdminClient, NewTopic
from builder import *

print('Giving Kafka a bit of time to start up…')
time.sleep(5)
print('Kafka Broker is warmed up.')

# 2. Prepare two Kafka topics.
# One topic is called "input_topic".
# The other topic is called "output_topic".

admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER)
delete_topics(admin_client)
create_topics(admin_client)


# 3. Populate the "input_topic" with five Kafka messages in JSON format. The Kafka messages are
# provided below.

populate_input_topic()

# Unfortunately, all the Kafka messages are malformed, the field "myTimestamp" is written in
# the wrong time zone Europe/Berlin. However, all timestamps (if available) should be by
# default in UTC.
# Example:
# Wrong: 2022-03-01T09:11:04+01:00 -> Europe/Berlin
# Correct: 2022-03-01T08:11:04+00:00 -> UTC
# 4. Write a simple transformer application in python to consume all the messages from
# "input_topic" and write the corrected messages to "output_topic".

transfomer_input_to_output()

# 5. Dockerize your application, so it can be run with Docker.


