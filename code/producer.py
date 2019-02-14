from kafka import KafkaProducer
from kafka.errors import KafkaError
import msgpack
import json
import requests
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Asynchronous by default
# future = producer.send('my-topic', b'raw_bytes')

# Block for 'synchronous' sends
# try:
    # record_metadata = future.get(timeout=10)
# except KafkaError:
    # Decide what to do if produce request failed...
    # log.exception()
    # pass

# Successful result returns assigned partition and offset
# print (record_metadata.topic)
# print (record_metadata.partition)
# print (record_metadata.offset)

# # produce keyed messages to enable hashed partitioning
# producer.send('my-topic', key=b'foo', value=b'bar')

# # encode objects via msgpack
# producer = KafkaProducer(value_serializer=msgpack.dumps)
# producer.send('msgpack-topic', {'key': 'value'})

# # produce json messages
# producer.send('json-topic', {'key': 'value'})

# # produce asynchronously
# for _ in range(2):
#     producer.send('my-topic', b'msg')

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception
def updates():
	producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
	response = requests.get("https://www.hassanapi.site/getapi.php")
	json_data = response.json()
	for column in json_data:
		producer.send('my-topic', column).add_errback(on_send_error)
	producer.flush()

	# configure multiple retries
	producer = KafkaProducer(retries=5)
# produce asynchronously with callbacks

# block until all async messages are sent

def run():
    while True:
        updates()
        time.sleep(30)

run()