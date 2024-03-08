import time

import pika
from settings import MQ_URL

# establish a connection
connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
channel = connection.channel()

# redeclaring queues in consumers is a good practice
# as it is not granted that publisher declared its queue before
channel.queue_declare(queue="work_queue", durable=True)


# a dummy time-consuming job
def callback(channel, method, properties, body):
    print("Consuming message:", body)
    time.sleep(body.count(b"."))
    print("Done")
    # if consumer dies before acknowledging the message,
    # the message is passed to other consumer
    channel.basic_ack(delivery_tag=method.delivery_tag)


# this defines how many messages can be assigned to a worker
# prefetch_count=1 --> worker: I do not take next message when I am still working on a previous one.
channel.basic_qos(prefetch_count=1)

channel.basic_consume(queue="work_queue", on_message_callback=callback)  # removed auto_ack=True

# enter the never-ending loop
print("Waiting for messages...")
channel.start_consuming()
