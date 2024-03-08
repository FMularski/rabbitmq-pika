import sys

import pika
from settings import MQ_URL

# establish a connection
connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
channel = connection.channel()

# marking queues as durable prevents losing them if rabbitmq dies
channel.queue_declare(queue="work_queue", durable=True)

# publish a message
try:
    message = sys.argv[1]
except IndexError:
    message = "Hello World!"

channel.basic_publish(
    exchange="",
    routing_key="hello",
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,  # persistent messages are not lost if rabbitmq dies
    ),
)
print("Message sent:", message)

# close the connection
connection.close()
