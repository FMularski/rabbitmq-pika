import sys

import pika
from settings import MQ_URL

# establish a connection
connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
channel = connection.channel()

# declaring a queue guarantees that the message will not be dropped
channel.queue_declare(queue="hello")

# publish a message
try:
    message = sys.argv[1]
except IndexError:
    message = "Hello World!"

channel.basic_publish(exchange="", routing_key="hello", body=message)
print("Message sent:", message)

# close the connection
connection.close()
