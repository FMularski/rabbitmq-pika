import sys

import pika
from settings import MQ_URL

# establish a connection
connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
channel = connection.channel()

# declare an exchange of type fanout - message will be delivered to all binded queues
channel.exchange_declare(exchange="logs", exchange_type="fanout")

# publish a message
try:
    message = sys.argv[1]
except IndexError:
    message = "Hello World!"

channel.basic_publish(
    exchange="logs",
    routing_key="",  # routing_key must be passed as an argument, but it is ignored for fanout exchanges
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,  # persistent messages are not lost if rabbitmq dies
    ),
)
print("Message sent:", message)

# close the connection
connection.close()
