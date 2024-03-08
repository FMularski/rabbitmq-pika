import sys

import pika
from settings import MQ_URL

# establish a connection
connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
channel = connection.channel()

# declare an exchange of type fanout - message will be delivered to all binded queues
channel.exchange_declare(exchange="typed_logs", exchange_type="direct")

# publish a message
try:
    log_type = sys.argv[1]
    message = f"[{log_type}] {sys.argv[2]}"
except IndexError:
    log_type = "info"
    message = "[info] Hello World!"

channel.basic_publish(
    exchange="typed_logs",
    routing_key=log_type,
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,  # persistent messages are not lost if rabbitmq dies
    ),
)
print("Message sent:", message)

# close the connection
connection.close()
