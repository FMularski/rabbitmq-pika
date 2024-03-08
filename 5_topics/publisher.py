import sys

import pika
from settings import MQ_URL

# establish a connection
connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
channel = connection.channel()

# declare an exchange of type topic - messages will be delivered to all matching queues
channel.exchange_declare(exchange="typed_user_logs", exchange_type="topic")

# publish a message
# example python3 publisher.py filip warning Achtung!
try:
    user = sys.argv[1]
    log_type = sys.argv[2]
    message = f"{user}: [{log_type}] {sys.argv[3]}"
except IndexError:
    user = "system"
    log_type = "info"
    message = "system: [info] Hello World!"

channel.basic_publish(
    exchange="typed_user_logs",
    routing_key=f"{user}.{log_type}",
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,  # persistent messages are not lost if rabbitmq dies
    ),
)
print("Message sent:", message)

# close the connection
connection.close()
