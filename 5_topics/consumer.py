import sys

import pika
from settings import MQ_URL

# establish a connection
connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
channel = connection.channel()

# declare a topic exchange
channel.exchange_declare(exchange="typed_user_logs", exchange_type="topic")

# declare a temporary queue  with a random name
# exclusive=True -> after closing the conneciton the queue will be deleted
result = channel.queue_declare(queue="", exclusive=True)
# get the name of the temporary queue
queue_name = result.method.queue

# this consumer's queue will receive messages by specific routing keys
# ex. consumer 1: python3 consumer.py filip.info - only receive filip's info logs
#     consumer 2: python3 consumer.py filip.* - receive all filip's logs
#     consumer 3: python3 consumer.py *.error - receive all error logs
user_log_types = sys.argv[1:]
for user_log_type in user_log_types:
    channel.queue_bind(exchange="typed_user_logs", queue=queue_name, routing_key=user_log_type)


# a dummy time-consuming job
def callback(channel, method, properties, body):
    print("Consumed message:", body)
    # if consumer dies before acknowledging the message,
    # the message is passed to other consumer
    channel.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=queue_name, on_message_callback=callback)  # removed auto_ack=True

# enter the never-ending loop
print("Waiting for messages...")
channel.start_consuming()
