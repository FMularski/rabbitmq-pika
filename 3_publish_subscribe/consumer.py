import pika
from settings import MQ_URL

# establish a connection
connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
channel = connection.channel()

# declare a fanout exchange
channel.exchange_declare(exchange="logs", exchange_type="fanout")

# declare a temporary queue  with a random name
# exclusive=True -> after closing the conneciton the queue will be deleted
result = channel.queue_declare(queue="", exclusive=True)
# get the name of the temporary queue
queue_name = result.method.queue

# bind the tempopary queue to the exchange
channel.queue_bind(exchange="logs", queue=queue_name)


# a dummy time-consuming job
def callback(channel, method, properties, body):
    print("Consumed message:", body)
    # if consumer dies before acknowledging the message,
    # the message is passed to other consumer
    channel.basic_ack(delivery_tag=method.delivery_tag)


# register the callback function as the code to be executed
# when a message is published to the "hello" queue
channel.basic_consume(queue=queue_name, on_message_callback=callback)  # removed auto_ack=True

# enter the never-ending loop
print("Waiting for messages...")
channel.start_consuming()
