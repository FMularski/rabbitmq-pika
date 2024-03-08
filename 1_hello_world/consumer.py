import pika
from settings import MQ_URL

# establish a connection
connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
channel = connection.channel()

# redeclaring queues in consumers is a good practice
# as it is not granted that publisher declared its queue before
channel.queue_declare(queue="hello")


# callback for receving a message from the queue
def callback(channel, method, properties, body):
    print("-------======== channel =========---------")
    print("instance:", channel)
    print(channel.__dict__)
    print("-------======== method =========---------")
    print("method:", method)
    print(method.__dict__)
    print("-------======== properties =========---------")
    print("properties:", properties)
    print(properties.__dict__)
    print("Message consumed:", body)


# register the callback function as the code to be executed
# when a message is published to the "hello" queue
channel.basic_consume(queue="hello", auto_ack=True, on_message_callback=callback)

# enter the never-ending loop
print("Waiting for messages...")
channel.start_consuming()
