import uuid

import pika
from settings import MQ_URL


class FibonacciRpcClient(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue="", exclusive=True)
        self.callback_queue = result.method.queue  # name of the queue for receiveing responses

        self.channel.basic_consume(  # consume incoming responses
            queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True
        )

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange="",
            routing_key="request_queue",  # send a request to the request queue
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,  # tell server to publish responses to this "response" queue
                correlation_id=self.corr_id,  # to distinguish request-response pairs
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
            body=str(n),
        )

        while self.response is None:
            self.connection.process_data_events()

        return int(self.response)


rpc_client = FibonacciRpcClient()

print(" [x] Requesting factorial(5)")
response = rpc_client.call(5)
print(f" [.] Got {response}")
