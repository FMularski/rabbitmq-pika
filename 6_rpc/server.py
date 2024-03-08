import pika
from settings import MQ_URL


def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n - 1)


connection = pika.BlockingConnection(pika.URLParameters(MQ_URL))
channel = connection.channel()

channel.queue_declare(queue="request_queue")


def on_request(ch, method, props, body):
    n = int(body)
    print(f" [.] factorial({n})")
    response = factorial(n)

    ch.basic_publish(
        exchange="",
        routing_key=props.reply_to,  # send response directly to the queue "pointed" by the client when sending the request
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id,
            delivery_mode=pika.DeliveryMode.Persistent,
        ),  # to distinguish request-response pairs
        body=str(response),
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue="request_queue", on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()
