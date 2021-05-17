import argparse
import uuid

import pika


class Client(object):

    def __init__(self, params):
        # connect to RabbitMQ broker
        print('Connecting...')
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        print('Connected')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        # create a queue
        print('Creating queue')
        self.channel.queue_declare(queue='pdf-processor')
        print('Queue created')

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, text):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='pdf-processor',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=text.encode())
        while self.response is None:
            self.connection.process_data_events()
        return self.response


parser = argparse.ArgumentParser()
parser.add_argument("user", help="RabbitMQ user")
parser.add_argument("password", help="RabbitMQ password")
args = parser.parse_args()

url = f'amqp://{args.user}:{args.password}@localhost/pdf-processor'

print(f'RabbitMQ URL: {url}')

params = pika.URLParameters(url)
params.socket_timeout = 5

# Read our text
f = open("tale_of_two_cities.txt", "r")
text = f.read()

pdf_processor_rpc = Client(params)
response = pdf_processor_rpc.call(text)
f = open("tale_of_two_cities.pdf", "wb")
f.write(response)
print(f'Written {f.name}')
