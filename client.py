import argparse

import pika

parser = argparse.ArgumentParser()
parser.add_argument("user", help="RabbitMQ user")
parser.add_argument("password", help="RabbitMQ password")
args = parser.parse_args()

url = f'amqp://{args.user}:{args.password}@localhost/pdf-processor'

print(f'RabbitMQ URL: {url}')

params = pika.URLParameters(url)
params.socket_timeout = 5

# connect to RabbitMQ broker
print('Connecting...')
connection = pika.BlockingConnection(params)
channel = connection.channel()
print('Connected')

# Declare a queue
print('Creating queue')
channel.queue_declare(queue='pdf-process')
print('Queue created')

# Read our text
f = open("tale_of_two_cities.txt", "r")
text = f.read()

channel.basic_publish(exchange='', routing_key='pdf-process', body=text)
print("Message sent to consumer")
