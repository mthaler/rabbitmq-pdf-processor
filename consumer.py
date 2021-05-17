import argparse
import time

import pika


# create a function which is called on incoming messages
def callback(ch, method, properties, body):
    pdf_process_function(body)


def pdf_process_function(msg):
    print(" PDF processing")
    print(" [x] Received " + str(msg))

    time.sleep(5) # delays for 5 seconds
    print(" PDF processing finished")
    return


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

# set up subscription on the queue
channel.basic_consume('pdf-process', callback, auto_ack=True)

# start consuming (blocks)
channel.start_consuming()
connection.close()
