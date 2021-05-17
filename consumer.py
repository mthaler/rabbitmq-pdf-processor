import argparse
from textwrap import wrap

import pika
from reportlab.pdfgen import canvas


# create a function which is called on incoming messages
def callback(ch, method, properties, body):
    create_pdf(body)


def create_pdf(msg):
    text = msg.decode()
    print(f'Creating PDF from {text}')
    c = canvas.Canvas("/tmp/out.pdf")
    y = 800
    for line in wrap(text, 100):
        print(line)
        c.drawString(15, y, line)
        y -= 15

    c.save()
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
