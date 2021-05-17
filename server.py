import argparse
from textwrap import wrap

import pika
from reportlab.pdfgen import canvas


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


def on_request(ch, method, props, body):
    create_pdf(body)
    file = open("/tmp/out.pdf", "rb")
    data = file.read()
    ch.basic_publish(exchange='', routing_key=props.reply_to, properties=pika.BasicProperties(correlation_id=props.correlation_id), body=data)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# set up subscription on the queue
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='pdf-processor', on_message_callback=on_request)

# start consuming (blocks)
channel.start_consuming()
connection.close()
