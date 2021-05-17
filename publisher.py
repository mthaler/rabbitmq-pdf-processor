import argparse

parser = argparse.ArgumentParser()
parser.add_argument("user", help="RabbitMQ user")
parser.add_argument("password", help="RabbitMQ password")
args = parser.parse_args()

url = f'amqp://{args.user}:{args.password}@localhost/pdf-processor'

print(url)
