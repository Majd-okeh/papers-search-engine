from services.extract_keywords import KeywordExtractor
import os
import pika
import json


keyword_extractor = KeywordExtractor()

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
queue = 'KeywordExtractor#en'

channel.queue_declare(queue=queue)

def extract(text):
    text = text.decode("utf-8") .strip()
    return keyword_extractor.get_keywords(text)

def on_request(ch, method, props, body):
    print(" [.] extract(%s)" % body)
    # response = encode(body)
    response = {'keywords': extract(body)}
    response = json.dumps(response)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue, on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()