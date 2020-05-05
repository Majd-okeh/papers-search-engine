from services.encoders.infer_sent import InferSent
import os
import pika
import json


config = {
    'modelPath': 'C:\zorba\storage\\',
    "modelName": "gru256.pkl",
    "word2VecModelName": "glove.840B.300d.lower.txt",
    "encLSTMDim": 256,
    "encoderType": "GRU",
}

infer = InferSent(config)

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
queue = 'infer#en'

channel.queue_declare(queue=queue)

def encode(text):
    text = text.decode("utf-8") .strip()
    return infer.encode(text)

def on_request(ch, method, props, body):
    print(" [.] encode(%s)" % body)
    # response = encode(body)
    response = {'encoded': encode(body).tolist()}
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