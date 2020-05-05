#!/usr/bin/env python
import json
import logging
import pika
import pandas as pd
import sentencepiece as spm
import tensorflow as tf
import tensorflow_hub as hub
import  numpy as np
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'


'''here to donload the model:
https://tfhub.dev/google/universal-sentence-encoder-large/5?tf-hub-format=compressed'''

embed = hub.load(r"C:\zorba\storage\USE\large-5")


queue_name = 'USE#en'
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.queue_declare(queue=queue_name)

def encode(text):
    text = text.decode("utf-8") .strip()
    input = tf.convert_to_tensor(np.asarray([text]))

    return embed(input)[0].numpy()

def on_request(ch, method, props, body):
    print(" [.] encode(%s)" % body)
    response = {'encoded': encode(body).tolist()}
    response = json.dumps(response)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue = queue_name, on_message_callback=on_request)

print('ready to serve')
channel.start_consuming()