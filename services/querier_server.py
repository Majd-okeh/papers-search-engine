import json
import pika
from services.cassandra_ import CassandraDatabase
import numpy as np
import os
import ast
from sklearn.metrics.pairwise import cosine_similarity
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

repo_name = 'sents_count_vec'


def get_docs(repo_name):
    docs = []
    ids = []
    vecs_repo = CassandraDatabase(project_name='papers', repo_name= repo_name, id_sql_type='BIGINT',
                                           content_sql_type="TEXT")
    for id, row in vecs_repo.list():
        docs.append(ast.literal_eval(row))
        ids.append(id)
    return np.array(ids), np.array(docs)


ids, docs = get_docs(repo_name)


queue_name = 'querier'
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.queue_declare(queue=queue_name)

def query(vector, count):
    sims = cosine_similarity([ast.literal_eval(vector)], docs)
    indexes = sims[0].argsort()[-count:][::-1]
    return ids[indexes]

def on_request(ch, method, props, body):
    print(" [.] quering(%s)" % body)
    body = json.loads(body.decode('utf8').replace("'", '"'))
    vector = body['vector']
    count = body['count']
    response = {'result': query(vector, count).tolist()}
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