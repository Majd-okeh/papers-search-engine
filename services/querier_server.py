import json
import pika
from services.cassandra_ import CassandraDatabase
import numpy as np
import os
import ast
from sklearn.metrics.pairwise import cosine_similarity
from services.encoders.USE_RPC import USERpcClient
from services.encoders.infer_RPC import InferRpcClient
from joblib import dump, load
import spacy
from collections import defaultdict

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'


'''vectors repos [title_, summary_], [USE, Infer, Gru5, upvotes, jokes, reviews
 upv_score256, upvotes256, reviews256, jokes256, gru256, count_vec] '''

nlp = spacy.load('en_core_web_sm')
meta_repo = CassandraDatabase(project_name='papers', repo_name='meta', id_sql_type='BIGINT', content_sql_type="TEXT")
repo_name = 'summary_USE'
encoder = USERpcClient()
# encoder = InferRpcClient()
# path = 'C:\zorba\storage\\vectorizer.joblib'
# vectorizer = load(path)
def encode(text):
    vector = str(encoder.encode(text)['encoded'])
    # vector = str(vectorizer.transform([text]).toarray()[0].tolist())
    return vector



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

def get_keywords_intersection(text):
    intersections = {}
    intersections = defaultdict(lambda: 0, intersections)
    for id, row in meta_repo.list():
        meta = ast.literal_eval(row.replace('nan', '\'\''))
        keywords_lemmas = meta['keywords_lemmas']
        for token in nlp(text):
            if not token.is_stop and len(token) > 3 and token.lemma_ in keywords_lemmas:
                intersections[id] += 1

    return intersections


def query(text, count):
    intersections = get_keywords_intersection(text)
    intersections = {k: v for k, v in sorted(intersections.items(), key=lambda item: item[1], reverse=True)[:count]}

    sims = cosine_similarity([ast.literal_eval(encode(text))], docs)
    '''sort the docs based on the cosine similarity'''
    indexes = sims[0].argsort()[-count:][::-1]
    cos = sims[0][indexes]
    docs_ids = ids[indexes]
    similarities = { docs_ids[i]:cos[i] for i in range(count)}
    print(similarities)
    print(intersections)
    return similarities, intersections
    # return np.array([1,1])

def on_request(ch, method, props, body):
    print(" [.] quering(%s)" % body)
    body = json.loads(str(body.decode('utf8')))
    vector = body['text']
    count = body['count']
    result, intersections = query(vector, count)
    response = {'result': result, 'keywords': intersections}
    response = json.dumps(str(response))
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