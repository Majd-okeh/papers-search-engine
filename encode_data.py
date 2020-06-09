from services.cassandra_ import CassandraDatabase
from services.encoders.USE_RPC import USERpcClient
from services.encoders.infer_RPC import InferRpcClient
from joblib import dump, load
import numpy as np
import nltk
import ast


'''vectors repos [title_, summary_], [USE, Infer, Gru5, upvotes, jokes, reviews
 upv_score256, upvotes256, reviews256, jokes256, gru256, count_vec] '''
def encode():
    title_repo = CassandraDatabase(project_name='papers', repo_name='title', id_sql_type='BIGINT',
                                   content_sql_type="TEXT")
    summary_repo = CassandraDatabase(project_name='papers', repo_name='summary', id_sql_type='BIGINT',
                                     content_sql_type="TEXT")

    encoded_title_repo = CassandraDatabase(project_name='papers', repo_name='title_reviews', id_sql_type='BIGINT',
                                           content_sql_type="TEXT")
    encoded_summary_repo = CassandraDatabase(project_name='papers', repo_name='summary_reviews', id_sql_type='BIGINT',
                                             content_sql_type="TEXT")
    # encoder = USERpcClient()
    encoder = InferRpcClient()
    # path = 'C:\zorba\storage\\vectorizer.joblib'
    # vectorizer = load(path)
    i=0
    for id, row in title_repo.list():
        print(i)
        i+=1
        title_vec = str(encoder.encode(row)['encoded'])
        summary_vec = str(encoder.encode(summary_repo.read(id)[0])['encoded'])
        # title_vec = str(vectorizer.transform([row]).toarray()[0].tolist())
        # summary_vec = str(vectorizer.transform([summary_repo.read(id)[0]]).toarray()[0].tolist())
        encoded_title_repo.write(id, title_vec)
        encoded_summary_repo.write(id, summary_vec)

def encode_sents():
    sents_repo = CassandraDatabase(project_name='papers', repo_name='sentences', id_sql_type='BIGINT',
                                   content_sql_type="TEXT")
    encoded_sents_repo = CassandraDatabase(project_name='papers', repo_name='sents_count_vec', id_sql_type='BIGINT',
                                             content_sql_type="TEXT")
    # encoder = InferRpcClient()
    # encoder = USERpcClient()
    path = 'C:\zorba\storage\\vectorizer.joblib'
    vectorizer = load(path)
    i=0
    for id, row in sents_repo.list():
        print(i)
        i+=1
        # sent_vec = str(encoder.encode(row)['encoded'])
        sent_vec = str(vectorizer.transform([row]).toarray()[0].tolist())
        encoded_sents_repo.write(id, sent_vec)

encode_sents()