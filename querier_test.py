from services.cassandra_ import CassandraDatabase
from services.querier_RPC import QuerierRpcClient
import numpy as np
from services.encoders.USE_RPC import USERpcClient
from services.encoders.infer_RPC import InferRpcClient
from joblib import dump, load
import json
import ast


q = QuerierRpcClient()
# path = 'C:\zorba\storage\\vectorizer.joblib'
# vectorizer = load(path)
# encoder = USERpcClient()
encoder = InferRpcClient()

summary_repo = CassandraDatabase(project_name='papers', repo_name= 'title', id_sql_type='BIGINT',
                                           content_sql_type="TEXT")
loc = 0
top3 = 0
top10 = 0
j = 0
for id, row in summary_repo.list():
    result = ast.literal_eval(q.query(json.dumps({"text":row, "count":203})))
    sims = result['result']
    inter = result['keywords']
    index = np.where(np.array(list(sims.keys())) == id)[0][0]
    print(index, id)
    j+=1
    if j==10:
        break

