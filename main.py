from services.cassandra_ import CassandraDatabase
from services.querier_RPC import QuerierRpcClient
import numpy as np
import ast
import random
import json

q = QuerierRpcClient()

def title_summary():
    repo = CassandraDatabase(project_name='papers', repo_name= 'title', id_sql_type='BIGINT',
                                               content_sql_type="TEXT")
    loc = 0
    top3 = 0
    top10 = 0
    for id, row in repo.list():
        result = ast.literal_eval(q.query(json.dumps({"text":row, "count":203})))
        sims = result['result']
        inter = result['keywords']
        index = np.where(np.array(list(sims.keys())) == id)[0][0]
        if index<3:
            top3+=1
        if index < 10:
            top10+=1
        loc += index

    print('{:.2f} top3 {} top 10 {}'.format(loc/ repo.count(), top3, top10))

    '''tootal count 203. USE: [13.62, 3: 106, 10: 148], upvotes: [20.56, 3: 83, 10: 124], 
    reviews:[21.75, 3: 84, 10: 122], jokes: [24.05, 3: 77, 10: 119],
     Infer: [27.19, 3: 60, 10: 106], Gru5: [49.05, 3: 41, 10: 68], upv_score256: [34.00, 3: 65, 10: 96]
    upvotes256: [28.29, 3: 71, 10: 104], reviews256: [33.76, 3: 55, 10: 95], gru256: [37.49, 3: 53, 10: 91],
     jokes256: [34.24, 3: 65, 10: 96], count_vec: [5.12, 3: 145, 10: 171]'''

def title_sents():
    repo = CassandraDatabase(project_name='papers', repo_name= 'title', id_sql_type='BIGINT',
                                               content_sql_type="TEXT")
    sent_sum_map_repo = CassandraDatabase(project_name='papers', repo_name='sent_sum_map', id_sql_type='BIGINT',
                                          content_sql_type="TEXT")
    loc = 0
    top3 = 0
    top10 = 0
    k=0
    for id, row in repo.list():
        k+=1
        print(k)
        result = ast.literal_eval(q.query(json.dumps({"text": row, "count": 203})))
        sims = result['result']
        inter = result['keywords']


        papers_ids = []
        for sent_id in list(sims.keys()):
            paper_id = int(sent_sum_map_repo.read(sent_id)[0])
            if paper_id not in papers_ids:
                papers_ids.append(paper_id)
            if paper_id == id:
                index = len(papers_ids)
                break
        # papers_ids = np.array(papers_ids)
        # index = np.where(np.array(papers_ids) == id)[0][0]
        if index<3:
            top3+=1
        if index < 10:
            top10+=1
        loc += index

    print('{:.2f} top3 {} top 10 {}'.format(loc/ repo.count(), top3, top10))

    '''tootal count 203. USE: [11.40, 3: 11, 10: 143], upvotes: [17.77, 3: 107, 10: 141],
     reviews: [17.71, 3: 105, 10: 143], jokes: [18.41, 3: 108, 10: 135],
     Infer: [17.05, 3: 101, 10: 137], Gru5: [37.89, 3: 47, 10: 83], upv_score256: [21.41, 3: 90, 10: 125]
    upvotes256: [19.49, 3: 99, 10: 131], reviews256: [20.05, 3: 99, 10: 133], gru256: [29.07, 3: 61, 10: 101],
     jokes256: [20.10, 3: 95, 10: 134], count_vec: [8.26, 3: 133, 10: 162]'''


def random_sent_summary():
    meta_repo = CassandraDatabase(project_name='papers', repo_name='meta', id_sql_type='BIGINT',
                                  content_sql_type="TEXT")
    encoded_sents_repo = CassandraDatabase(project_name='papers', repo_name='sents', id_sql_type='BIGINT',
                                           content_sql_type="TEXT")
    loc = 0
    top3 = 0
    top10 = 0
    for id, row in meta_repo.list():
        meta = ast.literal_eval(row.replace('nan', '\'\''))
        ids = meta['children']
        random_sent_id =  random.choice(ids)
        result = ast.literal_eval(q.query(json.dumps({"text": random_sent_id, "count": 205})))
        sims = result['result']
        inter = result['keywords']
        index = np.where(np.array(list(sims.keys())) == id)[0][0]
        if index<3:
            top3+=1
        if index < 10:
            top10+=1
        loc += index

    print('{:.2f} top3 {} top 10 {}'.format(loc/ meta_repo.count(), top3, top10))

    '''tootal count 203. USE: [9.46, 3: 135, 10: 159], upvotes: [9.68,3: 150, 10: 165], 
    reviews:[10.02, 3: 144, 10: 169], jokes: [10.32, 3: 153, 10: 166],
     Infer: [7.32, 3: 155, 10: 175], Gru5: [45.04, 3: 79, 10: 103], upv_score256: [25.11 top3 95 top 10 124]
    upvotes256: [17.69 top3 123 top 10 140], reviews256: [22.30 top3 124 top 10 140], gru256: [49.78 top3 62 top 10 84],
     jokes256: [25.33 top3 124 top 10 141], count_vec: [0.28 top3 199 top 10 201]'''

random_sent_summary()