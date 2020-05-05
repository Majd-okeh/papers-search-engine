from services.cassandra_ import CassandraDatabase
from services.querier_RPC import QuerierRpcClient
import numpy as np
import ast

q = QuerierRpcClient()

def title_summary():
    vecs_repo = CassandraDatabase(project_name='papers', repo_name= 'title_reviews', id_sql_type='BIGINT',
                                               content_sql_type="TEXT")
    loc = 0
    top3 = 0
    top10 = 0
    for id, row in vecs_repo.list():
        result = np.array(q.query({"vector":row, "count":205})['result'])
        index = np.where(np.array(result) == id)[0][0]
        if index<3:
            top3+=1
        if index < 10:
            top10+=1
        loc += index

    print('{:.2f} top3 {} top 10 {}'.format(loc/ vecs_repo.count(), top3, top10))

    '''tootal count 203. USE: [13.62, 3: 106, 10: 148], upvotes: [20.56, 3: 83, 10: 124], 
    reviews:[21.75, 3: 84, 10: 122], jokes: [24.05, 3: 77, 10: 119],
     Infer: [27.19, 3: 60, 10: 106], Gru5: [49.05, 3: 41, 10: 68], upv_score256: [34.00, 3: 65, 10: 96]
    upvotes256: [28.29, 3: 71, 10: 104], reviews256: [33.76, 3: 55, 10: 95], gru256: [37.49, 3: 53, 10: 91],
     jokes256: [34.24, 3: 65, 10: 96], count_vec: [5.12, 3: 145, 10: 171]'''

def title_sents():
    vecs_repo = CassandraDatabase(project_name='papers', repo_name= 'title_count_vec', id_sql_type='BIGINT',
                                               content_sql_type="TEXT")
    sent_sum_map_repo = CassandraDatabase(project_name='papers', repo_name='sent_sum_map', id_sql_type='BIGINT',
                                          content_sql_type="TEXT")
    loc = 0
    top3 = 0
    top10 = 0
    k=0
    for id, row in vecs_repo.list():
        k+=1
        print(k)
        result = q.query({"vector":row, "count":3000})['result']
        papers_ids = []
        for sent_id in result:
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

    print('{:.2f} top3 {} top 10 {}'.format(loc/ vecs_repo.count(), top3, top10))

    '''tootal count 203. USE: [11.40, 3: 11, 10: 143], upvotes: [17.77, 3: 107, 10: 141],
     reviews: [17.71, 3: 105, 10: 143], jokes: [18.41, 3: 108, 10: 135],
     Infer: [17.05, 3: 101, 10: 137], Gru5: [37.89, 3: 47, 10: 83], upv_score256: [21.41, 3: 90, 10: 125]
    upvotes256: [19.49, 3: 99, 10: 131], reviews256: [20.05, 3: 99, 10: 133], gru256: [29.07, 3: 61, 10: 101],
     jokes256: [20.10, 3: 95, 10: 134], count_vec: [8.26, 3: 133, 10: 162]'''

title_sents()