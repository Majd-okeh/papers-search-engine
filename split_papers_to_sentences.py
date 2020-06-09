from services.cassandra_ import CassandraDatabase
import ast
import random

meta_repo = CassandraDatabase(project_name='papers', repo_name='meta', id_sql_type='BIGINT', content_sql_type="TEXT")
sent_sum_map_repo = CassandraDatabase(project_name='papers', repo_name='sent_sum_map', id_sql_type='BIGINT', content_sql_type="TEXT")
encoded_sents_repo = CassandraDatabase(project_name='papers', repo_name='sents_count_vec', id_sql_type='BIGINT',
                                           content_sql_type="TEXT")

for id, row in meta_repo.list():
    meta = ast.literal_eval(row.replace('nan', '\'\''))
    ids = meta['children']
    print(id, ids, random.choice(ids))
    print(encoded_sents_repo.read(random.choice(ids))[0])
    break
    for child_id in ids:
        sent_sum_map_repo.write(child_id, str(id))



