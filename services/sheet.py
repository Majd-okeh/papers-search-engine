from services.cassandra_ import CassandraDatabase
import ast


# meta_repo = CassandraDatabase(project_name='papers', repo_name='meta', id_sql_type='BIGINT', content_sql_type="TEXT")
# sent_sum_map_repo = CassandraDatabase(project_name='papers', repo_name='sent_sum_map', id_sql_type='BIGINT', content_sql_type="TEXT")
#
# for id, row in meta_repo.list():
#     meta = ast.literal_eval(row.replace('nan', '\'\''))
#     ids = meta['children']
#     for child_id in ids:
#         sent_sum_map_repo.write(child_id, str(id))
#
# print(sent_sum_map_repo.count())

vecs_repo = CassandraDatabase(project_name='papers', repo_name= 'sents_gru256', id_sql_type='BIGINT',
                                               content_sql_type="TEXT")
print(vecs_repo.count())
print(len(ast.literal_eval(vecs_repo.read(10)[0])))