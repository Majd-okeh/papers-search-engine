from services.keywords_RPC import KeywordExtractorRpcClient
from services.cassandra_ import CassandraDatabase
from services.cassandra_ import CassandraDatabase
import ast
import spacy


nlp = spacy.load('en_core_web_sm')
meta_repo = CassandraDatabase(project_name='papers', repo_name='meta', id_sql_type='BIGINT', content_sql_type="TEXT")
summary_repo = CassandraDatabase(project_name='papers', repo_name='summary', id_sql_type='BIGINT',
                                     content_sql_type="TEXT")

extractor = KeywordExtractorRpcClient()
i = 0
for id, row in meta_repo.list():
    meta = ast.literal_eval(row.replace('nan', '\'\''))
    keywords = meta['keywords']
    text = summary_repo.read(id)[0]
    if len(keywords)!=1:
        keywords += list(extractor.extract(text)['keywords'])
    else:
        keywords = list(extractor.extract(text)['keywords'])

    keywords_lemmas = [token.lemma_ for token in nlp(' '.join(keywords))]
    meta['keywords_lemmas'] = list(set(keywords_lemmas))
    meta['keywords'] = list(set(keywords))
    meta_repo.write(id, str(meta))
