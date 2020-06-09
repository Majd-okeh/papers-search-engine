from sklearn.feature_extraction.text import CountVectorizer
from joblib import dump, load
from services.cassandra_ import CassandraDatabase
import time
from sklearn.feature_extraction import stop_words


title_repo = CassandraDatabase(project_name='papers', repo_name='title', id_sql_type='BIGINT', content_sql_type="TEXT")
summary_repo = CassandraDatabase(project_name='papers', repo_name='summary', id_sql_type='BIGINT',
                                 content_sql_type="TEXT")

stop_words = stop_words.ENGLISH_STOP_WORDS
corpus = []
for id, row in summary_repo.list():
    corpus.append(row)
    corpus.append(title_repo.read(id)[0])

path = 'C:\zorba\storage\\vectorizer.joblib'


vectorizer = CountVectorizer(stop_words = stop_words)
t1 = time.time()
x = vectorizer.fit(corpus)
print(time.time() - t1)
print(len(vectorizer.get_feature_names()))

dump(vectorizer, path)
# vectorizer = load(path)
