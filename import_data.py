import pandas as pd
from dateutil import parser
import nltk
import ast
from services.cassandra_ import CassandraDatabase

title_repo = CassandraDatabase(project_name='papers', repo_name='title', id_sql_type='BIGINT', content_sql_type="TEXT")
summary_repo = CassandraDatabase(project_name='papers', repo_name='summary', id_sql_type='BIGINT',
                                 content_sql_type="TEXT")
meta_repo = CassandraDatabase(project_name='papers', repo_name='meta', id_sql_type='BIGINT', content_sql_type="TEXT")

def split_into_sents():
    count = 0
    sents_repo = CassandraDatabase(project_name='papers', repo_name='sentences', id_sql_type='BIGINT', content_sql_type="TEXT")
    for id, text in summary_repo.list():
        row = meta_repo.read(id)[0]
        meta = ast.literal_eval(row.replace('nan', '\'\''))
        meta['children'] = []
        sent_text = nltk.sent_tokenize(text)
        for sentence in sent_text:
            sents_repo.write(count, sentence)
            meta['children'].append(count)
            count+=1
        meta_repo.write(id, str(meta))

def parse_record(record):
    title = record['paper'].lower().replace(u'\xa0', u' ').strip()
    summary = record['summary'].lower().replace(u'\xa0', u' ').strip()
    try:
        date = parser.parse(record['publish date'].replace(u'\xa0', u' ').strip())
    except:
        date = ''
    authors = record['authors'].lower().replace(u'\xa0', u' ').strip().split(', ')
    keywords = record['keywords'].lower().replace(u'\xa0', u' ').strip().split(', ')
    link = record['link']
    id = record['id']

    return title, summary, {'date': str(date), 'authors': authors, 'keywords': keywords, 'link': link, 'id': id}

def import_from_csv(file_path):
    df = pd.read_csv(file_path)
    df['keywords'] = df['keywords'].fillna('')
    df['authors'] = df['authors'].fillna('')
    df['publish date'] = df['publish date'].fillna('')
    for i in range(len(df)):
        record = df.iloc[i]
        parse_record(record)
        title, summary, dict = parse_record(record)
        title_repo.write(dict['id'], title)
        summary_repo.write(dict['id'], summary)
        meta_repo.write(dict['id'], str(dict))
        print(i)


file_path = 'C:\zorba\work\efop\papers.csv'
# import_from_csv(file_path)
split_into_sents()