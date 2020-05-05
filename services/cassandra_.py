from cassandra.cluster import Cluster

class CassandraDatabase():
    def __init__(self, project_name = 'papers', repo_name = 'test', id_sql_type = 'BIGINT',content_sql_type="TEXT"):
        cluster = Cluster()
        self.session = cluster.connect()
        self.session.set_keyspace(project_name)
        self.session.execute(
            f'''CREATE TABLE IF NOT EXISTS "{repo_name}" (id {id_sql_type} PRIMARY KEY, content {content_sql_type});''')
        self.write_statement = self.session.prepare(f'''UPDATE "{repo_name}" SET content=:content WHERE id=:id''')
        self.read_statement = self.session.prepare(f'''SELECT content FROM "{repo_name}" WHERE id=?''')
        self.count_statement = self.session.prepare(f'''SELECT COUNT(id) FROM "{repo_name}"''')
        self.list_statement = self.session.prepare(f'''SELECT id, content FROM "{repo_name}"''')

    def write(self, id, data):
        self.session.execute(self.write_statement,  {'id' : id, 'content' : data}, timeout=100.0)

    def read(self, id):
        return self.session.execute(self.read_statement, (id, ), timeout=100.0).one()

    def list(self):
        return self.session.execute(self.list_statement)

    def count(self):
        return  self.session.execute(self.count_statement).one()[0]


# repo = CassandraDatabase()
#
# data = 'hello1'
#
# id = 3
# repo.write(id, data)
# print(repo.read(3))


# print(results.one()[0])
#
#
# results = session.execute(list_statement)
# for row in results:
#     print(row[0], row[1])