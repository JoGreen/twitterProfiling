import MySQLdb
import connection_data as data

user = data.user
pwd = data.pwd
db = 'dblp-sigweb'
host = data.host
db = MySQLdb.connect(host, user, pwd, db)


def get_cursor():
    return db.cursor()

def send_query(query):
    c = get_cursor()
    c.execute(query)
    return c

