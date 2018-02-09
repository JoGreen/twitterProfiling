from trash.acm_crawler import mysql
from persistance_mongo_layer.twitterdb_instance import DbInstance

#take paper from mysql and create respective docs like {nodes:[author1, author2, etc .. ], count: nodes.length }

mysql_cursor = mysql.send_query("SELECT p.paper_id, author_id, p.dblp_key FROM papers p, paper_authors pa where p.paper_id = pa.paper_id;")

docs = []
doc = {'paper_id': None}
db = DbInstance().getDbInstance()
collection = db['cliques2analyze']
for row in mysql_cursor:
    paper_id = str(row[0])
    author  = str(row[1])
    dblp_key = str(row[2])
    if paper_id == doc['paper_id'] :
        doc['nodes'].append(author)
    else:
        if len(doc.keys() ) > 0 and doc['paper_id'] != None:
            doc['count'] = len(doc['nodes'])
            docs.append(doc)
            doc = {'nodes': [author], 'paper_id': paper_id, 'dblp_key': dblp_key}

        else:
            doc = {'nodes': [author], 'paper_id': paper_id, 'dblp_key': dblp_key}

    if len(docs) > 499:
        collection.insert_many(docs)
        docs = []
        print 'saved 500 docs'

collection.insert_many(docs)





