# -*- coding: utf-8 -*-

# prendere tutti gli articoli scritti da un autore
## per ogni articolo prendere topic, score, numero di citazioni
### creare doc= {info.interests.all:{id_topic{score, display} } }
#### generare score per topic -> rule:
    # sum(score*numero_cit per quell articolo)

from persistance_mongo_layer.twitterdb_instance import DbInstance
from trash.acm_crawler import mysql
from tqdm import tqdm

def topic_mining(author_doc, his_papers):
    # type:(dict, list)-> dict
    for p in his_papers:
        p2topics = db['paper2topic'].find({
            'dblp_key': p['dblp_key']
        }, {'acm_topic_id':1, 'topic': 1, 'score': 1} )
        cite_count_cursor = mysql.send_query('SELECT cite_count FROM papers p WHERE p.dblp_key ="'+p['dblp_key']+'"' )
        cite_count = int(cite_count_cursor.fetchone()[0]) + 1

        for doc_topic in p2topics:
            topic_id = doc_topic['acm_topic_id']
            try:
                current_score = author_doc['info']['interests']['all'][topic_id]['score']
                new_score = current_score + (int(doc_topic['score'] ) * cite_count ) # we could multiply for 1/num occurrencies
                author_doc['info']['interests']['all'][topic_id]['score'] = new_score
            except KeyError:
                topic = doc_topic['topic']
                score = int(doc_topic['score']) * cite_count
                author_doc['info']['interests']['all'][topic_id] = {}
                author_doc['info']['interests']['all'][topic_id]['score'] = score
                author_doc['info']['interests']['all'][topic_id]['display'] = topic

    return author_doc




# main
db = DbInstance().getDbInstance()
authors = set()
papers = db['cliques2analyze'].find({})
authors_docs = []

topic_occurrencies_in_papers = db['paper2topic'].aggregate([{'$group':{'_id':"$topic" ,'count':{'$sum': 1} } } ])
topic_occurrencies_in_papers = [o for o in topic_occurrencies_in_papers]

for paper in papers:
    authors = authors.union(set(paper['nodes'] ) )

for author in tqdm(authors):
    author_doc = {
        'user': author,
        'info':{'interests':{'all': {} } }
    }

    his_papers = db['cliques2analyze'].find({
        "nodes":{'$in':[author] }
    }, {'paper_id': 1, 'dblp_key': 1} )

    his_papers = [p for p in his_papers]
    #take paper topic and its score
    topic_mining(author_doc, his_papers)
    authors_docs.append(author_doc)


db['user_infos'].insert_many(authors_docs)




    #take cite count








