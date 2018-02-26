from persistance_mongo_layer.twitterdb_instance import DbInstance
from trash.acm_crawler.mysql import send_query
from tqdm import tqdm
#####{id, friends_count, friends}

query = "SELECT * from colleagues"
result = send_query(query)
friendship_map = {}

for record in tqdm(result) :
    author = record[0]
    friend = record[1]

    try:
        friendship_map[str(author) ].append(str(friend) )

    except Exception:
        friendship_map[str(author) ] = [str(friend)]

docs = []

for user in tqdm(friendship_map.keys() ):
    friends = friendship_map[user]
    doc = {}
    doc['id'] = user
    doc["friends_count"] = len(friends)
    doc['friends'] = friends
    docs.append(doc)



db = DbInstance().getDbInstance()
db['network'].insert_many(docs)