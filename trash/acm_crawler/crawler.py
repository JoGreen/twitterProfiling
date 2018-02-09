import urllib2, re, time
import mysql
from persistance_mongo_layer.twitterdb_instance import DbInstance

hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}

#site = "http://doi.acm.org/10.1145/1645953.1646033"
mysql_cursor = mysql.send_query("SELECT doi, dblp_key FROM papers")
docs = []
regex = 'font-size:[0-9]*%;.*ccs\/ccs\.cfm\?id=[^<]*'
i = 0
paper2topic = 'paper2topic'
timeout_step = 1
insert_step = 20
db = DbInstance().getDbInstance()

already_saved = db[paper2topic].distinct('dblp_key')
no_topic_papers = db['no_topic_papers'].distinct('dblp_key')
already_saved = set(already_saved).union(no_topic_papers)
for row in mysql_cursor:
    if not row[1] in already_saved:
        time.sleep(timeout_step)

        site = row[0]
        dblp_key = row[1]
        is2load = True
        while is2load:
            try:
                req = urllib2.Request(site, headers=hdr)
                response = urllib2.urlopen(req)
                page_content = response.read()
                is2load = False
            except Exception as e:
                print 'exception for url : ', site
                print e
                time.sleep(200)
                is2load = False
                page_content = ""

        print

        matches = re.findall(regex, page_content)
        if len(matches) == 0:
            print 'no index terms for url', site
            db['no_topic_papers'].insert({'dblp_key': dblp_key})
        for match in matches:
            try:
                relevance = re.findall('[0-9]{1,4}[^%]', match)
                score =  relevance[0]
                concept = re.findall('>[A-Z]+.*', match)
                topic =  concept[0].replace(">","")
                tree = re.findall('0\.[0-9.]*', match)[0]
                acm_topic_id = re.findall('[0-9]*$',tree)[0]
            except IndexError:
                print 'error on', dblp_key, site


            doc = {
                "acm_topic_id": acm_topic_id,
                "topic": topic,
                "topic_tree": tree,
                "score": score,
                "dblp_key": dblp_key
            }
            docs.append(doc)
        if(len(docs) > insert_step):
            db[paper2topic].insert_many(docs)
            docs = []
            print 'saved', insert_step, 'docs'

        print dblp_key

if len(docs) > 0:
   db[paper2topic].insert_many(docs)

