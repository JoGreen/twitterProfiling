from persistance_mongo_layer.twitterdb_instance import DbInstance
from trash.acm_crawler.mysql import send_query
from tqdm import tqdm

def create_docs(map):
    docs = [{'user': str(author), "mentions": filter(lambda a : a!=author, map[author])} for author in map.keys() ]
    # for author in map.keys():
    #     if author in map[author]:

    return docs


query = "SELECT c.paper_id, c.cite_id FROM cited_by c"
query2 = "SELECT p.paper_id, p.refer_id FROM paper_references p"
query3 = "SELECT pa.paper_id, pa.author_id FROM paper_authors pa"

result = send_query(query)
result2 = send_query(query2)
citation_map ={}
paper_authors = {}
author2author_map_citation = {}

for record in result:
    cited = record[0]
    paper = record[1]
    try:
        citation_map[str(paper) ].add(str(cited) )

    except Exception:
        citation_map[str(paper) ] = set([str(cited)] )

print 'query on cited_by', len(citation_map.keys() )

for record in result2:
    paper = record[0]
    cited = record[1]
    try:
        citation_map[str(paper) ].add(str(cited))

    except Exception:
        citation_map[str(paper) ] = set([str(cited)])

print 'query after refer_id and  cited_by', len(citation_map.keys() )
result3 = send_query(query3)

for record in result3:
    paper = record[0]
    author = record[1]
    try:
        paper_authors[str(paper)].append(str(author))

    except Exception:
        paper_authors[str(paper)] = [str(author)]

papers_with_no_auth_info = 0
author2cited_authors = {}
for paper in tqdm(citation_map.keys() ):
    try:# not all authors are mapped
        paper_writers = paper_authors[paper]
    except KeyError:
        papers_with_no_auth_info = papers_with_no_auth_info + 1
        continue

    cited_authors = []
    for cited_paper in citation_map[paper]:
        try:
            authors = paper_authors[cited_paper]
        except KeyError:
            papers_with_no_auth_info = papers_with_no_auth_info + 1
            continue
        cited_authors.extend(authors)
    for wr in paper_writers:
        try:
            author2cited_authors[wr].extend(cited_authors)
        except Exception:
            author2cited_authors[wr] =  cited_authors


print 'no authors info for', papers_with_no_auth_info, 'papers.'

docs = create_docs(author2cited_authors)
db = DbInstance().getDbInstance()
db['tweets'].insert_many(docs)
print 'done.'

