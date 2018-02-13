import os, sys, hashlib
import zipfile
from pymongo import MongoClient
import progressbar

from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
ps = PorterStemmer()

def parse_paper(lines):
    if len(lines) > 0:
        paper = {'reference_ids': []}
        for line in lines:
            if str.startswith(line, '#index'):
                paper['index_id'] = line.replace('#index ', "").replace('\n', '')

            if str.startswith(line, '#*'):
                paper['title'] = line.replace('#* ', "").replace('\n', '')

            if str.startswith(line, '#@'):
                paper['nodes'] = line.replace('#@ ', "").replace('\n', '').split(';')
                if len(paper['nodes']) == 1 and paper['nodes'][0] == "":
                    paper['nodes'] = []
                paper['count'] = len(paper['nodes'])

            if str.startswith(line, '#c'):
                paper['pub_venue'] = line.replace('#c ', "").replace('\n', '')

            #references id

            if str.startswith(line, '#%'):
                paper['reference_ids'].append(line.replace('#% ', '').replace('\n', '') )

        if paper['count'] > 2:
            return paper


def parse_author(lines):
    if len(lines) > 0:
        author= {'info':{'interests': {'all':{} } } }
        for line in lines:
            if str.startswith(line, '#index'):
                author['index_id'] = line.replace('#index ', "").replace('\n', '')
            if str.startswith(line, '#n'):
                author['user'] = line.replace('#n ', "").replace('\n', '')
                if author['user'] == 'Sonia Berman':
                    pass
                # add interests and affiliations
            #if str.startswith(line, '#t') or str.startswith(line, '#a'):
            if str.startswith(line, '#t'):
                interests = line.replace('#t ', "").replace('#a ','').replace('\n', '').lower().split(';')
                if interests != '':
                    for interest in interests:
                        words = word_tokenize(interest)
                        processed_interest = ""
                        for w in words:
                            processed_interest = processed_interest + ps.stem(w)+ ' '
                        if processed_interest.endswith(" "):
                            processed_interest = processed_interest[:-1]
                        author['info']['interests']['all'][hashlib.md5(processed_interest.encode()).hexdigest()] = {
                            'display': processed_interest, #interest,
                            'score': 1
                        }

        # try:
        #     save_author(author)
        # except:
        #     print author
        #     sys.exit(1)

        return author



def save_authors(authors):
    db = MongoClient('localhost', 27017)['dblp']
    db['user_infos'].insert_many(authors)

def save_papers(papers):
    db = MongoClient('localhost', 27017)['dblp']
    db['cliques2analyze'].insert_many(papers)

def save_author(author):
    db = MongoClient('localhost', 27017)['dblp']
    db['user_infos'].insert(author)



with zipfile.ZipFile('AMiner-Author.zip') as z:
    #authors = []
    docs = []
    index_ids = set()

    for filename in z.namelist():
        if not os.path.isdir(filename):
            # read the file
            with z.open(filename) as f:
                bar = progressbar.ProgressBar(redirect_stdout=True, max_value=progressbar.UnknownLength)
                lines=[]
                i = 0
                for line in f:
                    if not str.startswith(line,'#index'):
                        lines.append(line)
                    else:
                        doc = parse_author(lines)
                        #print lines, '\n'
                        #doc = parse_paper(lines)
                        if doc != None and not doc['index_id'] in index_ids:
                            index_ids.add(doc['index_id'])
                            #authors.append(parse_author(lines) )
                            docs.append(doc)
                        lines=[line]

                    if len(docs) > 1200:
                        print 'saving 1200 authors and interests\n'
                        #save_papers(docs)
                        #save_authors(docs)
                        docs = []

                    bar.update(i)
                    i = i +1
            #save_authors(docs)
            #save_papers(docs)
            print 'end of file'





