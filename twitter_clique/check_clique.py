import itertools
from undirected_link import UndirectedLink

def exist(clique_users):
    """:type clique is a mongo dict of a clique"""

    links = list(itertools.combinations(clique_users, 2) )
    print('clique links are', len(links) )
    not_detected_links = []
    for link in links:
        l = UndirectedLink(link[0], link[1])
        exist = l.check_existance(clique_users)
        if exist is not True:
            not_detected_links.append(l)
    if len(not_detected_links) > 0:
        exist_all = False
    else:
        exist_all = True
    return {'exist_all' : exist_all, 'wrong_links' : not_detected_links}
