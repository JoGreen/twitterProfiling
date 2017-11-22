


def constructor(doc, delete_if_useless= False):
    # type:(dict)->object
    try:
        print doc['com_id']
        return __constr_comm(doc)
    except KeyError :
        return __constr_clq(doc, delete_if_useless= delete_if_useless)


def __constr_comm(doc):
    from twitter_profiling.community.community import Community
    return Community(None,None,None, **doc)

def __constr_clq(doc, delete_if_useless= False):
    from twitter_profiling.clique_profiling.clique import Clique
    return Clique(doc['nodes'], doc['_id'], delete_if_useless=delete_if_useless)