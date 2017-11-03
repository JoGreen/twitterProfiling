from twitter_profiling.clique_profiling.clique import Clique

class Community(Clique):

    def __init__(self, users, id1, id2):
        try:
            id1 = str(id1)
            id2 = str(id2)
        except:
            print 'wron input type initializing Community instance'
        super(Community, self).__init__(users, id1+id2)

