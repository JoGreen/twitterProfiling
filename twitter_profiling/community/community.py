from twitter_profiling.clique_profiling.clique import Clique
import sys

graph_cohesion_threeshold = 1.2
vector_cohesion_threeshold = 0.008


class Community(Clique):

    def __init__(self, users, id1, id2):
        # type:(list[str], str, str)->None
        try:
            id1 = str(id1)
            id2 = str(id2)
        except:
            print 'wrong input type initializing Community instance'
            sys.exit(1)
        super(Community, self).__init__(users, id1+id2)
        self.cliques = [id1, id2]
        if self.get_vectors_cohesion() < vector_cohesion_threeshold:
            self.accept_in_links = True
        else:
            self.accept_in_links = False
