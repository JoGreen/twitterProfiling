from twitter_profiling.clique_profiling.clique import Clique
import sys, hashlib

graph_cohesion_threeshold = 1.2
vector_cohesion_threeshold = 0.018  # 0.008


class Community(Clique):

    def __init__(self, nodes, id1, id2, **kwargs):
        # type:(list[str], str, str)->None
        try:
            if not kwargs.has_key('cliques'):
                id1 = str(id1)
                id2 = str(id2)
        except Exception:
            print 'wrong input type initializing Community instance'
            sys.exit(1)
        self.com_id = kwargs.get('com_id', hashlib.md5(id1+id2).hexdigest() )
        nodes = kwargs.get('users', nodes)
        self.cliques = set(kwargs.get('cliques', [id1, id2]) )
        super(Community, self).__init__(nodes, list(self.cliques).pop() ) #random id
        self.clique = None

        self.check_acceptance()

    def __eq__(self, other):
        if isinstance(other, Community):
            return self.get_id() == other.get_id() and self.users == other.users
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def get_id(self):
        return self.com_id

    def check_acceptance(self):
        if self.get_vectors_cohesion() < vector_cohesion_threeshold:
            self.accept_in_links = True
        else:
            self.accept_in_links = False

    # def compute_weighted_profile(self, with_profiles_vectors_scores= False):
    #     pass

    def fusion(self, obj):
        # type: (Community, object)->None
        if not(isinstance(obj, Community) or isinstance(obj, Clique)):
            print 'input for community fusion has to be a clique or community, but it is not'
            sys.exit(1)
        if isinstance(obj, Clique):
            self.__fusion_with_clique(obj)
        if isinstance(obj, Community):
            self.__fusion_with_community(obj)
        # return c

    def __fusion_with_clique(self, clq):
        # type: (Community, Clique)->None
        self.users = self.users.union(set(clq.users) )
        self.cliques.add(clq.get_id() )
        self.check_acceptance()


    def __fusion_with_community(self, com):
        # type: (Community, Community)->None
        self.users = self.users.union(set(com.users))
        self.cliques = self.cliques.union(set(com.cliques) )
        self.check_acceptance()


# d = {'cliques': ['5946ad97f810d5692252d7b0', '5946a906f810d569224c766c'],
#      'users': ['245014472', '19267015', '29774933', '1143472657'], 'com_id': '9ac4d7addabf273bda65858351317736'}
#
# c = Community(None, "", "", **d)
