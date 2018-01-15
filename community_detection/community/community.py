from community_detection.clique_profiling.clique import Clique
import sys, hashlib
from bson.objectid import ObjectId
from bson.errors import InvalidId

graph_cohesion_threeshold = 1.2
vector_cohesion_threeshold = 0.018  # 0.008


class Community(Clique):

    def __init__(self, users, id1, id2, **kwargs):
        # type:(list[str], str, str)->None
        try:
            # if not kwargs.has_key('cliques'):
            id1 = str(id1)
            id2 = str(id2)
        except Exception:
            print 'wrong input type initializing Community instance'
            sys.exit(1)
        self.com_id = kwargs.get('com_id', hashlib.md5(str(id1)+str(id2) ).hexdigest() )
        nodes = kwargs.get('nodes', users)
        self.cliques = kwargs.get('cliques', [id1, id2])
        try: # to be sure to have right data
            self.cliques = map(ObjectId, self.cliques)
            self.cliques = map(str, self.cliques)
            self.cliques = set(self.cliques)
        except InvalidId:
            print 'dirty data inside cliques field in community', self.com_id
            sys.exit(1)

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
        # type: (Community, object)->bool
        if not(isinstance(obj, Community) or isinstance(obj, Clique)):
            print 'input for community fusion has to be a clique or community, but it is not'
            sys.exit(1)
        if isinstance(obj, Clique) and not isinstance(obj, Community):
            return self.__fusion_with_clique(obj)
        if isinstance(obj, Community) and isinstance(obj, Clique):
            return self.__fusion_with_community(obj)
        # return c

    def __fusion_with_clique(self, clq):
        # type: (Community, Clique)->None
        try: #security check
            ObjectId(clq.get_id() )
        except InvalidId:
            print 'fusion wit clique: need an objectId like but it s not'
            print clq.get_id()
            sys.exit(1)
        self.cliques.add(clq.get_id() )
        old_users = set(self.users)
        self.users = self.users.union(set(clq.users))
        if len(self.get_profile(force_recompute=True)) > self.minimum_num_of_interests:
            self.check_acceptance()
            return True
        else:
            print 'fusion not confirmed'
            self.cliques.remove(clq.get_id())
            self.users = old_users
            return False


    def __fusion_with_community(self, com):
        # type: (Community, Community)->None

        try:
            map(ObjectId, com.cliques)
        except InvalidId: # security check
            print 'fusion with community: need an objectId vector like but it s not'
            print com.get_id()
            sys.exit(1)
        old_clqs = set(self.cliques)
        old_users = set(self.users)
        self.cliques = self.cliques.union(set(com.cliques) )
        self.users = self.users.union(set(com.users))
        if len(self.get_profile(force_recompute=True)) > self.minimum_num_of_interests:
            self.check_acceptance()
            return True
        else:
            print 'fusion not confirmed'
            self.users = old_users
            self.cliques = old_clqs
            return False



# d = {'cliques': ['5946ad97f810d5692252d7b0', '5946a906f810d569224c766c'],
#      'users': ['245014472', '19267015', '29774933', '1143472657'], 'com_id': '9ac4d7addabf273bda65858351317736'}
#
# c = Community(None, "", "", **d)
