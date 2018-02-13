# coding=utf-8
import sys

import networkx as nx
import numpy as np
from bson.errors import InvalidId
# from twitter_clique_utilities.clique_dao import get_similar_cliques_on_nodes
from bson.objectid import ObjectId

from twitter_clique_utilities import check_clique
from twitter_clique_utilities.clique_profile_dao import CliqueProfileDao
from persistance_mongo_layer.dao import community_dao
from persistance_mongo_layer.dao.profile_dao import ProfileDao
from community_detection.clique_profiling.utility import constructor
from community_detection.profiling_operators import intersection
from community_detection.profiling_operators import similarity
from community_detection.profiling_operators.cohesion.graph_cohesion import cohesion_of_graph_profiles
from community_detection.profiling_operators.cohesion.set_cohesion import profiles_cohesion
from community_detection.profiling_operators.cohesion.vector_cohesion import cosine_cohesion_distance_optimized
import parameters

class Clique(object):

   # user_set_profiles ={}
    interests_data = {}
    minimum_num_of_interests = 5
    profile_dao = ProfileDao()

    graph_cohesion_threeshold = 1.2
    #vector_cohesion_threeshold = 0.018 # used for twitter kaggle dataset
    vector_cohesion_threshold = parameters.acm["cohesion_threshold"] #0.13
    k = parameters.acm['k']
    def __init__(self, users, id, delete_if_useless= False, **kwargs):
        # type: (list, str) -> None

        if users is not None and id is not None:
            try:
                users = kwargs.get('nodes', set(users) )
                ObjectId(id)
                id = kwargs.get('_id', str(id) )
            except TypeError:
                print('wrong type params to create Clique instance')
                sys.exit(1)
            except InvalidId:
                print "not a valid ObjectId:", id
                sys.exit(1)
            self.knowledge_graph = None
            self.profile = kwargs.get('profile', None)
            self.weighted_profile = kwargs.get('weighted_profile', {})
            self.users = users
            self.clique = id

            if delete_if_useless and len(self.get_profile() ) <=  self.minimum_num_of_interests:
                print 'autodestroying in 3 2 1 cause i m useless sigh sigh siBBBOOOOOOOOOMMMMMMM'
                community_dao.delete([self.get_id()])

    def __eq__(self, other):
        # type:(Clique, object)->bool
        if isinstance(other, Clique):
            return self.get_id() == other.get_id()
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def new_mongo_client(self):
        self.profile_dao.set_new_client()
    # search if a profile already exists ow compute and persist it for future usage
    # return a profile
    def get_profile(self, force_recompute= False, k_intersection=1):
        # type:(Clique, bool)->list[str]
        if self.profile is None or force_recompute == True:
            self.__linear_compute_profile(k_intersection)
        return self.profile



    def is_a_clique(self):
        clique_status = check_clique.exist(self.users)
        print('do all link exist ?', clique_status['exist_all'])
        for l in clique_status['wrong_links']:
            print(l.toString())
        print('not existing links:', len(clique_status['wrong_links']))
        return clique_status['exist_all']

    def get_weighted_profile(self, k_intersection= 1):
        if not len(self.weighted_profile.values() ) > 0:
            self.compute_weighted_profile(k_intersection= k_intersection)
        return self.weighted_profile

    def get_the_closers(self, k=1):  # move to Clique class ?
        # type: (Clique, int)->list[Clique]
        closers = community_dao.get_similar_community_on_nodes(self.get_id(), self.users, k)
        the_closers = map(constructor, closers) # [Clique(c['nodes'], c['_id']) for c in closers]
        closers.close()
        return the_closers

    def get_neighbours(self, k= 1, cohesion_type= 'vectors' ):
        # type:(Clique, int, str)->list(dict)
        neighbours = []
        closers = self.get_the_closers(k)
        if closers is not None and len(closers) > 0:
            # pool = Pool(8)
            cliques =[]
            if cohesion_type is 'vectors':
                # cliques = pool.map(get_clique_with_minimum_vectors_cohesion, closers)
                if len(self.get_profile()) > self.minimum_num_of_interests:
                    cliques = map(get_clique_with_minimum_vectors_cohesion_and_interests, closers)
            else:pass
                #cliques = pool.map(get_clique_with_minimum_graph_cohesion, closers)
            #pool.close()
            neighbours = [n for n in list(cliques) if n is not None]
            return neighbours



    def compute_weighted_profile(self, with_profiles_vectors_scores= False, k_intersection= 1, db=None):
        # type:(Clique, bool)->list
        raw_profiles = self.get_users_profiles(db= db)  # raw json from db
        # profiles_vector = []
        profiles_vector_score = []
        self.get_profile(force_recompute=True, k_intersection= k_intersection)
        for raw_prof in raw_profiles:
            profile = self.get_interests_from_profile(raw_prof)
            if profile is not None:

                scores = []
                if self.get_profile() == None: return 1.0 # not sure it is correct to return 1 and if none is a rel case
                for interest in self.get_profile():
                    try:
                        scores.append(raw_prof['info']['interests']['all'][interest]['score'])
                    except KeyError:
                        scores.append(0)
                profiles_vector_score.append(scores)
        clique_weights = [np.mean(values) for values in np.array(profiles_vector_score).T]

        for interest in self.get_profile():
            index = self.get_profile().index(interest)
            self.weighted_profile[interest] = clique_weights[index]

        if with_profiles_vectors_scores is True and len(self.get_profile()) > 0:
            return profiles_vector_score


    def __linear_compute_profile(self, k_intersection= 1):
        profiles = self.get_users_profiles()
        # print(len(profiles), 'number of profiles retrieved')
        interests = map(self.get_interests_from_profile, profiles)
        users_interests = filter(lambda x: x != None, interests)
        self.profile = intersection.minus_k_intersection_linear(users_interests, k_intersection)




    def __compute_profile(self, k_intersection= 1):
        profiles = self.get_users_profiles()  # return a Pymongo cursor
        # print(profiles.count(), 'number of profiles retrieved')
        print(len(profiles), 'number of profiles retrieved')
        users_interests = []
        interests = map(self.get_interests_from_profile, profiles)
        users_interests = filter(lambda x: x != None, interests)
        # self.profile = intersection.rawIntersection(users_interests)
        self.profile = list(
            intersection.minus_k_intersection(users_interests, k_intersection))  # add a parameter to overwrite default k = 1



    def __compute_knowledge_graph_from_set_profiles(self):
        if self.profile is None: self.__compute_profile()
        self.__retrieve_interests_data()
        knowledge_graph = nx.Graph()
        mapping = self.get_profile_interests_names()
        edges = []
        for interest_code in self.profile:
            #find parents
            try:
                parents = self.interests_data[interest_code]['parents']
                # append to list of edges
                parents_links = [(mapping[interest_code], mapping[parent]) for parent in parents]
                edges.extend(parents_links)
            except KeyError as e:
                print 'interest code not found, normal for languages and countries', e
                knowledge_graph.add_node(interest_code)

        #add list of edges to knowledge_graph
        knowledge_graph.add_edges_from(edges)
        self.knowledge_graph = knowledge_graph

    def get_knowledge_graph_from_set_profiles(self):
        if self.knowledge_graph is None:
            self.__compute_knowledge_graph_from_set_profiles()
        # for edge in list(self.knowledge_graph.edges):
        #     print edge
        return self.knowledge_graph


    def get_knowledge_graph_from_users_kgraphs(self):
        #type:(Clique)->nx.Graph
        if self.knowledge_graph is None:
            self.__compute_knowledge_graph_from_users_kgraphs()
        return self.knowledge_graph

    def __compute_knowledge_graph_from_users_kgraphs(self):
        #type:(Clique)->nx.Graph
        profiles = self.get_users_profiles()
        graphs = []
        for p in profiles:
            interests = self.get_interests_from_profile(p)
            if interests is not None:
                KG = self.__compute_user_knowledge_graph(interests)
                graphs.append(KG)
        #decide how combine this not weighted graphs
        self.knowledge_graph = intersection.minus_k_intersection_of_graph(graphs)

    def __compute_user_knowledge_graph(self, user_profile):
        #type:(Clique, list)->nx.Graph
        if user_profile is not None:
            try:
                list(user_profile)
            except TypeError:
                print 'input error-> user_profile has to be an iterable eg. list, set ..'
                sys.exit(1)
            if len(user_profile) < 3: pass #maybe not consider
            self.__retrieve_interests_data(user_profile)
            knowledge_graph = nx.Graph()
            mapping = self.get_profile_interests_names(user_profile)
            edges = []
            for interest_code in user_profile:
                try:
                    parents = self.interests_data[interest_code]['parents']
                    # append to list of edges
                    parents_links = [(mapping[interest_code], mapping[parent]) for parent in parents]
                    edges.extend(parents_links)
                except KeyError as e:
                    print 'interest code not found, normal for languages and countries', e
                    knowledge_graph.add_node(interest_code)

            knowledge_graph.add_edges_from(edges)
            knowledge_graph.add_edges_from([('User', n) for n in knowledge_graph.nodes])
            return knowledge_graph


    def profile_set_similarity_with(self, clique):  # clique is an other Clique instance
        return similarity.jaccard(self.get_profile(), clique.get_profile())

    def profile_graph_similarity_with(self, clique):
        #type:(Clique, Clique)->int
        return similarity.isomorphism_measure(self.get_knowledge_graph_from_users_kgraphs(), clique.get_knowledge_graph_from_users_kgraphs())

    def get_profile_vector_similarity_with(self, clique):
        all_interests = set(self.get_weighted_profile().keys() ).union(set(clique.get_weighted_profile().keys() ))
        v1 = []
        v2 = []
        for i in all_interests:
            if not self.get_weighted_profile().has_key(i):
                #self.get_weighted_profile()[i] = 0.
                v1.append(0.)
            else:
                v1.append(self.get_weighted_profile()[i])
            if not clique.get_weighted_profile().has_key(i):
                #clique.get_weighted_profile()[i] = 0.
                v2.append(0.)
            else:
                v2.append(clique.get_weighted_profile()[i])
        return similarity.vector_similarity(v1, v2 )

    def get_id(self):
        return self.clique


    def get_users_profiles(self, db= None):
        #type:(Clique)->list
        profiles_cursor = self.profile_dao.getSomeProfiles(list(self.users), db= db)  # return a Pymongo cursor
        profiles = list(profiles_cursor)
        # profiles = []
        # for p in profiles_cursor:
        #     profiles.append(p)
            #print p
        return profiles #list


    def get_users_profiles_interests(self):
        profiles_cursor = ProfileDao().getSomeProfiles(self.users)  # return a Pymongo cursor
        profiles = []
        for p in profiles_cursor:
            i = self.get_interests_from_profile(p)
            if i is not None: profiles.append(i)
        return profiles

    def get_set_cohesion(self):
        profiles = self.get_users_profiles()
        cohesion = profiles_cohesion(profiles)
        return cohesion

    def get_graph_cohesion(self, user_graphs = None): # not implemented yet !!!
        #type:(Clique, list)-> float
        #if user_graphs is None:

        user_graphs = [self.__compute_user_weight_on_clique_knowledge_graph(p) for p in self.get_users_profiles() ]
        # print 'usergraphs', user_graphs
        user_graphs = list(filter(lambda x: x is not None, user_graphs))
        # print 'usergraphs', user_graphs

        c = cohesion_of_graph_profiles(user_graphs)
        print len(user_graphs), 'analyzed profiles'
        return c

    def get_vectors_cohesion(self, db= None):
        profiles_vector_score = self.compute_weighted_profile(with_profiles_vectors_scores= True, db=db)
        if profiles_vector_score != None:
            return cosine_cohesion_distance_optimized(profiles_vector_score)  # in term of distance
        else:
            return 1.0


    def __compute_user_weight_on_clique_knowledge_graph(self, profile):
        #type:(Clique, list(str))->nx.Graph
        try:
            mapping = self.get_profile_interests_names(profile['info']['interests']['all'].keys() )
            G = self.get_knowledge_graph_from_users_kgraphs().copy()
            print 'G', len(G.nodes), len(G.edges)
            for n in G.nodes:
            #n could be an interest id or a display name but here i suppose it s an id because i m taking it directly from db
                if n is not 'User':
                    if not n in mapping.values():
                        mapping[n] = n
                        profile['info']['interests']['all'][n] = {'score': 1}
                    interest_id = mapping.keys()[mapping.values().index(n) ]
                    w = profile['info']['interests']['all'][interest_id]['score']
                    #print 'weight', w
                    G['User'][n]['weight'] = w

            return G
        except KeyError as e: print 'error', e
        return  None


    def __save_profile(self):
        if self.profile is not None:
            CliqueProfileDao().insert_clique_profile(self.clique, self.profile)

    def get_interests_from_profile(self, profile):  # profile is a dict
        interest_ids = set()

        try:
            interest_ids = set(profile['info']['interests']['all'].keys())
        except Exception as e:
            # print('error parsing interests', e)
            pass
        try:
            interest_ids.add(profile['info']['language']['primary']['name'])
        except Exception as e:
            # print('error parsing language', e)
            pass
        try:
            interest_ids.add(profile['info']['location']['primary']['country']['name'])
        except Exception as e:
            # print('error parsing location', e)
            pass
        finally:
            if (interest_ids != None and len(interest_ids) > 0):
                return interest_ids

    def __set_interest_data(self, profile):  # profile is a dict
        """ update interests map valid for all clique instances """
        try:
            data = profile['info']['interests']['all']
            # print(len(data), '# interests')
            Clique.interests_data.update(data)
        except KeyError as e: print ('error parsing interests for user',profile['user'], e)
        except Exception as e: print e

    def __print_profile_interests(self):
        for i in self.profile:
            try:
                print(self.interests_data[i]['display'])
            except Exception:
                print(i)

    def get_profile_interests_names(self, set_of_interests = None):
        #type:(Clique, list)->dict
        if set_of_interests is None:
            set_of_interests = self.profile

        mapping = {}
        self.__retrieve_interests_data(set_of_interests)
        for i in set_of_interests:
            try:
                mapping[i] = self.interests_data[i]['display']
            except KeyError as e:
                print e
                mapping[i] = i
        return mapping

    def __retrieve_interests_data(self, set_of_interests= None):
        if set_of_interests is None:
            set_of_interests = self.profile

        if not all(interest_code in self.interests_data for interest_code in set_of_interests):
            profiles = self.get_users_profiles()  # return list
            for p in profiles:
                self.__set_interest_data(p)

    def enough_cohesion(self):
        # type:(self)->bool
        return self.get_vectors_cohesion() < self.vector_cohesion_threshold

###################################################################################





def get_clique_with_minimum_graph_cohesion(clique):
    # type:(Clique)->dict
    cohesion = clique.get_graph_cohesion()
    if cohesion < clique.graph_cohesion_threeshold:
        return {'clique':clique, 'cohesion': cohesion}

def get_clique_with_minimum_vectors_cohesion_and_interests(clique):
    # type:(Clique)->dict
    cohesion = clique.get_vectors_cohesion()
    if cohesion < clique.vector_cohesion_threshold and len(clique.get_profile()) > clique.minimum_num_of_interests:
        user_count = {}
        user_count[clique.get_id()] = len(clique.users)
        return {'clique':clique, 'cohesion': cohesion, 'user_count': user_count}

def get_neighbours(clique):
    # type:(Clique)->list[Clique]
    clique.get_neighbours()

# not used yet:::maybe in future-cons is it computes similarity i don t need--pro it computes all i need in asingle step using numpy it should be more efficient...to check
def get_profile_vector_similarity_between_all(coms):
    # type:(list[Clique])->np.array
    all_interests = set().union( (com.get_weighted_profile().keys() for com in coms) )
    vectors = []
    for com in coms:
        v =[]
        for i in all_interests:
            if not com.get_weighted_profile().has_key(i):
                v.append(0.)
            else:
                v.append(com.get_weighted_profile()[i])
    return similarity.vector_similarity_between_all(vectors)


# c = Clique([ "1148580931", "568248162", "456739709", "248254612", "152978469", "124218221", "21710527", "298537212", "52991614", "63525184", "77047730", "91361852", "127340684", "392487826" ],'5946a8f4f810d569224bc865')
# c.get_vectors_cohesion()