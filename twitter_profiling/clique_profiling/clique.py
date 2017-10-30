# coding=utf-8
from twitter_profiling.user_profiling.profile_dao import ProfileDao
from twitter_profiling.profiling_operators import intersection
from twitter_clique import check_clique
from twitter_clique.clique_profile_dao import CliqueProfileDao
from twitter_clique.clique_dao import get_similar_cliques_on_nodes
from bson.objectid import ObjectId
import sys, numpy as np
from twitter_profiling.profiling_operators import similarity
import networkx as nx
from multiprocessing import Pool
from twitter_profiling.profiling_operators.cohesion.set_cohesion import profiles_cohesion
from twitter_profiling.profiling_operators.cohesion.graph_cohesion import cohesion_of_graph_profiles
from twitter_profiling.profiling_operators.cohesion.vector_cohesion import cosine_cohesion
from pymongo.cursor import Cursor


class Clique(object):

    user_set_profiles ={}
    interests_data = {}

    def __init__(self, users, id):
        # type: (list, str) -> None

        if users is not None and id is not None:
            try:
                users = list(users)
                ObjectId(id)
                id = str(id)
            except TypeError:
                print('wrong type params to create Clique instance')
                sys.exit(1)
            self.knowledge_graph = None
            self.profile = None
            self.weighted_profile = {}
            self.users = users
            self.clique = id

    # search if a profile already exists if not compute one and persist it for future usage
    # return a profile
    def get_profile(self):
        #type:(Clique)->list(str)
        if self.profile is None:
            self.profile = CliqueProfileDao().get_clique_profile(self.clique)
            if self.profile is None:
                self.__compute_profile()

        return self.profile

    def is_a_clique(self):
        clique_status = check_clique.exist(self.users)
        print('do all link exist ?', clique_status['exist_all'])
        for l in clique_status['wrong_links']:
            print(l.toString())
        print('not existing links:', len(clique_status['wrong_links']))
        return clique_status['exist_all']

    def get_weighted_profile(self):
        return self.weighted_profile

    def get_the_closers(self, k=1):  # move to Clique class ?
        # type: (Clique, int)->list[Clique]
        closers = get_similar_cliques_on_nodes(self.get_id(), self.users, k)
        return [Clique(c['nodes'], c['_id']) for c in closers]

    def get_neighbours(self, k= 1, cohesion_type= 'vectors' ):
        # type:(Clique, int, str)->list[Clique]
        neighbours = []
        closers = self.get_the_closers(k)
        if closers is not None and len(closers) > 0:
            pool = Pool(8)
            if cohesion_type is 'vectors':
                cliques = pool.map(get_clique_with_minimum_vectors_cohesion, closers)
            else:
                cliques = pool.map(get_clique_with_minimum_graph_cohesion, closers)
            neighbours = [n for n in list(cliques) if n is not None]
            return neighbours



    def __compute_profile(self):
        profiles = self.get_users_profiles()  # return a Pymongo cursor
        # print(profiles.count(), 'number of profiles retrieved')
        print(len(profiles), 'number of profiles retrieved')

        users_interests = []

        for p in profiles:
            self.__set_interest_data(p)
            interests = self.__get_interests_from_profile(p)
            if interests is not None:
                users_interests.append(interests)

        # self.profile = intersection.rawIntersection(users_interests)
        self.profile = list(
            intersection.minus_k_intesection(users_interests, 1))  # add a parameter to overwrite default k = 1
        self.__save_profile()
        # self.__print_profile_interests()


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
            interests = self.__get_interests_from_profile(p)
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


    def get_id(self):
        return self.clique


    def get_users_profiles(self):
        #type:(Clique)->list
        profiles_cursor = ProfileDao().getSomeProfiles(self.users)  # return a Pymongo cursor
        profiles = []
        for p in profiles_cursor:
            profiles.append(p)
            #print p
        return profiles #list


    def get_users_profiles_interests(self):
        profiles_cursor = ProfileDao().getSomeProfiles(self.users)  # return a Pymongo cursor
        profiles = []
        for p in profiles_cursor:
            i = self.__get_interests_from_profile(p)
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

    def get_vectors_cohesion(self):
        raw_profiles = self.get_users_profiles() #raw json from db
        # profiles_vector = []
        profiles_vector_score =[]
        for raw_prof in raw_profiles:
            profile = self.__get_interests_from_profile(raw_prof)
            if profile is not None:
                #profile = set(profile).intersection(set(self.get_profile() ) )
                # profiles_vector.append(profile)
                scores = []
                if self.get_profile() is None: return 1.0
                for interest in self.get_profile():
                    try:
                        scores.append(raw_prof['info']['interests']['all'][interest]['score'] )
                    except KeyError:
                        scores.append(0)
                profiles_vector_score.append(scores)
        clique_weights = [np.mean(values) for values in np.array(profiles_vector_score).T]
        for interest in self.get_profile():
            index = self.get_profile().index(interest)
            self.weighted_profile[interest] = clique_weights[index]
        return cosine_cohesion(profiles_vector_score) #in term of distance

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

    def __get_interests_from_profile(self, profile):  # profile is a dict
        interest_ids = set()

        try:
            interest_ids = set(profile['info']['interests']['all'].keys())
        except Exception as e:
            print('error parsing interests', e)

        try:
            interest_ids.add(profile['info']['language']['primary']['name'])
        except Exception as e:
            print('error parsing language', e)

        try:
            interest_ids.add(profile['info']['location']['primary']['country']['name'])
        except Exception as e:
            print('error parsing location', e)

        finally:
            if (interest_ids != None and len(interest_ids) > 2):
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
###################################################################################
graph_cohesion_threeshold = 1.2
vector_cohesion_threeshold = 0.008

def get_clique_with_minimum_graph_cohesion(clique):
    # type:(Clique)->Clique
    if clique.get_graph_cohesion() < graph_cohesion_threeshold:
        return clique

def get_clique_with_minimum_vectors_cohesion(clique):
    # type:(Clique)->Clique
    if clique.get_vectors_cohesion() < vector_cohesion_threeshold:
        return clique

def get_neighbours(clique):
    # type:(Clique)->list[Clique]
    clique.get_neighbours()
########################################################################################
# c1 = Clique([
#         "1148580931",
# 		"728283781",
# 		"525208028",
# 		"17387058",
# 		"19267015",
# 		"462346627",
# 		"20517081",
# 		"20600235",
# 		"21870376",
# 		"22903129",
# 		"24871896",
# 		"29337915",
# 		"53047484",
# 		"63525184",
# 		"86740435",
# 		"162779090",
# 		"164786871",
# 		"456739709"
#     ],'5946a8f0f810d569224b8c6e')#.get_knowledge_graph()
# c2 = Clique([
#         "1606948220",
# 		"1114036494",
# 		"16681420",
# 		"17387058",
# 		"19267015",
# 		"20517081",
# 		"20600235",
# 		"21870376",
# 		"22903129",
# 		"24871896",
# 		"53047484",
# 		"67606740",
# 		"67613844",
# 		"86740435",
# 		"140409019",
# 		"462346627"
# ], "5946a6a0f810d56922464552")
#
#
# cohesion_2 = c2.get_vectors_cohesion()
# print cohesion_2
# print c2.weighted_profile
# cohesion_1 = c1.get_graph_cohesion()
# print 'cohesion of c1 clique ', cohesion_1
# print 'cohesion of c2 clique ', cohesion_2
#
# chs = [cohesion_1, cohesion_2, 30.0, 440.0]
# chs.sort()
# print chs
#
# plt.hist(chs, 10)
# plt.show()
