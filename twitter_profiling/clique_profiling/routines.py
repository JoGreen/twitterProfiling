import itertools
import networkx as nx
import numpy as np
import operator
import sys

from twitter_clique import clique_dao
from twitter_profiling.clique_profiling.clique import Clique
from twitter_profiling.clique_profiling.clique_graph import neighbour_graph_with_id
from twitter_profiling.clique_profiling.utility import constructor
from twitter_profiling.community.community import Community
from twitter_profiling.community.dao.community_dao import delete, insert, get_communities_with_specific_cliques, get_communities
from bson.objectid import ObjectId
from bson.errors import InvalidId

# import findspark
# findspark.init()
# os.environ["PYSPARK_SUBMIT_ARGS"] = (
#     "--packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 pyspark-shell")


# try:
#     from graphframes import graphframe as GF
# except ImportError as ex:
#     print "Can not import Spark Modules", ex
#     sys.exit(1)

def find_attractor(G, map_num_nodes, pagerank_with_node_weight=True):  # node with max gravity
    # type:(nx.DiGraph, bool)->(str, float)
    # strategies: -> pagerank -> undestand better weights on edges and on nodes what do they mean in term of result
    if pagerank_with_node_weight is False:
        mapping_gravity = nx.pagerank_numpy(G)
        hubs, authorities = nx.hits_numpy(G)
        hubs = sorted(hubs.items(), key=operator.itemgetter(1), reverse=True)
        authorities = sorted(authorities.items(), key=operator.itemgetter(1), reverse=True)
        print hubs
        print
        print authorities

    else: # optimizable with matrix operation ?!!!!!!pandas ?
        attraction_forces = {}
        #exp = 2
        for dst in G.nodes:
            neighbors = G.predecessors(dst)
            orbital_gravities = []
            for src in neighbors:
                orbital_gravities.append(G.get_edge_data(src, dst)['weight'])
            if len(orbital_gravities) == 0:
                orbital_gravities.append(0.001)

            #else: num_exp = exp

            mean = np.mean(orbital_gravities)

            #####**********************************************************
            # attraction_forces[dst] = pow(20 * mean, num_exp) * 1./pow(map_num_nodes[dst], denom_exp) # to avoid big attractor with low mean values eats everybody
            attraction_forces[dst] = ( (1+mean)**3 ) * 1. / (map_num_nodes[dst] ** 0.5) # to avoid big attractor with low mean values eats everybody
            #attraction_forces[dst] =  1. / (map_num_nodes[dst] ** 0.1)
            #####**********************************************************
        try:
            mapping_gravity = nx.pagerank_numpy(G, personalization= attraction_forces)
        except np.linalg.LinAlgError:
            mapping_gravity = nx.pagerank_numpy(G)

    sorted_gravities = sorted(mapping_gravity.items(), key=operator.itemgetter(1), reverse=True)
    return sorted_gravities


def aggregation_step(clq, visited):
    # type:(Clique, set([str]))->(nx.DiGraph, set([str]) )
    comms = {}
    to_del = []
    G, expanded, map_num_nodes = neighbour_graph_with_id(clq, visited)
    print 'expanded cliques', len(expanded)
    print 'initial number of nodes in G', len(G.nodes)
    start_num_nodes = len(G.nodes)
    #while len(list(nx.weakly_connected_components(G))) < len(G.nodes):
    while len(list(G.edges)) > 0:
        attractors = find_attractor(G, map_num_nodes)
        black_hole, pagerank = attractors[0]
        new_community, comm_to_delete = aggregate_nodes(G, black_hole, comms)
        #should update map_num_nodes for next iter
        if new_community != None: # to prevent fail when fusion is aborted for cohesion reasons
            map_num_nodes[new_community.get_id()] = len(new_community.users)
        #***************************
        if not comm_to_delete == None:
            to_del.append(comm_to_delete)
        if not new_community == None:
            comms[new_community.get_id()] = new_community

    #print 'number of communities discovered at this step is', len(comms.keys() )
    print 'G final nodes are', len(G.nodes)
    #print len(comms.values() ), 'communities in this aggregation step'
    deleted = __update_db(comms, to_del)
    #print '# # # # # # # #'
    #print
    return G, expanded.union(visited), deleted
    # if nx.number_connected_components() is G.number_of_nodes():
    #   return com_map.values()


# return a graph with new nodes but old weights on edges
def aggregate_nodes(G, black_hole, comms):
    # type:(nx.DiGraph, str, dict)->(nx.DiGraph, Community)
    #print 'aggregation  nodes'
    edges = list(G.in_edges([black_hole], data=True))
    edges.sort(key=__take_weight)
    # edges contains at least on edge because this method is called only if graph has at least one edge.
    try:
        src, dst, weight = edges.pop()  # dst = black_hole
    except ValueError:
        print 'pdpdpdp'
    except IndexError:
        G.remove_node(black_hole)
        print 'blackhole has no edges'
        return None, None

    community, clq_to_delete, comm_to_delete = __fusion_4id_graph(comms, src, dst, G)
    if community == None and not clq_to_delete == None:
        G.remove_node(clq_to_delete)
    else:
        if community != None:
            __update_graph(G, src, dst, community)

    # comms[community.get_id()] = community

    # while community.enough_cohesion() and len(edges) d == dst> 0:
    #     src, dst, weight = edges.pop()
    #     community = __fusion_4id_graph(comms, src, community)
    #     __update_graph(G, src, dst, community)

    return community, comm_to_delete


def __update_graph(G, src, dst, community):
    # type: (nx.DiGraph, object, object, Community)->None
    nodes2remove = [src, dst]
    edges = [(s, community.get_id(), data['weight'])
             for s, d, data in G.in_edges([src, dst], data=True)
             if (s != src and s != dst)]
    G.remove_nodes_from(nodes2remove)
    G.add_node(community.get_id() )
    if community.accept_in_links:
        G.add_weighted_edges_from(edges)
        # safeness block --useless
        error_found = False
        nodes2remove = filter(lambda x: x != community.get_id(), nodes2remove )
        try:
            prev_num = G.number_of_nodes()
            G.remove_nodes_from(nodes2remove)
            if G.number_of_nodes() < prev_num : error_found = True
        except nx.NetworkXException:
            pass
        if error_found:
            print 'error fuckoff !!!'



def __take_weight(t):
    # type:(tuple)->float
    return t[2]['weight']

def __fusion(src, dst):
    # type:(dict, object, object)-> Community
    try:
        src.fusion(dst)
        community = src
    except AttributeError:
        try:
            dst.fusion(src)
            community = dst
        except AttributeError:
            users = set(src.users).union(set(dst.users))
            community = Community(users, src.get_id(), dst.get_id() )

    return community

def __update_db(comms, comms_to_delete = []):
    # type:(dict)->set(str)
    # delete all cliques  inside each com
    # add all coms
    clqs = map(__find_clqs_involved, comms.values() )
    clqs = itertools.chain(*clqs)
    delete(clqs)
    delete(comms_to_delete)
    delete(comms.keys() )
    insert(comms.values() )
    return set(clqs).union(comms_to_delete)



def __find_clqs_involved(com):
    # type:(Community)->list[str]
    return com.cliques







def __fusion_4id_graph_not_in_use(communities, src, dst):
    # type:(dict, str, str)-> Community
    # communities is a dict, its values have type Community
    if communities.has_key(src) and communities.has_key(dst):
        communities[dst].fusion(communities[src])
        del communities[src]
        community = communities[dst]
        return community
    else:
        clq2search = []
        if not communities.has_key(src ):
            clq2search.append(src)
        if not communities.has_key(dst ):
            clq2search.append(dst)

        cliques = []
        clqs = clique_dao.get_cliques(clq2search)
        for c in clqs:
            cliques.append(Clique(c['nodes'], c['_id']))
        print 'num cliques to retrieve', len(cliques)
        if len(cliques) == 2:
            users = set(cliques[0].users).union(set(cliques[1].users) )
            community = Community(users, src, dst)
        if len(cliques) == 1:
            if communities.has_key(src):
                communities[src].fusion(cliques[0])
                community = communities[src]
            else:
                try:
                    communities[dst].fusion(cliques[0])
                    community = communities[dst]
                except KeyError:
                    print 'key error during aggregate nodes function-->community not found and that s strange'
                    sys.exit(1)
        if len(cliques) == 0:
            print 'error, at least one clique has be retrieved'
            sys.exit(1)
    return community


# this method has to be splitted in 4 !!! important to mantain code
def __fusion_4id_graph(communities, src, dst, G):
    # type:(dict, str, str, nx.DiGraph)-> (Community, object, object)
    # communities is a dict, its values have type Community
    clq2search = []
    comms_to_load = []
    comm_to_delete = None
    clq_to_delete = None
    try:
        ObjectId(src)
        is_src_comm = False
        clq2search.append(src)
    except InvalidId:
        is_src_comm = True
        comms_to_load.append(src)

    try:
        ObjectId(dst)
        is_dst_comm = False
        clq2search.append(dst)
    except InvalidId:
        is_dst_comm = True
        comms_to_load.append(dst)

    __load(comms_to_load, communities)

    retrieved_cliques = []
    if len(clq2search) > 0:
        clqs = clique_dao.get_cliques(clq2search)
        retrieved_cliques = map(constructor, clqs)
        clqs.close()



    if len(clq2search) == len(retrieved_cliques):
        # fusion between cliques
        if (not is_src_comm) and (not is_dst_comm):
            return fusion_between_cliques(communities, src, dst, retrieved_cliques)
        #fusion com -clq
        if (not is_src_comm and is_dst_comm) or (is_src_comm and not is_dst_comm):
            return fusion_between_com_clq(communities, src, dst, retrieved_cliques, G)
        # fusion between communities
        if is_src_comm and is_dst_comm:
            return fusion_between_comms(communities, src, dst, G)

    else:
        print 'fusion impossible---error-> it should be a solved bug'

        comms_retrieved = list(get_communities_with_specific_cliques([src, dst]))
        if len(comms_retrieved) > 0:
            pass
           # print 'found one or both communities in the db .. so no fusion at this step..there is a clique you should not consider'
        else:
            print 'error cause: gggrrrrr no community in the db contains that clique'
            # why thos problem again ? deleted all useless cliques in main before starting computation
            #
            # sys.exit(1)
        community = None
        if is_src_comm == False: clq_to_delete= src
        if is_dst_comm == False: clq_to_delete= dst
        return community, clq_to_delete, comm_to_delete




# when retrieving a community as neighbour..not all communities are loaded in memory so ..you need to load them
def __load(ids, communities):
    # type:(list[str], dict)->None
    if len(ids) > 0 :
        try:
            [communities[id] for id in ids]
        except KeyError:
            #print 'loading communities from db ...'
            comms = get_communities(ids)
            for c in comms:
                com = constructor(c)
                communities[com.get_id()] = com

def fusion_between_cliques(communities, src, dst, cliques):
    comm_to_delete = None
    clq_to_delete = None
    users = set(cliques[0].users).union(set(cliques[1].users))
    community = Community(users, src, dst)
    return community, clq_to_delete, comm_to_delete


def fusion_between_com_clq(communities, src, dst, cliques, G):
    try:  # fusion between community and clique
        if communities[src].fusion(cliques[0]):
            community = communities[src]
            return community, None, None
        else:
            G.remove_node(src)
            return None, None, None # low number of interests
    except KeyError:
        # try:  # fusion between community and clique
        if communities[dst].fusion(cliques[0]):
            community = communities[dst]
            return community, None, None
        else:
            G.remove_node(dst)
            return None, None, None


def fusion_between_comms(communities, src, dst, G):
    clq_to_delete = None
    if communities[dst].fusion(communities[src]):
        del communities[src]
        community = communities[dst]
        comm_to_delete = src
        return community, clq_to_delete, comm_to_delete
    else:
        G.remove_node(dst) # dst is the black hole
        return None, None, None