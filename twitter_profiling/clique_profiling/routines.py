import itertools
import networkx as nx
import numpy as np
import operator
import sys

from twitter_clique import clique_dao
from twitter_profiling.clique_profiling.clique import Clique
from twitter_profiling.clique_profiling.clique_graph import neighbour_graph, neighbour_graph_with_id
from twitter_profiling.community.community import Community
from twitter_profiling.community.dao.community_dao import delete, insert, get_communities
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

def find_attractor(G, pagerank_with_node_weight=True):  # node with max gravity
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

    else: # optimizable with matrix operation !!!!!!pandas
        attraction_forces = {}
        for dst in G.nodes:
            neighbors = G.predecessors(dst)
            orbital_gravities = []
            for src in neighbors:
                orbital_gravities.append(G.get_edge_data(src, dst)['weight'])
            if len(orbital_gravities) == 0: orbital_gravities.append(0.)
            mean = np.mean(orbital_gravities)
            attraction_forces[dst] = mean
        mapping_gravity = nx.pagerank_numpy(G, personalization=attraction_forces)

    sorted_gravities = sorted(mapping_gravity.items(), key=operator.itemgetter(1), reverse=True)
    return sorted_gravities


def aggregation_step(clq, visited):
    # type:(Clique, set([str]))->(nx.DiGraph, set([str]) )
    comms = {}
    G, expanded = neighbour_graph_with_id(clq, visited)
    print 'expanded cliques', len(expanded)
    print 'initial number of nodes in G', len(G.nodes)

    while len(list(nx.weakly_connected_components(G))) < len(G.nodes):
        attractors = find_attractor(G)
        black_hole, pagerank = attractors[0]
        new_community = aggregate_nodes(G, black_hole, comms)
        if not new_community == None:
            comms[new_community.get_id()] = new_community

    #print 'number of communities discovered at this step is', len(comms.keys() )
    print 'G nodes are', len(G.nodes)
    print len(comms.values() ), 'communities in this aggregation step'
    __update_communities(comms)
    print '# # # # # # # #'
    print
    return G, expanded.union(visited)
    # if nx.number_connected_components() is G.number_of_nodes():
    #   return com_map.values()


# return a graph with new nodes but old weights on edges
def aggregate_nodes(G, black_hole, comms):
    # type:(nx.DiGraph, str, dict)->(nx.DiGraph, Community)
    edges = list(G.in_edges([black_hole], data=True))
    edges.sort(key=__take_weight)
    # edges contains at least on edge because this method is called only if graph has at least one edge.
    src, dst, weight = edges.pop()  # dst = black_hole


    community, to_delete = __fusion_4id_graph(comms, src, dst)
    if community == None and not to_delete == None:
        G.remove_node(to_delete)
    else: __update_graph(G, src, dst, community)

    # comms[community.get_id()] = community

    # while community.enough_cohesion() and len(edges) d == dst> 0:
    #     src, dst, weight = edges.pop()
    #     community = __fusion_4id_graph(comms, src, community)
    #     __update_graph(G, src, dst, community)

    return community


def __update_graph(G, src, dst, community):
    # type: (nx.DiGraph, object, object, Community)->None
    nodes2remove = [src, dst]
    edges = [(s, community.get_id(), data['weight'])
             for s, d, data in G.in_edges([src, dst], data=True)
             if not (s == src or s == dst)]
    G.remove_nodes_from(nodes2remove)
    G.add_node(community.get_id() )
    if community.accept_in_links:
        G.add_weighted_edges_from(edges)




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

def __update_communities(comms):
    # type:(dict)->None
    # delete all cliques  inside each com
    # add all coms
    clqs = map(__find_clqs_involved, comms.values() )
    clqs = itertools.chain(*clqs)
    delete(clqs)
    delete(comms.keys() )
    insert(comms.values() )



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



def __fusion_4id_graph(communities, src, dst):
    # type:(dict, str, str)-> (Community, object)
    # communities is a dict, its values have type Community
    clq2search = []
    try:
        ObjectId(src)
        is_src_comm = False
        clq2search.append(src)
    except InvalidId:
        is_src_comm = True

    try:
        ObjectId(dst)
        is_dst_comm = False
        clq2search.append(dst)
    except InvalidId:
        is_dst_comm = True

    cliques = []
    if len(clq2search) > 0:
        clqs = clique_dao.get_cliques(clq2search)
        for c in clqs:
            cliques.append(Clique(c['nodes'], c['_id']))
        clqs.close()

    if (not is_src_comm) and (not is_dst_comm):
        try:
            users = set(cliques[0].users).union(set(cliques[1].users))
            community = Community(users, src, dst)
        except IndexError:
            print 'expected 2 cliques found at most one--> same bug depicted above.'
            try:
                cliques[0]
                to_delete = [x for x in [src, dst] if not x == cliques[0].get_id() ]
            except IndexError: to_delete = src
        return None, to_delete

    else:

        try:
            communities[src].fusion(cliques[0] )
            community = communities[src]
        except Exception:
            try:
                communities[dst].fusion(cliques[0] )
                community = communities[dst]
            except Exception:
                try:
                    communities[dst].fusion(communities[src])
                    delete([src])
                    del communities[src]
                    community = communities[dst]
                except Exception as e:
                    print 'fusion impossible---error-> usually it search for a clique already deleted but at the same time it has loaded it in the graph..it s a bug i don t realize how to prevent'
                    print len(cliques), 'cliques retrieved'
                    comms_retrieved = list(get_communities([src, dst]))
                    if len(comms_retrieved) > 0:
                        print 'found one or both communities in the db .. so no fusion at this step..there is a clique you should not consider'
                        if is_src_comm == False: return None, src
                        if is_dst_comm == False: return None, dst
                    else:
                        sys.exit(1)
    return community, None