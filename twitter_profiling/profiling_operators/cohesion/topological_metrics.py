import networkx as nx
from twitter_profiling.user_profiling.user_dao import UserDao
# from twitter_profiling.clique_profiling.clique import Clique
from twitter_profiling.community.community import Community


def conductance(com, is_conductance_clique = False):
    #type:(Community, bool)-> int
	inter_links_count = 0
	# retrieving inter links
	if is_conductance_clique is True:
		inter_links_count = len(com.users) * (len(com.users) - 1) / 2
		print(inter_links_count, 'interlinks number')
	else:

		friendship = UserDao().get_friends_of(com.users)
		igs = map(lambda t: __get_user_ingraph(com.users, t[0], t[1]), friendship)
		ogs = map(lambda t: __get_user_outgraph(com.users, t[0], t[1]), friendship)
	IG = nx.compose_all(igs, 'internal_graph')
	OG = nx.compose_all(ogs, 'outside_graph')
	inter_links_count = IG.number_of_edges()
	boarder_links_count = OG.number_of_edges()

	conductance = boarder_links_count / float(2 * inter_links_count + boarder_links_count)

	print('boarder_links:', boarder_links_count),
	print('internal_links:', inter_links_count)
	print('conductance:', conductance)

	return conductance

def __get_user_graphs(user_id, users):
    friends = UserDao().get_friends(user_id)
    IG = nx.Graph()
    OG = nx.Graph()
    in_edges = set()
    out_edges = set()
    for friend in friends:
        if friend in users:
            in_edges.add((user_id, friend) )
        else:
            out_edges.add((user_id, friend))
    IG.add_edges_from(in_edges)
    OG.add_edges_from(out_edges)
    return IG, OG


def  internal_density(com):
    # type:(Community)->float
	friendship = UserDao().get_friends_of(com.users)
	friendship = ((d['id'], d['friends']) for d in friendship)
	IGs = map(lambda t: __get_user_ingraph(com.users, t[0], t[1]), friendship)

	IG = nx.compose_all(IGs)
	n = len(com.users)
	return IG.number_of_edges() / (n * (n - 1) / 2.)




def __get_user_ingraph(users, user_id, friends ):
    #friends = UserDao().get_friends(user_id)
    IG = nx.Graph()
    #OG = nx.Graph()
    in_edges = set()
    #out_edges = set()
    for friend in friends:
        if friend in users:
            in_edges.add((user_id, friend) )
        #else:
            #out_edges.add((user_id, friend))
    IG.add_edges_from(in_edges)
    #OG.add_edges_from(out_edges)
    return IG#, OG


def __get_user_outgraph(users, user_id, friends ):
    #friends = UserDao().get_friends(user_id)

	OG = nx.Graph()
	# edges = set()
	# out_edges = set()
	# for friend in friends:
	#     if not friend in users:
	#         edges.add((user_id, friend) )
	edges = ((user_id, friend for friend in friends if not friend in users))
	# else:
	# out_edges.add((user_id, friend))
	OG.add_edges_from(edges)
	return OG

def user_conductance(user_id, clique, is_conductance_clique = True):
    # type:(str, Clique, bool)-> int
    inter_links_count = 0
    # retrieving inter links
    if is_conductance_clique is True:
        inter_links_count = len(clique.users) - 1
        print(inter_links_count, 'interlinks number')
    else:
        inter_links_count = 0
        pass  # TODO

    # retrieving boarder links
    friends_count = __get_friends_count([user_id])
    boarder_links_count = float(sum(friends_count) - inter_links_count)

    conductance = boarder_links_count / (2 * inter_links_count + boarder_links_count)

    print('boarder_links:', boarder_links_count),
    print('internal_links:', inter_links_count)
    print('conductance:', conductance)

    return conductance


def __get_friends_count(users_ids):
    #type:(list)->int
    friends_count = UserDao().get_friends_count(users_ids)
    #print(friends_count, 'friends')
    return friends_count

def __launcher(t, users):
    return __get_user_ingraph(users, *t)

# user_conductance("24518857",Clique(["1977419539",
# 		"1496545255",
# 		"1417793336",
# 		"24518857",
# 		"975217231",
# 		"470744587",
# 		"581241345"], "5946a55bf810d56922432ffa"))
# conductance([
# 		"190052199",
# 		"379250311",
# 		"173528459",
# 		"271516560",
# 		"961244268",
# 		"344584770",
# 		"303074762",
# 		"175124549",
# 		"107167985",
# 		"279960370",
# 		"164786871",
# 		"499108538",
# 		"825906386",
# 		"11869952",
# 		"244495300",
# 		"118698085",
# 		"392487826",
# 		"298537212",
# 		"490653518",
# 		"184146579",
# 		"24560295",
# 		"205622300",
# 		"1572692082",
# 		"962728567",
# 		"63525184",
# 		"408807200",
# 		"322825184",
# 		"68674747",
# 		"26121908",
# 		"1143472657",
# 		"604768692",
# 		"1639897656",
# 		"293127661",
# 		"191552379",
# 		"280606089",
# 		"568248162",
# 		"207371354",
# 		"78664302",
# 		"258976292",
# 		"182428433",
# 		"23440052",
# 		"230904991",
# 		"248254612",
# 		"461906674",
# 		"22655140",
# 		"106828762",
# 		"1297483208",
# 		"216322133",
# 		"557235026",
# 		"203935615",
# 		"228291020",
# 		"420310681",
# 		"470336591",
# 		"116187235",
# 		"1432840862",
# 		"216372101",
# 		"115095199",
# 		"159882006",
# 		"210240661",
# 		"47568495",
# 		"20817934",
# 		"700992438",
# 		"124218221",
# 		"20236332",
# 		"724364101",
# 		"452266132",
# 		"218482045",
# 		"539439796",
# 		"111940614",
# 		"397250431",
# 		"1425338130",
# 		"21710527",
# 		"365340354",
# 		"152978469",
# 		"196758914",
# 		"317832296",
# 		"28961325",
# 		"1241708905",
# 		"439372416",
# 		"309328122",
# 		"48129434",
# 		"97875583",
# 		"74958016",
# 		"101188210",
# 		"411687378",
# 		"260670926",
# 		"516680180",
# 		"255043151",
# 		"77047730",
# 		"48737130",
# 		"93945790",
# 		"623738760",
# 		"281337902",
# 		"22634364",
# 		"255677704",
# 		"363111719",
# 		"432794829",
# 		"127340684",
# 		"248745292",
# 		"456739709"
# 	])