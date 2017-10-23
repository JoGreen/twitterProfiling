import itertools
from twitter_profiling.user_profiling.user_dao import UserDao
from twitter_profiling.clique_profiling.clique import Clique


def conductance(users, is_conductance_clique = True):
    #type:(list, bool)-> int
    inter_links_count = 0
    #retrieving inter links
    if is_conductance_clique is True:
        inter_links_count = len(list(itertools.combinations(users, 2) ) ) # len(users)*(len(users)-1)/2 should be the same
        print(inter_links_count, 'interlinks number')
    else :
        pass #TODO

    # retrieving boarder links
    friends_count = __get_friends_count(users)
    boarder_links_count = float(sum(friends_count) - inter_links_count)

    conductance = boarder_links_count / (2*inter_links_count + boarder_links_count)

    print('boarder_links:', boarder_links_count),
    print('internal_links:', inter_links_count)
    print('conductance:', conductance)

    return conductance



def user_conductance(user_id, clique, is_conductance_clique = True):
    # type:(str, Clique, bool)-> int
    inter_links_count = 0
    # retrieving inter links
    if is_conductance_clique is True:
        inter_links_count = len(clique.users) - 1
        print(inter_links_count, 'interlinks number')
    else:
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



user_conductance("24518857",Clique(["1977419539",
		"1496545255",
		"1417793336",
		"24518857",
		"975217231",
		"470744587",
		"581241345"], "5946a55bf810d56922432ffa"))
# conductance(["1977419539",
# 		"1496545255",
# 		"1417793336",
# 		"24518857",
# 		"975217231",
# 		"470744587",
# 		"581241345"])