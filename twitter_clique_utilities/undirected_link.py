from twitter_api.user_info import UserInfo
from persistance_mongo_layer.dao.user_dao import UserDao


class UndirectedLink:
    cached_links = {}

    def __init__(self, node1, node2):
        """
        :type node2: string  ->user id
        :type node1: string  ->user id
        """
        self.node1 = node1
        self.node2 = node2




    def check_existance(self, interesting_nodes = []):
        exist = self.__find_in(self.node1, self.node2, interesting_nodes)
        if exist is not True:
            exist = self.__find_in(self.node2, self.node1, interesting_nodes)
        #print(exist)
        return exist




    def __find_in(self, node, node2find, interesting_nodes):
        """
        :type node: string ->user id
        :type node2find: string ->user id
        """
        linked_nodes = self.__get_linked_nodes(node, interesting_nodes)
        # print(linked_nodes)
        exist = node2find in linked_nodes
        return exist






    def __get_linked_nodes(self, node, interesting_nodes):
        """
        :rtype: list
        """
        if UndirectedLink.cached_links.has_key(node) is True:
            friend_nodes = UndirectedLink.cached_links[node]

        else:
            friend_nodes = UserDao().get_friends(node)

            if friend_nodes is None:
                friend_nodes = UserInfo().get_friends(node)  # obtain friends
                UserDao().save_friends(node, friend_nodes)  # save friends
                UndirectedLink.cached_links.update({node: friend_nodes})  # update cache

        if len(interesting_nodes) > 0:  # filtering all friends returning just clique nodes if they are friends of node
            print('before filtering', len(friend_nodes)),
            friend_nodes = list(set(friend_nodes).intersection(set(interesting_nodes) ) )
            print('after filtering', len(friend_nodes))

        return friend_nodes







    def toString(self):
        return self.node1, self.node2
