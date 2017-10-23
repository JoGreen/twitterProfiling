from twitter_mongodb.twitterdb_instance import DbInstance
import sys

class CliqueProfileDao:
    db_name = 'clique_profiles'
    port = 27017
    collection = 'profiles'

    def get_clique_profile(self, clique_id):  # return a list or None
        db = DbInstance(self.port, self.db_name).getDbInstance()
        doc = db[self.collection].find_one({"clique" : clique_id})
        if doc is not None :
            return doc['kg'] #kg -> knowledge graph -> list of interests
        return None

    def insert_clique_profile(self, clique_id, profile):
        try:
            profile = list(profile)
        except TypeError :
            print('profile you passed as argument of "insert_clique_profile" function is not an iterable')
            sys.exit(1)
        db = DbInstance(self.port, self.db_name).getDbInstance()
        db[self.collection].save({
            "clique" : str(clique_id),
            "kg": profile
        })

