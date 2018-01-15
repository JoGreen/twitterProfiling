from itertools import combinations
from community_detection.profiling_operators.similarity import jaccard
from community_detection.profiling_operators.means import geometric_mean


def compute_cohesion(users):
    pass



def retweet_cohesion():
    pass

def topic_cohesion():
    pass


def profiles_cohesion(profiles):
    #type:(list[dict])->float
    try:
        profiles = list(profiles)
        profiles_with_display_name = []
        mean_profile_length = 0
        for p in profiles:
            interests = []
            try:
                interests = [i for i in p['info']['interests']['all'].keys()]
                # interests.append(p['info']['language']['primary']['name'])
            except KeyError: pass
            try:
                interests.append(p['info']['location']['country']['name'])
                print 'countr name added in profile interests'
            except KeyError: pass
            except Exception as e:
                print 'generic exception ->it s not key error exception',
                print e
            if len(interests) > 0:
                profiles_with_display_name.append(interests)
                mean_profile_length = mean_profile_length + len(interests)
        print profiles_with_display_name
        mean_profile_length = mean_profile_length/len(profiles)
    except TypeError :
        print 'input for profile cohesion has to be an iterable'
    profiles_indexes = range(0,len(profiles_with_display_name) )
    print profiles_indexes
    profiles_couples = combinations(profiles_indexes,2)
    #cohesion_vector = [jaccard(list(couple)[0], list(couple)[1]) for couple in profiles_couples]

    jaccard_vector = []
    intersection_vector = []
    for index in profiles_couples:
        #jaccars similarities
        jaccard_vector.append(jaccard(profiles_with_display_name[index[0] ], profiles_with_display_name[index[1] ]) )
        max_length = max(len(profiles_with_display_name[index[0] ]), len(profiles_with_display_name[index[1] ]) )

        #similarities based on intersection cardinality divided the mean of profiles length
        common_interests = len(set(profiles_with_display_name[index[0] ]).intersection(set(profiles_with_display_name[index[1] ]) ) )
        intersection_vector.append(float(common_interests)) #/mean_profile_length)

    denominatore = max(max(intersection_vector), mean_profile_length) #  or interests number in clique profile but ..
        # it risks to produce some value of intersection_vector > 1 anyway it seems not enough rapresentitive.
    intersection_vector = [e/denominatore for e in intersection_vector]

    print jaccard_vector
    print intersection_vector

    #geometric mean of cohesion
    jaccard_cohesion = geometric_mean(jaccard_vector)
    intesection_cohesion = geometric_mean(intersection_vector)

    print 'jaccard cohesion =', jaccard_cohesion
    print 'intersection cohesion', intesection_cohesion

    #return jaccard_cohesion
    return intesection_cohesion



def _get_followers_count(user_id):
    pass


