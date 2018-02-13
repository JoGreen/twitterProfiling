from persistance_mongo_layer.dao.profile_dao import ProfileDao
import operator
from tqdm import tqdm

limit = 5000000
authors = ProfileDao().getAllProfiles().limit(limit)
map = {}
#bar = progressbar.ProgressBar(redirect_stdout=True, max_value=authors.count() )
with tqdm(total=authors.count()) as pbar:
    for aut in authors:
        keys = aut['info']['interests']['all'].keys()

        for k in keys :
            interest = aut['info']['interests']['all'][k]['display']
            try:
                map[interest] = map[interest] + 1
            except KeyError:
                map[interest] = 1
            pbar.update(1)
sorted_map = sorted(map.items(), key=operator.itemgetter(1), reverse=True )
#print sorted_map
f = open('interests_destrib_dblp_4_author_displayname.txt', 'w')
#bar = progressbar.ProgressBar(redirect_stdout=True, max_value=len(sorted_map))
for interest in tqdm(sorted_map):
    f.write(interest[0]+': '+str(interest[1] )+ '\n' )
f.close()
