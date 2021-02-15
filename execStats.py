from mongo import Mongo
from pymongo import *
from query import Query
import json
from bson import json_util
from datetime import datetime
from workload import Workload


if __name__ == "__main__":

    # Connect to Mongo
    mongo = Mongo()
    db = mongo.getDb()

    pipeline = Workload().getWorkload()

    f = open("demofile.md", "w")

    lst = db.list_collection_names()
    lst.sort()
    f.write('# Execution stats for each query')
    f.write('\n')
    f.write('\n')
    for i in lst:
        f.write('- ['+i+'](#'+i+')')
        f.write('\n')

    f.write('\n')

    for i in lst:
        f.write('## ')
        f.write(i+'<a name="'+i+'"></a>')
        f.write('\n')
        f.write('\n')
        f.write('<a href="#top">Back to top</a>')
        f.write('\n')
        for k in range(len(pipeline)):
            f.write('- [Query '+str(k+1)+'](#Query'+str(k+1)+')')
            f.write('\n')
        f.write('\n')
        # pprint(i)
        query_num = 0
        for j in pipeline:
            query_num+=1
            q = Query(db,i,j)
            f.write('<a href="#top">Back to top</a>')
            f.write('\n')
            f.write('- Query ' + str(query_num)+'<a name="Query'+str(query_num)+'"></a>')
            # f.write('- Query ' + str(query_num))
            f.write('\n')

            f.write('```')
            f.write('\n')
            f.write(json.dumps(q.getExecStats(), indent=3, default=json_util.default))
            # f.write(json.dumps(q.getResults(), indent=3, default=json_util.default))
            # f.write(json.dumps(q.getQueryCostVector(), indent=3))
            # f.write(json.dumps(q.getQueryExecTime(), indent=3))
            f.write('\n')
            f.write('```')




            f.write('\n')
            f.write('\n')



    f.close()
