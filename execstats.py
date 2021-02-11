from query import Query
from workload import Workload
from mongo import Mongo


# recursive function to find the 'stage'
def getAllStages(execStats):
    s = set()
    if type(execStats) == type({}):
        for i in execStats:
            if i=='stage':
                s.add(execStats[i])
            else:
                s.update(getAllStages(execStats[i]))
    elif type(execStats) == type([]):
        for i in execStats:
            s.update(getAllStages(i))
    return s

if __name__ == "__main__":
    p = Workload()
    mongo = Mongo()
    db = mongo.getDb()
    list_of_collections = db.list_collection_names()
    list_of_collections.sort()
    pipeline = p.getWorkload()
    f = open("list_operations.md", "w")
    for i in list_of_collections:
        f.write('## ')
        f.write(i)
        f.write('\n')
        f.write('\n')
        f.write('| Query | Operations |')
        f.write('\n')
        f.write('|---|---|')
        f.write('\n')
        for idx, j in enumerate(pipeline):
            q = Query(db, i, j)
            rs = q.getExecStats()
            operations = list(getAllStages(rs))
            f.write('| Query '+str(idx) + '|' + ', '.join(operations)+'|')
            f.write('\n')
        f.write('\n')

    f.close()
