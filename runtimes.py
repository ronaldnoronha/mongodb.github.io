from mongo import Mongo
from workload import Workload
from query import Query
import pandas as pd
from table import Table

if __name__ == "__main__":
    mongo = Mongo()
    db = mongo.getDb()
    list_of_collections = db.list_collection_names()
    workload = Workload()
    pipeline = workload.getWorkload()

    data = []
    for i in pipeline:
        dct = {}
        for j in list_of_collections:
            q = Query(db, j, i)
            dct[j] = q.getQueryExecTime()
        data.append(dct)
    df = pd.DataFrame(data)

    list_of_queries= []
    for i in range(len(pipeline)):
        list_of_queries.append('Query '+str(i+1))


    df['Query'] = list_of_queries
    df.set_index('Query', inplace=True)

    table = Table(df)
    table.getJPG('images/execTime.jpg')


    # cm = sns.light_palette("green", as_cmap=True)
    # html = (df.style.background_gradient(cmap=cm))

