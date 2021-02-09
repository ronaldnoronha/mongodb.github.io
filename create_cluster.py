from pymongo import *
import pandas as pd

# list of databases
list_of_collections = ['AirlineData',
                       'AirlineData_range',
                       'AirlineData_FL_DATE',
                       'AirlineData_FL_DATE_range',
                       'AirlineData_MKT_UNIQUE_CARRIER',
                       'AirlineData_MKT_UNIQUE_CARRIER_range',
                       'AirlineData_ORIGIN_CITY_NAME',
                       'AirlineData_ORIGIN_CITY_NAME_range',
                       'AirlineData_DEST_CITY_NAME',
                       'AirlineData_DEST_CITY_NAME_range'
                       ]


def csv_to_json(filename, header=None):
    data = pd.read_csv(filename)
    data = data.drop(columns=['Unnamed: 8'])
    data['FL_DATE'] = pd.to_datetime(data['FL_DATE'],format="%Y-%m-%d")
    return data.to_dict('records')


if __name__ == "__main__":
    # Create Collection and index
    client = MongoClient("mongodb://localhost:27017/")
    db = client['SummerResearch']

    # enable sharding
    client.admin.command('enableSharding', 'SummerResearch')

    list_of_query_states = []

    for i in list_of_collections:
        mycol = db[i]
        if i.split('_')[len(i.split('_'))-1] == 'range':
            sharding = 1
            if len(i.split('_'))==2:
                index = '_id'
            else:
                index = '_'.join(i.split('_')[1:len(i.split('_')) - 1])
                mycol.create_index([(index, 1)])
        else:
            sharding = 'hashed'
            if len(i.split('_'))==1:
                index = '_id'
            else:
                index = '_'.join(i.split('_')[1:len(i.split('_'))])
                mycol.create_index([(index, 1)])

        # Create Database
        # if sharding == 1:
        #     mycol.insert_many(csv_to_json('data_sep.csv'))
        #     mycol.insert_many(csv_to_json('data_oct.csv'))
        #     mycol.insert_many(csv_to_json('data_nov.csv'))
        #     client.admin.command('shardCollection', 'SummerResearch.' + i, key={index: sharding})
        # else:
        #     client.admin.command('shardCollection', 'SummerResearch.' + i, key={index: sharding})
        #     mycol.insert_many(csv_to_json('data_sep.csv'))
        #     mycol.insert_many(csv_to_json('data_oct.csv'))
        #     mycol.insert_many(csv_to_json('data_nov.csv'))

        client.admin.command('shardCollection', 'SummerResearch.' + i, key={index: sharding})
        mycol.insert_many(csv_to_json('data_sep.csv'))
        mycol.insert_many(csv_to_json('data_oct.csv'))
        mycol.insert_many(csv_to_json('data_nov.csv'))








    #     # Shard collection
    #     # client.admin.command('shardCollection', 'SummerResearch.' + i, key={index: sharding},
    #     #                      unique=False, numInitialChunks=5, collation={'locale': 'simple'})
    #     client.admin.command('shardCollection', 'SummerResearch.' + i, key={index: sharding})
    #
    #     # Add data to collection
    #     mycol.insert_many(csv_to_json('data.csv'))
    #
    #     # Get Query Optimiser State
    #     cost_vector = {}
    #     for j in pipeline:
    #         q = Query(db, i, j)
    #         cost = q.getQueryCostVector()
    #         for k in cost:
    #             if k in cost_vector:
    #                 cost_vector[k] += cost[k]
    #             else:
    #                 cost_vector[k] = cost[k]
    #
    #     list_of_query_states.append(cost_vector)
    #
    # print(list_of_query_states)









