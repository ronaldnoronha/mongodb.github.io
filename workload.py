from datetime import datetime

pipeline = [
[{'$match':{'FL_DATE':datetime(2020,10,1)}},
 {'$match':{'ARR_DELAY':{'$gt':1000}}}],

[{'$group': {
            '_id' : '$MKT_UNIQUE_CARRIER',
            'late' : {'$sum':  {'$cond': {'if': {'$gt': ['$ARR_DELAY', 0] }, 'then': 1, 'else': 0 }}},
            'total' : {'$sum': 1 }}},
{'$project': {
            'carrier': '_id',
            'late_percent': {'$multiply': [{'$divide': ['$late','$total']},100]}
}},
{'$sort': {'late_percent': -1 }}],

[{'$group': {
    '_id': '$ORIGIN_CITY_NAME',
    'late': {'$sum': { '$cond':
        { 'if': { '$gt': ['$ARR_DELAY', 0] }, 'then': 1, 'else': 0 }
    }},
    'total' : { '$sum': 1 }
}},
{ '$project': {
    'origin': '_id',
    'late_percent': { '$multiply': [{'$divide': ['$late','$total']}, 100] }
}},
{ '$sort': { 'late_percent': -1}}],

[{'$group': {
    '_id': '$DEST_CITY_NAME',
    'late': {'$sum': { '$cond':
        { 'if': { '$gt': ['$ARR_DELAY', 0] }, 'then': 1, 'else': 0 }
    }},
    'total' : { '$sum': 1 }
}},
{ '$project': {
    'origin': '_id',
    'late_percent': { '$multiply': [{'$divide': ['$late','$total']}, 100] }
}},
{ '$sort': { 'late_percent': -1}}],

[{'$match': {'ARR_DELAY': {'$gt': 0}}},
{'$group': {
    '_id': '$ORIGIN_CITY_NAME',
    'avgDelay' : {'$avg': '$ARR_DELAY'},
    'maxDelay' : {'$max': '$ARR_DELAY'},
    'minDelay' : {'$min': '$ARR_DELAY'}
}},
{'$sort': {'avgDelay': -1}}],

[{'$match': {'ARR_DELAY': {'$gt': 0}}},
{'$group': {
    '_id': '$DEST_CITY_NAME',
    'avgDelay' : {'$avg': '$ARR_DELAY'},
    'maxDelay' : {'$max': '$ARR_DELAY'},
    'minDelay' : {'$min': '$ARR_DELAY'}
}},
{'$sort': {'avgDelay': -1}}],

[{'$match': {'ARR_DELAY': {'$gt': 0}}},
{'$group': {
    '_id': '$MKT_UNIQUE_CARRIER',
    'avgDelay' : {'$avg': '$ARR_DELAY'},
    'maxDelay' : {'$max': '$ARR_DELAY'},
    'minDelay' : {'$min': '$ARR_DELAY'}
}},
{'$sort': {'avgDelay': -1}}],

[{'$match': {'$and': [{'FL_DATE': {'$gte': datetime(2020,9,1)}}, {'FL_DATE': {'$lt': datetime(2020,10,1)}}]}},
{'$group': {
  '_id': '$ORIGIN_CITY_NAME',
  'avgDelay' : {'$avg': '$ARR_DELAY'},
  'maxDelay' : {'$max': '$ARR_DELAY'},
  'minDelay' : {'$min': '$ARR_DELAY'}
}},
{'$sort': {'avgDelay': -1}}],

[{'$match': {'$and': [{'FL_DATE': {'$gte': datetime(2020,9,1)}}, {'FL_DATE': {'$lt': datetime(2020,10,1)}}]}},
{'$group': {
'_id': '$DEST_CITY_NAME',
'avgDelay' : {'$avg': '$ARR_DELAY'},
'maxDelay' : {'$max': '$ARR_DELAY'},
'minDelay' : {'$min': '$ARR_DELAY'}
}},
{'$sort': {'avgDelay': -1}}],

[{'$match': {'$and': [{'FL_DATE': {'$gte': datetime(2020,9,1)}}, {'FL_DATE': {'$lt': datetime(2020,10,1)}}]}},
{'$group': {
'_id': '$MKT_UNIQUE_CARRIER',
'avgDelay' : {'$avg': '$ARR_DELAY'},
'maxDelay' : {'$max': '$ARR_DELAY'},
'minDelay' : {'$min': '$ARR_DELAY'}
}},
{'$sort': {'avgDelay': -1}}],

[]

]

class Workload:
    def __init__(self):
        self.pipeline = pipeline

    def getWorkload(self):
        return self.pipeline
