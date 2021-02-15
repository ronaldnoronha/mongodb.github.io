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
    '_id': '$ORIGIN',
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
    '_id': '$DEST',
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
    '_id': '$ORIGIN',
    'avgDelay' : {'$avg': '$ARR_DELAY'},
    'maxDelay' : {'$max': '$ARR_DELAY'},
    'minDelay' : {'$min': '$ARR_DELAY'}
}},
{'$sort': {'avgDelay': -1}}],

[{'$match': {'ARR_DELAY': {'$gt': 0}}},
{'$group': {
    '_id': '$DEST',
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
  '_id': '$ORIGIN',
  'avgDelay' : {'$avg': '$ARR_DELAY'},
  'maxDelay' : {'$max': '$ARR_DELAY'},
  'minDelay' : {'$min': '$ARR_DELAY'}
}},
{'$sort': {'avgDelay': -1}}],

[{'$match': {'$and': [{'FL_DATE': {'$gte': datetime(2020,9,1)}}, {'FL_DATE': {'$lt': datetime(2020,10,1)}}]}},
{'$group': {
'_id': '$DEST',
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

[{'$group' : {
  '_id': ['$ORIGIN','$MKT_UNIQUE_CARRIER'],
  'avgDelay' : {'$avg': '$ARR_DELAY'},
  'maxDelay' : {'$max': '$ARR_DELAY'},
  'minDelay' : {'$min': '$ARR_DELAY'}
}},
{'$project' : {
  '_id' : 0,
  'ORIGIN': {'$arrayElemAt' : ['$_id', 0]},
  'MKT_UNIQUE_CARRIER': {'$arrayElemAt' : ['$_id', 1]},
  'avgDelay' : '$avgDelay',
  'maxDelay' : '$maxDelay',
  'minDelay' : '$minDelay'
}},
{'$sort': {'ORIGIN': 1}}],

[{'$group': {
  '_id': ['$DEST','$MKT_UNIQUE_CARRIER'],
  'avgDelay' : {'$avg': '$ARR_DELAY'},
  'maxDelay' : {'$max': '$ARR_DELAY'},
  'minDelay' : {'$min': '$ARR_DELAY'}
}},
{'$project' : {
  '_id' : 0,
  'DEST': {'$arrayElemAt' : ['$_id', 0]},
  'MKT_UNIQUE_CARRIER': {'$arrayElemAt' : ['$_id', 1]},
  'avgDelay' : '$avgDelay',
  'maxDelay' : '$maxDelay',
  'minDelay' : '$minDelay'
}},
{'$sort': {'DEST': 1}}],

[{'$match' : { '$and' : [
  { 'FL_DATE' : {'$gte': datetime(2020,9,1)}},
  { 'FL_DATE' : {'$lt': datetime(2020,9,16)}}]
}},
{'$match' : { 'ARR_DELAY' : { '$gt' : 10}}}],

[{'$project': {'ORIGIN': '$ORIGIN', 'day': {'$dayOfWeek': '$FL_DATE'}, 'ARR_DELAY': '$ARR_DELAY'}},
{'$group': {
    '_id' : ['$ORIGIN', '$day'],
    'avgDelay' : {'$avg': '$ARR_DELAY'},
    'maxDelay' : {'$max': '$ARR_DELAY'},
    'minDelay' : {'$min': '$ARR_DELAY'}
}},
{'$project': {'_id':0 ,'ORIGIN': {'$arrayElemAt' : ['$_id', 0]}, 'day': {'$arrayElemAt' : ['$_id', 1]},
            'avgDelay':'$avgDelay', 'maxDelay':'$maxDelay', 'minDelay':'$minDelay'}}],

[{'$project': {'DEST': '$DEST', 'day': {'$dayOfWeek': '$FL_DATE'}, 'ARR_DELAY': '$ARR_DELAY'}},
{'$group': {
    '_id' : ['$DEST', '$day'],
    'avgDelay' : {'$avg': '$ARR_DELAY'},
    'maxDelay' : {'$max': '$ARR_DELAY'},
    'minDelay' : {'$min': '$ARR_DELAY'}
}},
{'$project': {'_id':0 ,'DEST': {'$arrayElemAt' : ['$_id', 0]}, 'day': {'$arrayElemAt' : ['$_id', 1]},
            'avgDelay':'$avgDelay', 'maxDelay':'$maxDelay', 'minDelay':'$minDelay'}}],

[{'$group': {'_id': '$MKT_UNIQUE_CARRIER',
  'count': {'$sum': 1}}}],

[{'$match': { '$and': [
  { 'FL_DATE': {'$gte': datetime(2020,10,1)}},
  { 'FL_DATE': {'$lt': datetime(2020,10,16)}}]
}},
{ '$group': {'_id': '$ORIGIN',
          'count': {'$sum': 1}}}],

[{'$match': { '$and': [
  { 'FL_DATE': {'$gte': datetime(2020,10,1)}},
  { 'FL_DATE': {'$lt': datetime(2020,10,16)}}]
}},
{ '$group': {'_id': '$DEST',
          'count': {'$sum': 1}}}]

]

class Workload:
    def __init__(self):
        self.pipeline = pipeline

    def getWorkload(self):
        return self.pipeline
