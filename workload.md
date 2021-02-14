 # Workload 

- Query 1: For a given date, find all flights that are delayed by a given number of minutes or more. 
```
db.getCollection('AirlineData').aggregate([
{$match : { FL_DATE : ISODate('2020-10-31')}},
{$match : { ARR_DELAY : { $gt : 10 }}}
])
```

- Query 2: Late percentage by airline over October. 
```
db.getCollection('AirlineData').aggregate([
{$group: {
    _id : '$MKT_UNIQUE_CARRIER',
    late : { $sum:  { 
        $cond: { if: { $gt : ['$ARR_DELAY', 0] }, then: 1, else: 0 }
    }},
    total : { $sum: 1 }
}},
{$project: {
    carrier: '_id',
    late_percent: { $multiply: [{$divide: ['$late','$total']}, 100] }
}},
{$sort: { late_percent: -1}}
])
```

- Query 3: Late percentage of flights by origin city
```
db.getCollection('AirlineData').aggregate([
{$group: {
    _id: '$ORIGIN',
    late: {$sum: { $cond: 
        { if: { $gt: ['$ARR_DELAY', 0] }, then: 1, else: 0 }
    }},
    total : { $sum: 1 }
}},
{ $project: { 
    origin: '_id',
    late_percent: { $multiply: [{$divide: ['$late','$total']}, 100] }
}},
{ $sort: { late_percent: -1}}
])
```

- Query 4: Late percentage flights by destination
```
db.getCollection('AirlineData').aggregate([
{$group: {
    _id: '$DEST',
    late: {$sum: { $cond: 
        { if: { $gt: ['$ARR_DELAY', 0] }, then: 1, else: 0 }
    }},
    total : { $sum: 1 }
}},
{ $project: { 
    origin: '_id',
    late_percent: { $multiply: [{$divide: ['$late','$total']}, 100] }
}},
{ $sort: { late_percent: -1}}
])
```

- Query 5: Avg/Max/Min delay by origin city
```
db.getCollection('AirlineData').aggregate([
{$match: {ARR_DELAY: {$gt: 0}}},
{$group: {
    _id: '$ORIGIN',
    avgDelay : {$avg: '$ARR_DELAY'},
    maxDelay : {$max: '$ARR_DELAY'},
    minDelay : {$min: '$ARR_DELAY'}
}},
{$sort: {avgDelay: -1}}
])
```

- Query 6: Avg/Max/Min delay by destination city

```
db.getCollection('AirlineData').aggregate([
{$match: {ARR_DELAY: {$gt: 0}}},
{$group: {
    _id: '$DEST',
    avgDelay : {$avg: '$ARR_DELAY'},
    maxDelay : {$max: '$ARR_DELAY'},
    minDelay : {$min: '$ARR_DELAY'}
}},
{$sort: {avgDelay: -1}}
])
```

- Query 7: Max/Min/Avg delay by airline

```
db.getCollection('AirlineData').aggregate([
{$match: {ARR_DELAY: {$gt: 0}}},
{$group: {
    _id: '$MKT_UNIQUE_CARRIER',
    avgDelay : {$avg: '$ARR_DELAY'},
    maxDelay : {$max: '$ARR_DELAY'},
    minDelay : {$min: '$ARR_DELAY'}
}},
{$sort: {avgDelay: -1}}
])
```

- Query 8: Max/Min/Avg delay by origin for a given date range
```
db.getCollection('AirlineData').aggregate([
{$match: {$and: [{FL_DATE: {$gte: ISODate('2020-09-01')}}, {FL_DATE: {$lt: ISODate('2020-10-01')}}]}},
{$group: {
  _id: '$ORIGIN',
  avgDelay : {$avg: '$ARR_DELAY'},
  maxDelay : {$max: '$ARR_DELAY'},
  minDelay : {$min: '$ARR_DELAY'}
}},
{$sort: {avgDelay: -1}}
])
```

- Query 9: Max/Min/Avg delay by destination for a given date range
```
db.getCollection('AirlineData').aggregate([
{$match: {$and: [{FL_DATE: {$gte: ISODate('2020-09-01')}}, {FL_DATE: {$lt: ISODate('2020-10-01')}}]}},
{$group: {
  _id: '$DEST',
  avgDelay : {$avg: '$ARR_DELAY'},
  maxDelay : {$max: '$ARR_DELAY'},
  minDelay : {$min: '$ARR_DELAY'}
}},
{$sort: {avgDelay: -1}}
])
```

- Query 10: Max/Min/Avg delay by airline for a given date range
```
db.getCollection('AirlineData').aggregate([
{$match: {$and: [{FL_DATE: {$gte: ISODate('2020-09-01')}}, {FL_DATE: {$lt: ISODate('2020-10-01')}}]}},
{$group: {
  _id: '$MKT_UNIQUE_CARRIER',
  avgDelay : {$avg: '$ARR_DELAY'},
  maxDelay : {$max: '$ARR_DELAY'},
  minDelay : {$min: '$ARR_DELAY'}
}},
{$sort: {avgDelay: -1}}
])
```

- Query 11: Max/Min/Avg delay for each origin by airline
```
db.getCollection('AirlineData').aggregate([
{$group : {
    _id: ['$ORIGIN','$MKT_UNIQUE_CARRIER'],
    avgDelay : {$avg: '$ARR_DELAY'},
    maxDelay : {$max: '$ARR_DELAY'},
    minDelay : {$min: '$ARR_DELAY'}  
}},
{$project : {
    '_id' : 0,
    'ORIGIN': {$arrayElemAt : ['$_id', 0]}, 
    'MKT_UNIQUE_CARRIER': {$arrayElemAt : ['$_id', 1]}, 
    'avgDelay' : '$avgDelay',
    'maxDelay' : '$maxDelay',
    'minDelay' : '$minDelay'
}},
{$sort: {ORIGIN: 1}}    
])
```

- Query 12: Max/Min/Avg delay for each destination by airline
```
db.getCollection('AirlineData').aggregate([
{$group : {
    _id: ['$DEST','$MKT_UNIQUE_CARRIER'],
    avgDelay : {$avg: '$ARR_DELAY'},
    maxDelay : {$max: '$ARR_DELAY'},
    minDelay : {$min: '$ARR_DELAY'}  
}},
{$project : {
    '_id' : 0,
    'DEST': {$arrayElemAt : ['$_id', 0]}, 
    'MKT_UNIQUE_CARRIER': {$arrayElemAt : ['$_id', 1]}, 
    'avgDelay' : '$avgDelay',
    'maxDelay' : '$maxDelay',
    'minDelay' : '$minDelay'
}},
{$sort: {DEST: 1}}    
])
```

- Query 13: For a given range of dates, find all flights that are delayed by a given number of minutes. 
```
db.getCollection('AirlineData').aggregate([
{$match : { $and : [
    { FL_DATE : {$gte: ISODate('2020-09-01')}},
    { FL_DATE : {$lt: ISODate('2020-09-16')}}]
}},
{$match : { ARR_DELAY : { $gt : 10}}}
])
```

[Back to Overview](index.md)
