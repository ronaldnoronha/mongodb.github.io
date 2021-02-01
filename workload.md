 # Workload 

- Query 1: For a given date, find all flights that are delayed by a given number of minutes or more. 
```
db.getCollection('AirlineData').aggregate([{$match:{FL_DATE:'2020-10-31'}},{$match:{ARR_DELAY:{$gt:10}}}])
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
    _id: '$ORIGIN_CITY_NAME',
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
    _id: '$DEST_CITY_NAME',
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
    _id: '$ORIGIN_CITY_NAME',
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
    _id: '$DEST_CITY_NAME',
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

[Back](index.md)
