# Partitions

The following are the different collections. All collections have the same data, equal number of documents, with the same attributes and values. The only difference is the index and the shard keys used to shard the collections and also if the shard key was range based or index based. 


| Collection | Index/Shard key | Sharding |
| ---- | ---- | ---- |
| AirlineData | _id | hashed |
| AirlineData_range | _id | range |
| AirlineData_FL_DATE | FL_DATE | hashed |
| AirlineData_FL_DATE_range | FL_DATE_range | range |
| AirlineData_MKT_UNIQUE_CARRIER | MKT_UNIQUE_CARRIER | hashed |
| AirlineData_MKT_UNIQUE_CARRIER_range | MKT_UNIQUE_CARRIER | range |
| AirlineData_ORIGIN_CITY_NAME | ORIGIN_CITY_NAME | hashed |
| AirlineData_ORIGIN_CITY_NAME_range | ORIGIN_CITY_NAME | range |
| AirlineData_DEST_CITY_NAME | DEST_CITY_NAME | hashed |
| AirlineData_DEST_CITY_NAME_range | DEST_CITY_NAME | range | 

[Back to Overview](index.md)
