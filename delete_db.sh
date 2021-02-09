#!/bin/sh

mlaunch stop
rm -rf data
mlaunch init --replicaset --nodes 1 --sharded 3
mongo config --eval "db.settings.save({_id:'chunksize', value:1})"
python create_cluster.py
