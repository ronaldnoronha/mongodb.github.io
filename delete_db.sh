#!/bin/sh

mlaunch stop
rm -rf data
mlaunch init --replicaset --nodes 1 --sharded 3
mongo config --eval "db.settings.save({_id:'chunksize', value:32})"
python create_cluster.py
