#!/bin/bash

echo "which file to upload ?"
read name
hdfs dfs -rm -r "sfbda_project"
hdfs dfs mkdir "sfbda_project"
hdfs dfs -put name "sfbda_project"