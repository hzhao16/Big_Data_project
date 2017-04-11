from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import Row
from pyspark.sql import SQLContext
from datetime import datetime

sc = SparkContext()

csvfile = sc.textFile('/user/hw1567/big_data_project_dataset/311.csv',1)
header = csvfile.first()
for i in range(5,6):
	data = csvfile.filter(lambda x: x != header)
        data = data.map(lambda x:x.encode('utf-8','ignore'))
	data = data.mapPartitions(lambda x: reader(x))
	data = data.map(lambda x: (x[i].upper(), 1)).reduceByKey(lambda x,y:x+y)
        data = data.sortBy(lambda x:x[1],False).map(lambda x: x[0]+'\t'+str(x[1]))
	data.saveAsTextFile('count_{0}.out'.format(str(i)))
sc.stop()

