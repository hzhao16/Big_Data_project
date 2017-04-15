from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

sc = SparkContext()

csvfile = sc.textFile('/user/hw1567/big_data_project_dataset/311.csv',1)
header = csvfile.first()

data = csvfile.filter(lambda x: x != header)
data = data.map(lambda x:x.encode('utf-8','ignore')).mapPartitions(lambda x: reader(x))
data = data.map(lambda x: datetime.strptime(x[1], '%m/%d/%Y %I:%M:%S %p'))

month = data.map(lambda x: ((x.year,x.month),1)).reduceByKey(lambda x,y:x+y)
month = month.sortBy(lambda x:x[0]).map(lambda x: str(x[0][0])+'/'+str(x[0][1]),x[1])


day = data.map(lambda x: ((x.year,x.month,x.day),1)).reduceByKey(lambda x,y:x+y)
day = day.sortBy(lambda x:x[0]).map(lambda x: str(x[0][0])+'/'+str(x[0][1])+'/'+str(x[0][2]),x[1])

month.saveAsTextFile('count_month.out')
day.saveAsTextFile('count_day.out')

sc.stop()