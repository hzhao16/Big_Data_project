from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

sc = SparkContext()

csvfile = sc.textFile('/user/jub205/311.csv')
header = csvfile.first()

data = csvfile.filter(lambda x: x != header)
data = data.map(lambda x:x.encode('utf-8','ignore')).mapPartitions(lambda x: reader(x))

data = data.map(lambda x: (datetime.strptime(x[1], '%m/%d/%Y %I:%M:%S %p'), x[5]))

data1 = data.filter(lambda x: x[1].upper() in ['HEATING', 'HEAT/HOT WATER'])

data2 = data.filter(lambda x: x[1].upper() not in ['HEATING', 'HEAT/HOT WATER'])

day1 = data1.map(lambda x: ((x[0].month, 'Heat'),1)).reduceByKey(lambda x,y:x+y)

day1 = day1.sortBy(lambda x:x[0])

day2 = data2.map(lambda x: ((x[0].month, 'Others'),1)).reduceByKey(lambda x,y:x+y)

day2 = day2.sortBy(lambda x:x[0])

day2.saveAsTextFile('day_not_heat.out')
day1.saveAsTextFile('day_heat.out')

sc.stop()


