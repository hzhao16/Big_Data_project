import os
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import Row
from pyspark.sql import SQLContext
from datetime import datetime
import re

sc = SparkContext()

column = sc.textFile('/user/jub205/columns.txt').map(lambda line: line.split('|'))
#column = sc.textFile('/user/hw1567/big_data_project_dataset/columns.txt').map(lambda line: line.split('|'))
column_semantic_type = column.map(lambda line: line[0]).collect()
column_basic_type = column.map(lambda line: line[2]).collect()
path = os.getcwd()
result = open(path+"/type_result.txt", 'w')
your_id = path.split('/')[1]

for i in range(52):
    outfile = sc.textFile('/user/' + your_id + '/Type_{0}.out'.format(column_semantic_type[i]),1)
    data = outfile.map(lambda line: line.split('\t'))
    data = data.map(lambda x: (x[2], 1)).reduceByKey(lambda x,y:x+y)
    data = data.sortBy(lambda x:x[1],False).map(lambda x: x[0]+'\t'+str(x[1])).collect()
    result.write("Column: {0}".format(column_semantic_type[i]) + "\n")
    result.write("Base Type: {0}".format(column_basic_type[i]) + "\n")
    result.write("Semantic Type: {0}".format(column_semantic_type[i]) + "\n")
    for i in data:
        result.write(i + '\n')
    result.write('\n\n')
sc.stop()

