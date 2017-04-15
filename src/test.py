import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def preprocess(filepath):

    sc = SparkContext()
    lines = sc.textFile(filepath,1)
    header = lines.first()
    lines = lines.filter(lambda x: x != header)
    lines = lines.map(lambda x: x.encode('utf-8')).mapPartitions(lambda x: reader(x))

    return lines

def get_frequency(column_name, index, data, save=1):

    freq = data.map(lambda x: (x[index], 1)).reduceByKey(add)
    if save == 1:
	output_path = column_name.replace(" ","") + ".out"
        freq.map(lambda x: "%s\t%s"%(x[0],x[1])).saveAsTextFile(output_path)
    else:
        print freq.take(10)

if __name__ == "__main__":

    lines = preprocess(sys.argv[1])
    
    with open('columns.txt','rb') as f:
        columns = f.read().split('\n')[:-1]
	columns = [i.split('|') for i in columns]

    for i in columns:	
	get_frequency(i[0], int(i[1]), lines)    

