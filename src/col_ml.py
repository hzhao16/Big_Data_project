from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime
from pyspark.mllib.clustering import KMeans, KMeansModel
import numpy as np
from math import sqrt


def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

def addclustercols(x):
    point = np.array([float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])])
    center = clusters.centers[0]
    mindist = sqrt(sum([y**2 for y in (point - center)]))
    cl = 0
    for i in range(1,len(clusters.centers)):
        center = clusters.centers[i]
        distance = sqrt(sum([y**2 for y in (point - center)]))
        if distance < mindist:
            cl = i
            mindist = distance
    clcenter = clusters.centers[cl]
    return (str(x[0]), int(x[1]), int(x[2]), int(x[3]), int(x[4]), float(x[5]), int(cl), float(clcenter[0]), float(clcenter[1]),  float(clcenter[2]), float(clcenter[3]), float(clcenter[4]), float(mindist))

def inclust(x, t):
    cl = x[6]
    c_1 = x[7]
    c_2 = x[8]
    c_3 = x[9]
    c_4 = x[10]
    c_5 = x[11]
    distance = x[12]
    if float(distance) > float(t):
        cl = -1
		c_1 = 0.0
		c_2 = 0.0
		c_3 = 0.0
		c_4 = 0.0
		c_5 = 0.0
    return (str(x[0]), int(x[1]), int(x[2]), int(x[3]), int(x[4]), float(x[5]), int(cl), float(c_1), float(c_2), float(c_3), float(c_4), float(c_5), float(distance))

sc = SparkContext()

csvfile = sc.textFile('/user/jub205/311all.csv')
header = csvfile.first()

data = csvfile.filter(lambda x: x != header)
data = data.map(lambda x:x.encode('utf-8','ignore')).mapPartitions(lambda x: reader(x))
data = data.map(lambda x: (datetime.strptime(x[1], '%m/%d/%Y %I:%M:%S %p'), x[5])).filter(lambda x: x[1].upper() in ['HEATING', 'STREET CONDITION', 'STREET LIGHT CONDITION', 'NOISE - RESIDENTIAL', 'HEAT/HOT WATER'])

day = data.map(lambda x: ((x[0].year,x[0].month,x[0].day, x[1]),1)).reduceByKey(lambda x,y:x+y)
day = day.sortBy(lambda x:x[0]).map(lambda x: (str(x[0][0])+'-'+str(x[0][1])+'-'+str(x[0][2]), str(x[0][3]), x[1]))

day1 = spark.createDataFrame(day, ('Day', 'Complaint', 'Count'))

day1.createOrReplaceTempView("ml")

day2 = spark.sql("SELECT Day, max(case when Complaint = 'HEATING' then Count else 0 END) as HEATING, max(case when Complaint = 'NOISE - RESIDENTIAL' then Count else 0 END) AS NOISE-RESIDENTIAL, max(case when Complaint = 'STREET CONDITION' then Count else 0 END) AS STREET_CONDITION, max(case when Complaint = 'STREET LIGHT CONDITION' then Count else 0 END) AS STREET_LIGHT_CONDITION, max(case when Complaint = 'HEAT/HOT WATER' then Count else 0 END) AS HEAT_HOT_WATER FROM ml group by Day")

day_rdd = day2.rdd

for i in range(1,11):
    clusters = KMeans.train(day_rdd, i, maxIterations=10, initializationMode="random")
    WSSSE = day_rdd.map(lambda point: error(point)).reduce(add)
    print("Within Set Sum of Squared Error, k = " + str(i) + ": "  + str(WSSSE))
	rdd_w_clusts = day_rdd.map(lambda x: addclustercols(x))


	schema_sd = spark.createDataFrame(rdd_w_clustsk5, ('Day','HEATING', 'NOISE-RESIDENTIAL', 'STREET_CONDITION', 'STREET_LIGHT_CONDITION', 'HEAT_HOT_WATER', 'p_1', 'p_2', 'p_3', 'p4', 'p5', 'cluster', 'c_1', 'c_2', 'c_3', 'c4', 'c5', 'dist'))
	schema_sd.createOrReplaceTempView("sdk5")

	spark.sql("SELECT cluster, c_1, c_2, c_3, c_4, c_5, count(*) AS num, max(dist) AS maxdist, avg(dist) AS avgdist,stddev_pop(dist) AS stdev FROM sdk5 GROUP BY cluster, c_1, c_2, c_3, c_4, c_5 ORDER BY cluster").show()
	rdd_w_clusts_wnullclustk5 = rdd_w_clustsk5.map(lambda x: inclust(x,20))
	rdd_w_clusts_wnullclustk5.map(lambda y: (y[6],1)).reduceByKey(add).top(len(clusters.centers))

	spark.sql("SELECT Day, cluster, count(*) AS num_outliers, avg(dist) AS avgdist FROM sdk5 WHERE  dist > 20 GROUP BY Day, cluster ORDER BY Day, cluster").show()

	#spark.sql("SELECT cluster, doy, time, c_v,c_s,c_o, p_v,p_s,p_o FROM sdk5 WHERE cluster=<insert-clust-id-here> and dist >20 ORDER BY dist").show()


"""
day.saveAsTextFile('count_day_top5_complaint_type.out')

sc.stop()
"""










