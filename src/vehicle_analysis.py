import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def preprocess(data):
    ''' Preprocess data by removing header '''

    header = data.first()
    data = data.filter(lambda x: x != header)
    data = data.map(lambda x: x.encode('utf-8')).mapPartitions(lambda x: reader(x))

    return data

def extract_date(date,dtype="y"):
    ''' Extract date info, 'y' = year, 'm' = month, 'd' = day' '''
    date = date.split("/")
    year = date[-1][:4]
    month = date[0]
    day = date[1]

    if dtype == 'y':
        return year
    elif dtype =='ym':
        return year+"/"+month
    elif dtype == "m":
        return month
    elif dtype == "ymd":
        return year+"/"+month+"/"+day
    elif dtype == "md":
        return month + "/" + day

def count_accident(data, date = 'ymd', fname = ''):
    '''Count number of complains by date'''

    year = data.map(lambda x: (extract_date(x[0], dtype=date), 1)).reduceByKey(add)

    if fname:
        year.map(lambda x: "%s\t%s"%(x[0],x[1])).saveAsTextFile(fname)
    else:
        print year.take(10)

def count_accident_by_column(data, ind = 3, date = 'y', fname = '', save = 1):
    ''' Count number of complains by column and date
        Provide index of the column name according to columns.txt
        Eg]Count by agency, ind = 3,  key = date + Agency, value = 1, (2017    NYPD, 1)
    '''

    if date != '':
        result = data.map(lambda x: ("%s\t%s"%(extract_date(x[0], dtype=date),x[ind]), 1)).reduceByKey(add)
    else:
        result = data.map(lambda x: (x[ind], 1)).reduceByKey(add)

    if fname:
        result.map(lambda x: "%s\t%s"%(x[0],x[1])).saveAsTextFile(fname)
    else:
        print result.take(10)

if __name__ == "__main__":

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1])
    
    data = preprocess(lines)
    
    #Count daily accident
    count_accident(data, date='ymd', fname='daily_accident_count.out')

    #Count accident by zipcode
    count_accident_by_column(data, ind=3, date='', fname='accident_by_zip.out')
    
    #Count accident by borough
    count_accident_by_column(data, ind=2, date='', fname='accident_by_borough.out')
    sc.stop()
