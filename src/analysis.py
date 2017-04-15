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

def count_complain_date(data, date = 'y', fname = ''):
    '''Count number of complains by date'''

    year = data.map(lambda x: (extract_date(x[1], dtype=date), 1)).reduceByKey(add)

    if fname:
        year.map(lambda x: "%s\t%s"%(x[0],x[1])).saveAsTextFile(fname)
    else:
        print year.take(10)

def count_column_date(data, ind = 3, date = 'y', fname = '', save = 1):
    ''' Count number of complains by column and date
        Provide index of the column name according to columns.txt
        Eg]Count by agency, ind = 3,  key = date + Agency, value = 1, (2017    NYPD, 1)
    '''

    result = data.map(lambda x: ("%s\t%s"%(extract_date(x[1], dtype=date),x[ind]), 1)).reduceByKey(add)

    if fname:
        result.map(lambda x: "%s\t%s"%(x[0],x[1])).saveAsTextFile(fname)
    else:
        print result.take(10)

if __name__ == "__main__":

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1])
    
    data = preprocess(lines)
    
    #Count number of complaints by year and year/month
    #count_complain_date(data, date='y', fname = "ycount.out")
    count_complain_date(data, date='ymd', fname = "ymdcount.out")
    '''
    #Count number of complaints by agency, year and year/month
    count_column_date(data, ind = 3, date='y', fname = "agencyy.out")
    count_column_date(data, ind = 3, date='ym', fname = "agencyym.out")

    #Count number of complaints by complaint type, year and year/month
    count_column_date(data, ind = 5, date='y', fname = "comptypey.out")
    count_column_date(data, ind = 5, date='ym', fname = "comptypeym.out")

    #Count number of complaints by zip, year and year/month
    count_column_date(data, ind = 8, date='y', fname = "zipcodey.out")
    count_column_date(data, ind = 8, date='ym', fname = "zipcodeym.out")

    #Count number of complaints by city ,year and year/month
    count_column_date(data, ind = 16, date='y', fname = "cityy.out")
    count_column_date(data, ind = 16, date='ym', fname = "cityym.out")

    #Count number of complaints by borough, year and year/month
    count_column_date(data, ind = 23, date='y', fname = "boroughy.out")
    count_column_date(data, ind = 23, date='ym', fname = "boroughym.out")
    '''
    sc.stop()
