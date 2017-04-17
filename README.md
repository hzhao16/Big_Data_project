# Big_Data_project Part 1

## Contributors
Hezhi Wang  (hw1567)

Han Zhao (hz1411)

Jin U Bak (jub205)

## Dataset
Dataset is downloaded from the following links:

Dataset for 2009:
[https://data.cityofnewyork.us/Social-Services/new-311/9s88-aed8](https://data.cityofnewyork.us/Social-Services/new-311/9s88-aed8)

Dataset for 2010-present: 
[https://data.cityofnewyork.us/Social-Services/311/wpe2-h2i5](https://data.cityofnewyork.us/Social-Services/311/wpe2-h2i5)

Two datasets are combined as one and analyzed. 

Dataset is also available on NYU HPC HDFS, **/user/jub205/311all.csv**

## Data Quality Issues
We first generated columns.txt for base type and semantic type of each columns, which can be accessed at **/user/jub205/columns.txt**.

For data quality issues, first we count the number of empty/missing/invalid values in each column. Sign in to dumbo, and run
```sh
$ spark-submit data_quality.py
```
To generate a summary of the results generated, run
```sh
$ spark-submit Type_analysis.py
```
For a csv version of the result above, run locally,

```sh
$ python transfer_type_result_to_csv.py
```

## Data Summary

### Figure 2
We first run count_by_datetime.py on dumbo to count the frequency of values in each column, and get the output file count_month.out in the folder Output. Then run locally
```sh
$ python plot_by_month.py
```
### Figure 4
We first run count.py to count the frequency of values in each column, and get the output file count_5.out in the folder Output. Then run locally

```sh
$ python plot_by_complaint_type.py
```

Python script, analysis.py in src directory contains functions to compute frequency of values in different columns in various time frequency.For example, if a user wants to get the frequency of values in "city" column in each year, the following function call can be used.

count_column_date(data, ind = 16, date='y', fname = "city.out")

data is an RDD of data file/csv file, ind is the index of the column(Details on column index can be found in columns.txt), date is the time frequency, 'y' is for year, 'ym' is year/month, 'ymd' is for full date. fname is the output file name.

The above count_column_date function call will compute the frequency of values in city column by year.

Running analysis.py will generate counts on different columns with various date frequency.

```sh
$ spark-submit analysis.py /user/jub205/311all.csv
```

In order to get output, you can run the following command.

```sh
$ hfs -getmerge <output filename> <name you want to save as>
```

After getting all the output files, visualize.py can be run to plot figures in the report.

```sh
$ python visualize.py
```
