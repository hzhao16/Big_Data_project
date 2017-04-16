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
[https://data.cityofnewyork.us/Social-Services/311/wpe2-h2i5]https://data.cityofnewyork.us/Social-Services/311/wpe2-h2i5

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
$ spark-submit type_quality.py
```

## Data Summary

### Figure 2
We first run count.py to count the frequency of values in each column, and get the output file count_month.out in the folder Output. Then run locally
```sh
$ python plot_by_month.py
```
### Figure 4
We first run count.py to count the frequency of values in each column, and get the output file count_5.out in the folder Output. Then run locally
```sh
$ python plot_by_complaint_type.py
```
