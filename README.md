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

Then for the summary of data quality, which is counting the number of empty/missing/invalid values in each column, sign in to dumbo, and run
```sh
$ spark-submit data_quality.py
```

