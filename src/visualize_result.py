# coding: utf-8

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import seaborn

#Util functions
def str2datetime(x):

    return datetime.strptime(x,'%Y/%m/%d')

def clean_zip(x):
    try:
        temp = int(x[:5])
        if temp>=10001 and temp<=14975:
            x = x[:5]
        else:
            x = "Invalid"
    except:
        x = "Invalid"

    return x

#Plot number of complaints by year
year = pd.read_csv('../output/ycount.out', sep='\t', header=None)
year.columns = ['year','count']
year = year.sort_values('year')
plt.figure(figsize=[12,12])
plt.bar(np.arange(len(year['count'])),year['count'])
plt.title('Number of complaints by year', fontsize=16)
plt.ylabel('Number of complaints',fontsize=16)
plt.xticks(np.arange(len(year['count'])),year['year'].apply(str))
plt.xlabel('Year',fontsize=16)
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)
plt.savefig('../plots/yearcount.png')

#Plot number of complaints by date
daily = pd.read_csv('../output/ymdcount.out', sep='\t', header=None)
daily.columns = ['date','count']
daily['datetime'] = daily['date'].apply(lambda x: str2datetime(x))
daily = daily.sort_values('datetime').reset_index(drop=True)
plt.figure(figsize=[15,15])
plt.plot(daily['datetime'],daily['count'])
plt.title('Number of complaints by date',fontsize=16)
plt.ylabel("Number of complaints",fontsize=16)
plt.xlabel('Date',fontsize=16)
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)
plt.savefig('../plots/datecount.png')

#Top 10 agencies with the most complaints in each year
agency = pd.read_csv('../output/agencyy.out', sep='\t', header=None)
agency.columns = ['year','agency','count']
agency = agency[agency['count']>100]
years = agency['year'].unique()
years.sort()
plt.figure(figsize=[24,24])
for n,i in enumerate(years):
    data = agency[agency['year']==i].reset_index(drop=True)
    data = data.sort_values('count',ascending=False)
    data = data[:10]
    plt.subplot(3,3,n+1)
    plt.barh(np.arange(len(data['count'])),data['count'])
    plt.yticks(np.arange(len(data['count'])),data['agency'])
    plt.title('Top 10 agencies with the most complaints in %s'%str(i),fontsize=16)
    plt.xlabel('Count',fontsize=16)
    plt.ylabel('Agency',fontsize=16)
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
plt.savefig('../plots/top10agency.png')

#Number of complaints by agency and year
agencies = agency['agency'].unique()
plt.figure(figsize=[28,24])
plt.subplots_adjust(hspace=0.4)
for n,i in enumerate(agencies):
    data = agency[agency['agency']==i].reset_index(drop=True)
    data = data.sort_values('year')
    plt.subplot(8,3,n+1)
    plt.bar(np.arange(len(data['count'])),data['count'])
    plt.title("Yearly complaints to %s"%i,fontsize=16)
    plt.xlabel("Year",fontsize=12)
    plt.ylabel("Number of complaints",fontsize=16)
    plt.xticks(np.arange(len(data['count'])),data['year'].apply(str))
    plt.xticks(fontsize=13)
    plt.yticks(fontsize=16)
plt.savefig('../plots/yearly_agency.png')


#top 20 zipcode with the most complaints from 2009-2017
zipyear = pd.read_csv('../output/zipcodey.out', sep='\t', header=None)
zipyear.columns = ['year','zipcode','count']
zipyear['zip'] = zipyear.zipcode.apply(lambda x: clean_zip(x))
zipyear = zipyear[zipyear['zip']!='Invalid']
top20 = zipyear.groupby(by=['zip'])['count'].sum().sort_values()[-20:]
plt.figure(figsize=[12,12])
plt.barh(np.arange(20),top20.values)
plt.yticks(np.arange(20),top20.index)
plt.xlabel('Number of complaints',fontsize=16)
plt.ylabel('Zipcode',fontsize=16)
plt.title('Total number of complaints from 2009-2017',fontsize=16)
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)
plt.savefig('../plots/top20zipcode.png')

#Number of complaints by zipcode and year
plt.figure(figsize=[28,24])
plt.subplots_adjust(hspace=0.4)
for n,i in enumerate(top20.index):
    data = zipyear[zipyear['zip']==i].reset_index(drop=True)
    data = data.sort_values('year')
    plt.subplot(7,3,n+1)
    plt.bar(np.arange(len(data['count'])),data['count'])
    plt.title("Yearly complaints in %s"%i,fontsize=16)
    plt.xlabel("Year",fontsize=16)
    plt.ylabel("Number of complaints",fontsize=16)
    plt.xticks(np.arange(len(data['count'])),data['year'].apply(str))
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
plt.savefig('../plots/zip_year.png')

#Number of complaints by borough and year
borough = pd.read_csv('../output/boroughy.out', sep='\t', header=None)
borough.columns = ['year','borough','count']
boroughs = borough['borough'].unique()
plt.figure(figsize=[28,24])
plt.subplots_adjust(hspace=0.4)
for n,i in enumerate(boroughs):
    data = borough[borough['borough']==i].reset_index(drop=True)
    data = data.sort_values('year')
    plt.subplot(3,2,n+1)
    plt.bar(np.arange(len(data['count'])),data['count'])
    plt.title("Yearly complaints in %s"%i,fontsize=16)
    plt.xlabel("Year",fontsize=16)
    plt.ylabel("Number of complaints",fontsize=16)
    plt.xticks(np.arange(len(data['count'])),data['year'].apply(str))
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
plt.savefig('../plots/borough_year.png')

#top 20 city with the most complaints from 2009-2017
city = pd.read_csv('../output/cityy.out', sep='\t', header=None)
city.columns = ['year','city','count']
top20 = city.groupby(by=['city'])['count'].sum().sort_values()[-20:]
plt.figure(figsize=[12,12])
plt.barh([i for i in range(20)],top20.values)
plt.yticks([i for i in range(20)],top20.index)
plt.xlabel('Number of complaints',fontsize=16)
plt.ylabel('City',fontsize=16)
plt.title('Total number of complaints from 2009-2017',fontsize=16)
plt.xticks(fontsize=16)
plt.yticks(fontsize=11)
plt.savefig('../plots/top20cityall.png')

#Top 20 cities with the most complaints in each year
cyears = city['year'].unique()
cyears.sort()
plt.figure(figsize=[24,24])
for n,i in enumerate(cyears):
    data = city[city['year']==i].reset_index(drop=True)
    data = data.sort_values('count',ascending=False)
    data = data[:20]
    plt.subplot(5,2,n+1)
    plt.barh([j for j in range(len(data['count']))],data['count'])
    plt.yticks([j for j in range(len(data['count']))],data['city'])
    plt.title('Top 20 cities with the most complaints in %s'%str(i),fontsize=16)
    plt.xlabel('Count',fontsize=12)
    plt.ylabel('city',fontsize=16)
    plt.xticks(fontsize=13)

plt.savefig('../plots/top20city.png')
