# coding: utf-8

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
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
plt.figure(figsize=[12,12])
plt.bar(year['year'],year['count'])
plt.title('Number of complaints by year')
plt.ylabel('Number of complaints')
plt.xlabel('Year')
plt.savefig('yearcount.png')


#Plot number of complaints by date
daily = pd.read_csv('../output/ymdcount.out', sep='\t', header=None)
daily.columns = ['date','count']
daily['datetime'] = daily['date'].apply(lambda x: str2datetime(x))
daily = daily.sort_values('datetime').reset_index(drop=True)
plt.figure(figsize=[15,15])
plt.plot(daily['datetime'],daily['count'])
plt.title('Number of complaints by date')
plt.ylabel("Number of complaints")
plt.xlabel('Date')
plt.savefig('datecount.png')

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
    plt.barh([j for j in range(len(data['count']))],data['count'])
    plt.yticks([j for j in range(len(data['count']))],data['agency'])
    plt.title('Top 10 agencies with the most complaints in %s'%str(i))
    plt.xlabel('Count')
    plt.ylabel('Agency')
plt.savefig('top10agency.png')

#Number of complaints by agency and year
agencies = agency['agency'].unique()
plt.figure(figsize=[28,24])
plt.subplots_adjust(hspace=0.4)
for n,i in enumerate(agencies):
    data = agency[agency['agency']==i].reset_index(drop=True)
    plt.subplot(7,4,n+1)
    plt.bar(data['year'],data['count'])
    plt.title("Yearly complaints to %s"%i)
    plt.xlabel("Year")
    plt.ylabel("Number of complaints")
plt.savefig('yearly_agency.png')

#top 20 zipcode with the most complaints from 2009-2017
zipyear = pd.read_csv('../output/zipcodey.out', sep='\t', header=None)
zipyear.columns = ['year','zipcode','count']
zipyear['zip'] = zipyear.zipcode.apply(lambda x: clean_zip(x))
zipyear = zipyear[zipyear['zip']!='Invalid']
top20 = zipyear.groupby(by=['zip'])['count'].sum().sort_values()[-20:]
plt.figure(figsize=[12,12])
plt.barh([i for i in range(20)],top20.values)
plt.yticks([i for i in range(20)],top20.index)
plt.xlabel('Number of complaints')
plt.ylabel('Zipcode')
plt.title('Total number of complaints from 2009-2017')
plt.savefig('top20zipcode.png')

#Number of complaints by zipcode and year
plt.figure(figsize=[28,24])
plt.subplots_adjust(hspace=0.4)
for n,i in enumerate(top20.index):
    data = zipyear[zipyear['zip']==i].reset_index(drop=True)
    plt.subplot(7,4,n+1)
    plt.bar(data['year'],data['count'])
    plt.title("Yearly complaints in %s"%i)
    plt.xlabel("Year")
    plt.ylabel("Number of complaints")
plt.savefig('zip_year.png')

#Number of complaints by borough and year
borough = pd.read_csv('../output/boroughy.out', sep='\t', header=None)
borough.columns = ['year','borough','count']
boroughs = borough['borough'].unique()
plt.figure(figsize=[28,24])
plt.subplots_adjust(hspace=0.4)
for n,i in enumerate(boroughs):
    data = borough[borough['borough']==i].reset_index(drop=True)
    plt.subplot(3,2,n+1)
    plt.bar(data['year'],data['count'])
    plt.title("Yearly complaints in %s"%i)
    plt.xlabel("Year")
    plt.ylabel("Number of complaints")
plt.savefig('borough_year.png')

#top 20 city with the most complaints from 2009-2017
city = pd.read_csv('../output/cityy.out', sep='\t', header=None)
city.columns = ['year','city','count']
top20 = city.groupby(by=['city'])['count'].sum().sort_values()[-20:]
plt.figure(figsize=[12,12])
plt.barh([i for i in range(20)],top20.values)
plt.yticks([i for i in range(20)],top20.index)
plt.xlabel('Number of complaints')
plt.ylabel('City')
plt.title('Total number of complaints from 2009-2017')
plt.savefig('top20cityall.png')

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
    plt.title('Top 20 cities with the most complaints in %s'%str(i))
    plt.xlabel('Count')
    plt.ylabel('city')
plt.savefig('cityyearly.png')


