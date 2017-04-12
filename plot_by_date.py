import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
from datetime import datetime
years = mdates.YearLocator()   # every year
months = mdates.MonthLocator()  # every month
yearsFmt = mdates.DateFormatter('%Y')

f1 = open('count_month.out','r')
xx = []
yy = []
for line in f1.readlines():
	month = line.split('\t')[0][1:-1].replace(", ","/")
	xx.append(month)
	yy.append(int(line.split('\t')[1]))
xx = [datetime.strptime(i, '%Y/%m') for i in xx]

plt.figure(figsize=[40,30])
plt.plot(yy[:-1],'o-')
#plt.xticks(range(2),[1,2], size='small')
plt.savefig('month.png')
f1.close()


f2 = open('count_day.out','r')
xx = []
yy = []
for line in f2.readlines():
	day = line.split('\t')[0]
	xx.append(day)
	yy.append(int(line.split('\t')[1]))
plt.figure(figsize=[40,30])
plt.plot(yy)
#plt.xticks(range(len(yy)), xx, size='small')
plt.savefig('day.png')
f2.close()