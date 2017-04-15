import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import pandas as pd

f = open('count_month.out','r')
xx = []
yy = []
for line in f.readlines():
	month = line.split('\t')[0]
	xx.append(month)
	yy.append(int(line.split('\t')[1]))
date = xx[::3]

plt.figure(figsize=[15,12])
plt.plot(yy[:-1],'o-')
plt.xticks(np.arange(0,len(yy),3),date,size='small',rotation='70')
plt.title('Number of Complaints per month')
plt.xlabel('Month')
plt.ylabel('Number of Complaints')
plt.savefig('month.png')
f.close()
plt.clf()

f = open('count_day.out','r')
xx = []
yy = []
for line in f.readlines():
	day = line.split('\t')[0]
	xx.append(day)
	yy.append(int(line.split('\t')[1]))
date = [datetime.strptime(x,'%Y-%m-%d') for x in xx]
plt.figure(figsize=[15,12])
l = pd.Series(data=yy,index=date)
l.plot()
plt.title('Number of Complaints per day')
plt.xlabel('Time')
plt.ylabel('Number of Complaints')
plt.savefig('day.png')
f.close()




