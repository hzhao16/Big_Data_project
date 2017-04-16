import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import pandas as pd

f = open('../output/count_month.out','r')
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
plt.savefig('../plots/month.png')
f.close()



