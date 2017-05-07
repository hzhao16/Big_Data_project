import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import pandas as pd
import os

heat = open('../output/day_heat.out')
heat1 = open('../output/day_not_heat.out')

month = []
value = []
for line in heat.readlines():
    month.append(line.replace('(', ' ').replace(')', ' ').replace(' ', '').strip().split(',')[0])
    value.append(line.replace('(', ' ').replace(')', ' ').replace(' ', '').strip().split(',')[2])

month1 = []
value1 = []
for line in heat1.readlines():
    month1.append(line.replace('(', ' ').replace(')', ' ').replace(' ', '').strip().split(',')[0])
    value1.append(line.replace('(', ' ').replace(')', ' ').replace(' ', '').strip().split(',')[2])

value = [int(i) for i in value]
value1 = [int(i) for i in value1]

value2 = np.add(np.array(value), np.array(value1))
value3 = value / value2

fig, ax1 = plt.subplots(figsize = (12, 8))
t = np.arange(1, 13)
ax1.plot(month, value, 'bo-', label = 'Heating Complaints')
ax1.set_xlabel('Month')
ax1.set_ylabel('Total number of complaints for heating problems', size = 'xx-large', color='b')
ax1.tick_params('y', colors='b')

plt.legend(loc = 2)

ax2 = ax1.twinx()
ax2.plot(month1, value1, 'g*-', label = 'Other Complaints')
ax2.set_ylabel('Total number of other complaints', size = 'xx-large', color='g')
ax2.tick_params('y', colors='g')
plt.legend(loc = 1)


fig.tight_layout()

plt.title('Heating complaints VS Other complaints')
plt.savefig('../plots/cor41.png')

fig, ax1 = plt.subplots(figsize = (12, 8))
t = np.arange(1, 13)
ax1.plot(month, value, 'bo-', label = 'Heating Complaints')
ax1.set_xlabel('Month')
ax1.set_ylabel('Total number of complaints for heating problems', size = 'xx-large', color='b')
ax1.tick_params('y', colors='b')
plt.legend(loc = 2)

ax2 = ax1.twinx()
ax2.plot(month1, value3, 'g*-', label = 'Percentage')
ax2.set_ylabel('Percentage of heating complaints', size = 'xx-large', color='g')
ax2.tick_params('y', colors='g')
plt.legend(loc = 1)

fig.tight_layout()

plt.title('Heating complaints and its percentage of all complaints')
plt.savefig('../plots/cor42.png')
