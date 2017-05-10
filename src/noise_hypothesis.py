import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import seaborn


#Util functions
def str2datetime(x):

    return datetime.strptime(x,'%Y/%m/%d')

#Load noise data
noise_data = pd.read_csv('../output/daily_noise.out', sep='\t', header=None)
noise_data.columns = ['date','noise_count']
noise_data['datetime'] = noise_data['date'].apply(lambda x: str2datetime(x))
noise_data = noise_data.sort_values('datetime').reset_index(drop=True)

#Load accident data
acc_data = pd.read_csv('../output/daily_accident_count.out', sep='\t', header=None)
acc_data.columns = ['date','acc_count']
acc_data['datetime'] = acc_data['date'].apply(lambda x: str2datetime(x))
acc_data = acc_data.sort_values('datetime').reset_index(drop=True)

df = pd.merge(noise_data, acc_data, on='datetime', how='outer')
df = df[~pd.isnull(df).any(axis=1)]
df = df.reset_index(drop=True)

ax = df.plot()
fig = ax.get_figure()
fig.savefig('../plots/noise_accident_plot.png')

import scipy.stats
print "Pearson correlation is " + str(scipy.stats.pearsonr(df['acc_count'], df['noise_count']))

from pandas.stats.api import ols
res = ols(y=df['noise_count'], x=df['acc_count'])
print res
