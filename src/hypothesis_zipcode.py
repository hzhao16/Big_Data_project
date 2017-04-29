import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import pandas as pd
import os

income = pd.read_csv('../output/income.csv')
population = pd.read_csv('../output/population.csv')
age_sex = pd.read_csv('../output/age_sex.csv')
zipcode = open('../output/zipcode.out','r')

zipc = []
complaints = []
for line in zipcode.readlines():
    zipc.append(line.split('\t')[0].strip())
    complaints.append(int(line.split('\t')[1].strip()))
    
zipc = zipc[1:101]
zipc = [int(x) for x in zipc]

complaint = pd.DataFrame({'Zipcode': np.asarray(zipc), 'Complaints': np.asarray(complaints[1:101])}, index = np.arange(100))
f1 = pd.merge(complaint, age_sex, how='left', on=['Zipcode'])
f2 = pd.merge(f1, population, how='left', on=['Zipcode'])
f3 = pd.merge(f2, income, how='left', on=['Zipcode'])
f3['Median age'] = np.asarray([float(i) for i in f3['Median age']])
f3['Sex ratio (males per 100 females)'] = np.asarray([float(i) for i in f3['Sex ratio (males per 100 females)']])
f3['Mean income'] = np.asarray([float(i) for i in f3['Mean income']])

from sklearn import linear_model

y = f3['Complaints'].values.reshape(-1, 1)
x = f3.drop(['Complaints', 'Zipcode'], axis = 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coefficients: \n', regr.coef_)
print("Mean squared error: %.2f"
      % np.mean((regr.predict(x) - y) ** 2))
print('Variance score: %.2f' % regr.score(x, y))

import numpy as np
import statsmodels.api as sm
x = sm.add_constant(x)
mod = sm.OLS(np.asarray(y), np.asarray(x))
res = mod.fit()
print(res.summary())

from scipy.stats import pearsonr 
r, p = pearsonr(f3['Median age'], f3['Complaints'])
print('Pearson’s correlation coefficient between Number of complaints and median age is {0} and its 2-tailed p-value is {1}'.format(r, p)+'\n')
r, p = pearsonr(f3['Sex ratio (males per 100 females)'], f3['Complaints'])
print('Pearson’s correlation coefficient between Number of complaints and Sex ratio (males per 100 females) is {0} and its 2-tailed p-value is {1}'.format(r, p)+'\n')
r, p = pearsonr(f3['Mean income'], f3['Complaints'])
print('Pearson’s correlation coefficient between Number of complaints and Mean income is {0} and its 2-tailed p-value is {1}'.format(r, p)+'\n')
r, p = pearsonr(f3['Population'], f3['Complaints'])
print('Pearson’s correlation coefficient between Number of complaints and Population is {0} and its 2-tailed p-value is {1}'.format(r, p)+'\n')