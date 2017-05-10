import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import seaborn
from wordcloud import WordCloud

type_data = pd.read_csv('../output/comptypey.out', sep='\t', header=None)
type_data.columns = ['year','complain','count']
df = type_data.groupby('complain',as_index=False).sum()
df.set_index("complain", drop=True, inplace=True)

wordcloud = WordCloud()
wordcloud.generate_from_frequencies(frequencies=df[['count']].to_dict()['count'])
plt.figure(figsize=(12,12))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.savefig('../plots/complain_type_word_cloud.png')
