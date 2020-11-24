import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import colors
from sklearn import preprocessing

sns.set_theme(style="darkgrid")
dataframe = pd.read_csv("../data/change_sentiment.csv")
x1 = dataframe.num_cases
x2 = dataframe.Coronavirus___Worldwide_
x3 = dataframe.sentiment
x4 = dataframe.changed

y = dataframe.close
d = dataframe.date
# plt.scatter(x1,y, color='b')
#
# plt.ylabel('Stock Price')
# plt.xlabel('Covid Cases')
# plt.title('Stocks vs New Covid Cases')
# plt.show()
# plt.scatter(x2, y, color='g')
# plt.ylabel('Stock Price')
# plt.xlabel('Google Search Trends')
# plt.title('Stocks vs Google Search Trends')
# plt.show()
# plt.scatter(x3, y, color='r')
# plt.ylabel('Stock Price')
# plt.xlabel('Sentiment Score')
# plt.title('Stocks vs Sentiment Score')
# plt.show()
y =(y-y.mean())/y.std()
x1 =(x1-x1.mean())/x1.std()


plt.plot(d, y, color='b')
plt.plot(d, x4,color='r')

plt.ylabel('Stocks and cases')
plt.xlabel('Date')
plt.title('Stocks and cases against time')
plt.legend()
plt.show()


