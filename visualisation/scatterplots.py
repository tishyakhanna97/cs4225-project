import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_theme(style="darkgrid")
dataframe = pd.read_csv("../data/all_results.csv")
#print(dataframe.head(10))
x1 = dataframe.num_cases
x2 = dataframe.Coronavirus___Worldwide_
x3 = dataframe.sentiment
y = dataframe.close
plt.scatter(x1, y, color='r')
plt.ylabel('Stock Price')
plt.xlabel('Covid Cases')
plt.title('Stocks vs New Covid Cases')
plt.show()
plt.scatter(x2, y, color='g')
plt.ylabel('Stock Price')
plt.xlabel('Google Search Trends')
plt.title('Stocks vs Google Search Trends')
plt.show()
plt.scatter(x3, y, color='r')
plt.ylabel('Stock Price')
plt.xlabel('Sentiment Score')
plt.title('Stocks vs Sentiment Score')
plt.show()

