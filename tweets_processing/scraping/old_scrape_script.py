from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
from urllib.request import build_opener, HTTPCookieProcessor, Request

tweet_id = 1324028733452247040
url = f"https://twitter.com/anyuser/status/{tweet_id}"

r = urlopen(Request(url, headers={'User-Agent': "Mozilla/5.0 (compatible;  MSIE 7.01; Windows NT 5.0)"})).read().decode('utf-8')
opener = build_opener(HTTPCookieProcessor())
response = opener.open(url, timeout=30)
content = response.read()
html = driver.get(url)
soup = BeautifulSoup(driver.page_source, features="lxml")
meta = soup.find('meta', attrs={'http-equiv': 'refresh'})
tweet_text = soup.find_all("span", {"class": "css-901oao"})

tweets = soup.find_all('li', {"data-item-type":"tweet"})
tweet = Tweet.from_html(urlopen(url))
tweet = Tweet.from_soup(soup)
for i in tweet.items():
  print(i)