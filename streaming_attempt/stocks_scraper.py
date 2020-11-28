from bs4 import BeautifulSoup
import requests


def get_latest_stock():
    url = 'https://www.marketwatch.com/investing/index/comp'
    page = requests.get(url)
    # print(page.content)
    soup = BeautifulSoup(page.content, 'lxml')
    close_comps = soup.find("td", "table__cell u-semi")
    print(close_comps.text)
    return close_comps.text


get_latest_stock()