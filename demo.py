# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

url = r'http://www.garnethill.com/sale-clearance/sale-of-the-day/#?p=Q&lbc=garnethill&uid=294457867&ts=custom&w=*&method=and&view=grid&af=cat2%3asaleclearance_saleoftheday%20cat1%3asaleclearance&isort=price'


#%% Retrieve HTML from a URL
# Method #1
import requests
res = requests.get(url)
html_1 = res.text


# Method #2
import urllib2
html_2 = urllib2.urlopen(url).read()

from bs4 import BeautifulSoup

