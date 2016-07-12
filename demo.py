# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import time

# This a client-side-generated webpage
url = r'http://www.garnethill.com/sale-clearance/sale-of-the-day/#?p=Q&lbc=garnethill&uid=294457867&ts=custom&w=*&method=and&view=grid&af=cat2%3asaleclearance_saleoftheday%20cat1%3asaleclearance&isort=price'

# Step 1: retrieve HTML
# Step 2: HTML -> BeautifulSoup
# or use Selenium handling HTML


#%% Retrieve HTML from a URL
# Method #1
import requests
res = requests.get(url)
html_1 = res.text


# Method #2
import urllib2
html_2 = urllib2.urlopen(url).read()


#%% Make a soup
from bs4 import BeautifulSoup

# All 3 methods results in the same soup
soup_1 = BeautifulSoup(html_1)
soup_2 = BeautifulSoup(html_2)
soup_3 = BeautifulSoup(urllib2.urlopen(url))


#%% Scraping with selenium
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

#br = webdriver.Firefox()
br = webdriver.Chrome( 'chromedriver' )
# Headless
#br = webdriver.PhantomJS()

br.get( url )
WebDriverWait( br, 200 ).until( EC.presence_of_element_located( (By.CLASS_NAME, "priceNow" ) ) )


# br.page_source = HTML
soup_4 = BeautifulSoup(br.page_source)

all_items = soup_4.find_all( 'span', class_='priceNow')

print
br.close()
