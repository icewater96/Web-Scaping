# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pandas as pd
import datetime
import time

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText


# This a client-side-generated webpage
url_dict = {}
url_dict['Sale of the Day'] = r'http://www.garnethill.com/sale-clearance/sale-of-the-day/#?p=Q&lbc=garnethill&uid=294457867&ts=custom&w=*&method=and&view=grid&af=cat2%3asaleclearance_saleoftheday%20cat1%3asaleclearance&isort=price'
url_dict['Women Fashion'  ] = r'http://www.garnethill.com/sale-clearance/womens-fashion/#?p=Q&lbc=garnethill&uid=294525647&ts=custom&w=*&method=and&view=grid&af=cat2%3asaleclearance_womensfashion%20cat1%3asaleclearance&isort=price'
url_dict['Girl Clothing'  ] = r'http://www.garnethill.com/sale-clearance/girls-clothing/#w=*&af=cat2:saleclearance_girlsclothing cat1:saleclearance'


#%% Scraping with selenium

while True:
    try:
        #br = webdriver.Firefox()
        br = webdriver.Chrome( 'chromedriver' )
        # Headless
        #br = webdriver.PhantomJS()
        
        price_list = []
        current_time = datetime.datetime.now()
        for url_name, url_link in url_dict.items():
            print url_name
            br.get( url_link )
            WebDriverWait( br, 200 ).until( EC.presence_of_element_located( (By.CLASS_NAME, "priceNow" ) ) )
        
            # br.page_source = HTML
            soup_4 = BeautifulSoup(br.page_source)
        
            all_items = soup_4.find_all( 'span', class_='priceNow')
        
            for item in all_items:
                # To find product name
                p1 = str(item.text)
                p2 = p1.split('-')
                p3 = p2[0].split('$')
                price = float(p3[1])
            
                a = item.find_parent('div')
                b = a.find_previous_sibling('h2')
                price_list.append( {'Category': url_name, 'Prodcut': b.text, 'Price': price, 'Log': item.text, 'Time': str(current_time)[:19] })
            
        br.close()
            
        price_df = pd.DataFrame(price_list)
        
        
        cutoff_price = 15.0;
        category_list = []
        count_list = []
        for category in url_dict.keys():
            category_list.append(category)
            count_list.append(str(len(price_df.ix[ (price_df['Category'] == category) & (price_df['Price'] < cutoff_price), 'Price' ])))
        
        
        #%% Send email
        
        
        with open( 'jianwang.txt', 'r' ) as f:
            lines = f.readlines()
            
        gmail_user = lines[0]
        gmail_pwd = lines[1]
        FROM = 'wawahaha@gmail.com'
        TO = ['songying@gmail.com', 'l96l96@gmail.com']
        SUBJECT = 'Updates'
        TEXT = 'dfadsdfa'
        
        # Prepare actual message
        msg = MIMEMultipart()
        msg['From'] = FROM
        msg['To'] = 'somewhere'
        msg['Subject'] = "Price Trends (" + ', '.join(count_list) + ') at ' + str(datetime.datetime.now())
         
        body = 'Number of ' + ', '.join(category_list) + ' < $' + str(cutoff_price) 
        msg.attach(MIMEText(body, 'plain'))
        message = msg.as_string()
        
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(gmail_user, gmail_pwd)
        server.sendmail(FROM, TO, message)
        server.close()
        
        print 'Sleeping ...'
        time.sleep(20*60)
        print 'Waking up ...'
    except:
        pass