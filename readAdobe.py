# -*- coding: utf-8 -*-
"""
Created on Tue Aug 02 11:28:47 2016

@author: arawlings
"""

import sys
import os
import pandas as pd
import csv
import datetime as date
import datetime
import codecs
import time
import numpy as np
import re
import sqlalchemy
import pyodbc 

os.chdir('N:\\analytics\\KPIs\\adobe\\data')


def fixPercentage(value):
    try:
        return float(value.strip('%'))/100
    except:
        return value
    

def fixInt(value):
    try:
        return int(value.replace(',',''))
    except:
        return value

def readFile(filename,dtypes=None):

    encoding = 'utf-8' 
    f = codecs.open (filename, 'rb', encoding,errors='replace')
        
        # Read first 10 lines and throw away
    for _ in xrange(17):
        next(f)
    lines = f.readlines()
    f.close()
        
    w = codecs.open ("test.csv", 'w', encoding)
    w.writelines(lines)
    w.close()
    f = codecs.open ("test.csv", 'rb', encoding,errors='replace')
    df =  pd.read_csv(f,sep=',')
    f.close()
    return df
  
    


def formatWeek(weekrange):
     if weekrange == 'Total':
        return 'Total'
     weekrange = re.search('(.+)(\s-+)',weekrange).group(1)

     d =time.strptime(weekrange,"%b %d, %Y")
     return  datetime.date(d.tm_year,d.tm_mon,d.tm_mday)
     

print('Read Internal')
internal = readFile('internal.csv')
internal.columns=['Dimension','Week','Total Internal','TopNav','AllProductPage','RCHP_TryAWebsite','RCHP_Marquee','NVHP_Marquee','NVHP_Featured','DigitalCategory','DigitalLeftNav','SpecialsZone']
internal['Week']=internal['Week'].apply(formatWeek)

print('Read External')

external = readFile('external.csv')
external.columns=['Dimension','Week','Total External','DTI','Organic Search','Paid Branded','Paid Non Branded','Retent Email','Affiliate']
external['Week']=external['Week'].apply(formatWeek)


print('Read Internal Take Rate')

itr = readFile('itr.csv')
del itr['Page URI + Internal Total (Percent)']

itr.columns=['Dimension','Week','Total Internal TR','TopNav TR','AllProductPage TR','RCHP_TryAWebsite TR','RCHP_Marquee TR','NVHP_Marquee TR','NVHP_Featured TR','DigitalCategory TR','DigitalLeftNav TR','SpecialsZone TR']
itr['DigitalCategory TR'].fillna('0.0%',inplace=True)

itr['Week']=itr['Week'].apply(formatWeek)
  
fixPercColumns = ['Total Internal TR','TopNav TR','AllProductPage TR','RCHP_TryAWebsite TR','RCHP_Marquee TR','NVHP_Marquee TR','NVHP_Featured TR','DigitalCategory TR','DigitalLeftNav TR','SpecialsZone TR']   
for col in fixPercColumns:
    itr[col] = itr[col].apply(fixPercentage)

     
print('Read External Take Rate')

extr = readFile('extr.csv')
extr.columns=['Dimension','Week','Total External TR','DTI TR','Organic Search TR','Paid Branded TR','Paid Non Branded TR','Retent Email TR','Affiliate TR']
extr['Week']=extr['Week'].apply(formatWeek)

  
fixPercColumns = ['Total External TR','DTI TR','Organic Search TR','Paid Branded TR','Paid Non Branded TR','Retent Email TR','Affiliate TR']   
for col in fixPercColumns:
    extr[col] = extr[col].apply(fixPercentage)
 
  
fixPercColumns = ['Total External TR','DTI TR','Organic Search TR','Paid Branded TR','Paid Non Branded TR','Retent Email TR','Affiliate TR']   
for col in fixPercColumns:
    extr[col] = extr[col].apply(fixPercentage)   
    
fixIntColumns = ['Total Internal','TopNav','AllProductPage','RCHP_TryAWebsite','RCHP_Marquee','NVHP_Marquee','NVHP_Featured','DigitalCategory','DigitalLeftNav','SpecialsZone']    
for col in fixIntColumns:
    internal[col] = internal[col].apply(fixInt)   
    
fixIntColumns = ['Total External','DTI','Organic Search','Paid Branded','Paid Non Branded','Retent Email','Affiliate']    
for col in fixIntColumns:
    external[col] = external[col].apply(fixInt)       

print('Merge')
   
merged = internal.merge(itr).merge(extr).merge(external)
    
    
print('Write Merged Dataframe to Database')

engine = sqlalchemy.create_engine("mssql+pyodbc://Vistaprint")
merged=merged[merged['Week'] <> 'Total']

merged['Other External'] = merged['Total External'] - (merged['Retent Email']+merged['Paid Non Branded']+merged['Paid Branded']+merged['Organic Search']+merged['DTI']+merged['Affiliate'])
merged['Other Internal'] = merged['Total Internal'] - merged[['TopNav','AllProductPage','RCHP_TryAWebsite','RCHP_Marquee','NVHP_Marquee','NVHP_Featured','DigitalCategory','DigitalLeftNav','SpecialsZone']].sum(axis=1,numeric_only=True)
merged.to_sql("scratch.arawlings_adobe_websites", engine, if_exists='replace')

print('Read Product Page Visits')

prod = readFile('product.csv')
prod.columns = ['Dimension','Week','Site_Visits','Websites_Visits','Social_Visits','LocalListings_Visits','DIFY_Visits','Tower_Visits_New','Tower_Visits_Old','Email Marketing Visits']
prod=prod[prod['Week'] <> 'Total']

prod['Week']=prod['Week'].apply(formatWeek)
fixIntColumns = ['Site_Visits','Websites_Visits','Social_Visits','LocalListings_Visits','DIFY_Visits','Tower_Visits_New','Tower_Visits_Old','Email Marketing Visits']    
for col in fixIntColumns:
    prod[col] = prod[col].apply(fixInt)
     
print('Write Product Page Visits to DB')

prod.to_sql("scratch.arawlings_adobe_product_visits", engine, if_exists='replace')



