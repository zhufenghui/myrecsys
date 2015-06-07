# -*- coding: utf-8 -*-
"""
Created on Sat Jun 06 11:35:02 2015

@author: admin
"""
import os 
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  #必须置于cx之前, 解决Oracle导入中文乱码问题
import cx_Oracle as co
import pandas as pd 
import numpy as np
import time
#from datetime import date, time, datetime, timedelta
import codecs 
from math import sqrt
dsn=co.makedsn('192.168.113.226','11521','boss')
db=co.connect('zsboss','zsboss123',dsn)
cursor=db.cursor()
cursor.execute("select * from recommendation.preference_tag where devno in \
       (SELECT devno from recommendation.user_sample_1000) ")
data=cursor.fetchall()
cols = [i[0] for i in cursor.description]
cursor.close()
db.close()
df=pd.DataFrame(data,columns=cols)
grouped1=df.groupby('DEVNO')
grouped2=df.groupby('TYPE2')
df_user_tags=grouped['TYPE2'].value_counts().reset_index()
df_user_tags[0]=df_user_tags[0].astype(float)
df_tag_items=grouped['PNAME'].value_counts().reset_index()
df_tag_items[0]=df_tag_items[0].astype(float)
df_user_items=df[['DEVNO','PNAME']]
arr_user_tags=np.asanyarray(df_user_tags)
arr_tag_items=np.asanyarray(df_tag_items)