# -*- coding: utf-8 -*-
"""
Created on Sat Jun 06 17:07:09 2015

@author: admin
"""
import os 
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  #必须置于cx之前, 解决Oracle导入中文乱码问题
import cx_Oracle as co
import pandas as pd 
import numpy as np
from lxml import etree as ET
import time
import codecs 
from math import sqrt
from Base import DataModel
from Strategy import Strategy
from Output import Output

D=DataModel(5,20)
S=Strategy(5,20)
O=Output(5,20)
rec_all={}

#############################
##user based
#data_arr,data_dic=D.preference()
#S.computeNearestNeighbor(data_dic)
#for user in data_dic.keys()[:10]:
#    rec_all[user]=S.UserBased(data_dic,user)
#O.Create_csv(rec_all,data_dic)




#############################
#tag recommend
user_tags,tag_items,user_items = D.preference_tag() 
for user in user_items.keys()[:10]:
    rec_all[user]=S.TagRec(user)
O.Create_csv(rec_all,data_dic)

################################
##output slopone
#data_arr,data_dic=D.preference()
#S.computeDeviations(data_dic)
#for user in data_dic.keys()[:10]:
#    rec_all[user]=S.SlopeOne(data_dic[user])
#O.Create_csv(rec_all,data_dic)

################################
#for user,reclist in rec_all.items():
#    print user
#    for item,rating in reclist:
#        print item.decode('utf-8'),rating
#    print "orignal list below:"
#    for itme,rating in data_dic[user]:
#        print item.decode('utf-8'),rating 