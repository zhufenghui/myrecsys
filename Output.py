# -*- coding: utf-8 -*-
"""
Created on Sat Jun 06 17:02:00 2015

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

class Output(DataModel):
  
  def __init__(self, k, n):
      DataModel.__init__(self,k,n)
      
  def Create_csv(self,recdict,data):
        pathdir='C:/recsys/output/'
        #filename=time.strftime("%Y%m%d%H%M%S",time.localtime(time.time()))
        filename='tag'
        file=pathdir+filename+'.csv'
        csv=[]
        for user,item_ratings in recdict.iteritems(): 
             csv.append(user)
             try:
                 ori=sorted(data[user].items(), key=lambda x: x[1],reverse=True)
                 for line in ori:
                    csv.append(line[0][1].decode('utf-8'))
             except:
                 for line in data[user]:
                    csv.append(line)
             csv.append("show recommendation list below -------------")
             rec=sorted(item_ratings.items(),key=lambda x: x[1],reverse=True)
             for line in rec:
                 try:  # type is (channelid,pname) ,rating
                    csv.append(line[0][1].decode('utf-8'))
                 except: #if type is pname,rating
                    csv.append(line[0])
             csv.append('\n\n')           
        df=pd.DataFrame(csv)
        df.to_csv(file,index=False,header=False,encoding='utf-8')
        return True
                 
  def Create_xml(self,dic): 
         #dic=dict(ls)
         recServices = ET.Element('recServices')
         for devno,items in dic.iteritems():
             user = ET.SubElement(recServices, 'user')
             uid = ET.SubElement(user, 'uid')
             uid.text = devno   
             regionCode = ET.SubElement(user, 'regionCode')
             regionCode.text = 'xxx '
             numOfResult = ET.SubElement(user, 'numOfResult')
             numOfResult.text = '2'
             policy = ET.SubElement(user, 'policy')
             policy.text = 'null'
             programs = ET.SubElement(user, "programs")
             c=0
             for key ,value in items.iteritems():   
                if c==self.n:
                    break
                program = ET.SubElement(programs, "program") 
                type = ET.SubElement(program, "type")
                type.text =  'null'
                channelid = ET.SubElement(program, "channelid") 
                channelid.text =  str(key[0]).decode('utf-8') 
                itemid = ET.SubElement(program, "itemid") 
                itemid.text =  str(key[1]).decode('utf-8')  
                c+=1
         tree = ET.ElementTree(recServices)
         filename=time.strftime("%Y%m%d%H%M%S",time.localtime(time.time()))
       #  print ET.tostring(recServices, pretty_print=True, xml_declaration=True, encoding='UTF-8')
         tree.write('c:/Python27/data/JF_RecService_'+filename+'.xml', pretty_print=True,xml_declaration=True, encoding='UTF-8')
         return True