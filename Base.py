# -*- coding: utf-8 -*-
"""
Created on Sat Jun 06 16:30:18 2015

@author: admin
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Jun 05 09:06:54 2015

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

class DataModel:

   def __init__(self, k, n):
      """ initialize recommender
      currently, if data is dictionary the recommender is initialized
      to it.
      For all other data types of data, no initialization occurs
      k is the k value for k nearest neighbor
      metric is which distance formula to use
      n is the maximum number of recommendations to make"""
      self.k = k
      self.n = n
      self.channelid2name = {}
      self.programid2name={}
      self.name2ProgramID={}
      self.data_dic = {}
      self.distances_dic={}


   def convertChannelID2name(self, id):
      """Given product id number return product name"""
      if id in self.channelid2name:
         return self.channelid2name[id]
      else:
         return id
         
   def convertProgramID2name(self, id):
      """Given product id number return product name"""
      if id in self.programid2name:
         return self.programid2name[id]
      else:
         return id  
   
   def convertname2ProgramID(self, name):
      """Given product id number return product name"""
      if name in self.name2ProgramID:
         return self.name2ProgramID[name]
      else:
         return name
         
   def sql2dict(self, data_arr):   
      for line in data_arr:
            user=line[0] 
            item=line[1]
            rating=line[2]
            if user in self.data_dic:
                item_rating=self.data_dic[user]  # 将行字典赋予用户
            else:
                item_rating={}
            item_rating[item]=rating
            self.data_dic[user]=item_rating
      return self.data_dic
    
   def get_data(self,sql):
        dsn=co.makedsn('xx','xx','xx')
        db=co.connect('xx','xx',dsn)
        cursor=db.cursor()
        cursor.execute(sql)
        data=cursor.fetchall()
        cols = [i[0] for i in cursor.description]
        cursor.close()
        db.close()
        df=pd.DataFrame(data,columns=cols)
        return df,data
   
   def preference(self):
        sql="""select * from recommendation.preference_02 
        where channelid<>947 and  devno in  
        (SELECT devno from recommendation.user_sample_1000) """
        df,data=self.get_data(sql)
        grouped=df.groupby('DEVNO')
        cut=lambda x:pd.cut(x,5).labels+1
        trans=grouped['VALIDE_SERIES'].transform(cut)
        # for xmloutput ,is a tmperory method, create a pname,channelid dict can solve it 
        channel_newpname=zip(df.CHANNELID,df.NEWPNAME)
        df2=pd.DataFrame(df['DEVNO'])
        df2['channel_newpname']=channel_newpname
        df2['valide_trans']=trans
        data_arr=np.asanyarray(df2)
        data_dic=self.sql2dict(data_arr)
        return data_arr,data_dic
        
   def preference_tag(self):
        sql="""select * from recommendation.preference_tag where devno in  
               (SELECT devno from recommendation.user_sample_1000) """
        df,data=self.get_data(sql)
        #create user_items dict 
        user_items={}
        for line in data:
            user=line[0]
            item=line[1]
            if user in user_items:
                tmp=user_items[user]
            else:
                tmp=[]
            tmp.append(item)
            user_items[user]=tmp
        #create user_tags and tag_items below
        grouped1=df.groupby('DEVNO')
        grouped2=df.groupby('TYPE2')
        df_user_tags=grouped1['TYPE2'].value_counts().reset_index()  # forget group 
        df_user_tags[0]=df_user_tags[0].astype(float)                # the 1L keyerror
        df_tag_items=grouped2['PNAME'].value_counts().reset_index()
        df_tag_items[0]=df_tag_items[0].astype(float)
        #df_user_items=df[['DEVNO','PNAME']]
        arr_user_tags=np.asanyarray(df_user_tags)
        arr_tag_items=np.asanyarray(df_tag_items)
        #arr_user_items=np.asanyarray(df_user_items)
        user_tags=self.sql2dict(arr_user_tags)
        tag_items=self.sql2dict(arr_tag_items)
        return user_tags,tag_items,user_items
    
        
   def userRatings(self, id, n):
      """Return n top ratings for user with id"""
      print ("Ratings for " + self.userid2name[id])
      ratings = self.data[id]
      print(len(ratings))
      ratings = list(ratings.items())[:n]
      ratings = [(self.convertProductID2name(k), v)
                 for (k, v) in ratings]
      # finally sort and return
      ratings.sort(key=lambda artistTuple: artistTuple[1],
                   reverse = True)      
      for rating in ratings:
         print("%s\t%i" % (rating[0], rating[1]))


   def showUserTopItems(self, user, n):
      """ show top n items for user"""
      items = list(self.data[user].items())
      items.sort(key=lambda itemTuple: itemTuple[1], reverse=True)
      for i in range(n):
         print("%s\t%i" % (self.convertChannelID2name(items[i][0]),
                           items[i][1]))
                       
    
   def splitdict(self,dic,num):# 分割字典by用户，将文件分割
      ls=sorted(dic.iteritems())
      ls_c=[]
      block_len=len(ls)/num
      for i in range(num):
        if i<(num-1):
              ls_c.extend([ls[block_len*i:block_len*(i+1)]]) #extend 方法要加[]表示将作为一个block存储
        else:
              ls_c.extend([(ls[block_len*i:])])
      return ls_c
      
if __name__=='__main__':
    D=DataModel(5,10)
    user_tags,tag_items,user_items = D.preference_tag()
    data_arr,data = D.preference()
    #8760003667670704
