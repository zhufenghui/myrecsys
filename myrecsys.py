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

class recommender:

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
      self.data_dic = {}
      self.distances_dic={}
      #
      # The following two variables are used for Slope One
      # 
      self.frequencies = {}
      self.deviations = {}
      # for some reason I want to save the name of the metric
      # if data is dictionary set recommender data to it
      #

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
      
   def preference(self):
        dsn=co.makedsn('192.168.113.226','11521','boss')
        db=co.connect('zsboss','zsboss123',dsn)
        cursor=db.cursor()
        cursor.execute("select * from recommendation.preference_02 where devno in \
                   (SELECT devno from recommendation.user _sample_1000) ")
        data=cursor.fetchall()
        cols = [i[0] for i in cursor.description]
        cursor.close()
        db.close()
        df=pd.DataFrame(data,columns=cols)
        #df2=df[:10]
        #print df2
        grouped=df.groupby('DEVNO')
        cut=lambda x:pd.cut(x,5).labels+1
        trans=grouped['VALIDE_SERIES'].transform(cut)
        channel_newpname=zip(df.CHANNELID,df.NEWPNAME)
        df2=pd.DataFrame(df['DEVNO'])
        df2['channel_newpname']=channel_newpname
        df2['valide_trans']=trans
        data_arr=np.asanyarray(df2)
        return df2,data_arr 
        
   def preference_tag(self):
        dsn=co.makedsn('192.168.113.226','11521','boss')
        db=co.connect('zsboss','zsboss123',dsn)
        cursor=db.cursor()
        cursor.execute("select * from recommendation.preference_tag where devno in \
               (SELECT devno from recommendation.user_sample_1000) ")
        data=cursor.fetchall()
        cols = [i[0] for i in cursor.description]
        cursor.close()
        db.close()
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
        df=pd.DataFrame(data,columns=cols)
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
   
   def TagRec(self,user):
        user_tags,tag_items,user_items = self.preference_tag() # do not run everynot 
        recommend={}
        tagged_items=user_items[user]
        for tag,wt in user_tags[user].items():
           for item, wi in tag_items[tag].items():
               if item not in tagged_items:
                   if item not in recommend:
                       recommend[item]=wt*wi
                   else:
                       recommend[item]+=wt*wi
        recommend_sort=sorted(recommend.items(),key=lambda x:x[1],reverse=True)[:20]
        return dict(recommend_sort)
                      
   def output(self,recdict):
        csv=[]
        for user,item_rating in recdict.iteritems(): 
             item_rating_old=userdata[user]
             old=sorted(item_rating_old.items(), key=lambda x: x[1],reverse=True)
             topN=sorted(item_rating.items(), key=lambda x: x[1],reverse=True)
             #print len(old),len(topN)
             for i in range(10):
                 try:
                  #  print user, topN[i][0].decode('utf-8'),topN[i][1] ,old[i][0].decode('utf-8'),\
                 #old[i][1]
                    csv.append((user, topN[i][0],topN[i][1] ,old[i][0],old[i][1]))
                 except :
                     pass   #不做任何处理 
        return csv
        #df_file=pd.DataFrame(csv)
        #df_file.to_csv('c:/Python27/data/rec_result.csv',index=False,header=False) #是/斜杠，有后缀
    
   def create_xml(self,dic): 
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
                       
   def computeDeviations(self,data):
      """关键在第一次进入循环设置默认为1，然后换一个user进入 +1 如果同时出现"""
      # for each person in the data:
      #    get their ratings
      for ratings in data.values():
         # for each item & rating in that set of ratings:
         for (item, rating) in ratings.items():
            self.frequencies.setdefault(item, {})
            self.deviations.setdefault(item, {})                    
            # for each item2 & rating2 in that set of ratings:
            for (item2, rating2) in ratings.items():
               if item != item2:
                  # add the difference between the ratings to our
                  # computation
                  self.frequencies[item].setdefault(item2, 0)
                  self.deviations[item].setdefault(item2, 0.0)
                  self.frequencies[item][item2] += 1
                  self.deviations[item][item2] += rating - rating2
        
      for (item, ratings) in self.deviations.items():
         for item2 in ratings:
            ratings[item2] /= self.frequencies[item][item2]


   def slopeOneRecommendations(self,userRatings):
      recommendations = {}
      frequencies = {}
      # for every item and rating in the user's recommendations
      for (userItem, userRating) in userRatings.items():
         # for every item in our dataset that the user didn't rate
         for (diffItem, diffRatings) in self.deviations.items():
            if diffItem not in userRatings and \
               userItem in self.deviations[diffItem]:
               freq = self.frequencies[diffItem][userItem]
               recommendations.setdefault(diffItem, 0.0)
               frequencies.setdefault(diffItem, 0)
               # add to the running sum representing the numerator
               # of the formula
               recommendations[diffItem] += (diffRatings[userItem] +
                                             userRating) * freq
               # keep a running sum of the frequency of diffitem
               frequencies[diffItem] += freq
      recommend_list=sorted(recommendations.items(),key=lambda x :x[1],reverse=True)[:10]
      return dict(recommend_list)
        
   def pearson(self, rating1, rating2):
      sum_xy = 0
      sum_x = 0
      sum_y = 0
      sum_x2 = 0
      sum_y2 = 0
      n = 0
      for key in rating1:
         if key in rating2:
            n += 1
            x = rating1[key]
            y = rating2[key]
            sum_xy += x * y
            sum_x += x
            sum_y += y
            sum_x2 += pow(x, 2)
            sum_y2 += pow(y, 2)
      if n == 0:
         return 0
      # now compute denominator
      denominator = sqrt(sum_x2 - pow(sum_x, 2) / n) * \
                    sqrt(sum_y2 - pow(sum_y, 2) / n)
     # print denominator
      if denominator == 0:
         return 0
      else:
         return (sum_xy - (sum_x * sum_y) / n) / denominator
      
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
      
   def computeNearestNeighbor(self, data):
      """creates distances dict for all  users"""
      for username in data:
          distances = []
          for instance in data:
             if instance != username:
                distance = self.pearson(data[username],data[instance])
                distances.append((instance, distance))
          # sort based on distance -- closest first
          distances.sort(key=lambda artistTuple: artistTuple[1],reverse=True)
          self.distances_dic[username]=distances[:20]   
      
   def UserBased(self,data,user):
      """Give list of recommendations"""
      recommendations = {}
      # first get list of users  ordered by nearness
      nearest = self.distances_dic[user]
      #
      # now get the ratings for the user
      #
      userRatings = data[user]
      #
      # determine the total distance
      totalDistance = 0.0
      for i in range(self.k):
         totalDistance += nearest[i][1]
      # now iterate through the k nearest neighbors
      # accumulating their ratings
      
      for i in range(self.k):
         # 如果和其他用户距离都是0，则平均权重，或许要用flag记录下来
         if totalDistance==0:
             weight=1/self.k
         else:
             weight = nearest[i][1] / totalDistance
         # get the name of the person
         name = nearest[i][0]
         # get the ratings for this person
         neighborRatings = data[name]
         # get the name of the person
         # now find bands neighbor rated that user didn't
         for artist in neighborRatings:
            if not artist in userRatings:
               if artist not in recommendations:
                  recommendations[artist] = neighborRatings[artist] * \
                                            weight
               else:
                  recommendations[artist] = recommendations[artist] + \
                                            neighborRatings[artist] * \
                                            weight
      recommend_list=sorted(recommendations.items(),key=lambda x :x[1],reverse=True)[:10]
      return dict(recommend_list) #导致顺序发生改变

if __name__=='__main__':
    rec=recommender(5,20)
#   df,data_arr=rec.preference() 
#   data=rec.sql2dict(data_arr) #data_dict 
    recommend_all={}
###    output slopone xml   ###
#    rec.computeDeviations(data)
#    for user in data:
#        recommend_all[user]=rec.slopeOneRecommendations(data[user]) 
#    rec.create_xml(recommend_all)
###    output userbased xml   ###   
#    rec.computeNearestNeighbor(data)
#    for user in data:
#       recommend_all[user]=rec.UserBased(data,user) 
#    rec.create_xml(recommend_all)
###    output userbased xml   ###  
    user_tags,tag_items,user_items = rec.preference_tag()
    k=1
    for user in user_items.keys()[:10]:
        recommend_all[user]=rec.TagRec(user)
        k+=1
        print k
    rec.create_xml(recommend_all)
        
  

      
"""    
    data=s.preference(dsn)
    data_arr=np.asanyarray(data)
    data=rec.sql2dict(data_arr)
    s.update(userdata)
    recommend_all={} 
    c=0
    for user in userdata:
        c+=1
        print c
        if c==100:
           break
        recommend_all[user]=s.predict(userdata[user]) 
    n=4    # 用户分割份数
    ls_c=splitdict(recommend_all,n)
    for i in range(0,n):
        s.create_xml(ls_c[i])
"""