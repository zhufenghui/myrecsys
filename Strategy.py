# -*- coding: utf-8 -*-
"""
Created on Sat Jun 06 16:51:02 2015

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

class Strategy(DataModel):
     
    def __init__(self, k, n):
         DataModel.__init__(self,k,n)         
         # The following two variables are used for Slope One
         self.frequencies = {}
         self.deviations = {}
         
    def TagRec(self,user):
         """按频道划分实际操作发现算法在纯用户，纯动漫，纯体育用户效果很好
         但是综合类的有特殊性，看的人数做到，综合类的热门节目权重很大，一旦用户有综合类的收视习惯
         往往导致推荐列表中全是综合类的
         按节目的不会有这个问题
         """
         #low efficient 
         user_tags,tag_items,user_items = DataModel.preference_tag(self)  # do not run everynot 
         recommend={}
         tagged_items=user_items[user]
         for tag,wt in user_tags[user].items():
            for item, wi in tag_items[tag].items():
               if item not in tagged_items:
                   if tag=='综合':
                       wi=(1/10)*wi  # low the weight
                   if item not in recommend:
                       recommend[item]=wt*wi
                   else:
                       recommend[item]+=wt*wi
         recommend_sort=sorted(recommend.items(),key=lambda x:x[1],reverse=True)[:self.n]
         return dict(recommend_sort) 
         
    def computeDeviations(self,data):
       """关键在第一次进入循环设置默认为1，然后换一个user进入 +1 如果同时出现"""
       # for each person in the data:
       # get their ratings
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

    def SlopeOne(self,userRatings):
          recommendations = {}
          frequencies = {}
          # for every item and rating in the user's recommendations
          for (userItem, userRating) in userRatings.items():
             # for every item in our dataset that the user didn't rate
             for (diffItem, diffRatings) in self.deviations.items():
                if diffItem not in userRatings and userItem in self.deviations[diffItem]:
                   freq = self.frequencies[diffItem][userItem]
                   recommendations.setdefault(diffItem, 0.0)
                   frequencies.setdefault(diffItem, 0)
                   # add to the running sum representing the numerator
                   # of the formula
                   recommendations[diffItem] += (diffRatings[userItem] +userRating) * freq
                   # keep a running sum of the frequency of diffitem
                   frequencies[diffItem] += freq
          recommend_list=sorted(recommendations.items(),key=lambda x :x[1],reverse=True)[:self.n]
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
          self.distances_dic[username]=distances[:self.k] #漏了一个：号害死人    
          
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
      recommend_list=sorted(recommendations.items(),key=lambda x :x[1],reverse=True)[:self.n]
      return dict(recommend_list) #导致顺序发生改变
    
if __name__=='__main__':
    S=Strategy(5,10)
    rd=S.TagRec('8760003667670704')
    
    
