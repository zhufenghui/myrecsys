# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 15:38:58 2015
1、组内的离散化，类似于组内标准化，在pandas doc 中叫做 split（按组分），transform（对组进行操作）
lambda x : (x-x.mean())/x.std()
2、先要类的实例化 s=slopeone 才能应用里面的方法
后续；可以考虑导入原数据，将原来在oracle中的数据处理迁移到程序中

@author: admin
"""
import os 
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  #必须置于cx之前, 解决Oracle导入中文乱码问题
import cx_Oracle as co
import pandas as pd 
import numpy as np
from lxml import etree as ET
import time
#from datetime import date, time, datetime, timedelta

class SlopeOne(object):
    def __init__(self):
        self.diffs = {}
        self.freqs = {}
        self.userRatings = {}
        self.n=10
                
    def sql2dict(self, data):   
        for line in data:
            user=line[0] 
            item=line[1]
            rating=line[2]
            if user in self.userRatings:
                item_rating=self.userRatings[user]  # 将行字典赋予用户
            else:
                item_rating={}
            item_rating[item]=rating
            self.userRatings[user]=item_rating
        return self.userRatings
        
    def predict(self, userprefs): #recieve the ItemRating from one user,output the recommend ItemRating
        preds, freqs = {}, {}
        for item, rating in userprefs.iteritems():
            for diffitem, diffratings in self.diffs.iteritems():
                try:
                    freq = self.freqs[diffitem][item]
                except KeyError:
                    continue
                preds.setdefault(diffitem, 0.0)
                freqs.setdefault(diffitem, 0)
                preds[diffitem] += freq * (diffratings[item] + rating)
                freqs[diffitem] += freq
        recommend = dict([(item, value / freqs[item])
                     for item, value in preds.iteritems()
                     if item not in userprefs and freqs[item] > 0])
        return recommend
                       
    def update(self, userdata): # calcu the card and diff
        for ratings in userdata.itervalues():
            for item1, rating1 in ratings.iteritems():
                self.freqs.setdefault(item1, {})
                self.diffs.setdefault(item1, {})
                for item2, rating2 in ratings.iteritems():
                    self.freqs[item1].setdefault(item2, 0)
                    self.diffs[item1].setdefault(item2, 0.0)
                    self.freqs[item1][item2] += 1
                    self.diffs[item1][item2] += rating1 - rating2
        for item1, ratings in self.diffs.iteritems():
            for item2 in ratings:
                ratings[item2] /= self.freqs[item1][item2]
                
    def preference(self,dsn):
        db=co.connect('zsboss','zsboss123',dsn)
        cursor=db.cursor()
        cursor.execute("select * from recommendation.preference_02 where devno in \
                   (SELECT devno from recommendation.user_sample_1000)\
                   and rownum<1000 ")
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
        return df2

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
    def splitdict(self,dic,num):# 分割字典by用户，将文件分割
        ls=sorted(dic.iteritems())
        ls_c=[]
        block_len=len(ls)/num
        for i in range(num):
            if i<(n-1):
                ls_c.extend([ls[block_len*i:block_len*(i+1)]]) #extend 方法要加[]表示将作为一个block存储
            else:
                ls_c.extend([(ls[block_len*i:])])
        return ls_c
        
    def create_xml(self,ls): 
         dic=dict(ls)
         recServices = ET.Element('recServices')
         for devno,items in dic.iteritems():
             print items
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
    
     
if __name__=='__main__':
    dsn=co.makedsn('192.168.113.226','11521','boss')
    s=SlopeOne()
    data=s.preference(dsn)
    data_arr=np.asanyarray(data)
    userdata=s.sql2dict(data_arr)
    s.update(userdata)
    recommend_all={} 
    c=0
    for user in userdata:
        c+=1
        print c
        if c==10:
           break
        recommend_all[user]=s.predict(userdata[user]) 
    num=4   # 用户分割份数
    ls_container=s.splitdict(recommend_all,num)
    for i in range(0,n):
        s.create_xml(ls_container[i])

 

'''       
c=0
for user in userdata:
    c+=1
    if c==1:
        break
    for item,rating in userdata['8760003513016664'].iteritems():
        print user,item.decode('utf-8')
    for item_rec,rating_rec in recommend_all['8760003513016664'].iteritems():
        print item_rec.decode('utf-8')

oppps!! dict type can be used like this 
if __name__ == '__main__':
    userdata = dict(
        alice=dict(squid=1.0,
                   cuttlefish=0.5,
                   octopus=0.2),
        bob=dict(squid=1.0,
                 octopus=0.5,
                 nautilus=0.2),
        carole=dict(squid=0.2,
                    octopus=1.0,
                    cuttlefish=0.4,
                    nautilus=0.4),
        dave=dict(cuttlefish=0.9,
                  octopus=0.4,
                  nautilus=0.5),
        )
    s = SlopeOne()
    s.update(userdata)
    print s.predict(dict(squid=0.4))
  
#for print   
c=0
for i in x :
    c+=1
    if c == 2 : 
        break
    print i 
    
for key,value in dic2.iteritems() 
   print key,value
     
'''