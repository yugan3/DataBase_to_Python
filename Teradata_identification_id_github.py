# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 11:17:58 2020

@author: yu.gan
"""

import pandas as pd
import sqlalchemy

eng = sqlalchemy.create_engine('teradatasql://username:password@port')
query = 'select top 1000000 home_store_id, cust_no, auth_person_id, identification_id from chnccp_dwh.dw_cust_auth_person'
df = pd.read_sql(query,eng)  
# 删除原本表中的空缺值及检查
df1 = df.dropna()
df1.isnull().sum()

# 截取生日和性别
df1['birth'] = df1['identification_id'].str[6:13]
df1['gender_code'] = df1['identification_id'].str[16]
# 删除截取完的数字中的空缺值及检查
df1 = df1.dropna()
df1.isnull().sum()

# 判断性别
def gender(x):
    if x % 2 == 0:
        return 'female'
    else:
        return 'male'
    
# 强制格式转换，方便性别判断
df1['gender_code'] = pd.Categorical(df1['gender_code']).codes 
df1['gender'] = df1['gender_code'].apply(lambda x: gender(x))

# 省份证号8位判断
df1['birth_length']= df1['birth'].astype(str).apply(len)
df2 = df1[df1['birth_length'] == 7]

# 最终输出表格
df_final = df2[['home_store_id', 'cust_no', 'auth_person_id','identification_id','birth', 'gender']]


from sqlalchemy import Column, String,Table, Integer, create_engine
from sqlalchemy import MetaData

# 将string格式转为对应需要的格式，数字为int，日期为datetime
sql_engine = create_engine("mysql+pymysql://username:password@host/database", pool_pre_ping=True)
metadata = MetaData()
identification_id = Table('identification_id', metadata,
                       Column('home_store_id', String(36),primary_key = True),
                       Column('cust_no',String(36), primary_key = True),
                       Column('auth_person_id',String(36), primary_key = True),
                       Column('identification_id', String(36)),
                       Column('birth', String(36)),
                       Column('gender', String(36))
                       )
metadata.create_all(sql_engine)
sql_engine = create_engine("mysql+pymysql://username:password@host:port/database")

df_final.to_sql('identification_id', sql_engine, schema='report', if_exists='append',index = False)





