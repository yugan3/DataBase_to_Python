# -*- coding: utf-8 -*-
"""
Created on Fri May  8 16:26:23 2020

@author: yu.gan
"""

import pandas as pd
import sqlalchemy


path1 = 'C://Users//yu.gan//Desktop//0429闪购//presell_report_20200430150008.csv'
df1 = pd.read_csv(path1,engine = 'python')
del df1['Name']
result = df1.dropna()

result['address'] = result['收货地址'].apply(lambda x: x.split(' '))  
result['province'] = result['address'].apply(lambda x: x[0])
result['city'] = result['address'].apply(lambda x: x[1])
result['district'] = result['address'].apply(lambda x: x[2])
result['detailed_address'] = result['address'].apply(lambda x: x[3])

# 建表字符串大小注意修改

df_final = []

df_final = result[['StoreKey', 'CustKey', 'CardholderKey','province', 'city', 'district','detailed_address']]
df_final.rename(columns = {"StoreKey": "home_store_id"},inplace=True)
df_final.rename(columns = {"CustKey": "cust_no"},inplace=True)
df_final.rename(columns = {"CardholderKey": "auth_person_id"},inplace=True)
df_final = df_final.drop_duplicates(keep = 'first')

eng = sqlalchemy.create_engine('teradatasql://username:code@host')
df_final.to_sql('table', schema = 'database', con = eng, if_exists='append', index = False)

df_final.groupby('province').count().sort_values(by = 'city', ascending = False)


