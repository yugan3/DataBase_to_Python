# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 11:17:58 2020

@author: yu.gan
"""

# from sqlalchemy import Column, String,Table, Integer, create_engine
# from sqlalchemy import MetaData
import pandas as pd
import sqlalchemy

eng = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro202004@164.61.235.21')

query = 'select top 100000 home_store_id, cust_no, auth_person_id, identification_id from chnccp_dwh.dw_cust_auth_person'
df = pd.read_sql(query,eng)  
df1 = df.dropna()
df1['birth'] = df1['identification_id'].str[6:13]
