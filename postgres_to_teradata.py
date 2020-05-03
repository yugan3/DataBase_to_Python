# -*- coding: utf-8 -*-
"""
Created on Sun May  3 17:51:39 2020

@author: Gan
"""


from sqlalchemy import engine
import pandas as pd

engine = create_engine('postgresql://username:password@host:port/first_channeil_userinfo')
query = 'select * from first_channel_userinfo'
df = pd.read_sql(query,engine)

# teradata 中建表

teradata_engine = sqlalchemy.create_engine('teradatasql://username:password@host/database')
df.to_qsl('temp',con = teradata_engine, index = False)


