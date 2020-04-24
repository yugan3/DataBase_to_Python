# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 16:37:52 2020

@author: yu.gan
"""


from sqlalchemy import Column, String,Table, Integer, create_engine
from sqlalchemy import MetaData


teradata_engine = create_engine('teradata://chnccp_db_u28074879:Metro202004@164.61.235.21:22/')
sql_select = 'select para1, para2, para3, para4 from ...'
result = teradata_engine.execute(sql_select)


engine = create_engine('mysql+pymysql://ganyu:Metro@1234@10.252.6.139:3306/report')
metadata = MetaData()
invoice = Table('invoice', metadata,
                Column('para1',String, primary_key = True),
                Column('para2', String),
                Column('para3', String),
                Column('para4', String)     
        )
metadata.create_all(engine)

label = ['para1','para2','para3', 'para4']
test = [1,2,3,4]

A =[(dict(zip(label,test)))]