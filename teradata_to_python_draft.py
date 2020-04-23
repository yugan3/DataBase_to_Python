# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 21:51:27 2020

@author: Gan
"""


import teradata

from sqlalchemy import Column, String,Table, Integer, create_engine
from sqlalchemy import MetaData

UdaExec = teradata.UdaExec(appName="HW", version="1.0",
        logConsole=False)
session = UdaExec.connect(method="odbc", system="164.61.235.21",
        username="chnccp_db_u28074879", password="Metro202004")

sql_select = 'select ...  from ...'

engine = create_engine('mysql+pymysql://ganyu:Metro@1234@10.252.6.139:3306/report')

metadata = MetaData()
invoice = Table('invoice', metadata,
                Column('invoice',String, primary_key = True),
                Column('date_of_day', String),
                Column('store_id', String),
                Column('region_desc', String)     
        )

metadata.create_all(engine)

for i in session.execute(sql_select):
        invoice.insert().values(invoice = i[0],
                      date_of_day = i[1],
                      store_id = i[2],
                      region_desc = i[3])



    

    