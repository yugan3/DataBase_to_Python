# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 16:37:52 2020

@author: yu.gan
"""


import teradata

from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import Integer, CHAR, String, Text

UdaExec = teradata.UdaExec(appName="HW", version="1.0",
        logConsole=False)
session = UdaExec.connect(method="odbc", system="164.61.235.21",
        username="chnccp_db_u28074879", password="Metro202004")

Base = declarative_base()
engine = create_engine('mysql+pymysql://ganyu:Metro@1234@10.252.6.139:3306/report')
sql_session = sessionmaker(bind = engine)
db_session = sql_session()



    
