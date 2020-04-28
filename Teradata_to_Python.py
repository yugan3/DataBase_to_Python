# -*- coding: utf-8 -*-
"""
Spyder 编辑器

这是一个临时脚本文件。

"""
import teradata

UdaExec = teradata.UdaExec(appName="HelloWorld", version="1.0",
        logConsole=False)

session = UdaExec.connect(method="odbc", system="164.61.235.21",
        username="chnccp_db_u28074879", password="Metro202004")

sql_select = 'select top 5 store_id, region_desc from chnccp_dwh.dw_cust_invoice_line '


for row in session.execute(sql_select):
    print(row)
    
sql_create = """CREATE VOLATILE TABLE EMPLOYEE( 
   EmployeeNo INTEGER, 
   FirstName VARCHAR(30), 
   LastName VARCHAR(30), 
   DOB DATE FORMAT 'YYYY-MM-DD', 
   JoinedDate DATE FORMAT 'YYYY-MM-DD', 
   DepartmentNo BYTEINT 
) 
ON COMMIT PRESERVE ROWS"""
session.execute(sql_create)
# 创建表

sql_insert = """ insert into EMPLOYEE(?,?,?,?,?,?)"""
session.executemany(sql_insert,[(2, 'GG', 'PP','2020-03-01', '2020-09-28',1),
                                (5, 'AA', 'CC','2014-12-30','2020-04-23',0),
                                (3, 'GR', 'EE','2018-06-08', '2021-06-16',1)])
# 填充表
# 查看test
sql_select_test = """ select * from employee"""
for row in session.execute(sql_select_test):
    print(row)

sql_drop = """ drop TABLE EMPLOYEE"""
session.execute(sql_drop)
