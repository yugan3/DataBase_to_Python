# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 15:42:02 2020

@author: yu.gan
"""


import pymssql

conn = pymssql.connect(server, user, password, database)
# server 服务器
# user 用户名
# password 密码
# database 需要操作的数据库
# 建立与数据库的连接

cursor = conn.cursor()
# 获取数据库

cursor.excute("""
If Object_id('persons', 'U') is not NULL
    Drop table persons
	# 如果表存在则清空
Create table persons(
    id INT Not NULL，
	name Varchar(100),
	salesrep Varchar(100),
	Primary Key(id))""")
	# 重新创建表
# 创建数据表，以及excute的操作对应到不同增删查命令

cursor.excutemany(
    "insert into persons Values (%d, %s, %s)",
    [(1,'J S', 'J D'),(2,'J D', 'J D'), (3, 'M T', 'S H')])
# 插入数据，但是不需要像SQL那样每次insert，而是直接将数值写入即可
# excutemany 用于多个数据插入

conn.commit() 
# 如果没有指定autocommit属性为True的话，需要调用commit()方法，即提交数据库事物的操作

cursor.select('select * from persons where salesrep = %s', 'J D')
row = cursor.fetchone()
# 查询操作
# fetchone()：返回单个的元组，也就是一条记录(row)，如果没有结果 则返回 None
# fetchall() ：返回多个元组，即返回多个记录(rows),如果没有结果 则返回 ()

# 获取前n行数据的方法，直接使用函数或者循环：
row_2 = cursor.fetchmany(3)  获取前三行数据，元组包含元组

while row:
    print ("ID = %d, Name = %s" % (row[0], row[1]))   #类似于槽函数
	row = cursor.fetchone()
	
for row in cursor:
    print("ID = %d, Name = %s" % (row[0], row[1]))    #类似于槽函数
	
cursor.execute("Delete from conn") 
#删除sql的命令

totalnumber = cursor.rowcount
# 统计数据记录中的记录数量



	