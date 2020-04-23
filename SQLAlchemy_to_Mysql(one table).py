# -*- coding: utf-8 -*-

"""
Created on Thu Apr 23 09:05:40 2020

@author: yu.gan
"""

from sqlalchemy import Column, String, create_engine
from sqlalchemy.types import CHAR, Integer, String, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
# 创建基类
engine = create_engine('mysql+pymysql://ganyu:Metro@1234@10.252.6.139:3306/report')
# mysql路径：“mysql+pymysql：//username+password@host:prot/database
session = sessionmaker(bind = engine)
db_session = session()
# 执行操作
##############################################################################
#################################创建表#######################################
##############################################################################
class User(Base):
# 建立表的结构，同时这个结构也是后面操作的对象
    __tablename__ = 'User'
    # 在mysql数据库中表的名称
    id = Column(Integer, primary_key = True)
    username = Column(String(64), nullable =True)
    job = Column(String(64), nullable = True)
    # Column表示数据库中的每一列， nullable = False表示这一列不能为空，True表示可以为空
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__,self.username)

Base.metadata.create_all(engine)
# 执行创建操作

#############################################################################
########################### 单表操作 Within One Table #######################
################################ 添加数据 ###################################
#############################################################################
session = sessionmaker(bind = engine)

db_session = session()
# 1.添加单条数据
u = User(id = '2', username = 'gg', job = 'doctor')
db_session.add(u)

# 2.添加多条数据
db_session.add_all([
    User(id = '4', username = 'yn', job = 'cook'),
    User(id = '5', username = 'li', job = 'teacher')])

db_session.commit()
# 一定要提交操作才会执行
db_session.close()
# 关闭会话

#############################################################################
#################################数据查询#####################################
#############################################################################

# 1.查询用户表中所有用户:session中的操作，以及对应的表名称
user_all = db_session.query(User).all()

# 2.where 筛选条件，用filter找出所有数据all
user = db_session.query(User).filter(User.id>2).all()
# 3.where 筛选条件，用filter找出第一条数据first
user = db_session.query(User).filter(User.id>2).first()
# 2.只查看某个字段，where 筛选条件，用filter找出所有数据all
user = db_session.query(User.id).filter(User.id>2).all()

#############################################################################
#################################修改数据#####################################
#############################################################################

# 更新单条数据
res = db_session.query(User).filter(User.id==5).update({"username":"new"})
# 以{key:value}来表示更新的字段和内容
db_session.commit()
db_session.close()

# 更新多条数据
res2 =db_session.query(User).filter(User.id<=20).update(
    {"username":"corredted"})
print(res2)
db_session.commit()
db_session.close()

res = db_session.query(User).filter(User.id ==2).update({"username":"Bob"})
res = db_session.query(User).filter(User.id ==4).update({"username":"Atlantis"})
##############################################################################
##################################删除数据#####################################
##############################################################################

res = db_session.query(User).filter(User.id == 5).delete()

db_session.commit()
db_session.close()

##############################################################################
################################复杂条件查询###################################
##############################################################################

 # 1.and_和or_条件查询(多条件查询)
from sqlalchemy import and_, or_
ret = db_session.query(User).filter(and_(User.id>1, User.job == 'doctor')).all()
ret2 = db_session.query(User).filter(or_(User.id == 4, User.job == 'doctor')).all()

# 2.查询所有数据
res = db_session.query(User).all()

# 3.查询数据，指定查询数据列加入别名
r3 = db_session.query(User.username.label('name'), User.id).first()

# 4.筛选条件格式
r4 = db_session.query(User).filter(User.username ='').all()
r5 = db_session.query(User).filter_by(name = '').first() #有点像group_by的用法
# filter的两种筛选格式

# 5.原生sql语句查询
r6 =db_session.query(User).from_statement(text("select * from User where name=:name")).parms(name = '').all()

# 6.筛选查询列，query的时候不再使用query ORM对象，而是使用其中的某一个字段User.name来对内容进行选取
user_list = db_session.query(User.name).all()
print(user_list)

# 读取每一个元素：
for row in user_list:
    print(row.name)

# 7.复杂查询：类似于槽函数用parms来设定
from sqlalchemy.sql import text
user_list = db_session.query(User).filter(text('select * from User id<:value and name :=name')).params(value =3, name ='xxx')
# value和name的用法类似于槽函数，后面设定params来指定

# 8.排序
user_list =db_session.query(User).order_by(User.id.desc()).all()
# order_by 降序排列

# 9.between 查询范围，between在filter内，且直接对需要的字段作用
ret = db_session.query(User).filter(User.id.between(1,3), User.name ='XX').all()

# 10. 只查询id等于1,3,4，in_在filter那日，直接作用于需要的字段上
ret = db_session.query(User).filter(User.id.in_([1,3,4])).all()

# 11. 取反:查询不等于1,3,4，使用~User.id.in_，包括~和in_
ret = db_session.query(User).filter(~User.id.in_([1,3,4])).all()

# 12. 子查询
ret =db_session.query(User).filter(User.id.in_(db_session.query(User).filter_by(name = "XX")).all()
                        
# 13. 切片（限制）:左闭右开
ret = db_session.query(User)[1:2]
                                   
# 14.通配符
ret = db_session.query(User).filter(~User.name.like('e%')).all()  

# 15.分组:聚合类函数
from sqlalchemy.sql import func
ret = db_session.query(User).group_by(User.extra).all()
ret = db_session.query(
    func.max(User.id),
    func.sum(User.id),
    func.min(User.id).goup_by(User.name).having(func.min(User.id)>2).all())

##############################################################################
#####################################删除数据##################################
##############################################################################
res = db_session.query(User).filter(User.id==4).delete()
db_session.commit()
db_session.close()                                
                                   
                                   
##############################################################################
#####################################删除表格##################################
##############################################################################
Base.metadata.drop_all(engine)
# 需要谨慎操作
                                   
##############################################################################
####################################批量处理###################################
##############################################################################                                   
                               