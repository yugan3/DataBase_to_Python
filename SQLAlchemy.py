# -*- coding: utf-8 -*-

"""
Created on Wed Apr 22 15:52:19 2020

@author: yu.gan
"""

######################################################################
########################### SQLAlchemy ###############################
######################################################################

from sqlalchmey import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
# 导入需要的库

Base = declarative_base()
# 创建对象的基类

class User(Base):
    __tablename__ = 'user'
# 表的名字
    id = Column(String(20), primary_key = True)
    name = Column(String(20))
# 表的结构，包含两个字段

engine = create_engine ('mysql+mysqlconnector://root:password@localhost:3306/test')
# 初始化数据库连接

DBSession = sessionmaker(bind = engine)
# 创建DBSession类型

#################################################################################

### 多个表格继续定义其他class，例如：
class School(Base):
    __tablename__ = 'School'
    
    no = Column(String(20),primary_key = True)
    name = Column(String(20))
    Class = Column(String(20))

engine2 = create_engine('mysql+mysqlconnector://root:password@localhost:3306/test')
DBSeesion2 = sessionmaker(bind = engine)

# '数据库类型+数据库驱动名称://用户名:口令@机器地址:端口号/数据库名'

######################################添加流程####################################

session = DBSession()
# 创建session对象，便于后续的操作

new_user = User(id ='5', name = 'Bob')
# 创建新的User对象

session.add(new_user)
# 添加到session

session.commit()
# 提交即保存到数据库

session.close()
# 关闭session

############################################查看流程#############################
session = DBSession()

user = session.query(User).filter(User.id = '5').one()
# 创建query查询，即select；然后用filter来筛选，即where；最后调用one返回唯一的行，如
# 果选择all()则返回所有查询行

print(user.name)
# 输出最后的结果

session.close()

#################################################################################














