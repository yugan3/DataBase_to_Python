# -*- coding: utf-8 -*-

"""
Created on Thu Apr 23 15:09:21 2020

@author: yu.gan
"""


from sqlalchemy import create_engine, Column, String
from sqlalchemy.types import CHAR, Integer, String, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
engine = create_engine('mysql+pymysql://ganyu:Metro@1234@10.252.6.139:3306/report')
session = sessionmaker(bind = engine)
db_session = session()

##############################################################################
#########################一对多操作：外键 Foreignkey###########################
##############################################################################

################# 额外需要引入的两个库的模块，注意下次合并#######################
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
##############################################################################

class User(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key = True)
    name = Column(String(36),nullable = True)
    job = Column(String(36),nullable = True)

class School(Base):
    __tablename__ = 'school'
    id = Column(Integer, primary_key = True)
    school = Column(String(36),nullable = True)
# 关联字段，让school_id和school的id进行关联，主外键关系（这里的ForeignKey一定是
# 表名.id，而不是对象。（数据库层面）
# 非常重要：外键的argu 必须和对应的另一个表中的字段属性相同，才能对应
    student_id = Column(Integer, ForeignKey('user.id'))
# orm层面的关系，数据库中不存在这个字段
    to_student = relationship("user", backref = 'to_user')


Base.metadata.create_all(engine)

##############################################################################
db_session.add_all([
    User(id = 1, name = 'AS', job = 'doc'),
    User(id = 2, name = 'VB', job = 'cook'),
    User(id = 3, name = 'LD', job = 'teacher'),
    User(id = 4, name = 'IU', job = 'serv')])

db_session.add_all([
    School(id = 1, school = 'SQ', student_id = 4),
    School(id = 2, school = 'WL', student_id = 2),
    School(id = 3, school = 'WQ', student_id = 1),
    School(id = 1, school = 'HY', student_id = 3)
    ])

db_session.commit()


# https://www.cnblogs.com/robertx/p/11122851.html
# https://www.cnblogs.com/bigberg/p/8329312.html#_label0









