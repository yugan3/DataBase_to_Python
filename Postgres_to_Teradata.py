# linux 中进去的命令，运行环境
psql -U metro -d metabase -h 10.250.131.75 -p 1526
python

# 连接数据库

from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd 


engine = sqlalchemy.create_engine('postgresql://username:password@host:port/metabase')
# 如果没有password则直接省略
query = 'select * from lightning_deals_userinfo'
df = pd.read_sql(query,engine)
df.info()
# 在teradata中建立相应格式的字段

teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro202004@164.61.235.21') # 只要ip地址，不需要详细数据库
df.to_sql('tablename', schema = 'database', con = teradata_engine,if_exists='append', index = False)
