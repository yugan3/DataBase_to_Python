# linux 中进去的命令，运行环境
psql -U metro -d metabase -h 10.250.131.75 -p 1526
python

# 连接数据库

from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd 


engine = sqlalchemy.create_engine('postgresql://metro@10.250.131.75:1526/metabase')
query = 'select * from lightning_deals_userinfo'
df = pd.read_sql(query,engine)
df.info()

teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro202004@164.61.235.21')
df.to_sql('chnccp_msi_z.ganyu_lighting_deals', schema = 'chnccp_msi_z', con = teradata_engine,if_exists='append', index = False)