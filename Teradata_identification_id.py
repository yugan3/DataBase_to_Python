# linux 中进去的命令，运行环境
psql -U metro -d metabase -h 10.250.131.75 -p 1526
python

engine = sqlalchemy.create_engine('postgresql://metro@10.250.131.75:1526/metabase')

teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro202004@164.61.235.21')

# 连接数据库

from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd 


engine = sqlalchemy.create_engine('postgresql://metro@10.250.131.75:1526/metabase')
query = 'select * from lightning_deals_userinfo'
df = pd.read_sql(query,engine)


teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro202004@164.61.235.21')
df.to_sql('ganyu_lighting_deals', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)

select top 10 * from chnccp_msi_z.ganyu_lighting_deals;

# 20200506 Grace 下表
query = 'select * from first_channel_userinfo'
df1 = pd.read_sql(query, engine)
df2.to_sql('ganyu_channel_first_userinfo', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)

 # 20200512 liveshow_userinfo_all
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd 

engine = sqlalchemy.create_engine('postgresql://metro@10.250.131.75:1526/metabase')
query = 'select storekey, custkey, cardholderkey from liveshow_userinfo_all'
df = pd.read_sql(query,engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro202004@164.61.235.21')
df.to_sql('liveshow_newuser', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)

 # 20200513
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd 
engine = sqlalchemy.create_engine('postgresql://metro@10.250.131.75:1526/metabase')
query = 'select * from diwangxie'
df = pd.read_sql(query,engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro202004@164.61.235.21')
df.to_sql('diwangxie', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)

######################################################################################################################################
######################################################POSTGRESQL对应字段选择及修改######################################################
######################################################################################################################################

### order for flashsales
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd 
engine = sqlalchemy.create_engine('postgresql://metro@10.250.131.75:1526/metabase')

### order for flashsales
query = '''select created_at, store_key as home_store_id, cust_key as cust_no, ch_key as auth_person_id, goods_name as art_name, deliver_type, count as qty, price, order_status from metro_order.release_order_oreders where campaign = '会员闪购' and created_at between '' and ''  '''
df = pd.read_sql(query,engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro202004@164.61.235.21')
df.to_sql('flashsales_temp', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)

### first_channel_userinfo table
query1 = 'select fromat, channel, storekey as home_store_id, custkey as cust_no, cardholderkey as auth_person_id, unionid, campaign_type as campaign_id from first_channel_userinfo'
df1 = pd.read_sql(query1, engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro202004@164.61.235.21')
df1.to_sql('first_channel_userinfo', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)

### liveshwo_userinfo_all
query2 = 'select storekey as home_store_id, custkey as cust_no, cardholderkey as auth_person_id, unionid, campaign_type as campaign_id from liveshow_userinfo_all'
df2 = pd.read_sql(query2, engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@052020@164.61.235.21')
df2.to_sql('liveshow_userinfo_all', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)

#########################################
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@052020@164.61.235.21')
df.to_sql('sms0527', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)

##########################################
lifecycle_sms_527_namelist

# 0527闪购
query = '''select buyer_id, home_store_id, cust_no, auth_person_id, item_id, title as art_name, price, sales, qty, deliver_type, falshsales from falshsales_0527 '''
df = pd.read_sql(query, engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@052020@164.61.235.21')
df.to_sql('flashsales_0527', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)


query1 = '''select fromat, channel, storekey as home_store_id, custkey as cust_no, cardholderkey as auth_person_id, unionid, campaign_type as campaign_id from first_channel_userinfo where campaign_type = '5.27' '''
df1 = pd.read_sql(query1,engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@052020@164.61.235.21')
df1.to_sql('first_channel_userinfo', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)


query2 = '''select storekey as home_store_id, custkey as cust_no, cardholderkey as auth_person_id, unionid, campaign_type as campaign_id from liveshow_userinfo_all where campaign_type = '5.27' '''
df2 = pd.read_sql(query2, engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@052020@164.61.235.21')
df2.to_sql('liveshow_userinfo_all', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)


# lifecycle_data wakeup
query = ''' select home_store_id, cust_no, auth_person_id, campaign_type, type_id, wave_num, coupon_type, gcn, epc, active_date, until_date from lifecycle_data '''
df = pd.read_sql(query,engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@052020@164.61.235.21')
df.to_sql('lifecycle_data', schema = 'chnccp_crm', con = teradata_engine, if_exists = 'append', index = False)

# lifecycle_data reactivation
query = ''' select * from lifecycle_reactivation_data '''
df = pd.read_sql(query,engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@052020@164.61.235.21')
df.to_sql('lifecycle_reactivation_data', schema = 'chnccp_msi_z', con = teradata_engine, if_exists = 'append', index = False)

# 0610闪购
query = '''select buyer_id, home_store_id, cust_no, auth_person_id, item_id, title as art_name, price, sales, qty, deliver_type, falshsales from falshsales_0610 '''
df = pd.read_sql(query,engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@052020@164.61.235.21')
df.to_sql('falshsales_0610', schema = 'chnccp_msi_z', con = teradata_engine, if_exists = 'append', index = False)

query1 = '''select fromat, channel, storekey as home_store_id, custkey as cust_no, cardholderkey as auth_person_id, unionid, campaign_type as campaign_id from first_channel_userinfo where campaign_type = '20200610' '''
df1 = pd.read_sql(query1,engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@052020@164.61.235.21')
df1.to_sql('first_channel_userinfo', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)

query2 = '''select storekey as home_store_id, custkey as cust_no, cardholderkey as auth_person_id, unionid, campaign_type as campaign_id from liveshow_userinfo_all where campaign_type = '20200610' '''
df2 = pd.read_sql(query2, engine)
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@052020@164.61.235.21')
df2.to_sql('liveshow_userinfo_all', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)

# Tmall
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@062020@164.61.235.21')
df.to_sql('tmall', schema = 'chnccp_msi_z', con = teradata_engine, if_exists='append', index = False)

### tags56 to evolve
### chunksize by group connecting
import pandas as pd
import sqlalchemy
teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@062020@164.61.235.21')
t_conn = teradata_engine.connect()
postgre_engine = sqlalchemy.create_engine('postgresql://metro@10.250.131.75:1526/metabase')
p_conn = postgre_engine.connect()
query = '''select * from chnccp_msi_z.FRANK_0701_MEMBER_TO_TC_FINNAL'''
for chunk in pd.read_sql(query, t_conn, chunksize=100000):
    print(chunk[['home_store_id','cust_no', 'auth_person_id']].head(1))
    chunk.to_sql('tags_56', p_conn, if_exists= 'append', index= False)
t_conn.close()
p_conn.close()

