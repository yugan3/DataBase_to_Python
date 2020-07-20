import pandas as pd 
import sqlalchemy
import datetime

teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@062020@164.61.235.21')
postgre_engine = sqlalchemy.create_engine('postgresql://metro@10.250.131.75:1526/metabase')
print(datetime.date.today())

query = '''select a.* from (
select home_store_id, cust_no, auth_person_id, 1 as idx, 'beijing' as category, date as dt 
from chnccp_dwh.dw_cust_auth_person
where home_store_id in (108,31,37,46) 
Union
select 10 as home_store_id, 913395 as cust_no, 1 as auth_person_id, 1 as idx, 'beijing' as category, date as dt from chnccp_dwh.dw_cust_auth_person
Union
select 10 as home_store_id, 914135 as cust_no, 1 as auth_person_id, 1 as idx, 'beijing' as category, date as dt from chnccp_dwh.dw_cust_auth_person
Union
select 10 as home_store_id, 912112 as cust_no, 1 as auth_person_id, 1 as idx, 'beijing' as category, date as dt from chnccp_dwh.dw_cust_auth_person
Union
select 10 as home_store_id, 914158 as cust_no, 1 as auth_person_id, 1 as idx, 'biejing' as category, date as dt from chnccp_dwh.dw_cust_auth_person
)a
'''
beijing = pd.read_sql(query, teradata_engine)

print('step1:load_in finished!')
print('total customer: %d'%beijing.shape[0])

beijing.to_sql("target_promotion_tag", postgre_engine, if_exists = 'append', index=False, chunksize=100000)
print('step2:upload finished!')
