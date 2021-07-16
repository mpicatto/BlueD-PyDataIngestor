import pandas as pd
import sqlalchemy as sql 

db_in = sql.create_engine('postgresql://pvczseja:X0_soxKtUMurTTbHzxe9ygObOxy9sb4R@batyr.db.elephantsql.com/pvczseja')
db_out= sql.create_engine('postgresql://nysggdyc:K137O8Co9J-pYpysZMo-QhKDuJ1FBVDn@batyr.db.elephantsql.com/nysggdyc')

#create output tables
def corn_table():
    query = 'SELECT stock.date as date, market.corn_price as corn_price, stock.corn as corn_stock FROM "public"."grain_stocks" AS stock INNER JOIN (SELECT g.year_month as "date", g.corn as "corn_price" FROM "public"."grain_prices" AS g ) market ON market.date=stock.date ORDER BY "date";'
    #ingest data
    df=pd.read_sql_query(query,db_in)
    #format date
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m")
    #From bushels to tons
    df["corn_price"] = df["corn_price"].apply(lambda x: x*39.368)
    #rename colums
    df.rename(columns={'corn_price':'price','corn_stock':'stock'}, inplace=True)
    #save to output db
    df.to_sql('corn_data',db_out,if_exists='replace',index=True, index_label='id')
    #add pk
    sql.text('ALTER TABLE "public"."corn_data" ADD PRIMARY KEY (id);',db_out)
 
def soy_table():
    query = 'SELECT stock.date as date, market.soy_price as soy_price, stock.soybeans as soy_stock FROM "public"."grain_stocks" AS stock INNER JOIN (SELECT g.year_month as "date", g.soybean as "soy_price" FROM "public"."grain_prices" AS g ) market ON market.date=stock.date ORDER BY "date" ;'
    #ingest data
    df=pd.read_sql_query(query,db_in)
    #format date
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m")
    #From bushels to tons
    df["soy_price"] = df["soy_price"].apply(lambda x: x*36.744)
    #rename colums
    df.rename(columns={'soy_price':'price','soy_stock':'stock'}, inplace=True) 
    #save to output db
    df.to_sql('soy_data',db_out,if_exists='replace', index=True, index_label='id')
    #add pk
    sql.text('ALTER TABLE "public"."soy_data" ADD PRIMARY KEY (id);',db_out)

def usdx_table():
    query = 'SELECT * FROM "public"."dollar_index";'
    #ingest data
    df=pd.read_sql_query(query,db_in)
    #rename "year_month" column to "date"
    df.rename(columns={'year_month':'date'}, inplace=True)
    #format date
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m")
    #save to output db
    df.to_sql('usdx_data',db_out,if_exists='replace',index=True, index_label='id')
    #add pk
    sql.text('ALTER TABLE "public"."usdx_data" ADD PRIMARY KEY (id);',db_out)

print("[+]Processing...")
try:
    corn_table()
    soy_table()
    usdx_table()
    print("[+]done")
except:
    print("[+]Unable to read/write data on databases. Check credentials and connection")   
