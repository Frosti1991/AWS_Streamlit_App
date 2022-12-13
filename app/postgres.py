import datetime
import pandas as pd
import sqlalchemy as db

def connect_to_postgres():
    '''this functions connects to a postgres server'''
    
    HOST = 'energy.czpisnsdrykp.us-east-1.rds.amazonaws.com' # local: 'localhost' 
    # default psql port for localhost
    PORT = '5432'
    # username for postgres (default 'postgres')
    USERNAME = 'postgres'
    # postgres password as environment variable
    PASSWORD = 'elec_price_data' # local: GarlicBoosting
    DB = 'Energy' #local: energy_db'

    #connection string for Linux
    cs_linux = f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB}"

    engine = db.create_engine(cs_linux, echo=False)

    return engine


def load_etl_to_postgres(engine):
    '''Functions loads a csv file to postgres and updates values'''
    #Fill new data df
    date_from=datetime.datetime.today().date()
    path_new_data='/home/christoph/OneDrive/Fortbildung_Weiterbildung_Arbeit/2022_Spiced_Data_Science/'\
                    'Data_Science_Course/Working_Area/final_project/productive/postgres_data/daily/all_merged_data/'
    file_name=path_new_data+date_from.strftime('%d_%m_%Y')+'_postgres_data.csv'
    new_data = pd.read_csv(file_name,index_col=0,infer_datetime_format=True,parse_dates=True)
    #new_data.rename(columns={'Biomasse[MWh]':'biomasse_mwh',
                            #'photovoltaik_mwh__real':'photovoltaik_mwh_real'},inplace=True)
    new_data.index.name="datum"

    #Fill new_data table
    new_data.to_sql('new_data',engine,if_exists='append', method='multi', chunksize=1000)

    #Insert into main table and empty new_data table
    query_list=["DELETE FROM elec_price_data WHERE datum IN ( SELECT datum FROM new_data);",
            "Insert INTO elec_price_data SELECT * FROM new_data;",
            "TRUNCATE new_data;"]           

    with engine.connect() as conn:
        for query in query_list:
            result = conn.execute(query)
    print("Data successfully updated!")


def query_df_by_date(query,engine,index_col):
    '''returns a df from postgres by a customized date query'''
    df_out=pd.read_sql(query,engine,index_col,parse_dates=True)
    return df_out

def update_sql(dict_sql,destin_column,engine,table):
    '''this functions updates price columns'''
    metadata = db.MetaData()
    connection = engine.connect()
    db_table= db.Table(table, metadata, 
                                autoload=True, autoload_with=engine)
    
    for key,value in dict_sql.items():
        if destin_column=='price_pred_dnn':
            query = db.update(db_table).values(price_pred_dnn = value)
        else:
            query = db.update(db_table).values(price_pred_sarimax = value)
        query = query.where(db_table.columns.datum == key)
        results = connection.execute(query)