import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import datetime
import os
from dotenv import load_dotenv

#user defined ETL functions
def get_env_var(key):
    load_dotenv()
    value = os.getenv(key) 
    #print(value)
    return value

def change_column_name(df_in,column_dict):
    '''changes column names of an input df by a dictionary with new names'''
    df_out=df_in.rename(columns = column_dict)
    return df_out

def keep_columns(df_in,column_list):
    '''returns a df with certain mentioned columns, drops the rest'''
    df_out=df_in[column_list]
    return df_out

def set_date_time_together(df_in,column_datetime, column_date, column_time,format_str):
    '''returns df where column date and time are in 1 column'''
    df_in[column_datetime]=pd.to_datetime(df_in[column_date]+ " " +df_in[column_time],format=format_str)
    df_out=df_in.drop([column_date, column_time], axis=1)
    return df_out

def datetime_to_index(df_in,column_index):
    '''sets date column to index'''
    df_out=df_in.set_index(column_index)
    return df_out

def convert_dtypes_to_int(df_in):
    non_numerical=df_in.dtypes[df_in.dtypes != 'int64'][df_in.dtypes != 'float64']
    df_out=df_in
    for column in non_numerical.index:
        df_out[column]=pd.to_numeric(df_in[column], errors='coerce')      
    return df_out

def resample(df_in, resample_time,resample_function):
    '''returns df where missing values are resampled by certain calculation'''
    if resample_function=='sum':
        df_out=df_in.resample(resample_time).sum()   
    elif resample_function=='mean':
        df_out=df_in.resample(resample_time).mean()
    elif resample_function=='ffill':
        df_out=df_in.resample(resample_time).ffill()
    
    return df_out

def interpolate_nans(df_in):
    '''returns df where all NaNs are interpolated with ffill'''
    df_out=df_in.interpolate('ffill')
    return df_out

def sum_wind(df_in,column_sum,column_onshore,column_offshore):
    '''returns df with sum of 2 columns, dropping those afterwards'''
    df_in[column_sum]=df_in[column_onshore]  +df_in[column_offshore]
    df_out=df_in.drop([column_onshore,column_offshore], axis=1)
    return df_out

def shift_6am(df_in,column_new,column_old,shift):
    '''returns df where a column is shifted by x steps'''
    df_in[column_new]=df_in[column_old].shift(shift)
    df_out=df_in.drop(column_old,axis=1)
    return df_out

def set_zeros_to_NaN_con(df_in):
    '''returns df where all 0 values up to NOW are set to NaN'''
    start=datetime.datetime.now()
    df_replaced=df_in[:start].replace(to_replace=0,value=pd.NA)
    df_replaced2=df_replaced.replace(to_replace="-",value=pd.NA)
    df_out=pd.concat([df_replaced2,df_in[start:]])
    return df_out

def concat_multiple_df(df_list):
    '''returns df which is result of concatenation of df list'''
    df_concat=None
    for df in df_list:
        df_concat=pd.concat([df_concat,df],axis=1)
    return df_concat

def extend_datetime_index(df_in,delta_start,delta_end,freq):
    '''returns a extended df, extended by datetime index till tomorrow'''
    new_dates=pd.date_range(start=datetime.datetime.today().date()-datetime.timedelta(days=delta_start), 
                            end=datetime.datetime.today().date()+datetime.timedelta(days=delta_end), freq=freq)
    df_out =df_in.reindex(new_dates)
    return df_out