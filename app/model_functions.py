#Dataframe
import pandas as pd

#Datetime
from datetime import datetime, timedelta

#Array
import numpy as np

#DB
import sqlalchemy as db
from sqlalchemy import update

#Sarimax
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.statespace.sarimax import SARIMAX

#DNN
from epftoolbox.data import read_data
from epftoolbox.evaluation import MAE, sMAPE
from epftoolbox.models import DNN

#OS
import os
from dotenv import load_dotenv

#User defined function
def get_env_var(key):
    load_dotenv()
    value = os.getenv(key) 
    #print(value)
    return value

def next_day_date(df_in):
    '''returns a timestamp which shows the next day after the last day of 
    a dataframe'''
    next_day_date=df_in.index[-1]+pd.DateOffset(hours=1)
    next_day_date=next_day_date.to_pydatetime()
    return next_day_date

def transpose_prediction(prediction,y_test):
    '''transposes the (1,24) prediction of DNN into (24,1) array'''
    prediction_tr=pd.DataFrame(prediction).transpose().set_index(y_test.index)
    return prediction_tr


def dict_from_arrays(array1,array2):
    dict_arr={}
    for i in range(len(array1)):
        dict_arr[array1[i]]=array2[i]
    return dict_arr

#DNN
def DNN_predict_process(nlayers,dataset,years_test,shuffle_train,data_augmentation,
                        new_recalibration,calibration_window,experiment_id,
                        begin_test_date,end_test_date,path_datasets_folder,
                        path_recalibration_folder,path_hyperparameter_folder):
    
    # Defining train and testing data and rename and order columns!
    df_train, df_test = read_data(dataset=dataset, years_test=years_test, path=path_datasets_folder,
                        begin_test_date=begin_test_date, end_test_date=end_test_date)
    
    model = DNN(experiment_id=experiment_id, path_hyperparameter_folder=path_hyperparameter_folder, nlayers=nlayers, 
            dataset=dataset, years_test=years_test, shuffle_train=shuffle_train, 
            data_augmentation=data_augmentation,calibration_window=calibration_window)
    
    #recab_df
    recab_df = pd.concat([df_train, df_test], axis=0)

    #next_day_date
    next_day=next_day_date(df_train)

    # Recalibrating the model with the most up-to-date available data and making a prediction
    # for the next day
    #defines df_test as day of interest plus last 2 weeks before
    #defines df_train as days of calibration window before day of interest
    #recalibrating model (weights) based on hyperparameters and train data
    #predict on test data
    Yp = model.recalibrate_and_forecast_next_day(df=recab_df, next_day_date=next_day)
    

    return Yp

#Sarimax
def sc_train_test_data(df,start_train,end_train,start_test,end_test):
  #1. define frequency
  df=df.asfreq('H')
  #2. important columns
  col_corr=['braunkohle_mwh','steinkohle_mwh','erdgas_mwh','wind_sum_mwh_real','kohle','gas_6am']
  X_train=df[col_corr][start_train:end_train]
  X_test=df[col_corr][start_test:end_test]
  #3. apply Standard Scaler
  sc=StandardScaler()
  X_train_sc_arr=sc.fit_transform(X_train)
  X_test_sc_arr=sc.transform(X_test)
  #4. transform back to df
  X_train_sc=pd.DataFrame(X_train_sc_arr, columns =col_corr,index=X_train.index)
  X_test_sc=pd.DataFrame(X_test_sc_arr, columns =col_corr,index=X_test.index)
  #5. Target
  y_train=df['price_real'][start_train:end_train]
  y_test=df['price_real'][start_test:end_test]
  return X_train_sc, y_train, X_test_sc, y_test

def appended_forecast(model,start,end,X_test_sc,y_test):
    '''returns a forecasting series based on the start and end parameter
    and the updated sarimax model'''
    #Define steps
    steps=24

    #Set new model variable to old model
    update_sarimax=model
    forecast_series=pd.Series([])
    while True:
      print(start)
      #Forecast next day based on new data, from current start+1hour to start+24hours --> 1 whole day
      forecast=update_sarimax.forecast(exog=X_test_sc.loc[start:start+timedelta(hours=steps-1)],steps=start+timedelta(hours=steps-1))

      #Define new data (from 00:00 to 00:00 to flatten the curve since there is a peak between day change!)
      new_X=X_test_sc.loc[start:start+timedelta(hours=steps-1)]
      new_X=new_X.asfreq('H')

      new_y=y_test.loc[start:start+timedelta(hours=steps-1)]

      #Extend start date
      start=start+timedelta(hours=steps)

      #Append new data to model
      update_sarimax = update_sarimax.append(endog=new_y,exog=new_X)

      #Append current forecast to forecast-series
      forecast_series=pd.concat([forecast_series,forecast])
      if start>=end:
        break
    return forecast_series,update_sarimax