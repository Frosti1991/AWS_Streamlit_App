#Dataframe
import pandas as pd

#Array
import numpy as np

#DB
import sqlalchemy as db
from sqlalchemy import update

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