def run_model(chose_model,begin_test_date,end_test_date):
    # SX2 --> StandardScaler on PV and Wind, already trained, just needs to predict
    # DNN_forec --> HyperOptimization on ['Price elec DE/LU [€/MWh]','Photovoltaik[MWh]','Wind_Sum[MWh]','Gas_6am','Netzlast'], saved in
    # experimental files
    # DNN _all--> HyperOpt on all FEatures
    # DNN's needs to be recalibrated before predicting

    from datetime import datetime, timedelta
    import numpy as np
    import model_functions as mof
    import pandas as pd
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    import pickle
    import postgres


    #################LOAD DATA FROM POSTGRES - START - ####################

    # #--------------- DEFINE START AND END DATE - START - -------------------------
    # #2 years backwards
    # data_start=(datetime.datetime.today().date()-datetime.timedelta(days=2*364)).strftime('%Y-%m-%d')
    # #2 days ahead
    # data_end=(datetime.datetime.today().date()+datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    # #--------------- DEFINE START AND END DATE - END - -------------------------

    # # ---------------CONNECT TO POSTGRES - START --------------------
    # engine=postgres.connect_to_postgres()
    # #a ---------------CONNECT TO POSTGRES - END --------------------

    # #---------------- SQL QUERY FROM START TO END AS DF - START --------------
    # query = "SELECT * FROM elec_price_data WHERE datum between ('" +data_start+ "') and ('" +data_end+ "');"
    # df_all = postgres.query_df_by_date(query,engine,"datum")
    # #ignore last value and predict_columns
    # df_all=df_all#[:-1]
    # df_all=df_all.drop(['price_pred_sarimax','price_pred_dnn'],axis=1)
    # #---------------- SQL QUERY FROM START TO END AS DF - END --------------
    # #################LOAD DATA FROM POSTGRES - END - ####################

    # #################PREDICT PRICES - START - ####################

    # #needs to be an user input from streamlit!
    # #begin_test_date = "22/11/2022 00:00"
    # #end_test_date = "22/11/2022 23:00"

    # #Set Y_test
    # Y_test=df_all[begin_test_date:end_test_date]['price_real']

    if chose_model=="DNN_all":

        pd.DataFrame.to_csv(df_all, 'model/df_dnn_all.csv', sep=',', na_rep='NaN', index=True,decimal='.')

        nlayers = 2
        dataset = "df_dnn_all"
        years_test = 2
        shuffle_train = 1
        data_augmentation = 0
        new_recalibration = 1 #new recalibration (1) or old one (0)?
        calibration_window = 1
        experiment_id = 3
        begin_test_date = begin_test_date
        end_test_date = end_test_date 

        path_datasets_folder = mof.get_env_var('PATH_DATASETS_FOLDER')
        path_recalibration_folder = "model/experimental_files/"
        path_hyperparameter_folder = "model/experimental_files/"

        Yp=mof.DNN_predict_process(nlayers,dataset,years_test,shuffle_train,
        data_augmentation,new_recalibration,calibration_window,experiment_id,
        begin_test_date,end_test_date,path_datasets_folder,path_recalibration_folder,
        path_hyperparameter_folder)

    elif chose_model=="DNN_forec":
        #if test date in future, then predict on feature forecast
        if pd.to_datetime(begin_test_date) > datetime.datetime.now():
            df_dnn_forec=df_all[['price_real', 'photovoltaik_mwh_forec','wind_sum_mwh_forec','gas_6am','netzlast_mwh_forec']]
        #if not: predict on real measured data
        else:
            df_dnn_forec=df_all[['price_real', 'photovoltaik_mwh_real','wind_sum_mwh_real','gas_6am','netzlast_mwh_real']]
        pd.DataFrame.to_csv(df_dnn_forec, 'model/df_dnn_forec.csv', sep=',', na_rep='NaN', index=True,decimal='.')

        nlayers = 2
        dataset = "df_dnn_forec"
        years_test = 2
        shuffle_train = 1
        data_augmentation = 0
        new_recalibration = 1 #new recalibration (1) or old one (0)?
        calibration_window = 1
        experiment_id = 1
        begin_test_date = begin_test_date
        end_test_date = end_test_date 

        path_datasets_folder = mof.get_env_var('PATH_DATASETS_FOLDER')
        path_recalibration_folder = "model/experimental_files/"
        path_hyperparameter_folder = "model/experimental_files/"

        Yp=mof.DNN_predict_process(nlayers,dataset,years_test,shuffle_train,
        data_augmentation,new_recalibration,calibration_window,experiment_id,
        begin_test_date,end_test_date,path_datasets_folder,path_recalibration_folder,
        path_hyperparameter_folder)

    elif chose_model=="sarimax":
        #Load latest model
        sarimax_usual = pickle.load(open('usual_sarimax_high_corr_sc.pkl', 'rb'))
        #Define relevant dates
        model_last_date=sarimax_usual.predict().tail(1).index[0]
        model_next_start=model_last_date+timedelta(hours=1)

        #start is user input in app
        forecast_start=datetime.strptime(begin_test_date,'%Y-%m-%d %H:%M')
        forecast_end=datetime.strptime(end_test_date,'%Y-%m-%d %H:%M')

        #train_period (rolling time windows 6 months before last model date)
        end_train=model_last_date
        start_train=end_train-timedelta(days=30*6)

        #test period
        end_test=forecast_end
        if model_next_start>=forecast_start:
            start_test=forecast_start
        else:
            start_test=model_next_start
        
        #Load data
        engine=postgres.connect_to_postgres()
        data_start=datetime(start_train.year,start_train.month,start_train.day,start_train.hour).strftime('%Y-%m-%d %H:00')
        print(data_start)
        data_end=datetime(end_test.year,end_test.month,end_test.day,end_test.hour).strftime('%Y-%m-%d %H:00')
        print(data_end)
        query = "SELECT * FROM elec_price_data WHERE datum between ('" +data_start+ "') and ('" +data_end+ "');"
        df=postgres.query_df_by_date(query,engine,"datum")

        X_train_sc, y_train, X_test_sc, y_test=mof.sc_train_test_data(df,start_train,end_train,start_test,end_test)

        #Forecast Process
        if model_next_start>=forecast_start:
            print("In-Sample Forecast")
            forecast=sarimax_usual.predict(exog=X_test_sc.loc[start_test:end_test],start=start_test,end=end_test)
        
        else:
            print("Out-of-Sample Forecast")
            #2. use appended-forecast method (return updated model and forecast series)
            forecast,updated_sarimax=mof.appended_forecast(sarimax_usual,start_test,end_test)
      
            #3. pickle updated model
            name='usual_sarimax_high_corr_sc.pkl'
            pickle.dump(updated_sarimax, open(path+name,'wb'))

        ################# PREDICT PRICES - END - ####################


    #################### UPDATE Yp/sx2_forecast TO POSTGRES - START - ########
    if chose_model=="sarimax":
        sql_column="price_pred_sarimax"
    else:
        sql_column="price_pred_dnn"
        Yp=Yp.flatten()

    date_array=np.array(y_test.index.to_pydatetime(), dtype=str)
    dict_sql=mof.dict_from_arrays(date_array,forecast)

    postgres.update_sql(dict_sql,sql_column,engine,"elec_price_data")

    print("SQL successfully updated!")

    #################### UPDATE Yp/sx2_forecast TO POSTGRES - END - ########