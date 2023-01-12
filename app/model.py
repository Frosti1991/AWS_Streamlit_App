def run_model(chose_model,begin_test_date,end_test_date):
    # SX2 --> StandardScaler on PV and Wind, already trained, just needs to predict
    # DNN_forec --> HyperOptimization on ['Price elec DE/LU [€/MWh]','Photovoltaik[MWh]','Wind_Sum[MWh]','Gas_6am','Netzlast'], saved in
    # experimental files
    # DNN _all--> HyperOpt on all FEatures
    # DNN's needs to be recalibrated before predicting

    import datetime
    import numpy as np
    import model_functions as mof
    import pandas as pd
    from sklearn.preprocessing import StandardScaler
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    import pickle
    import postgres


    #################LOAD DATA FROM POSTGRES - START - ####################

    #--------------- DEFINE START AND END DATE - START - -------------------------
    #2 years backwards
    data_start=(datetime.datetime.today().date()-datetime.timedelta(days=2*364)).strftime('%Y-%m-%d')
    #2 days ahead
    data_end=(datetime.datetime.today().date()+datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    #--------------- DEFINE START AND END DATE - END - -------------------------

    # ---------------CONNECT TO POSTGRES - START --------------------
    engine=postgres.connect_to_postgres()
    # ---------------CONNECT TO POSTGRES - END --------------------

    #---------------- SQL QUERY FROM START TO END AS DF - START --------------
    query = "SELECT * FROM elec_price_data WHERE datum between ('" +data_start+ "') and ('" +data_end+ "');"
    df_all = postgres.query_df_by_date(query,engine,"datum")
    #ignore last value and predict_columns
    df_all=df_all#[:-1]
    df_all=df_all.drop(['price_pred_sarimax','price_pred_dnn'],axis=1)
    #---------------- SQL QUERY FROM START TO END AS DF - END --------------
    #################LOAD DATA FROM POSTGRES - END - ####################

    #################PREDICT PRICES - START - ####################

    #needs to be an user input from streamlit!
    #begin_test_date = "22/11/2022 00:00"
    #end_test_date = "22/11/2022 23:00"

    #Set Y_test
    Y_test=df_all[begin_test_date:end_test_date]['price_real']

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
        print(begin_test_date, end_test_date)
        sx2 = pickle.load(open('model/sx2_with_sc.pkl', 'rb'))
        sc=StandardScaler()
        X_train_sc_arr2=sc.fit_transform(df_all[['photovoltaik_mwh_real','wind_sum_mwh_real']])
        X_train_sc_2 =pd.DataFrame(X_train_sc_arr2, columns =['photovoltaik_mwh_real','wind_sum_mwh_real'],index=df_all.index)
        Yp= sx2.predict(n_periods=Y_test.shape[0],X=X_train_sc_2[begin_test_date:end_test_date])

        ################# PREDICT PRICES - END - ####################

    # import matplotlib.dates as md
    # import matplotlib.pyplot as plt
    # # filter df and plot ticker on the new subplot axis
    # fig, ax = plt.subplots(figsize=(8,6))
    # plt.plot(Y_test.index, Y_test, label = "Price_Test")
    # if chose_model=='sarimax':
    #     plt.plot(Y_test.index, Yp, label = "Price_Prediction")
    # else:
    #     plt.plot(Y_test.index, Yp.transpose(), label = "Price_Prediction")
    # plt.legend()
    # plt.title(f'DayAhead Price {begin_test_date}')
    # ax.xaxis.set_major_formatter(md.DateFormatter('%H:%M:%S'))
    # plt.show()

    #################### UPDATE Yp/sx2_forecast TO POSTGRES - START - ########
    if chose_model=="sarimax":
        sql_column="price_pred_sarimax"
    else:
        sql_column="price_pred_dnn"
        Yp=Yp.flatten()

    date_array=np.array(Y_test.index.to_pydatetime(), dtype=str)
    dict_sql=mof.dict_from_arrays(date_array,Yp)

    postgres.update_sql(dict_sql,sql_column,engine,"elec_price_data")

    print("SQL successfully updated!")

    #################### UPDATE Yp/sx2_forecast TO POSTGRES - END - ########