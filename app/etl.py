def etl():

    import os
    from os.path import exists
    #Dataframe
    import pandas as pd
    import numpy as np
    #Time
    import datetime

    #ETL functions
    import etl_functions as etl
    from web_scrap_functions import get_dates

    #postgres
    import postgres 

    path_base=etl.get_env_var('PATH_POSTGRES_DATA_DAILY')

    #Define dates
    date_from,date_to=get_dates()
    date_suffix=datetime.datetime.today().strftime('%d_%m_%Y')

    #empty file list 
    file_list=[]

    #header
    header=9

    ################ EXTRACT AND TRANSFORM - START ######################

    # ---------- ETL - Real generation data - START -------------------
    file_real_gen=path_base+"real_generation/"+date_suffix+"_real_gen.xlsx"
    if exists(file_real_gen):
        #do ETL
        real_gen_raw = pd.read_excel(file_real_gen, header=header)
        real_gen_suffix=etl.change_column_name(real_gen_raw,{'Wind Offshore [MWh]': 'Wind Offshore[MWh]_real',
                                                        'Wind Onshore [MWh]': 'Wind Onshore[MWh]_real',
                                                        'Photovoltaik [MWh]': 'photovoltaik_mwh_real',
                                                        'Biomasse [MWh]': 'biomasse_mwh',
                                                        'Wasserkraft [MWh]': 'wasserkraft_mwh',
                                                        'Sonstige Erneuerbare [MWh]':'sonstige_ee_mwh',
                                                        'Kernenergie [MWh]':'kernenergie_mwh',
                                                        'Braunkohle [MWh]':'braunkohle_mwh',
                                                        'Steinkohle [MWh]':'steinkohle_mwh',
                                                        'Erdgas [MWh]':'erdgas_mwh',
                                                        'Pumpspeicher [MWh]':'pumpspeicher_mwh',
                                                        'Sonstige Konventionelle [MWh]':'sonstige_konv_mwh'
                                                        })
        print(real_gen_suffix.columns)
        column_list=[i for i in real_gen_suffix.columns if not "Ende" in i]
        print(column_list)
        real_gen_columns=etl.keep_columns(real_gen_suffix,column_list)
        real_gen_datetime=etl.set_date_time_together(real_gen_columns,'datum', 'Datum', 'Anfang','%d.%m.%Y %H:%M')
        real_gen_date_index=etl.datetime_to_index(real_gen_datetime,'datum')
        real_gen_int=etl.convert_dtypes_to_int(real_gen_date_index)
        real_gen_60=etl.resample(real_gen_int, '60min','sum')
        real_gen_imp=etl.interpolate_nans(real_gen_60)
        real_gen_sum_wind=etl.sum_wind(real_gen_imp,'wind_sum_mwh_real','Wind Onshore[MWh]_real','Wind Offshore[MWh]_real')
        df_real_gen=real_gen_sum_wind[date_from:date_to]
        file_list.append(file_real_gen)
    else:
        #Create NaN-Df
        df_real_gen=pd.DataFrame(np.nan,index=pd.date_range(start=date_from, end=date_to, freq='H'),
                    columns=['wind_sum_mwh_real','photovoltaik_mwh_real','biomasse_mwh','wasserkraft_mwh',
                            'sonstige_ee_mwh','kernenergie_mwh','braunkohle_mwh','steinkohle_mwh','erdgas_mwh','pumpspeicher_mwh','sonstige_konv_mwh'])
    #print(df_real_gen)
    # ---------- ETL - Real generation data - END -------------------

    # ---------- ETL - Forecast generation data - START -------------------
    file_forec_gen=path_base+"forecast_generation/"+date_suffix+"_forec_gen.xlsx"
    if exists(file_forec_gen):
        #do ETL
        forec_gen_raw = pd.read_excel(file_forec_gen, header=header)
        forec_gen_suffix=etl.change_column_name(forec_gen_raw,{'Wind Offshore [MWh]': 'Wind Offshore[MWh]_forec',
                                                        'Wind Onshore [MWh]': 'Wind Onshore[MWh]_forec',
                                                        'Photovoltaik [MWh]': 'photovoltaik_mwh_forec'})
        forec_gen_columns=etl.keep_columns(forec_gen_suffix,['Datum','Anfang','Wind Offshore[MWh]_forec','Wind Onshore[MWh]_forec','photovoltaik_mwh_forec'])
        forec_gen_datetime=etl.set_date_time_together(forec_gen_columns,'datum', 'Datum', 'Anfang','%d.%m.%Y %H:%M')
        forec_gen_date_index=etl.datetime_to_index(forec_gen_datetime,'datum')
        forec_gen_int=etl.convert_dtypes_to_int(forec_gen_date_index)
        forec_gen_60=etl.resample(forec_gen_int, '60min','sum')
        forec_gen_imp=etl.interpolate_nans(forec_gen_60)
        forec_gen_sum_wind=etl.sum_wind(forec_gen_imp,'wind_sum_mwh_forec','Wind Onshore[MWh]_forec','Wind Offshore[MWh]_forec')
        df_forec_gen=forec_gen_sum_wind[date_from:date_to]
        file_list.append(file_forec_gen)
    else: 
        #Create NaN-Df
        df_forec_gen=pd.DataFrame(np.nan,index=pd.date_range(start=date_from, end=date_to, freq='H'),
                    columns=['wind_sum_mwh_forec','photovoltaik_mwh_forec'])
    #print(df_forec_gen)
    # ---------- ETL - Forecast generation data - END -------------------

    #---------- ETL - Real consumption data - START -------------------
    file_real_con=path_base+"real_consumption/"+date_suffix+"_real_con.xlsx"
    if exists(file_real_con):
        real_con_raw = pd.read_excel(file_real_con, header=header)
        real_con_col_change=etl.change_column_name(real_con_raw,{'Gesamt (Netzlast) [MWh]':'netzlast_mwh_real'})
        real_con_columns=etl.keep_columns(real_con_col_change,["Datum","Anfang","netzlast_mwh_real"])
        real_con_datetime=etl.set_date_time_together(real_con_columns,'datum', 'Datum', 'Anfang','%d.%m.%Y %H:%M')
        real_con_date_index=etl.datetime_to_index(real_con_datetime,'datum')
        real_con_int=etl.convert_dtypes_to_int(real_con_date_index)
        real_con_60=etl.resample(real_con_int, '60min','sum')
        real_con_NaN=etl.set_zeros_to_NaN_con(real_con_60)
        real_con_int=etl.convert_dtypes_to_int(real_con_NaN)
        real_con_imp=etl.interpolate_nans(real_con_int)
        df_real_con=real_con_imp[date_from:date_to]
        file_list.append(file_real_con)
    else:
        #Create NaN-Df
        df_real_con=pd.DataFrame(np.nan,index=pd.date_range(start=date_from, end=date_to, freq='H'),
                    columns=['netzlast_mwh_real'])
    #print(df_real_con)  
    # ---------- ETL - Real consumption data - END -------------------

    # ---------- ETL - Forecast consumption data - START -------------------
    file_forec_con=path_base+"forecast_consumption/"+date_suffix+"_forec_con.xlsx"
    if exists(file_forec_con):
        forec_con_raw = pd.read_excel(file_forec_con, header=header)
        forec_con_col_change=etl.change_column_name(forec_con_raw,{'Gesamt (Netzlast) [MWh]':'netzlast_mwh_forec'})
        forec_con_columns=etl.keep_columns(forec_con_col_change,["Datum","Anfang","netzlast_mwh_forec"])
        forec_con_datetime=etl.set_date_time_together(forec_con_columns,'datum', 'Datum', 'Anfang','%d.%m.%Y %H:%M')
        forec_con_date_index=etl.datetime_to_index(forec_con_datetime,'datum')
        forec_con_int=etl.convert_dtypes_to_int(forec_con_date_index)
        forec_con_60=etl.resample(forec_con_int, '60min','sum')
        forec_con_NaN=etl.set_zeros_to_NaN_con(forec_con_60)
        forec_con_int=etl.convert_dtypes_to_int(forec_con_NaN)
        forec_con_imp=etl.interpolate_nans(forec_con_NaN)
        df_forec_con=forec_con_imp[date_from:date_to]
        file_list.append(file_forec_con)
    else:
        #Create NaN-Df
        df_forec_con=pd.DataFrame(np.nan,index=pd.date_range(start=date_from, end=date_to, freq='H'),
                    columns=['netzlast_mwh_forec'])
    #print(df_forec_con)  
    # ---------- ETL - Forecast consumption data - END -------------------

    # ---------- ETL - DA elec- START -------------------
    file_elec=path_base+"DA_electricity/"+date_suffix+"_DA_elec.xlsx"
    if exists(file_elec):
        DA_elec_raw = pd.read_excel(file_elec, header=header)
        DA_elec_col_change=etl.change_column_name(DA_elec_raw,{'Deutschland/Luxemburg [€/MWh]':'price_real'})
        DA_elec_columns=etl.keep_columns(DA_elec_col_change,["Datum","Anfang","price_real"])
        DA_elec_datetime=etl.set_date_time_together(DA_elec_columns,'datum', 'Datum', 'Anfang','%d.%m.%Y %H:%M')
        DA_elec_date_index=etl.datetime_to_index(DA_elec_datetime,'datum')
        DA_elec_int=etl.convert_dtypes_to_int(DA_elec_date_index)
        DA_elec_60=etl.resample(DA_elec_int, '60min','mean')
        DA_elec_imp=etl.interpolate_nans(DA_elec_60)
        df_DA_elec = DA_elec_imp[date_from:date_to]
        file_list.append(file_elec)
    else:
        #Create NaN-Df
        df_DA_elec=pd.DataFrame(np.nan,index=pd.date_range(start=date_from, end=date_to, freq='H'),
                    columns=['price_real'])
    #print(df_DA_elec)  
    # ---------- ETL - DA elec - END -------------------


    # ---------- ETL - DA gas und coc - START -------------------
    file_gas_coc=path_base+"DA_Gas_Oil_Coal_CO2/"+date_suffix+"_gas_coc_price.csv"
    if exists(file_gas_coc):
        DA_gas_coc_raw = pd.read_csv(file_gas_coc)
        DA_gas_coc_column_changed=etl.change_column_name(DA_gas_coc_raw,{'Unnamed: 0':'Datum'})
        DA_gas_coc_date_index=etl.datetime_to_index(DA_gas_coc_column_changed,'Datum')
        DA_gas_coc_date_index.index=pd.to_datetime(DA_gas_coc_date_index.index)
        DA_gas_coc_60=etl.resample(DA_gas_coc_date_index, '60min','ffill')
        DA_gas_coc_shift=etl.shift_6am(DA_gas_coc_60,'gas_6am','gas',6)
        print(DA_gas_coc_shift)
        DA_gas_coc_extend=etl.extend_datetime_index(DA_gas_coc_shift,date_from,date_to,'1h')
        DA_gas_coc_inter=etl.interpolate_nans(DA_gas_coc_extend)
        print(DA_gas_coc_extend)
        df_DA_gas_coc=DA_gas_coc_inter[date_from:date_to]
        file_list.append(file_gas_coc)
    else:
        #Create NaN-Df
        df_DA_gas_coc=pd.DataFrame(np.nan,index=pd.date_range(start=date_from, end=date_to, freq='H'),
                    columns=['gas','oil','kohle','co2'])
    #print(df_DA_gas_coc)  
    # ---------- ETL - DA gas und coc- END -------------------

    #----------- Concatenate all df's - START----------------
    #COncat
    df_list=[df_DA_elec,df_real_gen,df_real_con,df_forec_gen,df_forec_con,df_DA_gas_coc]
    df_all=etl.concat_multiple_df(df_list)
    #print(df_all)
    pd.DataFrame.to_csv(df_all, path_base+'all_merged_data/'+date_suffix+'_postgres_data.csv'
    , sep=',', na_rep='NaN', index=True)

    #----------- Concatenate all df's - END----------------

    #----------- Move existing files to archive - START----------------
    
    for file in file_list:
        folder=os.path.dirname(file)
        name=os.path.basename(file)
        os.replace(file, folder+'/archiv/'+name)
    #----------- Move files to archive - END ----------------

    ################ EXTRACT AND TRANSFORM - END ######################

    ################ CONNECT AND LOAD TO POSTGRES - START ######################
    engine=postgres.connect_to_postgres()
    postgres.load_etl_to_postgres(engine)

    ################ CONNECT AND LOAD TO POSTGRES - END ######################

if __name__=='__main__':
    #import etl_functions as etl
    #etl.get_env_var('PATH_POSTGRES_DATA_DAILY')
    etl()
    print("Success")