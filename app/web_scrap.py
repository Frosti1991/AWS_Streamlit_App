def web_scrap():

    import web_scrap_functions as wsf
    import time
    import datetime
    import pandas as pd
    # --------- SCRAPE SMARD.DE DATA -START- -----------------------

    #Define dictionaries
    folder_pre=wsf.get_env_var('PATH_POSTGRES_DATA_PRE')

    #1. real generation
    REAL_GEN={'main_cat': 'Oberkategorie: Stromerzeugung',
            'data_cat': 'Datenkategorie: Realisierte Erzeugung',
            'file_prefix': 'Realisierte_Erz*',
            'save_folder': folder_pre+'daily/real_generation',
            'save_name':datetime.datetime.today().strftime('%d_%m_%Y')+'_real_gen.xlsx'
            }

    #2. forecast generation
    FOREC_GEN={'main_cat': 'Oberkategorie: Stromerzeugung',
            'data_cat': 'Datenkategorie: Prognostizierte Erzeugung',
            'file_prefix': 'Prognostizierte_Erz*',
            'save_folder': folder_pre+'daily/forecast_generation',
            'save_name':datetime.datetime.today().strftime('%d_%m_%Y')+'_forec_gen.xlsx'
            }

    #3. real consumption
    REAL_CON={'main_cat': 'Oberkategorie: Stromverbrauch',
            'data_cat': 'Datenkategorie: Realisierter Stromverbrauch',
            'file_prefix': 'Realisierter_Stromv*',
            'save_folder': folder_pre+'daily/real_consumption',
            'save_name':datetime.datetime.today().strftime('%d_%m_%Y')+'_real_con.xlsx'
            }

    #4. forecast consumption
    FOREC_CON={'main_cat': 'Oberkategorie: Stromverbrauch',
            'data_cat': 'Datenkategorie: Prognostizierter Stromverbrauch',
            'file_prefix': 'Prognostizierter_Stromv*',
            'save_folder': folder_pre+'daily/forecast_consumption',
            'save_name':datetime.datetime.today().strftime('%d_%m_%Y')+'_forec_con.xlsx'
            }

    #5. DayAhead Prices
    DA_PRICES={'main_cat': 'Oberkategorie: Markt',
            'data_cat': 'Datenkategorie: Großhandelspreise',
            'file_prefix': 'Gro*',
            'save_folder': folder_pre+'daily/DA_electricity',
            'save_name':datetime.datetime.today().strftime('%d_%m_%Y')+'_DA_elec.xlsx'
            }

    #Define dates
    date_from,date_to=wsf.get_dates()

    #Download real generation
    wsf.get_download_csv_smard(REAL_GEN,date_from,date_to)
    #wsf.rename_file(REAL_GEN) 
    wsf.move_file(REAL_GEN)
     

    time.sleep(2)

    #Download forecast generation
    wsf.get_download_csv_smard(FOREC_GEN,date_from,date_to)
    #wsf.rename_file(FOREC_GEN) 
    wsf.move_file(FOREC_GEN)

    time.sleep(2)

    #Download real consumption
    wsf.get_download_csv_smard(REAL_CON,date_from,date_to)
    #wsf.rename_file(REAL_CON)
    wsf.move_file(REAL_CON)
    

    time.sleep(2)

    #Download forecast consumption
    wsf.get_download_csv_smard(FOREC_CON,date_from,date_to)
    #wsf.rename_file(FOREC_GEN) 
    wsf.move_file(FOREC_CON)

    time.sleep(2)

    #Download DayAhead prices
    wsf.get_download_csv_smard(DA_PRICES,date_from,date_to)
    #wsf.rename_file(DA_PRICES) 
    wsf.move_file(DA_PRICES)

    # --------- SCRAPE SMARD.DE DATA -END- -----------------------

    # --------- SCRAPE Finanzen.net DATA - START - ---------------
    gas_dict=wsf.get_data_finanzen_net("https://www.finanzen.net/rohstoffe/erdgas-preis-ttf/historisch"
                               ,"//*[@id='sp_message_iframe_735274']"
                               ,True
                               ,'#historic-price-list > div > table'
                               ,'td')
    df_gas=pd.DataFrame(data=gas_dict.values(),index=gas_dict.keys(),columns=['gas'])

    time.sleep(2)    

    oil_dict=wsf.get_data_finanzen_net("https://www.finanzen.net/rohstoffe/oelpreis/historisch"
                               ,"//*[@id='sp_message_iframe_735274']"
                               ,False
                               ,'#historic-price-list > div > table'
                               ,'td')
    df_oil=pd.DataFrame(data=oil_dict.values(),index=oil_dict.keys(),columns=['oil'])
    
    time.sleep(2)

    coal_dict=wsf.get_data_finanzen_net("https://www.finanzen.net/rohstoffe/kohlepreis/historisch"
                                ,"//*[@id='sp_message_iframe_735274']"
                                ,False
                                ,'#historic-price-list > div > table'
                                ,'td')
    df_coal=pd.DataFrame(data=coal_dict.values(),index=coal_dict.keys(),columns=['kohle'])
    # --------- SCRAPE Finanzen.net DATA - END - ---------------

    # --------- SCRAPE Boerse.de DATA - START - ---------------
    co2_dict=wsf.get_data_boerse_de("https://www.boerse.de/historische-kurse/Co2-Emissionsrechtepreis/XC000A0C4KJ2",
                                'div.histKurseDay>table.tablesorter',
                            0,
                            'td')
    df_co2=pd.DataFrame(data=co2_dict.values(),index=co2_dict.keys(),columns=['co2'])
    # --------- SCRAPE Boerse.de DATA - END - ---------------

    #Merge df's and save
    df_list=[df_gas,df_oil,df_coal,df_co2]
    df_gas_coc_price=wsf.concat_multiple_df(df_list)
    print(df_gas_coc_price)
    pd.DataFrame.to_csv(df_gas_coc_price, 
                        'postgres_data/daily/DA_Gas_Oil_Coal_CO2/'+datetime.datetime.today().strftime('%d_%m_%Y')+'_gas_coc_price.csv'
                        , sep=',', na_rep='NaN', index=True)
    #print(datetime.datetime.today().strftime('%d_%m_%Y')+'_gas_coc_price.csv')

if __name__=='__main__':
        import web_scrap_functions as wsf
        import pandas as pd
        #wsf.get_env_var('PATH_POSTGRES_DATA_PRE')
        df_gas=wsf.get_data_finanzen_net("https://www.finanzen.net/rohstoffe/erdgas-preis-ttf/historisch"
                               ,"//*[@id='sp_message_iframe_735274']"
                               ,True
                               ,'#historic-price-list > div > table'
                               ,'td')
        df_oil=wsf.get_data_finanzen_net("https://www.finanzen.net/rohstoffe/oelpreis/historisch"
                               ,"//*[@id='sp_message_iframe_735274']"
                               ,False
                               ,'#historic-price-list > div > table'
                               ,'td')
        df_coal=wsf.get_data_finanzen_net("https://www.finanzen.net/rohstoffe/kohlepreis/historisch"
                                ,"//*[@id='sp_message_iframe_735274']"
                                ,False
                                ,'#historic-price-list > div > table'
                                ,'td')
        df_co2=wsf.get_data_boerse_de("https://www.boerse.de/historische-kurse/Co2-Emissionsrechtepreis/XC000A0C4KJ2",
                                'div.histKurseDay>table.tablesorter',
                                0,
                                'td')
           #Merge df's and save
        df_list=[df_gas,df_oil,df_coal,df_co2]
        df_gas_coc_price=wsf.concat_multiple_df(df_list)
        pd.DataFrame.to_csv(df_gas_coc_price, 
                        'postgres_data/daily/DA_Gas_Oil_Coal_CO2/'+datetime.datetime.today().strftime('%d_%m_%Y')+'_gas_coc_price.csv'
                        , sep=',', na_rep='NaN', index=True)
        print("Success")