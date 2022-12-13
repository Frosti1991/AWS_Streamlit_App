import streamlit as st
import pandas as pd
import numpy as np
import streamlit_functions as strf
import web_scrap as ws
import etl
import postgres
import model
import datetime

#Tab Notation
st.set_page_config(page_title="DayAhead Price Predictor", page_icon=":electric_plug:",layout="wide")

with st.container():
    st.title('Welcome to the Energy Market!')
    st.header("Let's predict DayAhead Prices of European Power Exchange")

# 'Starting a long computation...'

# # Add a placeholder
# latest_iteration = st.empty()
# bar = st.progress(0)

# for i in range(100):
#   # Update the progress bar with each iteration.
#   latest_iteration.text(f'Iteration {i+1}')
#   bar.progress(i + 1)
#   time.sleep(0.1)


#Background Image
backg_image=strf.set_bg_hack("img/wind_turbines_2.jpg")

df_column_names=['price_real', 'biomasse_mwh', 'wasserkraft_mwh',
       'photovoltaik_mwh_real', 'sonstige_ee_mwh', 'kernenergie_mwh',
       'braunkohle_mwh', 'steinkohle_mwh', 'erdgas_mwh', 'pumpspeicher_mwh',
       'sonstige_konv_mwh', 'wind_sum_mwh_real', 'netzlast_mwh_real',
       'photovoltaik_mwh_forec', 'wind_sum_mwh_forec', 'netzlast_mwh_forec',
       'oil', 'kohle', 'co2', 'gas_6am', 'price_pred_dnn',
       'price_pred_sarimax']

with st.container():
    #Connect to Postgres
    st.subheader("Get new data!")
    col1, col2= st.columns(2)
        
    with col1:
        scrape_button=st.button("Scrape the Web")
        if scrape_button:
            ws.web_scrap()
            etl.etl()
            #st.success('Web Scraping and ETL finished!', icon="✅")
            st.balloons()

with st.container():

    st.subheader("Predict prices!")

    col1,col2,col3,col4=st.columns(4)
    with col1:
        model_selector=st.selectbox("Choose your model", options=['sarimax', 'DNN_all','DNN_forec'])
    with col2:
        data_start_model=str(st.date_input("Enter Start Date",key='model_start'))
        data_start_model=datetime.datetime.strptime(data_start_model,'%Y-%m-%d')
        #st.write(data_start_model)

    with col3:
        data_end_model=str(st.date_input("Enter End Date",key='model_end'))
        data_end_model=datetime.datetime.strptime(data_end_model,'%Y-%m-%d')-datetime.timedelta(hours=1)
        #st.write(data_end_model)

    with col4:
        model_button=st.button("Predict Prices")

        if model_button:
            #st.write(model_selector)
            model.run_model(model_selector,data_start_model,data_end_model)
            st.balloons()

with st.container():
    #Connection to Postgres
    st.subheader("Look at your data!")

    df=None
    engine=postgres.connect_to_postgres()
    #st.markdown(engine) 
    
    col1, col2, col3 = st.columns(3)
     
    with col1:
        data_start_df=str(st.date_input("Enter Start Date",key='df_start'))

    with col2:
        data_end_df=str(st.date_input("Enter End Date",key='df_end'))

    with col3:
        select_df_columns=st.multiselect("Select data you are interested in",key='postgres', options=df_column_names)
        

with st.container():
    col1, col2 = st.columns(2)
    with col1:
        df_button= st.button("Create Dataframe and Plot")

    if df_button:
        query = "SELECT * FROM elec_price_data WHERE datum between ('" +data_start_df+ "') and ('" +data_end_df+ "');"
        df=postgres.query_df_by_date(query,engine,'datum')
        st.balloons()
        #st.success('Get data from postgres finished!', icon="✅")

    df_checkbox=st.checkbox('Show Dataframe')
    if df_checkbox==True and df_button==True:
        st.write(df[select_df_columns])
    else:
        st.write("Please create a df by pressing the button before!")
    
    plot_checkbox=st.checkbox('Show plotted Data')
    if plot_checkbox==True and df_button==True:
        st.line_chart(df[select_df_columns])
    else:
        st.write("Please create a plot by pressing the button before!")
        
        

            

            
            