#OS functions
import glob
import os
from dotenv import load_dotenv

#(german) time 
import datetime
#from datetime import datetime
import time
import locale
# for German locale
locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')

#Webscraping
import requests
import bs4

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager 

#dataframe
import pandas as pd

#user defined functions
days_back=5

def get_dates():
    
    '''This function returns a date_from (d-5) and date_to (d+1)
    for the webscraping process'''
    
    date_from=datetime.datetime.now().date()-datetime.timedelta(days = days_back)
    date_to=datetime.datetime.now().date()+datetime.timedelta(days=2)
    
    return date_from, date_to


def get_download_csv_smard(download, date_from, date_to):
    
    '''This function downloads csv.file from smard.de depending
    on the passed dictionary and dates'''
    
    url="https://www.smard.de/home/downloadcenter/download-marktdaten/"
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(ChromeDriverManager().install(),options=options) #automatically installs the latest version chromedriver!
    driver.get(url)

    #wait implicit to find unlocated elements, set for the life of webdriver object
    driver.implicitly_wait(5)
    
    #Select Oberkategorie
    #print(download['main_cat'])
    select_main_cat = Select(driver.find_element(By.XPATH,"//*[@id='help-categories']/div/select"))
    select_main_cat.select_by_visible_text(download['main_cat'])
    
    #Select Datenkategorie
    select_data_cat = Select(driver.find_element(By.XPATH,"//*[@id='help-subcategories']/div/select"))
    select_data_cat.select_by_visible_text(download['data_cat'])
    
    #Select Marktgebiet
    select_market = Select(driver.find_element(By.XPATH,"//*[@id='help-regionpicker']/div/select"))
    select_market.select_by_visible_text('Marktgebiet: DE/LU (ab 01.10.2018)')

    #Select Auflösung
    select_market = Select(driver.find_element(By.XPATH,"//*[@id='help-resolutionpicker']/div/select"))
    select_market.select_by_visible_text('Auflösung wählen: Originalauflösung')
    
    #Select Download File typ
    select_file_type = Select(driver.find_element(By.XPATH,"//*[@id='help-filetype']/div/select"))
    select_file_type.select_by_visible_text('XLSX')     
    
    #Fill input form date from
    driver.find_element(By.XPATH,"//input[@name='daterange_from']").clear()
    driver.find_element(By.XPATH,"//input[@name='daterange_from']").send_keys(str(date_from.day)+"."+\
                                                                              str(date_from.month)+"."+\
                                                                              str(date_from.year))
    #Close date picker
    btn_apply=driver.find_elements(By.XPATH,"//button[@class='applyBtn btn btn-sm btn-success']")
    btn_apply[0].click() #btn exists 2 times, go to 1st one
    
    #Fill input form date to
    driver.find_element(By.XPATH,"//input[@name='daterange_to']").clear()
    driver.find_element(By.XPATH,"//input[@name='daterange_to']").send_keys(str(date_to.day)+"."+\
                                                                              str(date_to.month)+"."+\
                                                                              str(date_to.year))
    #Close date picker
    btn_apply=driver.find_elements(By.XPATH,"//button[@class='applyBtn btn btn-sm btn-success']")
    btn_apply[1].click() #btn exists 2 times, go to 2nd one
    
    #Click Cookie-Button accept
    btn_download = driver.find_element(By.XPATH,"//*[@class='smard-button js-cookie-accept mb-3 mb-sm-0 me-3']")
    btn_download.click()

    #Click download button
    btn_download = driver.find_element(By.XPATH,"//*[@id='help-download']")
    btn_download.click()

    time.sleep(5) #enough time for download before closing window

    driver.close()

def get_env_var(key):
    load_dotenv()
    value = os.getenv(key) 
    #print(value)
    return value

def rename_file(download):
    '''This function renames a file'''
    path_download=get_env_var('PATH_DOWNLOAD')
    os.rename(path_download+'export.xlsx',path_download+download['file_prefix'])

def move_file(download):
    '''This function moves all fitting files from old folder to new folder'''
    path_download=get_env_var('PATH_DOWNLOAD')
    for file in glob.glob(path_download + download['file_prefix']):
        print(download['save_name'])
        #file_name=os.path.basename(file)
        os.replace(file, download['save_folder']+'/'+download['save_name'])

def get_data_finanzen_net(url,selector_soup,index_soup,selector_table):
    
    '''this function returns a date:price dictionary
    for the last x days out of the webscraping soup'''
    res=requests.get(url)
    soup=bs4.BeautifulSoup(res.text, 'lxml')
    table_total=soup.select(selector_soup)[index_soup]
    table_price_date=table_total.select(selector_table)
    counter=0
    date_price_dict={}
    for i in table_price_date:
        if i.text!='-':  #not empty
            try:
                date=datetime.datetime.strptime(i.text,'%d.%m.%Y')
                if counter>=days_back:
                    break
                date_price_dict[date]=None #set key
                counter+=1
            except ValueError:   #is settlement price
                date_price_dict[date]=float(i.text.replace(",","."))  #set price to date as int
    return date_price_dict


def get_data_boerse_de(url,selector_soup,index_soup,selector_table):
    '''this function returns a date:price dictionary
    for the last x days out of the webscraping soup'''
    res=requests.get(url)
    soup=bs4.BeautifulSoup(res.text, 'lxml')
    table_total=soup.select(selector_soup)[index_soup]
    table_price_data=table_total.select(selector_table)
    counter=0
    check=None
    date_price_dict={}
    for i in table_price_data:
        try:
            date=datetime.datetime.strptime(i.text,'%d.%m.%Y') #check if entry is a date
            if counter>=days_back: #get just the 5 last days
                break        
            date_price_dict[date]=None #set key
            counter+=1
        except ValueError:   #if not: is settlement price
            try:
                date_price_dict[date]=float(i.text.replace(",","."))  #set price to date as int
            except:
                pass
            
    return date_price_dict


def concat_multiple_df(df_list):
    df_concat=None
    for df in df_list:
        df_concat=pd.concat([df_concat,df],axis=1)
    return df_concat

if __name__=='__main__':
    get_env_var('PATH_POSTGRES_DATA_DAILY')