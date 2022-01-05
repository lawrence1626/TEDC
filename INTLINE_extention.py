# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=E1101
#pip install regex, datetime, pandas, requests, openpyxl(2.4.9), lxml, xlrd(1.2.0), iteration_utilities, matplotlib, statsmodels, pathlib, bs4, selenium, webdriver_manager, quandl, pywin32, pycnnum, roman, html5lib, pyxlsb, dateparser, sqlalchemy, pymysql
#sql="SELECT * FROM intline_keytot202111"
#pd.read_sql_query(sql, engine)
import math, sys, calendar, os, copy, time, shutil, logging, ssl, zipfile, traceback, pycnnum, roman, dateparser
import regex as re
import pandas as pd
import numpy as np
import requests as rq
import win32com.client as win32
from pythoncom import com_error
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import ElementNotInteractableException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import JavascriptException
from selenium.common.exceptions import UnexpectedAlertPresentException
from selenium.common.exceptions import NoSuchFrameException
from pandas.errors import ParserError
from roman import InvalidRomanNumeralError
from io import BytesIO
import http.client
import webdriver_manager
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from urllib.error import HTTPError

# 回報錯誤、記錄錯誤檔案並結束程式
def ERROR(error_text, waiting=False):
    if waiting == True:
        sys.stdout.write("\r"+error_text)
        sys.stdout.flush()
    else:
        sys.stdout.write('\n\n')
        logging.error('= ! = '+error_text)
        sys.stdout.write('\n\n')
    sys.exit()

NAME = 'INTLINE_'
ASIA_NAME = 'ASIA_'
ENCODING = 'utf-8-sig'
data_path = "./data/"
out_path = "./output/"
BANK = input('Bank (INTLINE/ASIA): ')#'INTLINE'#
if BANK not in ['INTLINE','ASIA']:
    ERROR('Incorrect Name of Bank')
excel_suffix = input('Output file suffix (If test identity press 0): ')

def takeFirst(alist):
    return alist[0]

def readFile(dir, default=pd.DataFrame(), acceptNoFile=False,header_=None,names_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,nrows_=None,encoding_=ENCODING,engine_='python',sep_=None, wait=False):
    try:
        t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        names=names_,usecols=usecols_,nrows=nrows_,encoding=encoding_,engine=engine_,sep=sep_)
        #print(t)
        return t
    except (OSError, FileNotFoundError):
        if acceptNoFile:
            return default
        else:
            if wait == True:
                ERROR('Waiting for Download...', waiting=True)
            else:
                ERROR('找不到檔案：'+dir)
    except HTTPError as err:
        if acceptNoFile:
            return default
        else:
            ERROR(str(err))
    except ParserError:
        try:
            t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                            names=names_,usecols=usecols_,nrows=nrows_,encoding=encoding_,engine=engine_)
            return t
        except:
            return default
    except Exception as e:
        #print(str(e))
        try: #檔案編碼格式不同
            t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        names=names_,usecols=usecols_,nrows=nrows_,engine=engine_,sep=sep_)
            #print(t)
            return t
        except Exception as error:
            #print(traceback.format_exc())
            #return default
            raise error

def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, na_filter_=True, squeeze_=False, \
             header_=None,names_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,nrows_=None,sheet_name_=None,engine_=None, wait=False):
    try:
        if engine_ == 'pyxlsb':
            t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,names=names_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_,nrows=nrows_,na_filter=na_filter_,squeeze=squeeze_,engine=engine_)
        else:
            t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,names=names_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_,nrows=nrows_,na_filter=na_filter_,squeeze=squeeze_)
        #print(t)
        return t
    except (OSError, FileNotFoundError):
        if acceptNoFile:
            return default
        else:
            if wait == True:
                ERROR('Waiting for Download...', waiting=True)
            else:
                ERROR('找不到檔案：'+dir)
    except Exception as e:
        #print(str(e))
        try: #檔案編碼格式不同
            t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,names=names_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_,nrows=nrows_,na_filter=na_filter_,squeeze=squeeze_)
            #print(t)
            return t
        except Exception as error:
            #print(traceback.format_exc())
            #return default
            raise error

def Reading_Excel(file_path, tables, header, index_col, skiprows, usecols=None, names=None, specific_sheet=False, sheet_name_t=None, nrows=None, wait=True):
    if str(file_path)[-4:] == '.xls':
        eng = 'pyxlsb'
    else:
        eng = 'openpyxl'
    try:
        if tables != None and tables[0] == 0:
            INTLINE_t = readExcelFile(file_path, header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=0, acceptNoFile=False, usecols_=usecols, names_=names, nrows_=nrows, engine_=eng, wait=wait)
        elif specific_sheet == True:
            INTLINE_t = readExcelFile(file_path, header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=sheet_name_t, acceptNoFile=False, usecols_=usecols, names_=names, nrows_=nrows, engine_=eng, wait=wait)
        else:
            INTLINE_t = readExcelFile(file_path, header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=None, acceptNoFile=False, usecols_=usecols, names_=names, nrows_=nrows, engine_=eng, wait=wait)
    except SystemExit as e:
        #ERROR(str(e))
        raise SystemExit
    except Exception as e:
        #print(e)
        INTLINE_t = pd.DataFrame()
    if type(INTLINE_t) != dict and INTLINE_t.empty == True:
        h = None
        i = index_col
        while True:
            try:
                INTLINE_t = readExcelFile(file_path, header_=h, index_col_=i, skiprows_=skiprows, sheet_name_=None, acceptNoFile=False, usecols_=usecols, names_=names, nrows_=nrows, engine_=eng, wait=wait)
                for t in tables:
                    sheet = t
                    if file_path.find('ANFIA/') >= 0:
                        yr = re.sub(r'.*?([0-9]{4}).*', r"\1", sheet)
                        sheet_this_year = sheet.replace(yr, str(datetime.today().year))
                        if sheet_this_year not in INTLINE_t.keys():
                            sheet = sheet.replace(yr, str(int(yr)-1))
                    if t == 0:
                        sheet = list(INTLINE_t.keys())[0]
                    if h == None and header != None:
                        INTLINE_t[sheet].columns = pd.MultiIndex.from_frame(INTLINE_t[sheet].iloc[header].T)
                    if i == None and index_col != None:
                        INTLINE_t[sheet].index = pd.MultiIndex.from_frame(INTLINE_t[sheet].T.iloc[index_col].T)
            except:
                if h == None and i == None:
                    print(file_path)
                    print('The header and index of the dataframe are not correct.')
                    raise ParserError
                elif h == None:
                    h = header
                    i = None
                elif i == None:
                    h = None
            else:
                break
        if tables != None and tables[0] == 0:
            INTLINE_t = INTLINE_t[list(INTLINE_t.keys())[0]]
        elif specific_sheet == True:
            INTLINE_t = INTLINE_t[sheet_name_t]
    
    return INTLINE_t

def INTLINE_PRESENT(file_path, check_latest_update=False, latest_update=None, forcing_download=False, freq='A', discontinued=False):
    if os.path.isfile(file_path) and (datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%V') == datetime.today().strftime('%Y-%V') or datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%V') == (datetime.today()-timedelta(days=7)).strftime('%Y-%V')):
        if check_latest_update == True:
            if str(latest_update).find('discontinued') >= 0:
                return True
            try:
                datetime.strptime(str(latest_update),'%Y-%m-%d')
            except ValueError:
                try:
                    latest_update = datetime.strptime(str(latest_update),'%Y/%m/%d').strftime('%Y-%m-%d')
                except ValueError:
                    return False
        if check_latest_update == True and datetime.strptime(str(latest_update),'%Y-%m-%d').strftime('%Y-%V') != datetime.today().strftime('%Y-%V') and datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%V') != (datetime.today()-timedelta(days=7)).strftime('%Y-%V'):
            return False
        elif forcing_download == True:
            return False
        elif freq in ['W','D'] and datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%V') != datetime.today().strftime('%Y-%V'):
            return False
        else:
            if check_latest_update == True:
                logging.info('Latest Base Year Received.\n')
            else:
                logging.info('Present File Exists. Reading Data From Default Path.\n')
            return True
    elif file_path.find('discontinued') >= 0 or discontinued == True:
        return True
    else:
        return False

def GET_NAME(address, freq, country, code, check_exist=False, DF_KEY=None):
    suffix = '.'+freq
    if address.find('ln/') >= 0:
        name = freq+str(country)+code.replace('-','').strip()[:-5]+suffix
    elif address.find('pr/') >= 0:
        name = freq+str(country)+code.replace('-','').strip()[:5]+code.replace('-','').strip()[7:]+suffix
    elif address.find('STL') >= 0:
        name = freq+str(country)+code.replace('-','').replace('GBR','').strip()[:10]+suffix
    else:
        name = freq+str(country)+code.replace('-','').strip()+suffix
    
    if check_exist == True:
        if name in DF_KEY.index:
            return True
        else:
            return False
    else:
        return name

def NEW_LABEL(key, label, Series, Table, cat_idx=None, item=None):
    
    for l in range(label.shape[0]):
        label.loc[label.index[l]] = Series['CATEGORIES'].loc[Table[cat_idx+'_code'][label.index[l]], cat_idx+'_'+item].title().replace('And','and').replace("'S","'s").replace(', ',',')
    
    return label

def NEW_INDEX(INTLINE_previous, key_list, Series, freq, fname, DataSet='DataSet', label='keyword'):
    
    new_index = []
    for dex in INTLINE_previous.index:
        key_found = False
        for key in key_list:
            for item in re.split(r'//', str(key)):
                if str(dex).replace(' ','').find(item) >= 0:
                    new_index.append(Series[freq].loc[Series[freq][DataSet] == str(fname)].loc[Series[freq][label] == key]['keyword'].item())
                    key_found = True
                    break
            if key_found == True:
                break
        if key_found == False:
            new_index.append(None)
    INTLINE_previous.index = new_index
    INTLINE_previous = INTLINE_previous.loc[INTLINE_previous.index.dropna()]
    
    return INTLINE_previous

def INTLINE_DATASETS(chrome, data_path, country, address, fname, sname, freq, Series, Table, dealing_start_year, previous_data=False, specific_time_unit=False, interval=None, Zip_table=None):
    encode = ENCODING
    webnote = []
    INTLINE_previous = pd.DataFrame()
    time_units = None
    if str(fname).find('http') >= 0:
        Table = Table.reset_index().set_index('website')
    else:
        Table = Table.reset_index().set_index('File or Sheet')
    if country == 534:
        tables = [int(t) if str(t).isnumeric() else t for t in re.split(r'//', str(Table.loc[fname, 'Tables']))]
    elif address.find('ANFIA') >= 0:
        if previous_data == True:
            tables = [str(t)+' '+str(datetime.today().year-1) for t in re.split(r', ', str(Table.loc[fname, 'Tables']))]
            Table.loc[fname, 'previous_data'] = np.nan
        else:
            tables = [str(t)+' '+str(datetime.today().year) for t in re.split(r', ', str(Table.loc[fname, 'Tables']))]
    else:
        tables = [int(str(t).replace('.0','')) if str(t).replace('.0','').isnumeric() else t for t in re.split(r', ', str(Table.loc[fname, 'Tables']))]
    skip = None
    if str(Table.loc[fname, 'skip']) != 'nan':
        skip = list(range(int(Table.loc[fname, 'skip'])))
    head = None
    if str(Table.loc[fname, 'head']) != 'nan' and str(Table.loc[fname, 'head']).find(',') < 0:
        head = list(range(int(Table.loc[fname, 'head'])))
    elif str(Table.loc[fname, 'head']) != 'nan' and str(Table.loc[fname, 'head']).find(',') >= 0:
        head = [int(h) for h in re.split(r', ', str(Table.loc[fname, 'head']))]
    index_col = 0
    if str(Table.loc[fname, 'index_col']) == 'None':
        index_col = None
    elif str(Table.loc[fname, 'index_col']).replace('.0','') == '1':
        index_col = 1
    elif str(Table.loc[fname, 'index_col']) != 'nan' and str(Table.loc[fname, 'index_col']).find(',') < 0:
        index_col = list(range(int(Table.loc[fname, 'index_col'])))
    elif str(Table.loc[fname, 'index_col']) != 'nan' and str(Table.loc[fname, 'index_col']).find(',') >= 0:
        index_col = [int(h) for h in re.split(r', ', str(Table.loc[fname, 'index_col']))]
    trans = Table.loc[fname, 'transpose']
    try:
        csv = Table.loc[fname, 'csv']
    except KeyError:
        csv = False
    try:
        excel = str(Table.loc[fname, 'excel'])
        if excel not in ['x','m']:
            excel = ''
    except KeyError:
        excel = 'x'
    try:
        output = Table.loc[fname, 'output']
    except KeyError:
        output = False
    try:
        Zip = Table.loc[fname, 'zip']
    except KeyError:
        Zip = False
    try:
        nrows = None
        if str(Table.loc[fname, 'nrows']) != 'nan':
            nrows = Table.loc[fname, 'nrows']
    except KeyError:
        nrows = None
    try:
        usecols = None
        if str(Table.loc[fname, 'usecols']) != 'nan':
            usecols = list(range(int(Table.loc[fname, 'usecols'])))
    except KeyError:
        usecols = None
    try:
        Name = None
        if str(Table.loc[fname, 'Name']) != 'nan':
            Name = Table.loc[fname, 'Name']
    except KeyError:
        Name = None
    try:
        if str(Table.loc[fname, 'previous_data']) != 'nan':
            if str(fname).find('http') >= 0 and address.find('HKMA') < 0 and address.find('DOS') < 0:
                previous_fname = str(Table.loc[fname, 'previous_website'])
                previous_sname = str(Table.loc[fname, 'previous_data'])
            else:
                previous_fname = str(Table.loc[fname, 'previous_data'])
                if address.find('ITIA') >= 0 or address.find('SERIE') >= 0:
                    previous_sname = str(Table.loc[fname, 'previous_sheet'])
                elif address.find('HKMA') >= 0 or address.find('DOS') >= 0:
                    previous_sname = int(Table.loc[fname, 'previous_sheet'])
                else:
                    previous_sname = sname
    except KeyError:
        time.sleep(0)
    else:
        if str(Table.loc[fname, 'previous_data']) != 'nan':
            INTLINE_previous, INTLINE_previous2 = INTLINE_DATASETS(chrome, data_path, country, address, previous_fname, previous_sname, freq, Series, Table, dealing_start_year, previous_data=True, Zip_table=Zip_table)
            if address.find('NBS') >= 0 and INTLINE_previous2.empty == False:
                key_list = list(Series[freq].loc[Series[freq]['Previous_DataSet'] == str(Table.loc[fname, 'previous_data'])]['Previous_keyword'])
                INTLINE_previous2 = NEW_INDEX(INTLINE_previous2, key_list, Series, freq, Table.loc[fname, 'previous_data'], DataSet='Previous_DataSet', label='Previous_keyword')
                INTLINE_previous2 = INTLINE_previous2.dropna(axis=1, how='all')
                INTLINE_previous = NEW_INDEX(INTLINE_previous, key_list, Series, freq, Table.loc[fname, 'previous_data'], DataSet='Previous_DataSet', label='Previous_keyword')
                INTLINE_previous = INTLINE_previous.dropna(axis=1, how='all')
                if len(INTLINE_previous.index) != len(INTLINE_previous2.index):
                    print(INTLINE_previous.index)
                    print(INTLINE_previous2.index)
                    ERROR('The Indices of Two DataFrames are not identical.')
                INTLINE_previous = pd.concat([INTLINE_previous2, INTLINE_previous], axis=1)
                INTLINE_previous = INTLINE_previous.sort_index(axis=0)
    CDID = None
    if address.find('ONS') >= 0:
        CDID = Table.loc[fname, 'CDID']
    elif address.find('COJ') >= 0 or address.find('MHLW') >= 0 or address.find('MCPI') >= 0:
        with open(data_path+str(country)+'/'+'encode.txt','r',encoding='ANSI') as f:
            encode = f.read()
    if str(fname).find('http') >= 0:
        file_name = sname
        sheet_name = tables[0]
    else:
        file_name = fname
        sheet_name = sname
    if Zip == True:
        try:
            zip_file_name = Zip_table.loc[file_name, 'Zipname']
        except KeyError:
            ERROR('Name for Zipfile Not Found: '+str(file_name))
        file_path = data_path+str(country)+'/'+address+zip_file_name+'.zip'
    elif csv == True:
        file_path = data_path+str(country)+'/'+address+str(file_name)+'.csv'
    else:
        file_path = data_path+str(country)+'/'+address+str(file_name)+'.xls'+excel
        if (address.find('ISTAT') >= 0 and output == True) or (INTLINE_PRESENT(file_path) == False and INTLINE_PRESENT(file_path.replace('.xls'+excel,'.xlsx')) == True):
            file_path = file_path.replace('.xls'+excel,'.xlsx')
            excel = 'x'
    present_file_existed = INTLINE_PRESENT(file_path)
    if present_file_existed == False and address.find('DEUSTATIS') >= 0:
        if INTLINE_PRESENT(re.sub(r'\.[csvxlzip]+', "", file_path)+'_Notes.csv'):
            logging.info('Getting Data from Different Year Ranges.\n')
            time_units = pd.read_csv(re.sub(r'\.[csvxlzip]+', "", file_path)+'_Notes.csv', header=None, squeeze=True).tolist()
            if False not in [INTLINE_PRESENT(data_path+str(country)+'/'+address+str(file_name)+' - '+str(yr)+'.xls'+excel) for yr in time_units]:
                present_file_existed = True
    if str(fname).find('http') >= 0 and present_file_existed == False:
        if tables == ['None']:
            INTLINE_temp = INTLINE_WEB_TRADE(chrome, country, address, fname, sname, freq=freq, header=head, index_col=index_col, skiprows=skip, start_year=dealing_start_year)
        else:
            INTLINE_temp, webnote = INTLINE_WEB(chrome, country, address, fname, sname, freq=freq, tables=tables, header=head, index_col=index_col, skiprows=skip, usecols=usecols, nrows=nrows, csv=csv, encode=encode, renote=True, Series=Series, Table=Table, start_year=dealing_start_year, previous=previous_data, output=output, Zip=Zip, file_name=Name, specific_time_unit=specific_time_unit, interval=interval)
            if address.find('DEUSTATIS') >= 0 and not not webnote:
                file_name = file_name+' - '+str(webnote[-1])
    elif Zip == True and present_file_existed == False:
        zipname = INTLINE_WEB(chrome, country, address, Zip_table.loc[file_name, 'website'], zip_file_name, freq=freq, Zip=True, file_name=Name)
        zf = zipfile.ZipFile(file_path,'r')
        if Name != None and Name in zf.namelist():
            zip_fname = Name
        else:
            zip_fname = zf.namelist()[0]
        if csv == True:
            INTLINE_temp = readFile(zf.open(zip_fname), header_=head, index_col_=index_col, skiprows_=skip, acceptNoFile=False, encoding_=encode, nrows_=nrows)
        else:
            INTLINE_temp = Reading_Excel(zf.open(zip_fname), tables, head, index_col, skip, specific_sheet=True, sheet_name_t=sheet_name, nrows=nrows, wait=False)
    else:
        if str(fname).find('http') >= 0:
            specific_sheet = False
            sheet_name_t = None
            if index_col == 1 and country == 924:
                index_col = 0
        else:
            specific_sheet = True
            sheet_name_t = sname
        if Zip == True:
            zf = zipfile.ZipFile(data_path+str(country)+'/'+address+zip_file_name+'.zip','r')
            if Name != None and Name in zf.namelist():
                zip_fname = Name
            else:
                zip_fname = zf.namelist()[0]
        if csv == True:
            if Zip == True:
                file_path = zf.open(zip_fname)
            else:
                file_path = data_path+str(country)+'/'+address+str(file_name)+'.csv'
            INTLINE_temp = readFile(file_path, header_=head, index_col_=index_col, skiprows_=skip, acceptNoFile=False, encoding_=encode, nrows_=nrows)
        else:
            wait = False
            if Zip == True:
                file_path = zf.open(zip_fname)
            else:
                file_path = data_path+str(country)+'/'+address+str(file_name)+'.xls'+excel
            if time_units != None:
                INTLINE_temp = {}
                for yr in time_units:
                    time_file_path = data_path+str(country)+'/'+address+str(file_name)+' - '+str(yr)+'.xls'+excel
                    IN_temp = Reading_Excel(time_file_path, tables, head, index_col, skip, specific_sheet=specific_sheet, sheet_name_t=sheet_name_t, nrows=nrows, wait=wait)
                    INTLINE_temp[yr] = IN_temp
                file_name = file_name+' - '+str(time_units[-1])
            else:
                if address.find('ISTAT') >= 0 and output == True:
                    skip = None
                INTLINE_temp = Reading_Excel(file_path, tables, head, index_col, skip, usecols=usecols, specific_sheet=specific_sheet, sheet_name_t=sheet_name_t, nrows=nrows, wait=wait)
            #INTLINE_temp = readExcelFile(file_path, header_=head, index_col_=index_col, skiprows_=skip, sheet_name_=sheet_name_t, acceptNoFile=False)
        file_path = str(file_path)
        webnote = readFile(re.sub(r'\.[csvxlzip]+', "", file_path)+'_Notes.csv', acceptNoFile=True).values.tolist()
    if previous_data == True:
        if trans == True and type(INTLINE_temp) == dict:
            for t in INTLINE_temp:
                INTLINE_temp[t] = INTLINE_temp[t].T
        elif trans == True and type(INTLINE_temp) != dict:
            INTLINE_temp = INTLINE_temp.T
        return INTLINE_temp, INTLINE_previous
    elif specific_time_unit == True:
        return INTLINE_temp
    else:
        return INTLINE_temp, csv, encode, webnote, Table, tables, skip, head, index_col, trans, excel, CDID, file_name, sheet_name, INTLINE_previous, Name

def INTLINE_WEBDRIVER(chrome, country, address, sname, tables=None, header=None, index_col=None, skiprows=None, usecols=None, names=None, nrows=None, csv=True, Zip=False, US_address=None, encode=ENCODING, specific_sheet=False):
    
    destination = data_path+str(country)+'/'+address
    if US_address != None:
        destination = US_address
    chrome.execute_script("window.open()")
    chrome.switch_to.window(chrome.window_handles[-1])
    chrome.get('chrome://downloads')
    time.sleep(1)
    try:
        if chrome.execute_script("return document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('div#content #tag')").text == '已刪除':
            ERROR('The file was not properly downloaded')
    except JavascriptException:
        ERROR('The file was not properly downloaded')
    excel_file = chrome.execute_script("return document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('div#content  #file-link').text")
    if address.find('BPS') >= 0 or address.find('ISTAT') >= 0:
        path = (Path.home() / "Downloads" / excel_file).as_posix()
        timeStart = time.time()
        while os.path.isfile(path) == False:
            time.sleep(0)
            if int(time.time() - timeStart) > 60:
                ERROR('File Download Error')
        try:
            xl = win32.gencache.EnsureDispatch('Excel.Application')
        except:
            xl = win32.DispatchEx('Excel.Application')
        xl.DisplayAlerts=False
        xl.Visible = 0
        ExcelFile = xl.Workbooks.Open(path)
        ExcelFile.SaveCopyAs(path.replace('.xls','.xlsx'))
        ExcelFile.Close()
        os.remove(path)
        os.rename(path.replace('.xls','.xlsx'), path)
    new_file_name = str(sname)+re.sub(r'.+?(\.[csvxlszipm]+)$', r"\1", excel_file)
    if new_file_name.find(':') >= 0:
        ERROR('File name should not contain ":" for file "'+str(new_file_name)+'"')
    chrome.close()
    chrome.switch_to.window(chrome.window_handles[0])
    INTLINE_t = pd.DataFrame()
    while True:
        try:
            file_path = (Path.home() / "Downloads" / excel_file).as_posix()
            if Zip == True:
                INTLINE_zip = new_file_name
                if os.path.isfile((Path.home() / "Downloads" / excel_file)) == False:
                    sys.stdout.write("\rWaiting for Download...")
                    sys.stdout.flush()
                    raise SystemExit
            else:
                if csv == True:
                    INTLINE_t = readFile(file_path, header_=header, index_col_=index_col, skiprows_=skiprows, acceptNoFile=False, usecols_=usecols, names_=names, nrows_=nrows, wait=True, encoding_=encode)
                else:
                    INTLINE_t = Reading_Excel(file_path, tables, header, index_col, skiprows, usecols=usecols, names=names, specific_sheet=specific_sheet, sheet_name_t=tables[0], nrows=nrows)
            if type(INTLINE_t) != dict and INTLINE_t.empty == True and Zip == False:
                break
        except SystemExit as e:
            #ERROR(str(e))
            time.sleep(1)
        except Exception as err:
            print(file_path)
            ERROR(str(err))
        else:
            if address.find('GACC/CAT') < 0:
                sys.stdout.write('\nDownload Complete\n\n')
            if os.path.isfile((Path.home() / "Downloads" / new_file_name)) and excel_file != new_file_name:
                os.remove((Path.home() / "Downloads" / new_file_name))
            os.rename((Path.home() / "Downloads" / excel_file), (Path.home() / "Downloads" / new_file_name))
            if os.path.isfile(destination+new_file_name):
                if datetime.fromtimestamp(os.path.getmtime(destination+new_file_name)).strftime('%Y-%m') ==\
                     datetime.fromtimestamp(os.path.getmtime((Path.home() / "Downloads" / new_file_name))).strftime('%Y-%m'):
                    os.remove(destination+new_file_name)
                else:
                    if os.path.isfile(destination+'old/'+new_file_name):
                        os.remove(destination+'old/'+new_file_name)
                    shutil.move(destination+new_file_name, destination+'old/'+new_file_name)
            shutil.move((Path.home() / "Downloads" / new_file_name), destination+new_file_name)
            if new_file_name.find('Standard_Presentation_of_BoP_in_India_as_per_BPM6') >= 0:
                INTLINE_COPY_FILE(destination, new_file_name.replace('Net','Debit').replace('Credit','Debit'), new_file_name)
                INTLINE_COPY_FILE(destination, new_file_name.replace('Net','Credit').replace('Debit','Credit'), new_file_name)
                INTLINE_COPY_FILE(destination, new_file_name.replace('Credit','Net').replace('Debit','Net'), new_file_name)
            elif new_file_name.find('IndonesianGDP') >= 0:
                INTLINE_COPY_FILE(destination, new_file_name.replace('IndonesianGDPExp','IndonesianGDP').replace('IndonesianGDP','IndonesianGDPExp'), new_file_name)
                INTLINE_COPY_FILE(destination, new_file_name.replace('IndonesianGDPExp','IndonesianGDP'), new_file_name)
            break
    if type(INTLINE_t) != dict and INTLINE_t.empty == True and Zip == False:
        ERROR('Empty DataFrame')
    
    if Zip == True:
        return INTLINE_zip
    else:
        return INTLINE_t

def INTLINE_COPY_FILE(destination, new_file_name, old_file_name):
    if new_file_name != old_file_name:
        if os.path.isfile(destination+new_file_name):
            if datetime.fromtimestamp(os.path.getmtime(destination+new_file_name)).strftime('%Y-%m') ==\
                    datetime.fromtimestamp(os.path.getmtime(destination+old_file_name)).strftime('%Y-%m'):
                os.remove(destination+new_file_name)
            else:
                if os.path.isfile(destination+'old/'+new_file_name):
                    os.remove(destination+'old/'+new_file_name)
                shutil.move(destination+new_file_name, destination+'old/'+new_file_name)
        shutil.copyfile(destination+old_file_name, destination+new_file_name)

def INTLINE_BASE_YEAR(INTLINE_temp, chrome, data_path, country, address, file_name, freq, Series, csv, encode, sheet_name, excel, repl, Name, website=None):
    base_year = 0
    src = file_name
    is_period = False
    base_year_list = pd.DataFrame()
    if address.find('COJ') >= 0:
        if freq != 'M' and (str(file_name).find('def') >= 0 or str(file_name).find('dn') >= 0):
            if csv == True:
                base_year = re.sub(r'.+?([0-9]{4}).+', r"\1", str(readFile(data_path+str(country)+'/'+address+file_name+'.csv', acceptNoFile=False, encoding_=encode).iloc[0]).replace('\n',''))
            else:
                base_year = re.sub(r'.+?([0-9]{4}).+', r"\1", str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).iloc[1]).replace('\n',''))
    elif address.find('COMM') >= 0 and file_name.find('Index') >= 0:
        base_year = readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=0, acceptNoFile=False).iloc[0].iloc[0][:4]
    elif address.find('METI') >= 0:
        base_year = re.sub(r'.*?([0-9]{4}).*', r"\1", str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).iloc[0].iloc[0]))
        if address.find('WTRS') >= 0:
            row = 1
        else:
            row = 0
        repl = re.sub(r'\s+', " ", re.sub(r'\s*:\s*', ", ", re.sub(r"\(.+?\)|'s", "", str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).iloc[row].iloc[0])))).strip()
    elif address.find('WKHH') >= 0:
        base_year = re.sub(r'.+?([0-9]{4}).+', r"\1", str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).iloc[3]).replace('\n',''))
    elif address.find('MHLW') >= 0:
        if file_name.find('sisu') >= 0:
            base_found = False
            this_year = datetime.today().year
            for yr in list(reversed(range(1952, this_year))):
                IN_temp = INTLINE_temp.loc[(INTLINE_temp['種別'] == '指数') & (INTLINE_temp['月'] == 'CY') & (INTLINE_temp['年'] == yr)]
                if False not in [ind == 100 for ind in list(IN_temp['現金給与総額'])]:
                    base_found = True
                    base_year = str(yr)
                    break
            if base_found == False:
                ERROR('Base Year Not Found in file: '+str(file_name))
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp['月'] != 'CY']
    elif address.find('BOJ') >= 0 and freq == 'M':
        for ind in range(Series[freq].shape[0]):
            if Series[freq].iloc[ind]['DataSet'] == str(file_name) and bool(re.match(r'PR', str(Series[freq].iloc[ind]['keyword']))):
                key = str(Series[freq].iloc[ind]['keyword'])
                if key.find('PRCG') >= 0:
                    url = 'https://www.boj.or.jp/en/statistics/pi/cgpi_release/index.htm/'
                elif key.find('PRCS') >= 0:
                    url = 'https://www.boj.or.jp/en/statistics/pi/cspi_release/index.htm/'
                response = rq.get(url)
                search = BeautifulSoup(response.text, "html.parser")
                try:
                    result = search.find_all("ul", class_="page-link")[0]
                    idb = re.sub(r'.*?([0-9]{4}).+', r"\1", result.text.replace('\n',''), 1)[-2:]
                    Series[freq].loc[Series[freq].index[ind], 'keyword'] = key.replace('15', idb)
                except IndexError:
                    ERROR('Index Base Not Found for item: '+str(key))
    elif address.find('JPC') >= 0:
        file_path = data_path+str(country)+'/'+address+'base_year.csv'
        base_year_list = readFile(file_path, header_=[0], index_col_=0, acceptNoFile=False)
        try:
            latest = INTLINE_PRESENT(file_path, check_latest_update=True, latest_update=base_year_list.loc[file_name, 'last updated'])
        except KeyError:
            ERROR('File Name Not Found in base_year.csv: '+file_name)
        if latest == True:
            base_year = str(base_year_list.loc[file_name, 'base year'])
        else:
            chrome.get(website)
            base_year = re.sub(r'.*?([0-9]{4}).*', r"\1", chrome.find_element_by_xpath('.//li[contains(., "基準時")]').text.replace('\n',''))
    elif address.find('MCPI') >= 0:
        url = 'https://www.stat.go.jp/english/data/cpi/1588.html#his'
        repl = 'Consumer Price Index, CPI:'
        src = url
        response = rq.get(url)
        search = BeautifulSoup(response.text, "html.parser")
        try:
            result = search.find_all("h3", string=re.compile("[0-9]{4}\-Base"))[0]
            base_year = result.text.replace('\n','').strip()[:4]
        except IndexError:
            ERROR('Index Base Not Found for item: '+str(key))
    elif address.find('HKCSD') >= 0 and str(file_name).find('Index') >= 0:
        file_path = data_path+str(country)+'/'+address+'base_year.csv'
        base_year_list = readFile(file_path, header_=[0], index_col_=0, acceptNoFile=False)
        try:
            latest = INTLINE_PRESENT(file_path, check_latest_update=True, latest_update=base_year_list.loc[file_name, 'last updated'])
        except KeyError:
            ERROR('File Name Not Found in base_year.csv: '+file_name)
        if str(file_name).find('Retail Sales') >= 0:
            is_period = True
        if latest == True:
            base_year = str(base_year_list.loc[file_name, 'base year'])
        else:
            chrome.get(website)
            time.sleep(3)
            if str(file_name).find('Merchandise Trade') >= 0:
                base_year = re.sub(r'.*?([0-9]{4}).+', r"\1", chrome.find_element_by_id('w_content').text)
            elif str(file_name).find('Retail Sales') >= 0:
                base_temp = re.sub(r'.*?\(.*?from(.+?)=\s*100\s*\).*', r"\1", str(INTLINE_temp.columns[1][0])).strip().replace('to','')
                base_mth = re.split(r'[^A-Za-z]+', base_temp)[:2]
                base_yr = re.split(r'[^0-9]+', base_temp)[-2:]
                base_year = base_yr[0]+'.'+datetime.strptime(base_mth[0],'%b').strftime('%m')+'-'+base_yr[1]+'.'+datetime.strptime(base_mth[1],'%b').strftime('%m')
            else:
                base_year = re.sub(r'.*?([0-9]{4}).+', r"\1", str(INTLINE_temp.columns[0][1]))
    elif address.find('HKCPI') >= 0:
        is_period = True
        base_temp = re.sub(r'.*?\((.+?)=\s*100\s*\).*', r"\1", str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).iloc[1].iloc[0])).replace(' ','')
        base_mth = re.split(r'[^A-Za-z]+', base_temp)[:2]
        base_yr = re.split(r'[^0-9]+', base_temp)[-2:]
        base_year = base_yr[0]+'.'+datetime.strptime(base_mth[0],'%B').strftime('%m')+'-'+base_yr[1]+'.'+datetime.strptime(base_mth[1],'%B').strftime('%m')
    elif address.find('DOS') >= 0:
        file_path = data_path+str(country)+'/'+address+'base_year.csv'
        base_year_list = readFile(file_path, header_=[0], index_col_=0, acceptNoFile=False)
        try:
            latest = INTLINE_PRESENT(file_path, check_latest_update=True, latest_update=base_year_list.loc[file_name, 'last updated'])
        except KeyError:
            return base_year, INTLINE_temp, Series, repl, is_period
        if file_name == 'M212261':
            is_period = True
        if latest == True:
            base_year = str(base_year_list.loc[file_name, 'base year'])
        else:
            chrome.get(website)
            time.sleep(3)
            if bool(re.search(r'=\s*100', chrome.find_element_by_xpath('.//td[a[@class="metadata"]]').text)):
                if file_name == 'M212261':
                    base_temp = re.sub(r'.+?\((.+?)\s*=\s*100.*', r"\1", chrome.find_element_by_xpath('.//td[a[@class="metadata"]]').text)
                    base_year = base_temp[-4:]+'-Q'+base_temp[0]
                else:
                    base_year = re.sub(r'.+?([0-9]{4})\s*=\s*100.*', r"\1", chrome.find_element_by_xpath('.//td[a[@class="metadata"]]').text)
            elif bool(re.search(r'Base Year', chrome.find_element_by_xpath('.//td[a[@class="metadata"]]').text)):
                base_year = re.sub(r'.+?([0-9]{4})\s*As Base Year.*', r"\1", chrome.find_element_by_xpath('.//td[a[@class="metadata"]]').text)
    elif address.find('KOSTAT') >= 0 and freq == 'M':
        Meta_data = readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_='Meta Data', index_col_=0, acceptNoFile=False).squeeze()
        for dex in Meta_data.index:
            if str(dex).find('Unit') >= 0:
                base_year = re.sub(r'.*?([0-9]{4}).*', r"\1", str(Meta_data.loc[dex]))
                break
    elif address.find('RBA') >= 0 and freq == 'M':
        is_period = True
        unit = str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, index_col_=0, acceptNoFile=False).loc['Units'].iloc[0])
        if unit.find('Index') >= 0:
            base_year = re.sub(r'.+?,(.+?)=100.*', r"\1", unit).replace('/','-').strip()
        elif str(file_name).find('f11') >= 0:
            unit = str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, index_col_=0, acceptNoFile=False).loc['Title'])
            base_year = re.sub(r'.*?([0-9]{4})\s*=\s*100.*', r"\1", unit.replace('\n','')).replace('/','-').strip()
    elif address.find('ABS') >= 0 and (str(file_name).find('64') == 0 or str(file_name).find('6345') == 0):
        is_period = True
        try:
            xl = win32.gencache.EnsureDispatch('Excel.Application')
        except:
            xl = win32.DispatchEx('Excel.Application')
        xl.DisplayAlerts=False
        xl.Visible = 1
        ExcelFile = xl.Workbooks.Open(Filename=os.path.realpath(data_path+str(country)+'/'+address+str(file_name)+'.xls'+excel))
        for sh in range(1, ExcelFile.Sheets.Count+1):
            if ExcelFile.Worksheets(sh).Name == sheet_name:
                sheet_exist = True
                position = sh
                break
        if sheet_exist == True:
            Sheet = ExcelFile.Worksheets(position)
        else:
            ERROR('Excel Sheet Not Found: '+str(sheet_name))
        base_year = re.sub(r'.*?([0-9]{4}.*?)\s*=\s*100.*', r"\1", Sheet.Cells(1,2).Comment.Text()).replace('–','-').strip()
        ExcelFile.Close()
    elif address.find('SCB') >= 0 and freq == 'M' and file_name.find('Snabb') < 0:
        base_year = re.sub(r'.+?([0-9]{4}).+', r"\1", str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).iloc[0].iloc[0]))
        if str(base_year).isnumeric() == False:
            base_year = re.sub(r'.+?([0-9]{4}).+', r"\1", str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).iloc[3].iloc[1]))
    elif address.find('MOSPI/IIP') >= 0:
        is_period = True
        base_year = re.sub(r'.*?([0-9]{4}\-[0-9]{2}).*', r"\1", str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).iloc[1]).replace('\n',''))
    elif address.find('RBI') >= 0 and (str(file_name).find('CPI') >= 0 or str(file_name).find('WPI') >= 0):
        if str(file_name).find('Labourer') >= 0 or str(file_name).find('WPI') >= 0:
            is_period = True
            if sheet_name == '-1':
                Files = readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=None, acceptNoFile=False)
                target_file = Files[list(Files.keys())[-1]]
            else:
                target_file = readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False)
            base_year = re.sub(r'.*?([0-9]{4}\-[0-9]{2}).*', r"\1", str(target_file.iloc[:6]).replace('\n',''))
        else:
            Files = readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=None, acceptNoFile=False)
            target_file = Files[list(Files.keys())[-1]]
            base_year = re.sub(r'.*?([0-9]{4})\s*=\s*100.*', r"\1", str(target_file.iloc[:6]).replace('\n',''))
    elif address.find('CANSIMS') >= 0:
        if str(file_name).find('index') >= 0:
            is_period = True
            base_year = re.sub(r'.*?([0-9]{4})([0-9]{2}).*', r"\1-\2", str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).iloc[1:3].iloc[1]).replace('\n',''))
        elif str(file_name).find('Stock Exchange') >= 0:
            base_year = re.sub(r'.*?([0-9]{4})=.*', r"\1", str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).iloc[1:3].iloc[1]).replace('\n',''))
    elif address.find('DEUSTATIS') >= 0 or address.find('IFO') >= 0:
        if str(file_name).find('Volume index of stock of orders') >= 0 or str(file_name).find('Foreign trade - Value Index') >= 0:
            chrome.get(website)
            target = chrome.find_element_by_xpath('.//table[contains(., "=100")]')
            soup = BeautifulSoup(target.get_attribute('outerHTML'), "html.parser")
            base_year = re.sub(r'.*?([0-9]{4})=100.*', r"\1", soup.text.replace('\n',''))
        else:
            base_year = re.sub(r'.*?([0-9]{4})\s*=\s*100.*', r"\1", readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).T.iloc[:5].to_string().replace('\n',''))
            if str(base_year).isnumeric() == False:
                base_year = 0
    elif address.find('HWWI') >= 0:
        base_year = re.sub(r'.*?([0-9]{4})\s*=\s*100.*', r"\1", readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).iloc[:5].to_string().replace('\n',''))
    elif address.find('BPS') >= 0:
        base_year = re.sub(r'.*?([0-9]{4})\s*=\s*100.*', r"\1", readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False, engine_='pyxlsb').T.iloc[:5].to_string().replace('\n',''))
        if str(base_year).isnumeric() == False:
            base_year = 0
    elif address.find('GSO') >= 0:
        base_year = re.sub(r'.*?([0-9]{4})A.*', r"\1", readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, index_col_=0, acceptNoFile=False).loc['Index Reference Period'].iloc[0])
    elif address.find('INSEE') >= 0 and (str(file_name).find('index') >= 0 or str(website).find('index') >= 0 or str(website).find('indice') >= 0):
        file_path = data_path+str(country)+'/'+address+'base_year.csv'
        base_year_list = readFile(file_path, header_=[0], index_col_=0, acceptNoFile=False)
        try:
            latest = INTLINE_PRESENT(file_path, check_latest_update=True, latest_update=base_year_list.loc[file_name, 'last updated'])
        except KeyError:
            ERROR('File Name Not Found in base_year.csv: '+file_name)
        if str(file_name).find('Hourly wage rate indice for labourers') >= 0:
            is_period = True
        if latest == True:
            base_year = str(base_year_list.loc[file_name, 'base year'])
        else:
            chrome.get(website)
            time.sleep(3)
            if address.find('SERIE') >= 0:
                try:
                    WebDriverWait(chrome, 3).until(EC.visibility_of_element_located((By.XPATH, './/tr[@class="cliquable"][contains(., "'+str(Name)+'")][not(contains(., "Stopped series"))]'))).click()
                except TimeoutException:
                    ERROR('No correct present time series were found with the keyword: '+str(Name)+'. Please modify the website url with new keywords.')
                while True:
                    try:
                        WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/td[@class="echo echo-chevron"]')))
                    except TimeoutException:
                        time.sleep(0)
                    else:
                        break
                if str(sheet_name).isnumeric() == False:
                    title = WebDriverWait(chrome, 5).until(EC.visibility_of_element_located((By.XPATH, './/span[@class="sous-titre div-in-h"]'))).text
                    if re.sub(r'.*?([0-9]{4}).*', r"\1", title.replace('\n','')).isnumeric() == False:
                        title = WebDriverWait(chrome, 5).until(EC.visibility_of_element_located((By.XPATH, './/div[@class="echo-titre"]'))).text
                else:
                    WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/input[@title="Label or code"]'))).send_keys(str(file_name))
                    WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="rechercher"]'))).click()
                    time.sleep(5)
                    WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.XPATH, './/td[@class="echo echo-chevron"][contains(., "'+str(file_name)+'")][not(contains(., "Stopped series"))]'))).click()
                    title = WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.XPATH, './/h3[@class="titre-figure"]'))).text
                if is_period == True:
                    base_year = re.sub(r'.*?Base\s100\sin\s(Q[1-4])\sof\s([0-9]{4}).*', r"\2\1", title.replace('\n',''))
                else:
                    base_year = re.sub(r'.*?([0-9]{4}).*', r"\1", title.replace('\n',''))
            else:
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, website, keyword=str(Name), text_match=True)
                target = WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/div[@class="bloc fichiers"][contains(., "'+str(file_name)+'")]')))
                base_year = re.sub(r'.*?Base\s100\sin\s([0-9]{4}).*', r"\1", target.text.replace('\n',''))
    elif address.find('ISTAT') >= 0 and 'Index' in list(Series[freq].loc[Series[freq]['DataSet']==str(file_name)]['Unit']):
        file_path = data_path+str(country)+'/'+address+'base_year.csv'
        base_year_list = readFile(file_path, header_=[0], index_col_=0, acceptNoFile=False)
        try:
            latest = INTLINE_PRESENT(file_path, check_latest_update=True, latest_update=base_year_list.loc[file_name, 'last updated'])
            if str(file_name).find('discontinued') >= 0:
                latest = True
        except KeyError:
            ERROR('File Name Not Found in base_year.csv: '+file_name)
        if latest == True:
            base_year = re.sub(r'\.0$', "", str(base_year_list.loc[file_name, 'base year']))
            if str(file_name).find('collective labour agreement') >= 0 or str(file_name).find('Contract') >= 0:
                is_period = True
        else:
            count = 0
            while True:
                try:
                    chrome.set_page_load_timeout(20)
                    chrome.get(website)
                except TimeoutException:
                    chrome.execute_script("window.stop();")
                    try:
                        if str(file_name).find('chain linked') >= 0:
                            chrome.find_element_by_xpath('.//table[@class="DataTable"]/thead/tr[contains(., "Valuation")]')
                        elif str(file_name).find('collective labour agreement') >= 0 or str(file_name).find('Labour positions') >= 0 or (str(file_name).find('confidence Index') >= 0 and str(file_name).find('retail') < 0):
                            chrome.find_element_by_xpath('.//table[@class="DataTable"]/tbody//td[@class="RowDimLabel"]')
                        else:
                            chrome.find_element_by_xpath('.//table[@class="DataTable"]/thead/tr')
                    except NoSuchElementException:
                        count +=1
                        if count > 3:
                            ERROR('The website is unable to enter, please download the file manually: '+str(fname).replace('$',''))
                        else:
                            continue
                    else:
                        print('base year web page loaded')
                        break
                else:
                    break
            if str(file_name).find('chain linked') >= 0:
                target = chrome.find_element_by_xpath('.//table[@class="DataTable"]/thead/tr[contains(., "Valuation")]')
                base_year = re.sub(r'.*?reference\s*year\s*([0-9]{4}).*', r"\1", target.text.replace('\n',''))
            elif str(file_name).find('collective labour agreement') >= 0:
                is_period = True
                target = chrome.find_element_by_xpath('.//table[@class="DataTable"]/tbody//td[@class="RowDimLabel"]')
                chrome.execute_script("arguments[0].style.visibility = 'visible';", target)
                base_year = datetime.strptime(re.sub(r'.*?base\s*([a-z]+\s*[0-9]{4})=100.*', r"\1", target.text.replace('\n','')), '%B %Y').strftime('%Y.%m')
            else:
                if str(file_name).find('Labour positions') >= 0 or (str(file_name).find('confidence Index') >= 0 and str(file_name).find('retail') < 0):
                    target = chrome.find_element_by_xpath('.//table[@class="DataTable"]/tbody//td[@class="RowDimLabel"]')   
                else:
                    try:
                        target = chrome.find_element_by_xpath('.//table[@class="DataTable"]/thead/tr[contains(., "dicator")]')
                    except:
                        try:
                            target = chrome.find_element_by_xpath('.//table[@class="DataTable"]/thead/tr[contains(., "type")]')
                        except:
                            target = chrome.find_element_by_xpath('.//table[@class="DataTable"]/thead/tr[contains(., "Aggregate")]')
                if str(file_name).find('Contract') >= 0:
                    is_period = True
                    base_year = datetime.strptime(re.sub(r'.*?base\s*([a-z]+\s*[0-9]{4})=100.*', r"\1", target.text.replace('\n','')), '%B %Y').strftime('%Y.%m')
                else:
                    base_year = re.sub(r'.*?([0-9]{4})=100.*', r"\1", target.text.replace('\n',''))
    elif address.find('SIDRA') >= 0 and str(file_name).find('indice') >= 0:
        if str(file_name).find('Table') >= 0:
            data_text = str(readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).T.iloc[0].iloc[4])
        else:
            data_text = readExcelFile(data_path+str(country)+'/'+address+file_name+'.xls'+excel, sheet_name_=sheet_name, acceptNoFile=False).iloc[0].iloc[0]
        base_year = re.sub(r'.*?([0-9]{4})\s*=\s*100.*', r"\1", data_text)
    elif address.find('STANOR') >= 0 and (str(file_name).lower().find('price') >= 0 or str(file_name).lower().find('index') >= 0):
        file_path = data_path+str(country)+'/'+address+'base_year.csv'
        base_year_list = readFile(file_path, header_=[0], index_col_=0, acceptNoFile=False)
        try:
            latest = INTLINE_PRESENT(file_path, check_latest_update=True, latest_update=base_year_list.loc[file_name, 'last updated'])
        except KeyError:
            ERROR('File Name Not Found in base_year.csv: '+file_name)
        if latest == True:
            base_year = re.sub(r'\.0$', "", str(base_year_list.loc[file_name, 'base year']))
        else:
            chrome.get(website)
            WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/span[contains(., "Choose variables")]'))).click()
            target = chrome.find_element_by_xpath('.//div[@id="table-title"]')
            base_year = re.sub(r'.*?([0-9]{4})\s*=\s*100.*', r"\1", target.text.replace('\n',''))
    if (str(base_year).isnumeric() == False and is_period == False) or (str(base_year)[:4].isnumeric() == False and is_period == True):
        ERROR('Base Year Not Found in source: '+src)
    if base_year_list.empty == False:
        base_year_list.loc[file_name, 'base year'] = base_year
        base_year_list.loc[file_name, 'last updated'] = datetime.today().strftime('%Y-%m-%d')
        base_year_list.to_csv(file_path)
    
    return base_year, INTLINE_temp, Series, repl, is_period

def MERGE(merge_file, DB_TABLE, DB_CODE, freq):
    i = 0
    found = False
    while found == False:
        i += 1
        if DB_TABLE+freq+'_'+str(i).rjust(4,'0') not in list(merge_file['db_table']) and i > 1:
            found = True
            code_t = []
            for c in range(merge_file.shape[0]):
                if merge_file['db_table'][c] == DB_TABLE+freq+'_'+str(i-1).rjust(4,'0'):
                    code_t.append(merge_file['db_code'][c])
            if max(code_t) == DB_CODE+str(199).rjust(3,'0'):
                table_num = i
                code_num = 1
            else:
                table_num = i-1
                code_num = int(max(code_t).replace(DB_CODE, ''))+1
        elif DB_TABLE+freq+'_'+str(i).rjust(4,'0') not in list(merge_file['db_table']) and i == 1:
            found = True
            table_num = 1
            code_num = 1
    
    return table_num, code_num

def NEW_KEYS(f, freq, FREQLIST, DB_TABLE, DB_CODE, df_key, DATA_BASE, db_table_t, start_table, start_code, DATA_BASE_new, DB_name_new):
    
    if start_code >= 200:
        if freq == 'W':
            db_table_t = db_table_t.reindex(FREQLIST['W_s'])
        DATA_BASE_new[DB_TABLE+freq+'_'+str(start_table).rjust(4,'0')] = db_table_t
        DB_name_new.append(DB_TABLE+freq+'_'+str(start_table).rjust(4,'0'))
        start_table += 1
        start_code = 1
        db_table_t = pd.DataFrame(index = FREQLIST[freq], columns = [])
    db_table = DB_TABLE+freq+'_'+str(start_table).rjust(4,'0')
    db_code = DB_CODE+str(start_code).rjust(3,'0')
    #db_table_t[db_code] = DATA_BASE[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
    db_table_t = pd.concat([db_table_t, pd.DataFrame(list(DATA_BASE[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]), index=DATA_BASE[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']].index, columns=[db_code])], axis=1)
    df_key.loc[f, 'db_table'] = db_table
    df_key.loc[f, 'db_code'] = db_code
    start_code += 1
    db_table_new = db_table
    db_code_new = db_code
    
    return df_key, DATA_BASE_new, DB_name_new, db_table_t, start_table, start_code, db_table_new, db_code_new

def CONCATE(NAME, suf, data_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, KEY_DATA_t, DB_dict, DB_name_dict, find_unknown=True, DATA_BASE_t=None):
    if find_unknown == True:
        repeated_standard = 'start'
    else:
        repeated_standard = 'last'
    #logging.info('Reading file: '+NAME+'key'+suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
    #KEY_DATA_t = readExcelFile(data_path+NAME+'key'+suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
    if DATA_BASE_t == None:
        logging.info('Reading file: '+NAME+'database'+suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
        DATA_BASE_t = readExcelFile(data_path+NAME+'database'+suf+'.xlsx', header_ = 0, index_col_=0)
    if KEY_DATA_t.empty == False and type(DATA_BASE_t) != dict:
        ERROR(NAME+'database'+suf+'.xlsx Not Found.')
    elif type(DATA_BASE_t) != dict:
        DATA_BASE_t = {}
    
    logging.info('Concating file: '+NAME+'key'+suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
    KEY_DATA_t = pd.concat([KEY_DATA_t, df_key], ignore_index=True)
    
    logging.info('Concating file: '+NAME+'database'+suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
    for f in FREQNAME:
        for d in DB_name_dict[f]:
            sys.stdout.write("\rConcating sheet: "+str(d))
            sys.stdout.flush()
            if d in DATA_BASE_t.keys():
                DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_dict[f][d], how='outer')
                if f == 'D':
                    DATA_BASE_t[d] = DATA_BASE_t[d].sort_index(ascending=False)
            else:
                DATA_BASE_t[d] = DB_dict[f][d]
        sys.stdout.write("\n")

    logging.info('Time: '+str(int(time.time() - tStart))+' s'+'\n')
    KEY_DATA_t = KEY_DATA_t.sort_values(by=['freq', 'name', 'db_table', 'snl'], ignore_index=True)
    
    repeated = 0
    repeated_index = []
    Repeat = {}
    Repeat['key'] = []
    Repeat[repeated_standard] = []
    for i in range(1, len(KEY_DATA_t)):
        if i in Repeat['key']:
            continue
        Repeat['key'] = []
        Repeat[repeated_standard] = []
        if KEY_DATA_t.iloc[i]['name'] == KEY_DATA_t.iloc[i-1]['name']:
            j = i
            Repeat['key'].append(j-1)
            Repeat[repeated_standard].append(str(KEY_DATA_t.iloc[j-1][repeated_standard]))
            while KEY_DATA_t.iloc[j]['name'] == KEY_DATA_t.iloc[j-1]['name']:
                repeated += 1
                Repeat['key'].append(j)
                Repeat[repeated_standard].append(str(KEY_DATA_t.iloc[j][repeated_standard]))
                j += 1
                if j >= len(KEY_DATA_t):
                    break
            if repeated_standard == 'start':
                keep = Repeat['key'][Repeat[repeated_standard].index(min(Repeat[repeated_standard]))]
            elif repeated_standard == 'last':
                keep = Repeat['key'][Repeat[repeated_standard].index(max(Repeat[repeated_standard]))]
            for k in Repeat['key']:
                if k != keep and ((repeated_standard == 'start' and Repeat[repeated_standard][Repeat['key'].index(k)] == min(Repeat[repeated_standard])) or (repeated_standard == 'last' and Repeat[repeated_standard][Repeat['key'].index(k)] == max(Repeat[repeated_standard]))):
                    if k < keep or Repeat[repeated_standard][Repeat['key'].index(keep)] == 'Nan':
                        repeated_index.append(keep)
                        keep = k
                    else:
                        repeated_index.append(k)
                elif k != keep and ((repeated_standard == 'start' and Repeat[repeated_standard][Repeat['key'].index(k)] > min(Repeat[repeated_standard])) or (repeated_standard == 'last' and Repeat[repeated_standard][Repeat['key'].index(k)] < max(Repeat[repeated_standard]))):
                    if Repeat[repeated_standard][Repeat['key'].index(keep)] == 'Nan':
                        repeated_index.append(keep)
                        keep = k
                    else:
                        repeated_index.append(k)
            #print(KEY_DATA_t.iloc[keep]) 
        sys.stdout.write("\r"+str(repeated)+" repeated data key(s) found ("+str(round((i+1)*100/len(KEY_DATA_t), 1))+"%)*")
        sys.stdout.flush()
    sys.stdout.write("\n")
    #rp_idx = []
    for target in repeated_index:
        sys.stdout.write("\rDropping repeated database column(s)...("+str(round((repeated_index.index(target)+1)*100/len(repeated_index), 1))+"%)*")
        sys.stdout.flush()
        DATA_BASE_t[KEY_DATA_t.iloc[target]['db_table']] = DATA_BASE_t[KEY_DATA_t.iloc[target]['db_table']].drop(columns = KEY_DATA_t.iloc[target]['db_code'])
        #rp_idx.append([KEY_DATA_t.iloc[target]['name'], KEY_DATA_t.iloc[target]['form_c']])
    sys.stdout.write("\n")
    #logging.info('Dropping repeated database column(s)')
    #pd.DataFrame(rp_idx, columns = ['name', 'fname']).to_excel(data_path+"repeated.xlsx", sheet_name='repeated')
    KEY_DATA_t = KEY_DATA_t.drop(repeated_index)
    KEY_DATA_t.reset_index(drop=True, inplace=True)
    #print(KEY_DATA_t)
    print('Time: '+str(int(time.time() - tStart))+' s'+'\n')
    for s in range(KEY_DATA_t.shape[0]):
        sys.stdout.write("\rSetting new snls: "+str(s+1))
        sys.stdout.flush()
        KEY_DATA_t.loc[s, 'snl'] = s+1
    sys.stdout.write("\n")
    #if repeated > 0:
    logging.info('Setting new files, Time: '+str(int(time.time() - tStart))+' s'+'\n')
    
    start_table_dict = {}
    start_code_dict = {}
    DB_new_dict = {}
    db_table_t_dict = {}
    DB_name_new_dict = {}
    for f in FREQNAME:
        start_table_dict[f] = 1
        start_code_dict[f] = 1
        DB_new_dict[f] = {}
        db_table_t_dict[f] = pd.DataFrame(index = FREQLIST[f], columns = [])
        DB_name_new_dict[f] = []
    db_table_new = 0
    db_code_new = 0
    for f in range(KEY_DATA_t.shape[0]):
        sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
        sys.stdout.flush()
        freq = KEY_DATA_t.iloc[f]['freq']
        if not DB_name_dict[freq]:
            db_table_new = KEY_DATA_t.iloc[f]['db_table']
            db_code_new = KEY_DATA_t.iloc[f]['db_code']
            if db_table_new not in DB_dict[freq].keys():
                DB_dict[freq][db_table_new] = DATA_BASE_t[db_table_new]
            continue
        df_key, DB_new_dict[freq], DB_name_new_dict[freq], db_table_t_dict[freq], start_table_dict[freq], start_code_dict[freq], db_table_new, db_code_new = \
            NEW_KEYS(f, freq, FREQLIST, DB_TABLE, DB_CODE, KEY_DATA_t, DATA_BASE_t, db_table_t_dict[freq], start_table_dict[freq], start_code_dict[freq], DB_new_dict[freq], DB_name_new_dict[freq])
    sys.stdout.write("\n")
    for f in FREQNAME:
        if db_table_t_dict[f].empty == False:
            #if freq == 'W':
            #    db_table_t_dict[freq] = db_table_t_dict[freq].reindex(FREQLIST['W_s'])
            DB_new_dict[f][DB_TABLE+f+'_'+str(start_table_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
            DB_name_new_dict[f].append(DB_TABLE+f+'_'+str(start_table_dict[f]).rjust(4,'0'))
        if not not DB_name_new_dict[f]:
            DB_dict[f] = DB_new_dict[f]
            DB_name_dict[f] = DB_name_new_dict[f]

    logging.info('Concating new files: '+NAME+'database, Time: '+str(int(time.time() - tStart))+' s'+'\n')
    DATA_BASE_dict = {}
    for f in FREQNAME:
        if not DB_name_dict[f]:
            for key in DB_dict[f]:
                sys.stdout.write("\rConcating sheet: "+str(key))
                sys.stdout.flush()
                DATA_BASE_dict[key] = DB_dict[f][key]
            sys.stdout.write("\n")
            continue
        #DATA_BASE_dict[f] = {}
        for d in DB_name_dict[f]:
            sys.stdout.write("\rConcating sheet: "+str(d))
            sys.stdout.flush()
            #DATA_BASE_dict[f][d] = DB_dict[f][d]
            DATA_BASE_dict[d] = DB_dict[f][d]
        sys.stdout.write("\n")
    
    #print(KEY_DATA_t)
    print('Time: '+str(int(time.time() - tStart))+' s'+'\n')

    return KEY_DATA_t, DATA_BASE_dict

def UPDATE(original_file, updated_file, key_list, NAME, data_path, orig_suf, up_suf, original_database=None, updated_database=None):
    updated = 0
    tStart = time.time()
    logging.info('Updating file: '+str(int(time.time() - tStart))+' s'+'\n')
    if original_database == None:
        logging.info('Reading original database: '+NAME+'database'+orig_suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
        original_database = readExcelFile(data_path+NAME+'database'+orig_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
    if updated_database == None:
        logging.info('Reading updated database: '+NAME+'database'+up_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        updated_database = readExcelFile(data_path+NAME+'database'+up_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
    CAT = ['desc_e', 'unit', 'type', 'form_e', 'form_c']

    original_file = original_file.set_index('name')
    updated_file = updated_file.set_index('name')
    for ind in updated_file.index:
        sys.stdout.write("\rUpdating latest data time ("+str(round((list(updated_file.index).index(ind)+1)*100/len(updated_file.index), 2))+"%): "+ind+" ")
        sys.stdout.flush()

        if ind in original_file.index:
            for c in CAT:
                original_file.loc[ind, c] = updated_file.loc[ind, c]
            if updated_file.loc[ind, 'last'] == 'Nan':
                continue
            elif (original_file.loc[ind, 'last'] == 'Nan' and updated_file.loc[ind, 'last'] != 'Nan') or updated_file.loc[ind, 'last'] > original_file.loc[ind, 'last']:
                updated+=1
            if updated_file.loc[ind, 'last'] != 'Nan':
                original_file.loc[ind, 'last'] = updated_file.loc[ind, 'last']
            if updated_file.loc[ind, 'start'] != 'Nan' and (original_file.loc[ind, 'start'] == 'Nan' or updated_file.loc[ind, 'start'] < original_file.loc[ind, 'start']):
                original_file.loc[ind, 'start'] = updated_file.loc[ind, 'start']
            for period in updated_database[updated_file.loc[ind, 'db_table']].index:
                if updated_file.loc[ind, 'db_table'][3] == 'W' and type(period) != str:
                    period = period.strftime('%Y-%m-%d')
                if str(updated_database[updated_file.loc[ind, 'db_table']].loc[period, updated_file.loc[ind, 'db_code']]) != 'nan':
                    original_database[original_file.loc[ind, 'db_table']].loc[period, original_file.loc[ind, 'db_code']] = updated_database[updated_file.loc[ind, 'db_table']].loc[period, updated_file.loc[ind, 'db_code']]
                elif period >= updated_file.loc[ind, 'start'] and str(updated_database[updated_file.loc[ind, 'db_table']].loc[period, updated_file.loc[ind, 'db_code']]) == 'nan':
                    original_database[original_file.loc[ind, 'db_table']].loc[period, original_file.loc[ind, 'db_code']] = ''
        else:
            ERROR('Updated file index does not belongs to the original file index list: '+ind)
    sys.stdout.write("\n\n")
    for key in original_database.keys():
        if str(key).find('D_') >= 0:
            original_database[key] = original_database[key].sort_index(axis=0, ascending=False)
        else:
            original_database[key] = original_database[key].sort_index(axis=0)
    original_file = original_file.reset_index()
    original_file = original_file.reindex(key_list, axis='columns')
    logging.info('updated: '+str(updated)+'\n')

    return original_file, original_database

def INTLINE_NOTE(LINE, sname=None, LABEL=[], address='', other=False, fname=None):
    note = []
    footnote = []
    FOOT = ['nan', 'Legend / Footnotes:']
    if other == True:
        for n in range(LINE.shape[0]):
            line = LINE.index[n]
            if str(line).isnumeric():
                line = int(line)
            elif address.find('ln/') >= 0:
                for code in LABEL['footnote_codes']:
                    footnote = LABEL['footnote_codes'][code]
                    if type(footnote) == float and footnote.is_integer():
                        footnote = int(footnote)
                    if footnote == line:
                        Note = LINE.iloc[n]['footnote_text'].strip()
                        note.append([str(LINE.index[n]), Note])
                        break
            else:
                note.append([LINE.index[n], LINE.iloc[n]['note']])
        return note
    for n in range(len(LINE)):
        if sname != 0 and bool(re.match(r'[0-9]+\.', str(LINE[n]))):
            whole = str(LINE[n])[str(LINE[n]).find('.')+1:]
            whole = re.sub(r'\s\([0-9]\)|\s\(see\sfootnote\s[0-9]+\)|<.*?>',"",whole)
            if bool(re.search(r'[Ll]ine', whole)):
                if address.find('ITAS') < 0:
                    whole = re.sub(r'\s\([Ll]ine\s[0-9]+\)|[Ll]ine\s[0-9]+,\s',"",whole)
                if whole.find('residual') >= 0:
                    whole = whole.replace('the first line',LABEL['1'].strip()).replace('detailed lines','detailed items')
                if bool(re.search(r'[Ll]ine\s[0-9]+', whole)) or bool(re.search(r'[Ll]ines\s[0-9]+', whole)):
                    whole = re.sub(r'\s[Ll]ine'," Item of line", whole)
                else:
                    whole = re.sub(r'\s[Ll]ine'," item", whole)
            note.append([int(str(LINE[n])[:str(LINE[n]).find('.')]),whole.strip()])
        elif (address.find('FTD') >= 0 or address.find('DOE') >= 0) and bool(re.match(r'[0-9]+\s*[A-Z]+', str(LINE[n]).strip())):
            whole = str(LINE[n])[re.search(r'[A-Z]',str(LINE[n])).start():]
            m = n
            while str(LINE[m+1]) != 'nan' and bool(re.match(r'[0-9]+\s*[A-Z]+', str(LINE[m+1]).strip())) == False and address.find('DOE') < 0:
                whole = whole+str(LINE[m+1])
                m+=1
                if m+1 >= len(LINE):
                    break
            note.append([int(str(LINE[n])[:re.search(r'[A-Z]',str(LINE[n])).start()]),whole.replace('\xa0',' ').strip()])
        elif address.find('BOE') >= 0 and bool(re.match(r'\[[a-z]\]', str(LINE[n]).strip())):
            whole = str(LINE[n])[re.search(r'\]',str(LINE[n])).start()+1:].strip()
            note.append([re.sub(r'(\[[a-z]\]).+', r"\1", str(LINE[n]), 1).strip(), whole])
        elif sname != 0 and str(LINE[n]).find('Note.') >= 0:
            whole = str(LINE[n])[str(LINE[n]).find('Note.')+5:].replace('table are','item is').replace('Except as noted in footnotes 1, 2 and 3, c','C').replace('This table is','This item is').replace(' (see table footnotes)','')
            whole = re.sub(r'\s\([0-9]\)|\s\(see\sfootnote\s[0-9]+\)',"",whole)
            if bool(re.search(r'[Ll]ine', whole)):
                whole = re.sub(r'\s\([Ll]ine\s[0-9]+\)|[Ll]ine\s[0-9]+,\s',"",whole)
                if whole.find('residual') >= 0:
                    whole = whole.replace('the first line',LABEL['1'].strip()).replace('detailed lines','detailed items')
                if bool(re.search(r'[Ll]ine\s[0-9]+', whole)) or bool(re.search(r'[Ll]ines\s[0-9]+', whole)):
                    whole = re.sub(r'\s[Ll]ine'," Item of line", whole)
                else:
                    whole = re.sub(r'\s[Ll]ine'," item", whole)
            note.append(['Note',whole.strip()])
        elif bool(re.search(r'Note[s]*:', str(LINE[n]))) and address.find('BOE') < 0:
            whole = str(LINE[n])[re.search(r'Note[s]*:', str(LINE[n])).start()+5:]
            whole = whole.strip("',(): ")
            m = n
            if m+1 < len(LINE):
                while str(LINE[m+1]) != 'nan' and bool(re.match(r'Source:', str(LINE[m+1]))) == False and bool(re.search(r'Note:', str(LINE[m+1]))) == False and address.find('DOE') < 0:
                    whole = whole+' '+str(LINE[m+1])
                    m+=1
                    if m+1 >= len(LINE):
                        break
            key = 'Note'
            whole = re.sub(r'\s+', " ", whole)
            note.append([key, whole.replace("'",'').replace('\xa0',' ').strip()])
        elif sname != 0 and str(LINE[n]) not in FOOT and str(LINE[n]).isnumeric() == False and str(LINE[n]).strip() != '':
            not_footnote = False
            for no in note:
                if no[1].find(re.sub(r'\s+', " ", str(LINE[n])).strip()) >= 0:
                    not_footnote = True
                    break
            if not_footnote == True:
                continue
            if address.find('BEA') >= 0:
                foot = re.split(r'[\s=:]+', str(LINE[n]), 1)
            else:
                foot = re.split(r'[\s=:]+', re.sub(r'\.$', "", str(LINE[n])), 1)
            if len(foot) == 2 and foot[0].isnumeric() == False and foot[1] != '00:00:00':
                footnote.append(foot)
    return note, footnote

def INTLINE_BLS(US_temp, Table, freq, QUAR, index_base, address, DF_KEY, start=None, key2='main', lab_base='series_title', find_unknown=False, note=[], footnote=[]):
    if address.find('ln/') >= 0:
        US_temp = US_temp.loc[lambda US_temp: US_temp.series_id.str.match('.+?000000\s+$')]
    
    US_t = pd.DataFrame()
    new_item_t = []
    new_index_t = []
    new_code_t = []
    new_label_t = []
    new_unit_t = []
    new_dataframe = []
    new_start_t = []
    new_last_t = []
    base_year_t = []
    firstfound = False
    code = ''
    for i in range(US_temp.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/US_temp.shape[0], 1))+"%)*")
        sys.stdout.flush()
        if find_unknown == True and GET_NAME(address, freq, country=111, code=code, check_exist=True, DF_KEY=DF_KEY) == True:
            continue
        if US_temp.iloc[i]['series_id'] != code:
            if firstfound == True:
                new_dataframe.append(new_item_t)
                US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
                if US_new.empty == False:
                    new_code_t.append(code)
                    new_label_t.append(lab)
                    new_unit_t.append(unit)
                    new_start_t.append(str(Table['begin_year'][code])+'-'+str(Table['begin_period'][code]))
                    new_last_t.append(str(Table['end_year'][code])+'-'+str(Table['end_period'][code]))
                    base_year_t.append(base_year)
                    US_t = pd.concat([US_t, US_new], ignore_index=True)
                new_dataframe = []
                new_item_t = []
                new_index_t = []
            code = US_temp.iloc[i]['series_id']
            lab = Table[lab_base][code]
            base_year = ''
            if address.find('pr/') >= 0 and str(Table[index_base][code]).isnumeric():
                unit = 'Index base: '+str(Table[index_base][code])+' = 100'
                base_year = str(Table[index_base][code])
            elif address.find('pr/') >= 0:
                unit = Table['duration_code'][code]
            else:
                unit = Table[index_base][code]
            firstfound = True
        if address.find('pr/') >= 0 and (code.find('8500610') < 0 and code.find('3000611') < 0):
            continue
        if start != None and find_unknown == False:
            if US_temp.iloc[i]['year'] < start:
                continue
        new_item_t.append(US_temp.iloc[i]['value'])
        if freq == 'M':
            period_index = str(US_temp.iloc[i]['year'])+'-'+str(US_temp.iloc[i]['period']).replace('M','')
        elif freq == 'Q':
            period_index = str(US_temp.iloc[i]['year'])+'-'+QUAR[key2][str(US_temp.iloc[i]['period'])]
        new_index_t.append(period_index)  
    sys.stdout.write("\n\n")
    new_dataframe.append(new_item_t)
    US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
    if US_new.empty == False:
        new_code_t.append(code)
        new_label_t.append(lab)
        new_unit_t.append(unit)
        new_start_t.append(str(Table['begin_year'][code])+'-'+str(Table['begin_period'][code]))
        new_last_t.append(str(Table['end_year'][code])+'-'+str(Table['end_period'][code]))
        base_year_t.append(base_year)
        US_t = pd.concat([US_t, US_new], ignore_index=True)
    US_t = US_t.sort_index(axis=1)
    US_t.insert(loc=0, column='Index', value=new_code_t)
    US_t.insert(loc=1, column='Label', value=new_label_t)
    US_t.insert(loc=2, column='unit', value=new_unit_t)
    US_t.insert(loc=3, column='start', value=new_start_t)
    US_t.insert(loc=4, column='last', value=new_last_t)
    US_t.insert(loc=5, column='base', value=base_year_t)
    US_t = US_t.set_index('Index', drop=False)
    label = US_t['Label']

    return US_t, label, note, footnote

def INTLINE_STL(US_temp, address, DIY_series, sname=None, freq=None, note=[], footnote=[]):  
    
    if freq == 'M' and str(sname) == 'Daily':
        US_temp = US_temp.T
        new_columns = [dex.strftime('%Y-%m') for dex in US_temp.index]
        US_temp['month'] = new_columns
        US_temp = US_temp.set_index('month', append=True)
        US_temp = US_temp.apply(pd.to_numeric).mean(level='month').T
    keycolumn = US_temp.index
    new_label = []
    new_form = []
    new_unit = []
    isadjusted = []
    base_year_t = []
    head = 0
    for i in range(US_temp.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/US_temp.shape[0], 1))+"%)*")
        sys.stdout.flush()
        note_num = 1
        for r in range(head, len(DIY_series)):
            if DIY_series[r] == keycolumn[i]:
                for rr in range(r,len(DIY_series)):
                    base_year = ''
                    if DIY_series[rr] == 'Title:':
                        if address.find('AUS') >= 0:
                            m = rr+1
                            whole = ''
                            while str(DIY_series[m]) != 'Source:':
                                if str(DIY_series[m]) == 'nan':
                                    m+=1
                                    continue
                                whole = whole+str(DIY_series[m])+' '
                                m+=1
                            new_label.append(re.sub(r'\s+', " ", whole.strip()))  
                        else:
                            new_label.append(DIY_series[rr+1].strip())
                    elif DIY_series[rr] == 'Release:':
                        if address.find('JP') >= 0:
                            new_form.append('Merchandise Trade')
                        else:
                            new_form.append(re.sub(r'.+?[0-9]+\s*', "", DIY_series[rr+1]).strip())
                    elif DIY_series[rr] == 'Units:':
                        if address.find('JP') >= 0:
                            new_unit.append('Japanese Yen')
                        else:
                            if str(DIY_series[rr+1]).find('Index') >= 0:
                                base_year = re.sub(r'.*?([0-9]{4}).*', r"\1", str(DIY_series[rr+1])).strip()
                            new_unit.append(DIY_series[rr+1].strip())
                        base_year_t.append(base_year)
                    elif DIY_series[rr] == 'Seasonal Adjustment:':
                        isadjusted.append(DIY_series[rr+1].strip())    
                    elif DIY_series[rr] == 'Notes:':
                        m = rr+1
                        while str(DIY_series[m]) != 'Series ID:':
                            if str(DIY_series[m]) == 'nan':
                                m+=1
                                continue
                            whole = ''
                            while True:
                                whole = whole+str(DIY_series[m])+' '
                                if m+1 >= len(DIY_series) or bool(re.search(r'\.$', str(DIY_series[m]))) or DIY_series[m+1] == 'Series ID:' or bool(re.search(r'©', str(DIY_series[m+1]))):
                                    break
                                m+=1
                            note.append([str(note_num)+US_temp.index[i]+'.', re.sub(r'\s+', " ", whole.strip())])
                            note_num += 1
                            m+=1
                            if m+1 >= len(DIY_series) or bool(re.search(r'©', str(DIY_series[m]))):
                                break  
                    elif DIY_series[rr] == 'Series ID:':
                        head = rr
                        break
                break
    sys.stdout.write("\n\n")
    US_t = US_temp.reset_index()
    US_t = US_t.set_index('index', drop=False)
    US_t = US_t.rename(columns={'index':'Index'})
    US_t.insert(loc=1, column='Label', value=new_label)
    US_t.insert(loc=2, column='form_e', value=new_form)
    US_t.insert(loc=3, column='unit', value=new_unit)
    US_t.insert(loc=4, column='is_adj', value=isadjusted)
    US_t.insert(loc=5, column='base', value=base_year_t)
    label = US_t['Label']
    
    return US_t, label, note, footnote

def INTLINE_FTD(US_address, fname, sname, Series, header=None, index_col=None, skiprows=None, freq=None, x='', trans=True, prefix=None, middle=None, suffix=None, chrome=None, Zip_table=None, final_name=None, ft900_name=None):
    PASS = ['nan', '(-)', 'Balance of Payment', 'Net Adjustments', 'Total, Census Basis', 'Total Census Basis', 'Item', 'Residual', 'Unnamed', 'Selected commodities', 'Country', 'TOTAL']
    MONTH = ['January','February','March','April','May','June','July','August','September','October','November','December']
    new_columns = []
    new_index = []
    new_label = []
    new_order = []

    if fname == 'AGDSCSB' or fname == 'UGDSCSB':
        fname_t = fname+'_'+freq
        file_path = US_address+str(fname_t)+'_historical.xlsx'
        US_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        if INTLINE_PRESENT(file_path) == False:
            address = US_address
            US_his = INTLINE_FTD_HISTORICAL(US_his, chrome, data_path, address, fname, fname_t, Series, prefix, middle, suffix, freq, trans, Zip_table, excel=x, skip=skiprows, head=header, index_col=index_col, final_name=final_name, ft900_name=ft900_name)
            US_his.to_excel(file_path, sheet_name=fname)
        label = US_his['Label']
        return US_his, label, [], []
    
    file_path = US_address+str(sname)+'.xls'+x
    if INTLINE_PRESENT(file_path):
        US_t = readExcelFile(US_address+sname+'.xls'+x, header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=0, acceptNoFile=False)
    else:
        US_t = INTLINE_WEB(chrome, 111, US_address, fname, sname, freq=freq, tables=[0], header=header, index_col=index_col, skiprows=skiprows, US_address=US_address)
    if type(US_t) != dict and US_t.empty == True:
        ERROR('Sheet Not Found: '+US_address+sname+'.xls'+x)
    note, footnote = INTLINE_NOTE(US_t.index, sname, address=US_address)
    footnote = []
    
    if trans == True:
        US_t = US_t.T
        year = 0
        for ind in range(US_t.shape[1]):
            if str(US_t.columns[ind]).strip().isnumeric():
                year = str(US_t.columns[ind]).strip()
            if freq == 'M' and re.sub(r'\s+\([A-Z]+\)\s*$|\s*\.\s*$', "", str(US_t.columns[ind])).strip() in MONTH:
                new_columns.append(year+'-'+str(datetime.strptime(re.sub(r'\s+\([A-Z]+\)\s*$|\s*\.\s*$', "", str(US_t.columns[ind])).strip(),'%B').month).rjust(2,'0'))
            else:
                new_columns.append(None)
        US_t.columns = new_columns
        US_t = US_t.loc[:, US_t.columns.dropna()]
    if sname == 'NSAEXP' or sname == 'NSAIMP' or sname == 'SAEXP' or sname == 'SAIMP':
        for dex in US_t.index:
            middle = ''
            for item in list(Series['CATEGORIES']['name']):
                if re.sub(r'\(.+\)', "", str(dex).replace('\n','')).strip() in re.split(r'//', item):
                    middle = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item].index[0]
                    new_label.append(Series['CATEGORIES'].loc[middle, 'cat_desc']+',  '+Series['DATA TYPES'].loc[suffix[:2], 'dt_desc']+',  '+Series['GEO LEVELS'].loc[suffix[2:], 'geo_desc'])
                    new_order.append(Series['CATEGORIES'].loc[middle, 'order'])
            if middle == '':
                if re.sub(r'\(.+\)|:\s*[0-9]+', "", str(dex).replace('\n','')).strip() not in PASS:
                    ERROR('Item index not found in '+sname+': '+re.sub(r'\(.+\)', "", str(dex).replace('\n','')).strip())
                else:
                    new_index.append(None)
                    new_label.append(None)
                    new_order.append(10000)
            else:
                new_index.append(prefix+middle+suffix)
    
    US_t = US_t.sort_index(axis=1)
    for item in new_order:
        if type(item) == pd.core.series.Series:
            print(item)
            ERROR('Order type incorrect: '+str(item.index[0]))
    US_t.insert(loc=0, column='Index', value=new_index)
    US_t.insert(loc=1, column='order', value=new_order)
    US_t = US_t.set_index('Index', drop=False)
    for item in new_label:
        if type(item) == pd.core.series.Series:
            print(item)
            ERROR('Label type incorrect: '+str(item.index[0]))
    US_t.insert(loc=1, column='Label', value=new_label)
    US_t = US_t.loc[US_t.index.dropna()]
    US_t = US_t.sort_values(by=['order','Label'])
    label = US_t['Label']
    
    return US_t, label, note, footnote

def INTLINE_FTD_HISTORICAL(US_his, chrome, data_path, address, fname, fname_t, Series, prefix, middle, suffix, freq, transpose, Zip_table, excel='x', skip=None, head=None, index_col=None, usecols=None, names=None, datatype='', AMV='', final_name=None, ft900_name=None, start_year=datetime.today().year-2):
    PASS = ['nan', '(-)', 'Balance of Payment', 'Net Adjustments', 'Total, Census Basis', 'Total Census Basis', 'Item', 'Residual', 'Unnamed', 'Selected commodities', 'Country', 'TOTAL']
    MONTH = ['January','February','March','April','May','June','July','August','September','October','November','December']
    YEAR = ['Jan.-Dec.']
    TYPE = {'EX': 'Exports', 'IM': 'Imports'}
    EPYT = {'IM': 'Exports', 'EX': 'Imports'}
    new_index = []
    PERIOD = {'final':range(start_year, datetime.today().year),'ft900':list(range(datetime.today().month, 13))+list(range(1, datetime.today().month))}
    FNAME = {'final':final_name, 'ft900':ft900_name}
    last_year_monthly = True
    
    for key in ['final','ft900']:
        if FNAME[key] == None:
            continue
        for period in PERIOD[key]:
            if period >= datetime.today().month:
                if last_year_monthly == True:
                    last_year = True
                else:
                    continue
            else:
                last_year = False
            if last_year == True:
                process_year = datetime.today().year-1
            else:
                process_year = datetime.today().year
            if key == 'final':
                logging.info('Reading file: '+Zip_table.at[fname+key, 'Zipname']+str(period).rjust(2,'0')+'\n')
            elif key == 'ft900':
                logging.info('Reading file: '+Zip_table.at[fname+key, 'Zipname']+'_'+str(process_year)[-2:]+str(period).rjust(2,'0')+'\n')
            KEYWORD = {'final':'finalxls','ft900':str(process_year)[-2:]+str(period).rjust(2,'0')+'.zip'}
            SITC = {'exh14cy':period,'exh14py':period-1,'exh14ppy':period-2}
            Zip_path = re.sub(r'FTD[EC]/', "", address)+'historical_data/'+Zip_table.at[fname+key, 'Zipname']+str(period).rjust(2,'0')+'.zip'
            if INTLINE_PRESENT(Zip_path):
                zf = zipfile.ZipFile(Zip_path,'r')
            else:
                website = re.sub(r'[0-9]{4}pr', str(period)+"pr", Zip_table.at[fname+key, 'website'])
                if key == 'final' and rq.get(website).status_code != 200:
                    if period == datetime.today().year-1:
                        last_year_monthly = True
                        logging.info('Process data from monthly data of last year.')
                    continue
                elif key == 'ft900':
                    keydate = datetime.strptime(str(process_year)[-2:]+str(period).rjust(2,'0'), '%y%m').strftime('%B %Y')
                    if BeautifulSoup(rq.get(website).text, "html.parser").text.find(keydate) < 0:
                        continue
                zipname = INTLINE_WEB(chrome, 111, re.sub(r'FTD[EC]/', "", address)+'historical_data/', website, Zip_table.at[fname+key, 'Zipname']+str(period).rjust(2,'0'), Zip=True, US_address=address+'historical_data/', file_name=KEYWORD[key])
                zf = zipfile.ZipFile(Zip_path,'r')
            if key == 'final' and period == datetime.today().year-1 and INTLINE_PRESENT(Zip_path):
                last_year_monthly = False
            for ffname in FNAME[key]:
                key_fname = ffname+'.xls'+excel
                if key_fname not in zf.namelist():
                    key_fname = ffname+'.xls'
                    if key_fname not in zf.namelist():
                        continue
                US_temp = readExcelFile(zf.open(key_fname), skiprows_=skip, header_=head, index_col_=index_col, sheet_name_=0, usecols_=usecols, names_=names)
                US_temp = readExcelFile(zf.open(key_fname), skiprows_=skip, header_=head, sheet_name_=0, usecols_=usecols, names_=names)
                US_temp = US_temp.set_index(US_temp.columns[0])
                
                new_index = []
                if transpose == True:
                    US_temp = US_temp.T
                    new_columns = []
                    year = 0
                    for col in US_temp.columns:
                        if str(col).strip().isnumeric():
                            year = str(col).strip()
                        if freq == 'A' and re.sub(r'\s+\([A-Z]+\)\s*$', "", str(col)).replace(' ', '').strip() in YEAR:
                            new_columns.append(year)
                        elif freq == 'M' and re.sub(r'\s+\([A-Z]+\)\s*$|\s*\.\s*$', "", str(col)).strip() in MONTH:
                            new_columns.append(year+'-'+str(datetime.strptime(re.sub(r'\s+\([A-Z]+\)\s*$|\s*\.\s*$', "", str(col)).strip(),'%B').month).rjust(2,'0'))
                        elif freq == 'A' and fname.find('SA') >= 0 and str(col).strip().isnumeric():
                            new_columns.append(year)
                        else:
                            new_columns.append(None)
                    US_temp.columns = new_columns
                    US_temp = US_temp.loc[:, US_temp.columns.dropna()]
                    US_temp = US_temp.loc[:, ~US_temp.columns.duplicated()]
                if fname == 'AGDSCSB' or fname == 'UGDSCSB':
                    new_index_t = []
                    trade = ''
                    for dex in US_temp.index:
                        if str(dex[0]) != 'nan' and str(dex[0]).find('Unnamed') < 0:
                            trade = str(dex[0]).strip()
                        new_index_t.append([trade, str(dex[1]).strip()])
                    US_temp.index = pd.MultiIndex.from_tuples(new_index_t)
                    add_value = []
                    for ind in range(US_temp.shape[1]):
                        TBOP = str(US_temp.loc[('Balance', 'Total Balance of Payments Basis'), US_temp.columns[ind]])
                        TCSB = str(US_temp.loc[('Balance', 'Total Census Basis'), US_temp.columns[ind]])
                        if US_temp.columns[ind] == 'nan':
                            add_value.append(None)
                        elif TBOP != 'nan' and TCSB != 'nan' and TBOP.strip() != '' and TCSB.strip() != '':
                            add_value.append(US_temp.loc[('Balance', 'Total Balance of Payments Basis'), US_temp.columns[ind]]-US_temp.loc[('Balance', 'Total Census Basis'), US_temp.columns[ind]])
                        else:
                            add_value.append(None)
                    US_new = pd.DataFrame([add_value], columns=US_temp.columns, index=[('Balance', 'Net Adjustments')])
                    US_temp = pd.concat([US_temp, US_new])
                    #if fname == 'UGDSCSB':
                    US_temp.index = pd.MultiIndex.from_arrays([US_temp.index.get_level_values(0), US_temp.index.get_level_values(1).str.replace(r'\s*Total Balance of Payments Basis.*', 'BOP', regex=True)])
                    US_temp.index = pd.MultiIndex.from_tuples([[str(dex[0]).strip(), re.sub(r'\s+\([0-9]+\)\s*$', "", str(dex[1])).strip()] for dex in US_temp.index])
                    suf = ''
                    for dex in US_temp.index:
                        suffix = ''
                        if dex[0] in list(Series['DATA TYPES']['dt_desc']):
                            suf = Series['DATA TYPES'].loc[Series['DATA TYPES']['dt_desc'] == dex[0]].index[0]
                        for item in list(Series['GEO LEVELS']['name']):
                            if dex[1] in re.split(r'//', item):
                                suffix = suf+Series['GEO LEVELS'].loc[Series['GEO LEVELS']['name'] == item].index[0]
                                break
                        if suffix == '' and dex[1] != 'Total Balance of Payments Basis':
                            ERROR('Item index not found in '+fname+': '+dex[1])
                        elif suffix == '' and dex[1] == 'Total Balance of Payments Basis':
                            new_index.append(None)
                        else:
                            new_index.append(prefix+middle+suffix)
                    US_temp.index = new_index
                US_temp = US_temp.sort_index(axis=1)
                US_temp = US_temp.loc[US_temp.index.dropna()]
                US_his = pd.concat([US_temp, US_his], axis=1)
                US_his = US_his.loc[US_his.index.dropna(), US_his.columns.dropna()]
                US_his = US_his.loc[:, ~US_his.columns.duplicated()]
                US_his = US_his.sort_index(axis=1)
    
    return US_his

def INTLINE_DOE(US_t, data_path, country, address, fname, sname, freq, transpose=True, note=[], footnote=[]):
    DEAL = ['Crude Oil','Hydrocarbon Gas Liquids','Other Liquids','Finished Petroleum Products']
    if type(US_t) != dict and US_t.empty == True:
        ERROR('Sheet Not Found: '+data_path+address+fname+'.xls'+x+', sheet name: '+str(sname))
    if transpose == True:
        US_t = US_t.T
    new_code_t = []
    new_label_t = []
    new_unit_t = []
    
    US_t.columns = [col+timedelta(days=1) if type(col) == pd._libs.tslibs.timestamps.Timestamp else col for col in US_t.columns]
    for ind in range(US_t.shape[0]):
        deal = False
        for dealing in DEAL:
            if str(US_t.index[ind][1]).find(dealing) >= 0:
                deal = True
                break
        if deal == False:
            new_code_t.append('nan')
            new_label_t.append(None)
            new_unit_t.append(None)
            continue
        new_code_t.append(re.sub(r'[\-_]+', r"", str(US_t.index[ind][0]).strip()).strip())
        new_label_t.append(re.sub(r'^(.+?)\s+\([^\)\(]+\)$', r"\1", str(US_t.index[ind][1]).strip()).strip())
        new_unit_t.append(re.sub(r'.+?\s+\(([^\)\(]+)\)$', r"\1", str(US_t.index[ind][1]).strip()).strip())
    
    US_t = US_t.loc[:, ~US_t.columns.duplicated()]
    US_t = US_t.sort_index(axis=1)
    US_t.insert(loc=0, column='Index', value=new_code_t)
    US_t.insert(loc=1, column='Label', value=new_label_t)
    US_t.insert(loc=2, column='unit', value=new_unit_t)
    US_t = US_t.set_index('Index', drop=False)
    label = US_t['Label']

    return US_t, label, note, footnote

def INTLINE_STOCK(chrome, data_path, country, address, fname, sname, freq, keyword, STOCK_start=None, find_unknown=False, note=[], footnote=[]):
    FREQ = {'A':'Monthly', 'Q':'Monthly', 'M':'Monthly', 'D':'Daily', 'W':'Weekly'}
    QUAR = ['03','06','09','12']
    YEAR = ['12']
    file_path = data_path+str(country)+'/'+address+str(sname)+' - '+FREQ[freq]+'.xlsx'
    update_path = data_path+str(country)+'/'+address+str(sname)+' - '+FREQ[freq]+' - '+keyword+'.txt'
    if freq == 'M' or freq == 'Q' or freq == 'A':
        start = "01/01/"+str(datetime.today().year-20)
    else:
        start = "01/01/"+str(datetime.today().year-18)
    start_year = int(start[-4:])
    if STOCK_start != None and STOCK_start > start_year and find_unknown == False:
        start = "01/01/"+str(STOCK_start)
    IHS = readExcelFile(file_path, header_=0, index_col_=0, sheet_name_=0)
    if freq == 'M' or freq == 'Q' or freq == 'A':
        IHS.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in IHS.columns]
        if freq == 'A':
            IHS.columns = [datetime.strptime(col, '%Y-%m').strftime('%Y') if str(col)[-2:] in YEAR else col for col in IHS.columns]
        elif freq == 'Q':
            IHS.columns = [pd.Period(col, freq='Q').strftime('%Y-Q%q') if str(col)[-2:] in QUAR else col for col in IHS.columns]
    elif freq == 'D' or freq == 'W':
        IHS.columns = [col.strftime('%Y-%m-%d') if type(col) != str else col for col in IHS.columns]
        if freq == 'W':
            IHS.columns = [(datetime.strptime(col, '%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d') if (col[:4].isnumeric() and datetime.strptime(col, '%Y-%m-%d').weekday() == 4) else col for col in IHS.columns]
            """IHS_D = readExcelFile(data_path+str(country)+'/'+address+str(sname)+' - Daily.xlsx', header_=0, index_col_=0, sheet_name_=0)
            IHS_D.columns = [col.strftime('%Y-%m-%d') if type(col) != str else col for col in IHS_D.columns]
            for i in range(IHS.shape[1]):
                if IHS.columns[i][:4].isnumeric():
                    d=6
                    while True:
                        weekdate = (datetime.strptime(str(IHS.columns[i]), '%Y-%m-%d')+timedelta(days=d)).strftime('%Y-%m-%d')
                        try:
                            if str(IHS_D.loc[keyword, weekdate]) == 'nan' and d > 2:
                                d=d-1
                            else:
                                break
                        except KeyError:
                            weekdate = (datetime.strptime(str(IHS.columns[i]), '%Y-%m-%d')+timedelta(days=6)).strftime('%Y-%m-%d')
                            if d > 2:
                                d=d-1
                            else:
                                break
                    try:
                        IHS.loc[keyword, IHS.columns[i]] = float(IHS_D.loc[keyword, weekdate])
                    except ValueError:
                        IHS.loc[keyword, IHS.columns[i]] = IHS_D.loc[keyword, weekdate]"""
    
    if INTLINE_PRESENT(update_path, freq=freq) == True:# and find_unknown == False
        INTLINE_t = IHS.loc[[keyword]]
        label = INTLINE_t['Label']
        return INTLINE_t, label, note, footnote
    modified = pd.Series(np.array([datetime.now().strftime('%Y-%m-%d, %H:%M:%S')]))
    if str(chrome.current_url) != str(fname):
        try:
            chrome.get(fname)
        except TimeoutException:
            chrome.execute_script("window.stop();")
    if address.find('SGSE') >= 0:
        price = 'Close'
        daydelta = 1
        link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Download data in csv file', text_match=True)
        time.sleep(2)
        IN_t = INTLINE_WEBDRIVER(chrome, country, address, 'STI - '+FREQ[freq], header=[0], index_col=0, csv=True)
        IN_t = pd.DataFrame(IN_t[price]).T
        if freq == 'D':
            IN_t.columns = [str(col).strip() if str(col).strip()[:4].isnumeric() else '' for col in IN_t.columns]
        elif freq == 'W':
            IN_t.columns = [(datetime.strptime(str(col).strip(), '%Y-%m-%d')-timedelta(days=daydelta)).strftime('%Y-%m-%d') if str(col).strip()[:4].isnumeric() else '' for col in IN_t.columns]
    else:
        try:
            WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/i[@class="popupCloseIcon largeBannerCloser"]'))).click()
        except TimeoutException:
            time.sleep(0)
        price = 'Price'
        daydelta = 1
        Select(chrome.find_element_by_id("data_interval")).select_by_value(FREQ[freq])
        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, "widgetFieldDateRange"))).click()
        ActionChains(chrome).key_down(Keys.CONTROL).key_down("a").send_keys(Keys.BACKSPACE).key_up(Keys.CONTROL).key_up("a").send_keys(start).send_keys(Keys.ENTER).perform()
        while True:
            try:
                IN_t = pd.DataFrame(pd.read_html(chrome.page_source, header=[0], index_col=0)[0][price]).T
            except KeyError:
                time.sleep(1)
            else:
                break
        if freq == 'M' or freq == 'Q' or freq == 'A':
            IN_t.columns = [datetime.strptime(col, '%b %y').strftime('%Y-%m') for col in IN_t.columns]
            if freq == 'A':
                IN_t.columns = [datetime.strptime(col, '%Y-%m').strftime('%Y') if str(col)[-2:] in YEAR else '' for col in IN_t.columns]
            elif freq == 'Q':
                IN_t.columns = [pd.Period(col, freq='Q').strftime('%Y-Q%q') if str(col)[-2:] in QUAR else '' for col in IN_t.columns]
            IN_t = IN_t.loc[:, IN_t.columns.dropna()]
        elif freq == 'D':
            IN_t.columns = [datetime.strptime(col, '%b %d, %Y').strftime('%Y-%m-%d') if col[-4:].isnumeric() else '' for col in IN_t.columns]
        elif freq == 'W':
            IN_t.columns = [(datetime.strptime(col, '%b %d, %Y')-timedelta(days=daydelta)).strftime('%Y-%m-%d') if col[-4:].isnumeric() else '' for col in IN_t.columns]
    
    IN_t = IN_t.sort_index(axis=1)
    for h in range(IN_t.shape[0]):
        for i in range(IN_t.shape[1]):
            if IN_t.columns[i][:4].isnumeric():
                try:
                    IHS.loc[keyword, IN_t.columns[i]] = float(IN_t.iloc[h][IN_t.columns[i]])
                except ValueError:
                    IHS.loc[keyword, IN_t.columns[i]] = IN_t.iloc[h][IN_t.columns[i]]
    IHS = IHS.sort_index(axis=1)
    INTLINE_t = IHS.loc[[keyword]]
    label = INTLINE_t['Label']
    
    if freq != 'A' and freq != 'Q':
        IHS.to_excel(data_path+str(country)+'/'+address+str(sname)+' - '+FREQ[freq]+'.xlsx', sheet_name=FREQ[freq])
        modified.to_csv(update_path, header=False, index=False)
    return INTLINE_t, label, note, footnote

def INTLINE_LATEST_STEEL(INTLINE_steel):
    new_columns = []
    for col in INTLINE_steel.columns:
        try:
            new_columns.append(re.sub(r'.*?([0-9]{4}).*', r"\1", str(col[1]))+'-'+datetime.strptime(str(col[0]).strip()[:3], '%b').strftime('%m'))
        except ValueError:
            new_columns.append(None)
    INTLINE_steel.columns = new_columns
    new_index = []
    for dex in INTLINE_steel.index:
        lab = [bool(re.search(r'Unnamed|Total|MT', str(d))) == False for d in dex]
        lab_found = False
        for d in reversed(range(len(dex))):
            if lab[d] == True:
                new_index.append(str(dex[d]).replace('\n',' ').replace('\u3000\u3000',' '))
                lab_found = True
                break
        if lab_found == False:
            new_index.append(None)
    INTLINE_steel.index = new_index
    INTLINE_steel = INTLINE_steel.loc[INTLINE_steel.index.dropna(), INTLINE_steel.columns.dropna()]

    return INTLINE_steel

def INTLINE_STEEL(data_path, country, address, fname, INTLINE_steel, Countries, note=[], footnote=[]):
    INTLINE_t = readExcelFile(data_path+str(country)+'/'+address+fname+'.xlsx', header_=0, index_col_=0, sheet_name_=0)
    INTLINE_t.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_t.columns]
    INTLINE_steel = INTLINE_steel.sort_index(axis=1)
    
    for i in range(INTLINE_steel.shape[1]):
        if INTLINE_steel.columns[i][:4].isnumeric():
            INTLINE_t.loc[INTLINE_t.index[0], INTLINE_steel.columns[i]] = INTLINE_steel.loc[Countries.loc[country, 'Country_Name'].replace('South Korea','Korea').strip(), INTLINE_steel.columns[i]]
    INTLINE_t = INTLINE_t.sort_index(axis=1)
    label = INTLINE_t['Label']

    INTLINE_t.to_excel(data_path+str(country)+'/'+address+fname+'.xlsx', sheet_name='Monthly')
    return INTLINE_t, label, note, footnote

def INTLINE_WEB_LINK(chrome, fname, keyword, get_attribute='href', text_match=False, driver=None):
    
    link_list = WebDriverWait(chrome, 5).until(EC.presence_of_all_elements_located((By.XPATH, './/*[@href]')))
    link_found = False
    error_count = 0
    for link in link_list:
        if (text_match == True and link.text.find(keyword) >= 0) or (text_match == False and link.get_attribute(get_attribute).find(keyword) >= 0):
            while True:
                try:
                    link.click()
                except ElementClickInterceptedException:
                    if fname.find('bea.gov') >= 0:
                        error_count+=1
                        if error_count > 3:
                            raise ElementClickInterceptedException
                        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                    else:
                        raise ElementClickInterceptedException
                else:
                    link_found = True
                    break
            break
    link_meassage = None
    if link_found == False:
        if text_match == True:
            key_string = link.text
        else:
            key_string = link.get_attribute(get_attribute)
        link_meassage = 'Link Not Found in key string: '+key_string+', key = '+keyword
    return link_found, link_meassage

def INTLINE_WEB(chrome, country, address, fname, sname, freq=None, tables=None, header=None, index_col=None, skiprows=None, usecols=None, nrows=None, csv=False, encode=ENCODING, renote=False, Series=None, Table=None, start_year=None, previous=False, output=False, Zip=False, US_address=None, file_name=None, specific_sheet=False, specific_time_unit=False, interval=None):
    note = []
    link_found = False
    link_message = None
    done = False
    INTLINE_temp = None
    logging.info('Downloading file: '+str(sname)+'\n')
    if address.find('BEIS') >= 0:
        chrome.get('https://www.gov.uk/government/collections/uk-house-price-index-reports')
        date = chrome.find_element_by_xpath('.//a[@data-track-action="content_item 1"]').text.lower().replace(' ','-')
        chrome.get(fname+'-'+date)
    elif address.find('HKMA') >= 0:
        ssl._create_default_https_context = ssl._create_unverified_context
        INTLINE_temp = pd.DataFrame.from_dict(pd.read_json(fname).loc['records','result']).set_index('end_of_month')
        INTLINE_temp = INTLINE_temp.sort_index(axis=0)
        done = True
    elif address.find('MAS/OFRV') >= 0:
        json_data = rq.get(fname).content
        INTLINE_temp = pd.DataFrame.from_dict(pd.read_json(json_data).loc['records','result']).set_index('end_of_month')
        INTLINE_temp = INTLINE_temp.sort_index(axis=0)
        done = True
    elif (address.find('RBI') >= 0 or address.find('ISTAT') >= 0 or (address.find('BCB') >= 0 and str(sname).find('general government debt') >= 0) or address.find('COMEX') >= 0) \
        and re.sub(r'#[0-9]+', "", str(chrome.current_url)) != re.sub(r'#[0-9]+', "", str(fname)):
        count = 0
        time_limit = 20
        if address.find('ISTAT') >= 0 and freq == 'M':
            time_limit = 30
        while True:
            try:
                chrome.set_page_load_timeout(time_limit)
                chrome.get(fname.replace('$',''))
            except TimeoutException:
                if count > 2:
                    input('加載時間過長，請手動重新整理後按Enter鍵繼續:')
                chrome.execute_script("window.stop();")
                try:
                    if re.sub(r'#[0-9]+', "", str(chrome.current_url)) != re.sub(r'#[0-9]+', "", str(fname)):
                        raise NoSuchElementException
                    chrome.find_element_by_tag_name('div')
                    if address.find('RBI') >= 0:
                        WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/a')))
                    if address.find('ISTAT') >= 0:
                        chrome.find_element_by_xpath('.//table[@class="DataTable"]')
                except NoSuchElementException:
                    count +=1
                    if count > 3:
                        ERROR('The website is unable to enter, please download the file manually: '+str(fname).replace('$',''))
                    else:
                        continue
                else:
                    print('web page loaded')
                    break
            else:
                break
    elif address.find('DEUSTATIS') >= 0:
        chrome.get(fname.replace('#','&language=en#'))
    elif address.find('BOF') >= 0:
        client_ID = open(data_path+str(country)+'/'+address+'Client ID.txt','r',encoding='ANSI').read()
        conn = http.client.HTTPSConnection("api.webstat.banque-france.fr")
        headers = { 'accept': "application/json" }
        conn.request("GET", "/webstat-en/v1/data/"+str(file_name)+"?client_id="+client_ID+"&format=csv&detail=dataonly", headers=headers)
        logging.info('Reading Data from API')
        data = conn.getresponse().read()
        INTLINE_temp = pd.read_csv(BytesIO(data), header=[2], index_col=0, low_memory=False)
        done = True
    else:
        chrome.get(fname)
        if address.find('MEASTF') >= 0:
            time.sleep(2)
    y = 0
    height = chrome.execute_script("return document.documentElement.scrollHeight")
    while True:
        if done == True:
            break
        try:
            chrome.execute_script("window.scrollTo(0,"+str(y)+")")
            if address.find('DOE') >= 0 or address.find('EUC') >= 0:
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=str(sname))
            elif address.find('BEA') >= 0:
                time.sleep(2)
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='categories=flatfiles', driver=chrome)
                time.sleep(2)
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=str(sname)+'.zip', driver=chrome)
            elif address.find('FTD') >= 0:
                if file_name != None:
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=file_name)
                else:
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=str(sname)+'.xls')
            elif address.find('STL') >= 0:
                email = open(data_path+'email.txt','r',encoding='ANSI').read()
                password = open(data_path+'password.txt','r',encoding='ANSI').read()
                try:
                    WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.ID, 'eml'))).send_keys(email)
                    WebDriverWait(chrome, 5).until(EC.visibility_of_element_located((By.ID, 'pw'))).send_keys(password)
                    WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.XPATH, './/input[@type="submit"]'))).click()
                    time.sleep(2)
                except TimeoutException:
                    time.sleep(0)
                target = WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.ID, 'content-table')))
                link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword=str(sname).replace('_xls',''), text_match=True)
                time.sleep(2)
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='download')
                WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/input[@name="download_data"]'))).click()
                link_found = True
            elif address.find('ONS') >= 0:
                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="margin-top-md--4 margin-bottom-sm--4 margin-bottom-md--5"]/a[@title="Download as xlsx"]'))).click()
                link_found = True
            elif address.find('BOE') >= 0:
                try:
                    WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/button[contains(., "Accept recommended cookies")]'))).click()
                except TimeoutException:
                    time.sleep(2)
                #note = {}
                #WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@id="ALL"]'))).click()
                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/label[@id="ALL_LBL"]'))).click()
                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@name="html"]'))).click()
                time.sleep(2)
                #chrome.switch_to.window(chrome.window_handles[-1])
                INTLINE_temp = pd.read_html(chrome.page_source, index_col=0, header=[0])[0]
                #chrome.close()
                #chrome.switch_to.window(chrome.window_handles[0])
                INTLINE_temp = INTLINE_temp[INTLINE_temp.columns[:usecols]]
                new_columns = []
                for col in INTLINE_temp.columns:
                    if len(header) == 2:
                        code = re.sub(r'.+?([A-Z]{3,}[0-9]*).*', r"\1", str(col)).strip()
                        label = re.sub(r'(.+?)[A-Z]{3,}[0-9]*.*', r"\1", str(col)).strip()
                        new_columns.append([code, label])
                    else:
                        if bool(re.search(r'\[[a-z]\]', str(col))):
                            code = re.sub(r'.+?([A-Z]{3,}[0-9]*).*', r"\1", str(col)).strip()
                            label = re.sub(r'(.+?)(\[[a-z0-9]+\]\s+)+.*', r"\1", str(col)).strip()
                            note_label = re.sub(r'.+?((\[[a-z0-9]+\]\s+)+).*', r"\1", str(col)).strip()
                        else:
                            code = re.sub(r'.+?([A-Z]{3,}[0-9]*).*', r"\1", str(col)).strip()
                            label = re.sub(r'(.+?)[A-Z]{3,}[0-9]*.*', r"\1", str(col)).strip()
                            note_label = None
                        new_columns.append([code, label, note_label])
                INTLINE_temp.columns = pd.MultiIndex.from_tuples(new_columns)
                if len(header) > 2:
                    web_notes = WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/section[contains(., "Notes:")]'))).text
                    note_data = re.split(r'\n', web_notes)
                    for no in note_data:
                        if bool(re.match(r'\[[a-z]\]', no.strip())):
                            note.append([re.sub(r'(\[[a-z]\]).+', r"\1", no.strip()), re.sub(r'\[[a-z]\](.+)', r"\1", no.strip()).strip()])
                link_found = True
                """for col in range(INTLINE_temp.shape[1]):
                    if type(INTLINE_temp.columns) == pd.core.indexes.multi.MultiIndex:
                        code = str(INTLINE_temp.columns[col][0]).strip()
                    else:
                        code = str(INTLINE_temp.columns[col]).strip()
                    try:
                        note[code] = chrome.find_element_by_xpath('.//tr[td[@width="12%"]/label/b[text()="'+code+'"]]/td[@width="78%"]/b').text
                    except NoSuchElementException:
                        note[code] = re.sub(r'\(\*[0-9]+\)', "", chrome.find_element_by_xpath('.//tr[td[@width="12%"]/label/b[text()="'+code+'"]]/td[@width="78%"]').text).strip()"""
            elif address.find('EST') >= 0:
                chrome.find_element_by_xpath('.//a[contains(., "Download")]').click()
                chrome.find_element_by_xpath('.//input[@value="Download in CSV Format"]').click()
                while True:
                    try:
                        WebDriverWait(chrome, 5).until(EC.presence_of_element_located((By.XPATH, './/p[contains(., "Extraction complete")]')))
                    except TimeoutException:
                        break
                link_found = True
            elif address.find('ECB') >= 0:
                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/span[@class="download"]'))).click()
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Excel', text_match=True)
            elif address.find('BEIS') >= 0:
                #WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/a[text()="'+str(sname)+'"]'))).click()
                target = WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/a[text()="'+str(sname)+'"]'))).get_attribute('href')
                chrome.get(target)
                link_found = True
            elif address.find('COJ') >= 0 or address.find('JGBY') >= 0 or address.find('JPC') >= 0 or address.find('WKHH') >= 0 or sname == 'WorldCrudeSteelProduction':
                if address.find('COJ') >= 0 and freq != 'M':
                    SNA = 0
                    note_content = re.sub(r'\-+.*', "", chrome.find_element_by_xpath('.//div[@id="mainContents"]/h2').text).strip()
                if fname.find('kakuhou') >= 0:
                    WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[@id="mainContents"]/h3/a'))).click()
                elif fname.find('sokuhou') >= 0:
                    li = 1
                    while True:
                        bulletList = chrome.find_elements_by_xpath('.//div[@id="mainContents"]/ul[@class="bulletList"]')
                        if li > len(bulletList[SNA].find_elements_by_xpath('.//li')):
                            SNA += 1
                            li = 1
                            note_content = re.sub(r'\-+.*', "", chrome.find_element_by_xpath('.//div[@id="mainContents"]/h2[position()='+str(SNA+1)+']').text).strip()
                        bulletList[SNA].find_element_by_xpath('.//li[position()='+str(li)+']/a').click()
                        p = 1
                        if str(sname).find('kdef') >= 0 or str(sname).find('kgaku') >= 0:
                            targets = chrome.find_elements_by_xpath('.//div[@id="mainContents"]/table/tbody/tr')
                            position_found = False
                            for t in range(len(targets)):
                                if targets[t].text.find('Second') >= 0:
                                    position_found = True
                                    p = t+1
                                    break
                                else:
                                    continue
                            if position_found == False:
                                li += 1
                                chrome.back()
                                continue
                        break
                    note_content = note_content+', '+re.sub(r'.+?(Benchmark.+?)\).*', r"\1", chrome.find_element_by_xpath('.//div[@id="mainContents"]/table/tbody/tr[position()='+str(p)+']/td[position()=2]').text.replace('=',' = ')).strip()
                    WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[@id="mainContents"]/table/tbody/tr[position()='+str(p)+']/td[position()=2]/a'))).click() 
                if sname == 'Indexes of Business Conditions':
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Historical Data', text_match=True)
                else:
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=str(sname))
                if address.find('COJ') >= 0 and freq != 'M':
                    note.append(['Note', re.sub(r'\s+', " ", note_content)])
            elif address.find('BOJ') >= 0 and str(sname).find('BOJ') >= 0:
                dataFrame = Series[freq].loc[Series[freq]['DataSet'] == str(sname)]
                search = chrome.find_element_by_name('txtDirect')
                for ind in range(dataFrame.shape[0]):
                    search.send_keys(str(dataFrame.iloc[ind]['keyword'])+'\n')
                chrome.find_element_by_name('btmSubmit').click()
                time.sleep(2)
                chrome.find_element_by_class_name('tableDataCodeHeader').click()
                chrome.find_element_by_xpath('.//label[@for="directInputDataCodeListSelectAll"]').click()
                chrome.find_element_by_id('columnNameSearch').click()
                chrome.find_element_by_id('fromYear').send_keys(start_year)
                chrome.find_element_by_xpath('.//a[@onclick="submit_code_main(document.nme_S050_en_form)"]').click()
                chrome.switch_to.window(chrome.window_handles[-1])
                chrome.find_element_by_xpath('.//label[text()="Header"]').click()
                chrome.find_element_by_xpath('.//a[text()="Download"]').click()
                chrome.close()
                chrome.switch_to.window(chrome.window_handles[-1])
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='.csv')
                chrome.close()
                chrome.switch_to.window(chrome.window_handles[0])
                chrome.refresh()
            elif address.find('FSA') >= 0:
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='March', text_match=True)
                target = chrome.find_element_by_xpath('.//tr[td/text()="'+str(sname)+'"]')
                link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='.xls')
            elif address.find('METI') >= 0:
                if address.find('IIPD') >= 0:
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Historical Data', text_match=True)
                if address.find('WTRS') >= 0:
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=str(sname))
                else:
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='_'+str(sname))
            elif address.find('MHLW') >= 0 or address.find('EMPL') >= 0:
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='file-download')
            elif address.find('MCPI') >= 0:
                FILE = {'s': 'Subgroup', 'r': 'less imputed rent', 'b': 'Goods and Service', 'c': 'Seasonally Adjusted'}
                target = chrome.find_element_by_xpath('.//div[@class="stat-search_result-list js-items"]')
                link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='Consumer Price Index', text_match=True)
                if str(sname)[0] == 'z':
                    AREA = chrome.find_element_by_xpath('.//li[div[contains(., "Japan")]]')
                elif str(sname)[0] == 't':
                    AREA = chrome.find_element_by_xpath('.//li[div[contains(., "Ku-area of Tokyo")]]')
                link_found, link_meassage = INTLINE_WEB_LINK(AREA, fname, keyword='Monthly', text_match=True)
                category = chrome.find_element_by_xpath('.//ul[li[contains(., "Table number")][contains(., "1")]][contains(., "'+FILE[str(sname)[-1]]+'")]')
                link_found, link_meassage = INTLINE_WEB_LINK(category, fname, keyword='file-download')
            elif address.find('RPKT') >= 0:
                month_list = chrome.find_elements_by_xpath('.//div[@class="stat-cycle_sheet"]/ul[contains(., "'+str(datetime.today().year)+'")]/li[@class="stat-cycle_item"]/div')
                target_year = datetime.today().year
                if not month_list:
                    month_list = chrome.find_elements_by_xpath('.//div[@class="stat-cycle_sheet"]/ul[contains(., "'+str(datetime.today().year-1)+'")]/li[@class="stat-cycle_item"]/div')
                    target_year = datetime.today().year-1
                    if not month_list:
                        ERROR('Latest Monthly Data Not Found.')
                target = month_list[-1]
                link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword=str(target_year))
                target2 = chrome.find_element_by_xpath('.//article[contains(., "Retail Prices of Major Items for Ku-area of Tokyo")]/following-sibling::article[contains(., "Kimono")]')
                link_found, link_meassage = INTLINE_WEB_LINK(target2, fname, keyword='file-download')
            elif address.find('NIKK') >= 0:
                chrome.switch_to.frame('chart_iframe')
                chrome.find_element_by_xpath('.//button[@id="dataDownload"]').click()
                link_found = True
            elif address.find('NBS') >= 0:
                try:
                    WebDriverWait(chrome, 1).until(EC.visibility_of_element_located((By.XPATH, './/div[contains(., "验证码访问")]')))
                except TimeoutException:
                    time.sleep(0)
                else:
                    input('此網站需輸入驗證碼，確認輸入完成後按Enter')
                    chrome.get(fname)
                if previous == True:
                    first_key = re.split(r', ', str(Series[freq].loc[Series[freq]['Previous_DataSet'] == str(sname)].iloc[0]['keyword']))[0]
                    route = re.split(r'//', str(Series[freq].loc[Series[freq]['Previous_DataSet'] == str(sname)].iloc[0]['Previous_Routes']))
                else:
                    first_key = re.split(r', ', str(Series[freq].loc[Series[freq]['DataSet'] == str(sname)].iloc[0]['keyword']))[0]
                    route = re.split(r'//', str(Series[freq].loc[Series[freq]['DataSet'] == str(sname)].iloc[0]['Routes']))
                while True:
                    try:
                        target = chrome
                        for item in route:
                            count = 0
                            while True:
                                sys.stdout.write("\rGetting Route Item: "+str(item)+" "*200)
                                sys.stdout.flush()
                                try:
                                    if bool(re.search(r'\[[0-9]+\]$', item)):
                                        time.sleep(1)
                                        target.find_elements_by_link_text(re.sub(r'\[[0-9]+\]\s*$', "", item))[int(re.sub(r'.+?\[([0-9]+)\]\s*$', r"\1", item))].click()
                                    else:
                                        WebDriverWait(target, 3).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, str(item)))).click()
                                        target = target.find_element_by_partial_link_text(str(item)).find_element_by_xpath('..')
                                    ActionChains(chrome).send_keys(Keys.DOWN).send_keys(Keys.DOWN).send_keys(Keys.DOWN).perform()
                                except (ElementNotInteractableException, ElementClickInterceptedException):
                                    if count > 3:
                                        raise ElementNotInteractableException
                                    ActionChains(chrome).send_keys(Keys.DOWN).send_keys(Keys.DOWN).send_keys(Keys.DOWN).perform()
                                    count += 1
                                else:
                                    break
                            time.sleep(1)
                        chrome.find_element_by_xpath('.//div[@class="dtHtml"]').click()
                        if freq == 'A':
                            default_year = 1949
                        elif freq == 'Q':
                            default_year = 1986
                        elif freq == 'M':
                            default_year = 1983
                        if start_year != None and start_year > default_year:
                            styr = start_year
                        else:
                            styr = default_year
                        time.sleep(1)
                        chrome.find_element_by_xpath('.//input[@class="dtText"]').send_keys(str(styr)+'-')
                        sys.stdout.write("\rGetting Table from Year: "+str(styr)+" "*200)
                        sys.stdout.flush()
                        chrome.find_element_by_xpath('.//div[@class="dtTextBtn f10"]').click()
                        time.sleep(2)
                        INTLINE_temp = pd.read_html(chrome.page_source, header=header, index_col=index_col)[0]
                        if str(INTLINE_temp.columns[-1])[-4:] != str(styr):
                            raise KeyError
                        if str(INTLINE_temp.index).replace(' ','').find(first_key) < 0:
                            chrome.refresh()
                            raise KeyError
                    except (KeyError, ElementNotInteractableException):
                        time.sleep(1)
                    except TimeoutException as t:
                        print(traceback.format_exc())
                        ERROR('Link Text Not Found for item: '+str(item))
                    except Exception as e:
                        print(traceback.format_exc())
                        ERROR(str(e))
                    else:
                        break
                sys.stdout.write("\rDownload Completed"+" "*200)
                sys.stdout.flush()
                link_found = True
                chrome.refresh()
                sys.stdout.write('\n\n')
            elif address.find('PPI') >= 0:
                INTLINE_temp = pd.DataFrame()
                try:
                    pages = int(re.sub(r'.*?页次:[0-9]+/([0-9]+).*', r"\1", chrome.find_element_by_xpath('.//div[@id="wrap_list_data"]/div[contains(., "页次")]').text.replace('\n','')))
                except NoSuchElementException:
                    chrome.refreah()
                    raise FileNotFoundError
                IN_t = pd.read_html(chrome.page_source, header=header, index_col=index_col)[0]
                INTLINE_temp = pd.concat([INTLINE_temp, IN_t])
                for page in range(2, pages+1):
                    target = chrome.find_element_by_xpath('.//div[@id="wrap_list_data"]/div[contains(., "页次")]')
                    link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='btn_page_jump_'+str(page), get_attribute='id')
                    time.sleep(1)
                    IN_t = pd.read_html(chrome.page_source, header=header, index_col=index_col)[0]
                    INTLINE_temp = pd.concat([INTLINE_temp, IN_t])
            elif address.find('SIPR') >= 0:
                INTLINE_temp = pd.DataFrame()
                for yr in range(datetime.today().year-1, datetime.today().year+1):
                    chrome.find_element_by_xpath('.//div[@class="wengao2"]/a[text()="'+str(yr)+'年统计数据"]').click()
                    try:
                        chrome.find_element_by_xpath('.//a[contains(., "Financial Market Statistics")]').click()
                    except NoSuchElementException:
                        time.sleep(1)
                    try:
                        target = chrome.find_element_by_xpath('.//tr[td[div[contains(text(), "回购交易")]]]')
                        link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='htm', text_match=True)
                    except NoSuchElementException:
                        chrome.find_element_by_xpath('.//a[contains(text(), "回购交易")]').click()
                    chrome.switch_to.window(chrome.window_handles[-1])
                    IN_t = pd.read_html(chrome.page_source, skiprows=skiprows, header=header, index_col=index_col)[0]
                    IN_t.columns = [str(col[0]).strip() if str(col[1]).find('加权平均利率') >= 0 else None for col in IN_t.columns]
                    IN_t = IN_t.loc[:, IN_t.columns.dropna()]
                    INTLINE_temp = pd.concat([INTLINE_temp, IN_t])
                    INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna()]
                    chrome.close()
                    chrome.switch_to.window(chrome.window_handles[0])
                    chrome.get(fname)
            elif address.find('GACC/SUM') >= 0:
                target = chrome.find_element_by_xpath('.//tr[contains(., "Summary of Imports and Exports") and contains(., "Monthly")]')
                target.find_elements_by_xpath('.//*[@href]')[-1].click()
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Excel')
            elif address.find('HKCSD') >= 0:
                if str(fname).find('web_table') >= 0:
                    while True:
                        try:
                            WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, 'full_series_button'))).click()
                            if chrome.find_element_by_id('full_series_button').get_attribute('style') == '':
                                raise IndexError
                            target = chrome.find_element_by_id('default_table').get_attribute('outerHTML')
                            INTLINE_temp = pd.read_html(target, skiprows=skiprows, header=header, index_col=index_col)[0]
                        except IndexError:
                            time.sleep(1)
                        else:
                            time.sleep(2)
                            link_found = True
                            break
                else:
                    time.sleep(2)
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='xlsx')
            elif address.find('HKCPI') >= 0:
                target = WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/tr[contains(., "E501 ")]')))
                link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='xlsx')
            elif address.find('DOS') >= 0:
                target = chrome.find_element_by_id('TableContainerDiv').get_attribute('outerHTML')
                INTLINE_temp = pd.read_html(target, skiprows=skiprows, header=header, index_col=index_col)[0]
                note_content = re.sub(r'.+?(In Chained.+?Dollars).*|.+?(SSIC.+?)\).*|.+?\((At.+?Prices)\).*', r"\1, \2, \3", chrome.find_element_by_xpath('.//td[a[@class="metadata"]]').text).strip(', ')
                if note_content != ''and note_content.find(str(sname)) < 0:
                    note.append(['Note', re.sub(r'\s+', " ", note_content)])
                link_found = True
            elif address.find('MAS/SGSY') >= 0:
                Select(chrome.find_element_by_id('ContentPlaceHolder1_StartYearDropDownList')).select_by_index(0)
                Select(chrome.find_element_by_id('ContentPlaceHolder1_StartMonthDropDownList')).select_by_index(0)
                Select(chrome.find_element_by_id('ContentPlaceHolder1_EndYearDropDownList')).select_by_index(len(Select(chrome.find_element_by_id('ContentPlaceHolder1_EndYearDropDownList')).options)-1)
                Select(chrome.find_element_by_id('ContentPlaceHolder1_EndMonthDropDownList')).select_by_index(len(Select(chrome.find_element_by_id('ContentPlaceHolder1_EndMonthDropDownList')).options)-1)
                Select(chrome.find_element_by_id('ContentPlaceHolder1_FrequencyDropDownList')).select_by_value('M')
                while True:
                    chrome.find_element_by_xpath('.//label[@for="ContentPlaceHolder1_FiveYearBondYieldCheckBox"]').click()
                    chrome.find_element_by_xpath('.//input[@value="Download"]').click()
                    try:
                        if chrome.find_element_by_xpath('.//div[@id="ContentPlaceHolder1_FormValidationSummary"]').text != '':
                            continue
                        else:
                            break
                    except NoSuchElementException:
                        break
                link_found = True
            elif address.find('BOK') >= 0:
                FREQ = {'A':['Yearly','1953'], 'Q':['Quarterly','1960'], 'M':['Monthly','1960']}
                Routes = [re.split(r'//', str(Series[freq].loc[Series[freq]['DataSet'] == str(sname)].iloc[k]['Routes'])) for k in range(Series[freq].loc[Series[freq]['DataSet'] == str(sname)].shape[0])]
                count = 0
                while True:
                    try:
                        WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="form-group-circle"][contains(., "Cycle")]/div'))).click()
                        chrome.find_element_by_xpath('.//div[@role="option"][contains(., "'+FREQ[freq][0]+'")]').click()
                        for route in Routes:
                            target = chrome
                            section_change = False
                            for item in route:
                                sys.stdout.write("\rGetting Route Item: "+str(item)+" "*100)
                                sys.stdout.flush()
                                if WebDriverWait(target, 20).until(EC.element_to_be_clickable((By.XPATH, './/tr[contains(., "'+str(item)+'")]'))).get_attribute('aria-expanded') == 'true':
                                    continue
                                elif WebDriverWait(target, 20).until(EC.element_to_be_clickable((By.XPATH, './/tr[contains(., "'+str(item)+'")]'))).get_attribute('aria-expanded') == None:
                                    section_change = True
                                WebDriverWait(target, 20).until(EC.element_to_be_clickable((By.XPATH, './/tr[contains(., "'+str(item)+'")]'))).click()
                                time.sleep(1)
                                if section_change == True:
                                    target = chrome.find_element_by_xpath('.//section[@store-domain="classSearch"]')
                            WebDriverWait(target, 3).until(EC.element_to_be_clickable((By.XPATH, './/td[contains(., "'+str(route[-1])+'")]/div/div[@role="checkbox"]'))).click()
                            if str(sname).find('_NA2') >= 0:
                                WebDriverWait(target, 3).until(EC.element_to_be_clickable((By.XPATH, './/td[contains(., "'+str(route[-2])+'")]/div/div[@role="checkbox"]'))).click()
                            chrome.find_element_by_xpath('.//button[@class="add"]').click()
                            sys.stdout.write('\n\n')
                    except (TimeoutException, StaleElementReferenceException):
                        count += 1
                        if count > 3:
                            raise ElementNotInteractableException
                        chrome.refresh()
                    else:
                        break
                while True:
                    try:
                        chrome.find_element_by_xpath('.//div[@class="form-group"][contains(., "Order")]/div').click()
                        chrome.find_element_by_xpath('.//div[@role="option"][contains(., "Asc.")]').click()
                        chrome.find_element_by_xpath('.//div[@class="calendar"]//div[@class="dx-texteditor-container"]').click()
                        ActionChains(chrome).send_keys(Keys.BACKSPACE).send_keys(FREQ[freq][1]).send_keys(Keys.ENTER).perform()
                        #if freq != 'M':
                        chrome.find_element_by_xpath('.//div[@role="checkbox"][contains(., "So far")]').click()
                        chrome.find_element_by_xpath('.//span[contains(., "Download")]').click()
                    except ElementClickInterceptedException:
                        time.sleep(0)
                    else:
                        break
                time.sleep(3)
                chrome.switch_to.window(chrome.window_handles[-1])
                chrome.find_element_by_xpath('.//a[contains(., "Download")]').click()
                chrome.switch_to.window(chrome.window_handles[0])
                link_found = True
                chrome.refresh()
            elif address.find('KOSTAT') >= 0:
                FREQ = {'Q':'Quarterly', 'M':'Monthly'}
                key_suffix = ''
                if previous == True:
                    key_suffix = 'Previous_'
                route = re.split(r'//', str(Series[freq].loc[Series[freq][key_suffix+'DataSet'] == str(sname)].iloc[0][key_suffix+'Routes']))
                count = 0
                while True:
                    try:
                        target = chrome
                        for item in route:
                            sys.stdout.write("\rGetting Route Item: "+str(item)+" "*100)
                            sys.stdout.flush()
                            target = WebDriverWait(target, 20).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, str(item)))).find_element_by_xpath('..')
                            if target.get_attribute('class') == 'FolderOpen':
                                continue
                            if WebDriverWait(target, 20).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, str(item)))).text.find('Standards') >= 0:
                                note_content = re.sub(r'.*?([0-9]{4}.*?Standards).*', r"\1", WebDriverWait(target, 20).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, str(item)))).text)
                            WebDriverWait(target, 20).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, str(item)))).click()
                            time.sleep(1)
                    except (TimeoutException, StaleElementReferenceException) as e:
                        print(traceback.format_exc())
                        count += 1
                        if count > 3:
                            raise ElementNotInteractableException
                    else:
                        sys.stdout.write('\n\n')
                        break
                chrome.switch_to.frame(chrome.find_element_by_xpath('.//iframe[@title[contains(., "'+str(route[-1])+'")]]'))
                WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/p[@class="leftBtn"]'))).click()
                chrome.switch_to.frame('ifrSearchDetail')
                for button in WebDriverWait(chrome, 20).until(EC.presence_of_all_elements_located((By.XPATH, './/img[@title="Clear all"]'))):
                    button.click()
                Attributes = []
                for k in range(Series[freq].loc[Series[freq][key_suffix+'DataSet'] == str(sname)].shape[0]):
                    Attributes.extend(re.split(r', ', str(Series[freq].loc[Series[freq][key_suffix+'DataSet'] == str(sname)].iloc[k]['keyword'])))
                Attributes = list(set(Attributes))
                for attri in Attributes:
                    chrome.find_element_by_xpath('.//option[contains(., "'+str(attri)+'")]').click()
                    chrome.find_element_by_xpath('.//div[@class="detailPart"][contains(., "'+str(attri)+'")]//img[@title="Additional"]').click()
                chrome.find_element_by_xpath('.//li[text()="'+FREQ[freq]+'"]').click()
                chrome.find_element_by_xpath('.//div[@class="detailPart"][contains(., "'+FREQ[freq]+'")]//img[@title="Additional all"]').click()
                for r in range(6):
                    ActionChains(chrome).send_keys(Keys.RIGHT).perform()
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Apply', text_match=True)
                try:
                    chrome.switch_to.parent_frame()
                except UnexpectedAlertPresentException:
                    chrome.switch_to.alert.accept()
                    chrome.switch_to.parent_frame()
                time.sleep(3)
                WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Pivot"]'))).click()
                for item in chrome.find_elements_by_xpath('.//ul[@id="ulRight"]//li'):
                    if item.text.find('Time Period') < 0:
                        item.click()
                        chrome.find_element_by_xpath('.//img[@title="Move to the left"]').click()
                link_found, link_meassage = INTLINE_WEB_LINK(chrome.find_element_by_xpath('.//div[@id="pop_pivotfunc"]'), fname, keyword='Apply', text_match=True)
                time.sleep(3)
                WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/button[@id="ico_download"]'))).click()
                chrome.find_element_by_xpath('.//input[@value="original"]').click()
                link_found, link_meassage = INTLINE_WEB_LINK(chrome.find_element_by_xpath('.//div[@id="pop_downgrid"]'), fname, keyword='downGridSubmit')
                chrome.switch_to.default_content()
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Statistical list', text_match=True)
                if freq == 'Q' and previous == False:
                    note.append(['Note', re.sub(r'\s+', " ", note_content)])
            elif address.find('KERI') >= 0:
                file_month = file_name[-3:].replace('-0','-').replace('-','')
                page = 1
                while True:
                    try:
                        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/tr[contains(., "'+file_month+'월")][contains(., "'+file_name[:4]+'.")]//a[@class="download"]'))).click()
                    except TimeoutException:
                        page+=1
                        try:
                            WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/a[text()="'+str(page)+'"]'))).click()
                        except TimeoutException:
                            raise KeyError('Date of data file '+str(file_name)+' Not Found.')
                    else:
                        break
                chrome.switch_to.window(chrome.window_handles[-1])
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='xlsx', text_match=True)
                chrome.close()
                chrome.switch_to.window(chrome.window_handles[0])
            elif address.find('RBA') >= 0:
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=str(sname)+'.')
                try:
                    ActionChains(chrome).send_keys(Keys.ALT).perform()
                except UnexpectedAlertPresentException:
                    time.sleep(0)
            elif address.find('ABS') >= 0:
                if str(fname).find('data-download') < 0:
                    WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/div[@id="content"]//div[contains(., "Latest release")]//a'))).click()
                download_area = 'series spreadsheets'
                if str(fname).find('labour-force-australia-detailed') >= 0:
                    download_area = 'Unemployment'
                try:
                    WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/div[contains(., "'+download_area+'")]//button[@class="button button--showall"]'))).click()
                except TimeoutException:
                    try:
                        WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/div[contains(., "Data downloads")]//button[@class="button button--showall"]'))).click()
                    except TimeoutException:
                        time.sleep(0)
                count = 0
                while True:
                    try:
                        link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=str(sname).replace('_Trend','')+'.')
                    except ElementClickInterceptedException:
                        chrome.execute_script("window.scrollTo(0,0)")
                        count += 1
                        if count > 3:
                            ERROR('Download File Not Found.')
                    else:
                        time.sleep(2)
                        break
            elif address.find('DEMP') >= 0:
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='xls')
            elif address.find('SNDO') >= 0:
                chrome.switch_to.frame(WebDriverWait(chrome, 20).until(EC.presence_of_element_located((By.TAG_NAME, 'iframe'))))
                WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/td[contains(., "DETAILED STATEMENT")]')))
                time.sleep(3)
                WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/td[@title="Select year"]'))).click()
                time.sleep(3)
                WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/td[contains(., "DETAILED STATEMENT")]'))).click()
                time.sleep(3)
                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/body[@class="QvPageBody"]'))).click()
                time.sleep(3)
                ActionChains(chrome).context_click(WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/body[@class="QvPageBody"]')))).perform()
                target = WebDriverWait(chrome, 3).until(EC.presence_of_element_located((By.XPATH, './/ul[@class="ctx-menu popup-shadow ctx-menu-no-icons"]')))
                chrome.execute_script("arguments[0].setAttribute('style', 'display: block;')", target)
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Excel', text_match=True)
            elif address.find('SCB') >= 0:
                try:
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="breadcrumb_container"][contains(., "Old tables not updated")]')))
                except TimeoutException:
                    time.sleep(0)
                else:
                    if str(sname).find('2005') < 0:
                        ERROR('The base year of Index has been modified. Please find the table with the latest Index and do the modification on tableINT.')
                note_content = re.sub(r'.*?\((.+?)\).*', r"\1", chrome.find_element_by_xpath('.//span[@class="hierarchical_tableinformation_title"]').text)
                target = WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/select[@class="commandbar_saveas_dropdownlist"]')))
                Select(target).select_by_visible_text('Excel (xlsx)')
                ActionChains(chrome).send_keys(Keys.ENTER).perform()
                link_found = True
                if note_content.find('ESA') >= 0:
                    note.append(['Note', re.sub(r'\s+', " ", note_content)])
            elif address.find('RKB') >= 0:
                try:
                    WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/a[@class="button js-accept-cookies"]'))).click()
                except TimeoutException:
                    time.sleep(0)
                try:
                    WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/button[contains(., "I accept")]'))).click()
                except TimeoutException:
                    time.sleep(0)
                if chrome.find_element_by_xpath('.//div[a[contains(., "Swedish Market (based) rates")]]').get_attribute('class').find('open') < 0:
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Swedish Market (based) rates', text_match=True)
                if chrome.find_element_by_xpath('.//div[a[contains(., "Swedish Government Bonds")]]').get_attribute('class').find('open') < 0:
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Swedish Government Bonds', text_match=True)
                chrome.find_element_by_xpath('.//label[contains(., "SE GVB 10Y")]/input').click()
                if chrome.find_element_by_xpath('.//div[a[contains(., "Mortgage Bonds")]]').get_attribute('class').find('open') < 0:
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Mortgage Bonds', text_match=True)
                chrome.find_element_by_xpath('.//label[contains(., "MB 2Y")]/input').click()
                chrome.find_element_by_xpath('.//label[contains(., "MB 5Y")]/input').click()
                chrome.find_element_by_id('Month').click()
                chrome.find_element_by_id('Dot').click()
                start = '01/01/'+str(datetime.today().year-10)
                end = datetime.today().strftime('%d/%m/%Y')
                while True:
                    ActionChains(chrome).click(chrome.find_element_by_id('datetime-from')).key_down(Keys.CONTROL).send_keys('A').send_keys(Keys.BACKSPACE).key_up(Keys.CONTROL).send_keys(start).send_keys(Keys.ENTER).perform()
                    ActionChains(chrome).click(chrome.find_element_by_id('datetime-to')).key_down(Keys.CONTROL).send_keys('A').send_keys(Keys.BACKSPACE).key_up(Keys.CONTROL).send_keys(end).send_keys(Keys.ENTER).perform()
                    ActionChains(chrome).send_keys(Keys.ENTER).perform()
                    try:
                        WebDriverWait(chrome, 3).until(EC.presence_of_element_located((By.XPATH, './/span[@class="js-validation-date error-text error-text--visible"]')))
                    except TimeoutException:
                        time.sleep(0)
                    else:
                        start = (datetime.strptime(start, '%d/%m/%Y')+timedelta(days=1)).strftime('%d/%m/%Y')
                        continue
                    try:
                        WebDriverWait(chrome, 3).until(EC.presence_of_element_located((By.XPATH, './/span[@class="js-validation-date error-text second-col error-text--visible"]')))
                    except TimeoutException:
                        break
                    else:
                        end = (datetime.strptime(end, '%d/%m/%Y')-timedelta(days=1)).strftime('%d/%m/%Y')
                        continue
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='xlsx')
            elif address.find('MOSPI') >= 0:
                if address.find('KAPSARC') >= 0:
                    time.sleep(2)
                    target = WebDriverWait(chrome, 20).until(EC.presence_of_element_located((By.XPATH, './/div[@format-extension="csv"]')))
                    link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='selected records', text_match=True)
                elif address.find('NAD') >= 0:
                    note_content = re.sub(r'.*?,\s*(.+)', r"\1", chrome.find_element_by_xpath('.//a[contains(., "'+str(file_name)+'")]').text).strip()
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=file_name, text_match=True)
                    note.append(['Note', re.sub(r'\s+', " ", note_content)])
                elif address.find('NAS') >= 0:
                    Select(WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.ID, 'edit-main-cat')))).select_by_value('All')
                    WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.ID, 'edit-combine'))).send_keys('National Accounts Statistics')
                    WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.ID, 'edit-submit-statistical-publications'))).click()
                    time.sleep(2)
                    WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/table/tbody/tr//a'))).click()
                    chrome.switch_to.window(chrome.window_handles[-1])
                    target = WebDriverWait(chrome, 20).until(EC.presence_of_element_located((By.XPATH, './/tr[contains(., "'+str(file_name)+'")]')))
                    link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='xlsx')
                    chrome.close()
                    chrome.switch_to.window(chrome.window_handles[0])
                    chrome.refresh()
                elif address.find('STATYB') >= 0:
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Statistical Year Book India', text_match=True)
                    chrome.switch_to.window(chrome.window_handles[-1])
                    pages = len(WebDriverWait(chrome, 20).until(EC.presence_of_all_elements_located((By.XPATH, './/li[@class="pager-item"]'))))+1
                    p = 1
                    while True:
                        link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='TOURISM', text_match=True)
                        if link_found == False:
                            p+=1
                            if p > pages:
                                ERROR(link_meassage)
                            WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/li[@class="pager-item"][a[@title="Go to page '+str(p)+'"]]'))).click()
                            time.sleep(2)
                        else:
                            break
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=file_name, text_match=True)
                    chrome.close()
                    chrome.switch_to.window(chrome.window_handles[0])
                elif address.find('IIP') >= 0:
                    WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/button[contains(., "Data")]'))).click()
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=file_name, text_match=True)
                elif address.find('APEDA') >= 0:
                    target = WebDriverWait(chrome, 20).until(EC.presence_of_element_located((By.ID, 'GDvwyear')))
                    INTLINE_temp = pd.read_html(target.get_attribute('outerHTML'), header=[0,1], index_col=1)[0].applymap(lambda x: float(x) if str(x)[0].isnumeric() else x)
                    link_found = True
            elif address.find('RBI') >= 0:
                try:
                    route = re.split(r'//', str(Series[freq].loc[Series[freq]['DataSet'] == str(sname)].iloc[0]['Routes']))
                except IndexError:
                    route = re.split(r'//', str(Series[freq].loc[Series[freq]['DataSet'] == str(sname)+', '+str(tables[0])].iloc[0]['Routes']))
                if str(fname).find('statistics') >= 0:
                    ActionChains(chrome).move_to_element(WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/a[contains(., "'+str(route[0])+'")]')))).perform()
                    time.sleep(1)
                    WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/a[contains(., "'+str(route[1])+'")]'))).click()
                    if freq == 'M':
                        ActionChains(chrome).move_to_element(WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/a[contains(., "Yearly")]')))).perform()
                        WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/a[contains(., "'+str(route[2])+'")]'))).click()
                        time.sleep(2)
                    elif freq == 'A':
                        time.sleep(2)
                    while True:
                        try:
                            WebDriverWait(chrome, 20).until(EC.presence_of_element_located((By.XPATH, './/iframe[@name="_ddajaxtabsiframe-petsdivcontainer"]')))
                            chrome.switch_to.frame('_ddajaxtabsiframe-petsdivcontainer')
                            WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/a[text()="'+str(route[3])+'"]'))).click()
                        except TimeoutException:
                            WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/a[contains(., "'+str(route[2])+'")]'))).click()
                        else:
                            break
                elif str(fname).find('publications') >= 0:
                    chrome.refresh()
                    if str(fname).find('!6') < 0:
                        WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/tr[@id="'+str(route[0])+'"]//img'))).click()
                    WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/a[contains(., "'+str(route[1])+'")]'))).click()
                while True:
                    try:
                        for frame_item in ['reportFrame','openDocChildFrame','webiViewFrame']:
                            sys.stdout.write("\rLoading frame: "+frame_item+" "*200)
                            sys.stdout.flush()
                            chrome.set_page_load_timeout(150)
                            WebDriverWait(chrome, 150).until(EC.frame_to_be_available_and_switch_to_it((By.ID, frame_item)))
                        sys.stdout.write("\rRefresh"+" "*200)
                        sys.stdout.flush()
                        try:
                            WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/table[@title="Refresh"]'))).click()
                            while True:
                                if WebDriverWait(chrome, 20).until(EC.presence_of_element_located((By.ID, 'waitDlg'))).get_attribute('style').find('hidden') >= 0:
                                    time.sleep(2)
                                    break
                            ActionChains(chrome).send_keys(Keys.ENTER).perform()
                            while True:
                                if WebDriverWait(chrome, 20).until(EC.presence_of_element_located((By.ID, 'waitDlg'))).get_attribute('style').find('hidden') >= 0:
                                    time.sleep(2)
                                    break
                        except TimeoutException:
                            time.sleep(0)
                        sys.stdout.write("\rExport Document"+" "*200)
                        sys.stdout.flush()
                        WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/table[@title="Export"]'))).click()
                        WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/span[contains(., "Export Document As")]'))).click()
                        WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/span[contains(., ".xlsx")]'))).click()
                    except (TimeoutException, NoSuchFrameException, ElementClickInterceptedException):
                        if str(fname).find('statistics') >= 0:
                            chrome.switch_to.default_content()
                            WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/a[contains(., "'+str(route[2])+'")]'))).click()
                            chrome.switch_to.frame('_ddajaxtabsiframe-petsdivcontainer')
                            time.sleep(1)
                            WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/a[text()="'+str(route[3])+'"]'))).click()
                        elif str(fname).find('publications') >= 0:
                            chrome.refresh()
                            if str(fname).find('!6') < 0:
                                WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/tr[@id="'+str(route[0])+'"]//img'))).click()
                            WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/a[contains(., "'+str(route[1])+'")]'))).click()
                    else:
                        sys.stdout.write("\rDownload Completed"+" "*200)
                        sys.stdout.flush()
                        sys.stdout.write('\n\n')
                        time.sleep(2)
                        break
                link_found = True
            elif address.find('MOCI') >= 0:
                INTLINE_temp = pd.DataFrame()
                for yr in [datetime.today().year-1, datetime.today().year]:
                    Select(WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/select[@name="yy1"]')))).select_by_visible_text(str(yr)+'-'+str(yr+1))
                    Select(WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/select[@name="rgnid"]')))).select_by_visible_text('All')
                    WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.ID, 'radiousd'))).click()
                    WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="Submit"]'))).click()
                    IN_t = pd.read_html(chrome.page_source, header=[0], index_col=1)[0]
                    IN_t.columns = [int(col.strip()[:4]) if (str(col.strip()[-4:]).isnumeric() and int(col.strip()[-4:]) == yr) else None for col in IN_t.columns]
                    IN_t = IN_t.loc[:, IN_t.columns.dropna()]
                    INTLINE_temp = pd.concat([INTLINE_temp, IN_t], axis=1)
                    chrome.back()
                link_found = True
            elif address.find('CANSIMS') >= 0:
                if str(sname).find('housing') >= 0 or str(sname).find('Stock') >= 0 or str(sname).find('Financial') >= 0:
                    html_skip = [0]
                elif str(sname).find('Building permits') >= 0:
                    html_skip = [0,1,2]
                elif str(sname).find('International merchandise trade') >= 0:
                    html_skip = [0,1,2,3]
                try:
                    WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.ID, 'timeframe-lnk'))).click()
                except TimeoutException:
                    time.sleep(0)
                if freq == 'W':
                    WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.ID, 'enddate'))).send_keys(str(datetime.today().year)+'/12/31')
                else:
                    try:
                        Select(WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.ID, 'endYear')))).select_by_value(str(datetime.today().year))
                    except NoSuchElementException:
                        Select(WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.ID, 'endYear')))).select_by_value(str(datetime.today().year-1))
                WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/button[contains(., "Apply")]'))).click()
                WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/button[contains(., "Apply")]')))
                INTLINE_temp = pd.read_html(chrome.page_source, skiprows=html_skip, header=header, index_col=index_col)[0]
                INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x)[0].isnumeric() else x)
                link_found = True
            elif address.find('CFIB') >= 0:
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='economic-indicators')
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=str(sname))
            elif address.find('DEUSTATIS') >= 0:
                if str(sname).find('Indices of labour costs') >= 0:
                    while WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/select[@name="name"]/option[contains(., "'+str(sname).replace('Indices of labour costs - ','')+'")]'))).get_attribute('selected') != 'true':
                        Select(WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/select[@name="name"]')))).select_by_visible_text(str(sname).replace('Indices of labour costs - ',''))
                try:
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, 'disclaimerAcceptId'))).click()
                except TimeoutException:
                    time.sleep(0)
                if str(sname).find('Construction work completed') >= 0:
                    ITEM = ['Residential buildings','Non-residential buildings']
                    WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'Construction activities\'"]'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][contains(., "Construction of new buildings")]//input'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                    WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'Type of building / builder\'"]'))).click()
                    for item in ITEM:
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][contains(., "'+str(item)+'")]//input'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                elif str(sname).find('Construction price indices') >= 0:
                    WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'Indices including/excluding turnover tax\'"]'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][contains(., "Indices including turnover tax")]//input'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                    if freq == 'A':
                        WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'Maintenance of residential buildings\'"]'))).click()
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][contains(., "Resident. buildings excl. int. decorative repairs")]//input'))).click()
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                elif str(sname).find('Productivity') >= 0:
                    WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'Industries\'"]'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][contains(., "Manufacturing")]//input'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                elif str(sname).find('New orders in manufacturing') >= 0:
                    ITEM = ['Manufacturing','Intermediate goods','Capital goods','Consumer goods','Durable goods','Non-durable goods','Manufacturing (except 30)','Capital goods (except 29.10)']
                    WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'WZ2008 (main groups, aggregates): Manufacturing\'"]'))).click()
                    for item in ITEM:
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="'+str(item)+'"]]//input'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                elif str(sname).find('Index of production in manufacturing (main groups and aggregates)') >= 0:
                    ITEM = ['Industry','Industry, except construction','Industry, except energy and construction','Mining and quarrying and manufacturing','Intermediate goods','Capital goods','Durable goods','Non-durable goods',\
                        'Consumer goods','Energy (except section E)','Energy (except section D and E)','Mining and quarrying','Manufacturing','Electricity, gas, steam, air conditioning supply','Construction','Main construction industry',\
                            'Construction of buildings','Civil engineering','Building completion work','Manufacturing (except 30)','Capital goods (except 29.10)','Consumer goods (except 10, 11 and 12)','Manufacture of food products, beverages, tobacco',\
                                'Manufacture of chemical a. pharmaceutical products','Manuf. of basic metals a.fabricated metal products','Manufacture of computer and electrical equipment']
                    WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'WZ2008 (main groups, aggregates): Manufacturing\'"]'))).click()
                    try:
                        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@title="length of page: 100"]'))).click()
                    except TimeoutException:
                        time.sleep(0)
                    for item in ITEM:
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="'+str(item)+'"]]//input'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                elif str(sname).find('Index of production in manufacturing (2-3-4-digit codes)') >= 0:
                    ITEM = ['Manufacture of chemicals and chemical products','Manufacture of basic metals','Manuf. of computer,electronic and optical products','Manufacture of electrical equipment','Manufacture of machinery and equipment n.e.c.','Manuf. of motor vehicles, trailers, semi-trailers']
                    try:
                        WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'WZ2008 (2-digit codes): Manufacturing\'"]'))).click()
                    except TimeoutException:
                        Select(WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/select[@name="name"]')))).select_by_value('WZ08V2')
                        WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'WZ2008 (2-digit codes): Manufacturing\'"]'))).click()
                    try:
                        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@title="length of page: 100"]'))).click()
                    except TimeoutException:
                        time.sleep(0)
                    for item in ITEM:
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="'+str(item)+'"]]//input'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                elif str(sname).find('wholesale trade') >= 0:
                    ITEM = ['Wholesale trade, except motor vehicles,motorcycles','Wholesale of agric. raw materials, live animals','Wholesale of food, beverages and tobacco','Wholesale of household goods','Wholesale of information, communication equipment',\
                        'Wholesale of other machinery, equipment, supplies','Other specialised wholesale']
                    WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'WZ2008 (2-4 digit codes)\'"]'))).click()
                    try:
                        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@title="length of page: 100"]'))).click()
                    except TimeoutException:
                        time.sleep(0)
                    for item in ITEM:
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="'+str(item)+'"]]//input'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                elif str(sname).find('retail trade') >= 0:
                    ITEM = ['Retail trade, except motor vehicles a. motorcycles','Retail sale in non-specialised stores','Retail sale of food, beverages and tobacco','WZ08-473','Retail sale of information a. communication equip.','Retail sale of other household equipment',\
                        'Retail trade not in stores, stalls or markets','Motor trade and retail trade']
                    ITEM2 = ['Retail sale of food','Retail sale of textiles,clothing,footwear,leather','Retail sale of furniture,furnishings,hh.equip.etc.','Dispensing chemist,retail sale of medic. goods etc']
                    if str(sname).find('Persons employed') >= 0:
                        WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'WZ2008 (selected items): Retail trade\'"]'))).click()
                    else:
                        WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'WZ2008 (2-4 digit codes): Retail trade\'"]'))).click()
                    try:
                        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@title="length of page: 100"]'))).click()
                    except TimeoutException:
                        time.sleep(0)
                    for item in ITEM:
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="'+str(item)+'"]]//input'))).click()
                    if str(sname).find('Turnover') >= 0:
                        for item in ITEM2:
                            WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="'+str(item)+'"]]//input'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                elif str(sname).find('Foreign trade') >= 0 and str(fname).find('genesis') >= 0:
                    for item in ['Exports: Net mass','Exports: Value (US-Dollar)','Imports: Net mass','Imports: Value (US-Dollar)']:
                        while WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/p[contains(., "'+str(item)+'")]/input'))).get_attribute('selected') == 'true':
                            WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/p[contains(., "'+str(item)+'")]/input'))).click()
                    for item in ['Exports: Value','Imports: Value']:
                        while WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/p[contains(., "'+str(item)+'")]/input'))).get_attribute('selected') != 'true':
                            WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/p[contains(., "'+str(item)+'")]/input'))).click()
                    if str(sname).find('EU') >= 0 or str(sname).find('countries') >= 0:
                        WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'Countries\'"]'))).click()
                        try:
                            WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@title="length of page: 100"]'))).click()
                        except TimeoutException:
                            time.sleep(0)
                        if str(sname).find('EU') >= 0:
                            try:
                                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="STLAH959"]]//input')))
                            except TimeoutException:
                                ERROR('Unable to decide countries of EU members.')
                            countries = WebDriverWait(chrome, 5).until(EC.presence_of_all_elements_located((By.XPATH, './/div[@class="tr"][//input[@type="checkbox"]]')))
                            for coun in countries:
                                try:
                                    coun.find_element_by_xpath('.//div[text()="STLAH959"]')
                                except NoSuchElementException:
                                    coun.find_element_by_xpath('.//input').click()
                                else:
                                    break
                        elif str(sname).find('countries') >= 0:
                            ITEM = ['Canada','New Zealand']
                            for item in ITEM:
                                while True:
                                    try:
                                        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="'+str(item)+'"]]//input'))).click()
                                    except TimeoutException:
                                        try:
                                            WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="Next page"]'))).click()
                                        except TimeoutException:
                                            ERROR('Country: '+str(item)+' is not found in the country list.')
                                    else:
                                        break
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                    elif str(sname).find('EGW1') >= 0:
                        Select(WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/select[@name="name"]')))).select_by_value('EGWV1')
                        WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'Commodity groups (EGW 2002: 1-digit codes)\'"]'))).click()
                        ITEM = ['Food','Raw materials','Semi-finished goods','Finished goods','Industrial products']
                        for item in ITEM:
                            WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="'+str(item)+'"]]//input'))).click()
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                    elif str(sname).find('SITC2') >= 0:
                        Select(WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/select[@name="name"]')))).select_by_value('SITC2A')
                        WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'SITC (2-digit codes): Foreign trade\'"]'))).click()
                        try:
                            WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@title="length of page: 100"]'))).click()
                        except TimeoutException:
                            time.sleep(0)
                        ITEM = ['Petroleum, petroleum products, related materials']
                        for item in ITEM:
                            WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="'+str(item)+'"]]//input'))).click()
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                elif str(sname).find('Harmonised') >= 0 or str(sname).find('Retail price') >= 0 or str(sname).find('GP9') >= 0:
                    if str(sname).find('Harmonised') >= 0:
                        ITEM = [re.sub(r'(CH[0-9]+).*', r"\1", str(item)).strip() if str(item).find('CH') == 0 else None for item in list(Series[freq].loc[Series[freq]['DataSet']==str(sname)]['keyword'])]
                        if None in ITEM:
                            while None in ITEM:
                                ITEM.remove(None)
                        WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'Individual consumption by purpose\'"]'))).click()
                    elif str(sname).find('Retail price') >= 0:
                        ITEM = list(Series[freq].loc[Series[freq]['DataSet']==str(sname)]['keyword'])
                        WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'Value added tax\'"]'))).click()
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][contains(., "Excluding value added tax")]//input'))).click()
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                        WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'WZ2008 (selected items)\'"]'))).click()
                    elif str(sname).find('Producer price') >= 0:
                        ITEM = list(Series[freq].loc[Series[freq]['DataSet']==str(sname)]['keyword'])
                        Select(WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"]//select[@name="name"]')))).select_by_value('GP09N1')
                        WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div/p/select[@name="name"]]//button'))).click()
                    elif str(sname).find('Import price') >= 0:
                        ITEM = list(Series[freq].loc[Series[freq]['DataSet']==str(sname)]['keyword'])
                        Select(WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"]//select[@name="name"]')))).select_by_value('GP09W7')
                        WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div/p/select[@name="name"]]//button'))).click()
                    ITEM.sort()
                    try:
                        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@title="length of page: 100"]'))).click()
                    except TimeoutException:
                        time.sleep(0)
                    for item in ITEM:
                        while True:
                            try:
                                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="'+str(item)+'"]]//input'))).click()
                            except TimeoutException:
                                try:
                                    WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="Next page"]'))).click()
                                except TimeoutException:
                                    ERROR('ITEM: '+str(item)+' is not found in the item list.')
                            else:
                                break
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                elif str(sname).find('Producer price') >= 0 and str(sname).find('total') < 0:
                    VALUE = {'2':'GP09M2','3':'GP09M3','4':'GP09M4','S':'GP09N2'}
                    option = re.sub(r'.+?GP([2-9S]).*', r"\1", str(sname)).strip()
                    Select(WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"]//select[@name="name"]')))).select_by_value(VALUE[option])
                elif str(sname).find('Index of wholesale prices - WZ') >= 0:
                    WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'Economic activities (WZ2008)\'"]'))).click()
                    try:
                        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@title="length of page: 100"]'))).click()
                    except TimeoutException:
                        time.sleep(0)
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="Wholesale of food, beverages and tobacco"]]//input'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="Wholesale of solid,liquid,gaseous fuels, rel.prod."]]//input'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                elif str(sname).find('Local units - construction') >= 0:
                    for item in ['Local units','Remunerations','Turnover','Turnover from construction activities']:
                        while WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/p[contains(., "'+str(item)+'")]/input'))).get_attribute('selected') == 'true':
                            WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/p[contains(., "'+str(item)+'")]/input'))).click()
                    for item in ['Persons employed','Hours worked']:
                        while WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/p[contains(., "'+str(item)+'")]/input'))).get_attribute('selected') != 'true':
                            WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/p[contains(., "'+str(item)+'")]/input'))).click()
                    Select(WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"]//select[@name="name"]')))).select_by_value('WZ08Z2')
                    WebDriverWait(chrome, 0.1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'WZ2008(selected items): Main construction industry\'"]'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="tr"][div[text()="Main construction industry"]]//input'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                elif str(sname).find('Arrivals and overnight stays in accommodation establishments') >= 0:
                    for item in ['Arrivals','Overnight stays']:
                        while WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/p[contains(., "'+str(item)+'")]/input'))).get_attribute('selected') != 'true':
                            WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/p[contains(., "'+str(item)+'")]/input'))).click()
                try:
                    WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'Year\'"]'))).click()
                    if specific_time_unit == True:
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/input[@name="ZI_VON"]'))).send_keys(str(start_year))
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/input[@name="ZI_BIS"]'))).send_keys(str(start_year+interval))
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="zeitspannen"]'))).click()
                    else:
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="alles"]'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                except TimeoutException:
                    time.sleep(0)
                if str(fname).find('genesis') < 0:
                    target = chrome.find_element_by_xpath('.//table[contains(., "'+str(file_name)+'")]')
                    INTLINE_temp = pd.read_html(target.get_attribute('outerHTML'), header=header, index_col=index_col)[0]
                    INTLINE_temp = INTLINE_temp.applymap(lambda x: int(x) if str(x).isnumeric() else x)
                else:
                    SWITCH = ['Construction work completed in structural engineering years','Indices of agreed earnings weekly working hours','Indices of labour costs - Index of labour costs per hour worked',\
                        'Indices of labour costs - Index of gross earnings per hour worked','Indices of labour costs - Index of non-wage labour costs per hour worked','Construction price indices by types of buildings quarters',\
                            'Construction price indices by civil engineering quarters','Construction price indices by maintenance of residential buildings quarters']
                    if str(sname) in SWITCH or (freq == 'M' and str(sname).find('price index') < 0):
                        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Switch columns and rows"]'))).click()
                    while True:
                        chrome.find_element_by_xpath('.//button[@name="werteabruf"]').click()
                        try:
                            WebDriverWait(chrome, 0.1).until(EC.presence_of_element_located((By.XPATH, './/p[contains(., "reduce the table size")]')))
                        except TimeoutException:
                            if str(sname).find('EU') >= 0:
                                WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Hide empty columns"]'))).click()
                            WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.ID, 'XLSX'))).click()
                            break
                        else:
                            if str(sname).find('Foreign trade') >= 0 or str(sname).find('Local units - construction') >= 0:
                                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="Select: Attributes for Variable \'Year\'"]'))).click()
                                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/input[@name="ZI_ANZ_LETZTE"]'))).send_keys(str(10))
                                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="zeitscheiben"]'))).click()
                                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[@value="Accept"]'))).click()
                            else:
                                logging.info('Getting Data from Different Year Ranges.\n')
                                INTLINE_temp = {}
                                interval = 10
                                if freq == 'M' and str(sname).find('Consumer price') >= 0:
                                    interval = 5
                                for yr in range(int(file_name), datetime.today().year, interval):
                                    IN_temp = INTLINE_DATASETS(chrome, data_path, country, address, fname, sname+' - '+str(yr), freq, Series, Table, yr, specific_time_unit=True, interval=interval)
                                    INTLINE_temp[yr] = IN_temp
                                    note.append(yr)
                                break
                link_found = True
            elif address.find('BUNDES') >= 0:
                chrome.set_window_size(1080,1020)
                chrome.refresh()
                if str(sname).find('special trade') >= 0 or str(sname).find('MFI interest rate statistics') >= 0:
                    ITEM = list(Series[freq].loc[Series[freq]['DataSet']==str(sname)]['keyword'])
                    for item in ITEM:
                        sys.stdout.write("\rGetting Item: "+str(item)+" "*10)
                        sys.stdout.flush()
                        target = WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/a[@data-basket-tsid="'+str(item)+'"]')))
                        while True:
                            try:
                                target.click()
                            except ElementClickInterceptedException:
                                time.sleep(1)
                            else:
                                break
                    sys.stdout.write('\n\n')
                else:
                    try:
                        WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/a[text()="Remove all"]')))
                    except TimeoutException:
                        WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/a[text()="Add all"]'))).click()
                if str(sname).find('Consumer price index SA') >= 0:
                    chrome.get('https://www.bundesbank.de/dynamic/action/en/statistics/time-series-databases/time-series-databases/745582/745582?tsTab=2&tsId=BBDP1.M.DE.Y.VPI.C.SVXR.I15.A')
                    WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/a[@data-basket-tsid="BBDP1.M.DE.Y.VPI.C.SVXR.I15.A"]'))).click()
                while True:
                    try:
                        #chrome.execute_script("window.scrollTo(0,100)")
                        link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='data-basket')
                    except (UnexpectedAlertPresentException, ElementClickInterceptedException):
                        time.sleep(0.5)
                    else:
                        break
                chrome.execute_script("window.scrollTo(0,200)")
                ActionChains(chrome).move_to_element(WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/input[@name="its_from"]')))).send_keys(str(start_year)).send_keys(Keys.RIGHT).send_keys('01').perform()
                chrome.find_element_by_xpath('.//span[text()="English"]').click()
                chrome.refresh()
                chrome.execute_script("window.scrollTo(0,0)")
                chrome.find_element_by_xpath('.//input[@value="Go to download"]').click()
                time.sleep(10)
                # for i in range(5):
                #     time.sleep(10)
                #     chrome.refresh()
                input('請手動重新整理後按Enter鍵繼續:')
                target = WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/div[@class="listTable"]')))
                if freq == 'M' or str(sname).find('monthly average') >= 0 or str(sname).find('monthly end') >= 0:
                    link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='Monthly series', text_match=True)
                elif freq == 'A':
                    link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='Anually series', text_match=True)
                elif freq == 'Q':
                    link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='Quarterly series', text_match=True)
                chrome.find_element_by_xpath('.//input[@value="Back"]').click()
                while True:
                    try:
                        chrome.execute_script("window.scrollTo(0,3000)")
                        WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/input[@value="Delete all"]'))).click()
                        #time.sleep(5)
                        #chrome.execute_script("window.scrollTo(0,3000)")
                        #WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/input[@value="Refresh"]'))).click()
                    except ElementClickInterceptedException:
                        time.sleep(1)
                    else:
                        break
            elif address.find('IFO') >= 0:
                target = WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/li[contains(., "'+str(sname)+'")]')))
                link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='xlsx')
            elif address.find('HWWI') >= 0:
                WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="EN"]'))).click()
                if WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="J"]'))).get_attribute('checked') == 'true':
                    WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="J"]'))).click()
                if WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="M"]'))).get_attribute('checked') != 'true':
                    WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="M"]'))).click()
                Select(WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'mjVon')))).select_by_visible_text('1982')
                Select(WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'mmVon')))).select_by_visible_text('01')
                Select(WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'mmBis')))).select_by_visible_text('12')
                try:
                    Select(WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'mjBis')))).select_by_visible_text(str(datetime.today().year))
                except NoSuchElementException:
                    Select(WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'mjBis')))).select_by_visible_text(str(datetime.today().year-1))
                WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/input[@name="buttonRefresh"]'))).click()
                Select(WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.XPATH, './/select[@name="export"]')))).select_by_visible_text('Excel')
                WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="Go"]'))).click()
                link_found = True
            elif address.find('ARBEIT') >= 0:
                try:
                    WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.XPATH, './/button[contains(., "Alle zulassen")]'))).click()
                except TimeoutException:
                    time.sleep(0)
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='publicationFile')
            elif address.find('BPS') >= 0:
                Routes = [re.split(r'//', str(Series[freq].loc[Series[freq]['DataSet'] == str(sname)].iloc[k]['Routes'])) for k in range(Series[freq].loc[Series[freq]['DataSet'] == str(sname)].shape[0])]
                Routes = [list(s) for s in list(set([tuple(r) for r in Routes]))]
                Routes.sort()
                try:
                    WebDriverWait(chrome, 1).until(EC.visibility_of_element_located((By.XPATH, './/a[@title="Close"]'))).click()
                except TimeoutException:
                    time.sleep(0)
                WebDriverWait(chrome, 1).until(EC.visibility_of_element_located((By.XPATH, './/a[text()="English"]'))).click()
                time.sleep(3)
                for route in Routes:
                    chrome.execute_script("window.scrollTo(0,0)")
                    Select(WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.ID, 'subject')))).select_by_visible_text(str(route[0]))
                    try:
                        ActionChains(chrome).drag_and_drop_by_offset(WebDriverWait(chrome, 0.5).until(EC.visibility_of_element_located((By.ID, 'jqxScrollThumbverticalScrollBarVariabel'))),0,-200).perform()
                    except TimeoutException:
                        time.sleep(0)
                    max_year = None
                    while True:
                        try:
                            Indicators = WebDriverWait(chrome, 0.5).until(EC.presence_of_all_elements_located((By.XPATH, './/div[@role="option"][contains(., "'+str(route[1])+'")]')))
                        except TimeoutException:
                            try:
                                WebDriverWait(chrome, 0.5).until(EC.visibility_of_element_located((By.ID, 'jqxScrollAreaDownverticalScrollBarVariabel')))
                            except TimeoutException:
                                if max_year == None:
                                    ERROR('Indicator Not Found: '+str(route[1]))
                                else:
                                    break
                            else:
                                try:
                                    ActionChains(chrome).drag_and_drop_by_offset(WebDriverWait(chrome, 0.5).until(EC.visibility_of_element_located((By.ID, 'jqxScrollThumbverticalScrollBarVariabel'))),0,38).perform()
                                except TimeoutException:
                                    time.sleep(0)
                                continue
                        for indicator in Indicators:
                            if indicator.text == '' or indicator.text.find('Province') >= 0:
                                continue
                            while True:
                                try:
                                    base_year = re.sub(r'.*?\s*([0-9]{4}).*', r"\1", indicator.text)
                                    if max_year == None or (base_year > max_year and base_year.isnumeric):
                                        ActionChains(chrome).click(indicator).perform()
                                        max_year = base_year
                                        time.sleep(2)
                                except ElementNotInteractableException:
                                    ActionChains(chrome).click(WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'jqxScrollBtnDownverticalScrollBarVariabel')))).perform()
                                else:
                                    try:
                                        characteristic = WebDriverWait(chrome, 0.5).until(EC.visibility_of_element_located((By.ID, 'panel-turunan-variabel')))
                                        Chars = WebDriverWait(characteristic, 2).until(EC.presence_of_all_elements_located((By.XPATH, './/div[@role="option"]')))
                                        for char in Chars:
                                            if char.text.strip() != str(route[2]) and char.get_attribute('aria-selected') == 'true':
                                                ActionChains(chrome).click(char).perform()
                                            elif char.text.strip() == str(route[2]) and char.get_attribute('aria-selected') != 'true':
                                                ActionChains(chrome).click(char).perform()
                                    except TimeoutException:
                                        time.sleep(0)
                                    break
                        try:
                            WebDriverWait(chrome, 0.5).until(EC.visibility_of_element_located((By.ID, 'jqxScrollAreaDownverticalScrollBarVariabel')))
                        except TimeoutException:
                            if max_year == None:
                                ERROR('Indicator Not Found: '+str(route[1]))
                            else:
                                break
                        else:
                            ActionChains(chrome).drag_and_drop_by_offset(WebDriverWait(chrome, 0.5).until(EC.visibility_of_element_located((By.ID, 'jqxScrollThumbverticalScrollBarVariabel'))),0,38).perform()
                    ActionChains(chrome).click(WebDriverWait(chrome, 5).until(EC.visibility_of_element_located((By.ID, 'legendWaktu')))).perform()
                    for i in range(10):
                        ActionChains(chrome).send_keys(Keys.DOWN).perform()
                    time.sleep(1)
                    Data_Periode = WebDriverWait(chrome, 5).until(EC.visibility_of_element_located((By.ID, 'panel-tahun')))
                    while True:
                        Periods = WebDriverWait(Data_Periode, 2).until(EC.presence_of_all_elements_located((By.XPATH, './/div[@role="option"]')))
                        for period in Periods:
                            try:
                                if period.text == '':
                                    continue
                                if period.get_attribute('aria-selected') != 'true':
                                    ActionChains(chrome).click(period).perform()
                            except StaleElementReferenceException:
                                continue
                        try:
                            WebDriverWait(chrome, 0.5).until(EC.visibility_of_element_located((By.ID, 'jqxScrollAreaDownverticalScrollBarWaktu')))
                        except TimeoutException:
                            break
                        else:
                            for i in range(10):
                                ActionChains(chrome).click(WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.ID, 'jqxScrollBtnDownverticalScrollBarWaktu')))).perform()
                    if freq != 'A':
                        Periode_Detail = WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'panel-turunan-tahun')))
                        while True:
                            Details = WebDriverWait(Periode_Detail, 2).until(EC.presence_of_all_elements_located((By.XPATH, './/div[@role="option"]')))
                            for detail in Details:
                                try:
                                    WebDriverWait(detail, 0.5).until(EC.visibility_of_element_located((By.XPATH, './/span[contains(., "Annually")]')))
                                except TimeoutException:
                                    if detail.text == '':
                                        continue
                                    if detail.get_attribute('aria-selected') != 'true':
                                        ActionChains(chrome).click(detail).perform()
                                except ElementClickInterceptedException:
                                    ActionChains(chrome).click(WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'jqxScrollBtnDownverticalScrollBarturunanTahun')))).perform()
                                else:
                                    continue
                            try:
                                WebDriverWait(chrome, 0.5).until(EC.visibility_of_element_located((By.ID, 'jqxScrollAreaDownverticalScrollBarturunanTahun')))
                            except TimeoutException:
                                break
                            else:
                                for i in range(10):
                                    ActionChains(chrome).click(WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'jqxScrollBtnDownverticalScrollBarturunanTahun')))).perform()
                    ActionChains(chrome).click(WebDriverWait(chrome, 5).until(EC.visibility_of_element_located((By.ID, 'buttonDataSelect')))).perform()
                    for i in range(5):
                        ActionChains(chrome).send_keys(Keys.DOWN).perform()
                    time.sleep(1)
                    WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'bttAdd'))).click()
                    try:
                        WebDriverWait(chrome, 0.5).until(EC.visibility_of_element_located((By.XPATH, './/div[@class="jqx-widget jqx-fill-state-normal jqx-tooltip-text"][contains(., "Kategori Variabel Belum Dipilih")]')))
                    except TimeoutException:
                        time.sleep(0)
                    else:
                        ERROR('Characteristic was not correctly selected.')
                    time.sleep(2)
                ActionChains(chrome).click(WebDriverWait(chrome, 5).until(EC.visibility_of_element_located((By.ID, 'verticalVariableSelect')))).perform()
                for i in range(20):
                    ActionChains(chrome).send_keys(Keys.DOWN).perform()
                time.sleep(1)
                target = WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.ID, 'verticalVariableSelect')))
                if str(sname).find('WPI') >= 0 or str(sname).find('TRADE') >= 0:
                    if WebDriverWait(target, 2).until(EC.visibility_of_element_located((By.XPATH, './/div[@role="checkbox"][contains(., "Select All")]'))).get_attribute('aria-checked') != "true":
                        WebDriverWait(target, 2).until(EC.visibility_of_element_located((By.XPATH, './/div[@role="checkbox"][contains(., "Select All")]'))).click()
                else:
                    if WebDriverWait(target, 2).until(EC.visibility_of_element_located((By.XPATH, './/div[@role="checkbox"][contains(., "Select All")]'))).get_attribute('aria-checked') == "true":
                        WebDriverWait(target, 2).until(EC.visibility_of_element_located((By.XPATH, './/div[@role="checkbox"][contains(., "Select All")]'))).click()
                    ITEMS = re.split(r', ', str(file_name))
                    while True:
                        Variables = WebDriverWait(target, 2).until(EC.presence_of_all_elements_located((By.XPATH, './/div[@role="option"]')))
                        for var in Variables:
                            if True in [var.text.find(item) >= 0 for item in ITEMS] and var.get_attribute('aria-selected') != 'true':
                                ActionChains(chrome).click(var).perform()
                        try:
                            WebDriverWait(target, 0.5).until(EC.visibility_of_element_located((By.XPATH, './/div[contains(@id, "jqxScrollAreaDownverticalScrollBar")]')))
                        except TimeoutException:
                            break
                        else:
                            for i in range(24):
                                ActionChains(chrome).click(WebDriverWait(target, 2).until(EC.visibility_of_element_located((By.XPATH, './/div[contains(@id, "jqxScrollBtnDownverticalScrollBar")]')))).perform()
                for i in range(20):
                    ActionChains(chrome).send_keys(Keys.DOWN).perform()
                time.sleep(1)
                WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'bttSubmit'))).click()
                while True:
                    try:
                        chrome.switch_to.window(chrome.window_handles[-1])
                        WebDriverWait(chrome, 5).until(EC.visibility_of_element_located((By.ID, 'tableLeftUp')))
                        WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.XPATH, './/td[contains(., "Download Data")]'))).click()
                        WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'xlsBtn'))).click()
                        chrome.close()
                        chrome.switch_to.window(chrome.window_handles[0])
                    except TimeoutException:
                        chrome.close()
                        chrome.switch_to.window(chrome.window_handles[0])
                        WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.ID, 'bttSubmit'))).click()
                    else:
                        break
                chrome.refresh()
                link_found = True
            elif address.find('BIDN') >= 0:
                if str(fname).find('headingFour') >= 0:
                    if WebDriverWait(chrome, 1).until(EC.visibility_of_element_located((By.XPATH, './/a[@href="#headingFour"]'))).get_attribute('aria-expanded') != 'true':
                        link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='headingFour')
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=str(file_name))
                chrome.refresh()
            elif address.find('BKPM') >= 0:
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Investment Growth', text_match=True)
                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/span[contains(., "Choose the group report")]'))).click()
                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/li[contains(., "'+str(file_name)+'")]'))).click()
                WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/span[contains(., "Yearly")]'))).click()
                target1 = WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[@id="tahunAwal_chosen"]')))
                target1.click()
                WebDriverWait(target1, 1).until(EC.element_to_be_clickable((By.XPATH, './/li[contains(., "1990")]'))).click()
                target2 = WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[@id="tahunAkhir_chosen"]')))
                target2.click()
                try:
                    WebDriverWait(target2, 0.5).until(EC.element_to_be_clickable((By.XPATH, './/li[contains(., "'+str(datetime.today().year)+'")]'))).click()
                except TimeoutException:
                    WebDriverWait(target2, 0.5).until(EC.element_to_be_clickable((By.XPATH, './/li[contains(., "'+str(datetime.today().year-1)+'")]'))).click()
                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/button[@type="submit"]'))).click()
                WebDriverWait(chrome, 20).until(EC.element_to_be_clickable((By.XPATH, './/table[@role="button"][@aria-labelledby="_NS_runInlabel"]'))).click()
                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/table[@role="menuitem"][@aria-labelledby="_NS_viewInExcellabel"]'))).click()
                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/table[@role="menuitem"][@aria-labelledby="_NS_viewInspreadsheetMLlabel"]'))).click()
                chrome.switch_to.window(chrome.window_handles[-1])
                time.sleep(2)
                while True:
                    try:
                        #WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/span[@id="_NS__workingMsg"]')))
                        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/td[contains(.,"您的報告已準備就緒")]')))
                    except TimeoutException:
                        continue
                    else:
                        link_found = True
                        time.sleep(10)
                        break
                chrome.close()
                chrome.switch_to.window(chrome.window_handles[0])
            elif address.find('GSO') >= 0:
                if previous == True:
                    INTLINE_temp = pd.DataFrame()
                    WebDriverWait(chrome, 1).until(EC.visibility_of_element_located((By.ID, 'Keyword'))).send_keys('消費者物價指數')
                    WebDriverWait(chrome, 1).until(EC.visibility_of_element_located((By.XPATH, './/div[text()="搜尋"]'))).click()
                    for page in range(1,8):
                        WebDriverWait(chrome, 1).until(EC.visibility_of_element_located((By.XPATH, './/ul[@class="pagination"]//a[text()="'+str(page)+'"]'))).click()
                        link_list = WebDriverWait(chrome, 5).until(EC.presence_of_all_elements_located((By.XPATH, './/div[@class="j_search_listshell"]/a')))
                        for link in link_list:
                            if link.text.find('越南') >= 0:
                                ActionChains(chrome).key_down(Keys.CONTROL).click(link).key_up(Keys.CONTROL).perform()
                                chrome.switch_to.window(chrome.window_handles[-1])
                                try:
                                    IN_t = pd.read_html(chrome.page_source, header=[0,1,2], index_col=0)[0]
                                    IN_t.columns = [re.sub(r'(\-)0(1[0-2])', r"\1\2", re.sub(r'.*?([0-9]{4})年([0-9]{1,2})月.*', r"\1-0\2", str(col[0]))) if str(col[1]).find('原始據數') >= 0 and str(col[2]).find('全國') >= 0 else None for col in IN_t.columns]
                                    IN_t.index = ['CPI' if str(dex).find('物價消費指數') >= 0 else None for dex in IN_t.index]
                                    IN_t = IN_t.loc[IN_t.index.dropna(), IN_t.columns.dropna()]
                                    IN_t = IN_t.applymap(lambda x: float(x)/pow(10,len(str(int(x)))-3) if len(str(int(x))) > 3 else float(x))
                                    INTLINE_temp = pd.concat([IN_t, INTLINE_temp], axis=1)
                                    INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
                                except ValueError:
                                    chrome.close()
                                    chrome.switch_to.window(chrome.window_handles[0])
                                    continue
                                else:
                                    chrome.close()
                                    chrome.switch_to.window(chrome.window_handles[0])
                else:
                    WebDriverWait(chrome, 40).until(EC.element_to_be_clickable((By.XPATH, './/td[@class="Custom PPTextBoxSideContainer"]/div/div[@id="RibbonButton2220"]')))
                    while True:
                        time.sleep(5)
                        WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/td[@class="Custom PPTextBoxSideContainer"]/div/div[@id="RibbonButton2220"]'))).click()
                        ActionChains(chrome).move_to_element(chrome.find_element_by_xpath('.//td[//input[@class="PPTextBoxInput"]]')).send_keys('Vietnam').perform()
                        time.sleep(5)
                        WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/table[@class="PPTLVNodesTable"]/tbody/tr'))).click()
                        try:
                            WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/div[@class="PPTSCellConText"][contains(text(), "Vietnam")]')))
                        except (NoSuchElementException, StaleElementReferenceException):
                            time.sleep(1)
                        else:
                            break
                    chrome.find_element_by_id('ExportSplitButton').click()
                    chrome.find_element_by_id('ExportMenuItemXLSX').click()
                    chrome.find_element_by_xpath('.//div[div[div[text()="OK"]]]').click()
                link_found = True
            elif address.find('INSEE') >= 0:
                time.sleep(8)
                if address.find('SERIE') >= 0:
                    try:
                        WebDriverWait(chrome, 3).until(EC.visibility_of_element_located((By.XPATH, './/tr[@class="cliquable"][contains(., "'+str(file_name)+'")][not(contains(., "Stopped series"))]'))).click()
                    except TimeoutException:
                        ERROR('No correct present time series were found with the keyword: '+str(file_name)+'. Please modify the website url with new keywords.')
                    while True:
                        try:
                            WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/td[@class="echo echo-chevron"]')))
                        except TimeoutException:
                            time.sleep(0)
                        else:
                            time.sleep(2)
                            break
                    if Zip == True:
                        link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='xlsx')
                    else:
                        WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/input[@title="Label or code"]'))).send_keys(str(sname))
                        WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/button[@title="rechercher"]'))).click()
                        time.sleep(5)
                        WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/td[@class="echo echo-chevron"][contains(., "'+str(sname)+'")][not(contains(., "Stopped series"))]'))).click()
                        WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/button[text()="Transpose table"]'))).click()
                        INTLINE_temp = pd.read_html(chrome.page_source, header=[0,1], index_col=0)[0]
                        if INTLINE_temp.empty:
                            ERROR('Incorrect time serie: '+str(sname))
                        link_found = True
                else:
                    if freq == 'A':
                        link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=str(file_name), text_match=True)
                        if link_found == False:
                            ERROR('Link Not Found: '+str(file_name))
                        try:
                            target = WebDriverWait(chrome, 3).until(EC.visibility_of_element_located((By.XPATH, './/div[@class="bloc fichiers"][contains(., "'+str(sname)[-50:]+'")]')))
                        except TimeoutException:
                            target = WebDriverWait(chrome, 3).until(EC.visibility_of_element_located((By.XPATH, './/div[@class="bloc fichiers"][contains(., "'+str(sname)[:50]+'")]')))
                        link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='.xls')
                    elif freq == 'Q':
                        link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=re.split(r'//', str(file_name))[0], text_match=True)
                        if link_found == False:
                            ERROR('Link Not Found: '+re.split(r'//', str(file_name))[0])
                        link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword=re.split(r'//', str(file_name))[1])
            elif address.find('DOUANES') >= 0:
                target = WebDriverWait(chrome, 3).until(EC.visibility_of_element_located((By.XPATH, './/div[@class="bande"][contains(., "Séries mensuelles CVS-CJO et brutes estimées")]')))
                link_found, link_meassage = INTLINE_WEB_LINK(target, fname, keyword='Transfert_file')
            elif address.find('MEASTF') >= 0:
                username = open(data_path+'email.txt','r',encoding='ANSI').read()
                password = open(data_path+'password.txt','r',encoding='ANSI').read()
                WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.ID, 'Login'))).send_keys(username)
                WebDriverWait(chrome, 5).until(EC.visibility_of_element_located((By.ID, 'Pwd'))).send_keys(password)
                WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.XPATH, './/input[@type="submit"]'))).click()
                WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.XPATH, './/span[contains(., "Période")]'))).click()
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='SelectAll')
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='SelectLevel(0,0)')
                WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.XPATH, './/input[@class="ShowReport"]'))).click()
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='DownloadButton')
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='.xls', text_match=True)
                chrome.switch_to.window(chrome.window_handles[-1])
                WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.XPATH, './/input[@type="button"]'))).click()
                chrome.close()
                chrome.switch_to.window(chrome.window_handles[0])
            elif address.find('MLF') >= 0:
                link_found, link_message = INTLINE_WEB_LINK(chrome, fname, keyword=str(file_name), get_attribute='title')
            elif address.find('ISTAT') >= 0:
                try:
                    dex = 1
                    edition = Select(WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'PDim_T_BIS'))))
                    edition.select_by_index(len(edition.options)-dex)
                except TimeoutException:
                    time.sleep(0)
                else:
                    while True:
                        print('dex: '+str(dex))
                        time.sleep(3)
                        try:
                            target = chrome.find_element_by_xpath('.//table[@class="DataTable"]/tbody')
                            chrome.execute_script("arguments[0].style.visibility = 'visible';", target)
                            WebDriverWait(chrome, 2).until(EC.visibility_of_element_located((By.XPATH, './/table[@class="DataTable"]/tbody/tr')))
                        except TimeoutException:
                            dex += 1
                            edition = Select(WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'PDim_T_BIS'))))
                            edition.select_by_index(len(edition.options)-dex)
                        else:
                            break
                ActionChains(chrome).send_keys(Keys.ESCAPE).perform()
                target = chrome.find_element_by_xpath('.//table[@class="DataTable"]')
                if output == True:
                    try:
                        pages = Select(WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, 'PAGE'))))
                    except TimeoutException:
                        try:
                            WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/img[@title="Next page"]')))
                        except TimeoutException:
                            INTLINE_temp = pd.read_html(target.get_attribute('outerHTML'), header=0, index_col=index_col)[0]
                            INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated()]
                            if type(index_col) == list:
                                INTLINE_temp = INTLINE_temp.T.set_index(tuple(['Select time' for d in index_col])).T
                            else:
                                try:
                                    INTLINE_temp = INTLINE_temp.T.set_index('Select time').T
                                except KeyError:
                                    INTLINE_temp = INTLINE_temp.T.set_index('Time and frequency').T
                        else:
                            INTLINE_temp = pd.DataFrame()
                            while True:
                                print(str(sname))
                                try:
                                    IN_temp = pd.read_html(chrome.find_element_by_xpath('.//table[@class="DataTable"]').get_attribute('outerHTML'), header=0, index_col=index_col)[0]
                                    IN_temp = IN_temp.loc[~IN_temp.index.duplicated()]
                                    if type(index_col) == list:
                                        IN_temp = IN_temp.T.set_index(tuple(['Select time' for d in index_col])).T
                                    else:
                                        try:
                                            IN_temp = IN_temp.T.set_index('Select time').T
                                        except KeyError:
                                            IN_temp = IN_temp.T.set_index('Time and frequency').T
                                    INTLINE_temp = pd.concat([INTLINE_temp, IN_temp])
                                except:
                                    break
                                try:
                                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/img[@title="Next page"]'))).click()
                                except TimeoutException:
                                    break
                                else:
                                    timeStart = time.time()
                                    while True:
                                        try:
                                            WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/img[@title="Previous page"]')))
                                        except TimeoutException:
                                            if int(time.time()-timeStart) > 20:
                                                break
                                            time.sleep(0)
                                        else:
                                            break
                                    time.sleep(1)
                    else:
                        INTLINE_temp = pd.DataFrame()
                        for i in range(len(pages.options)):
                            Select(WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.ID, 'PAGE')))).select_by_index(i)
                            timeStart = time.time()
                            while True:
                                print('pages: '+str(pages))
                                try:
                                    WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.ID, 'PAGE')))
                                except TimeoutException:
                                    if int(time.time()-timeStart) > 20:
                                        break
                                    time.sleep(0)
                                else:
                                    break
                            time.sleep(1)
                            IN_temp = pd.read_html(chrome.find_element_by_xpath('.//table[@class="DataTable"]').get_attribute('outerHTML'), header=0, index_col=index_col)[0]
                            IN_temp = IN_temp.loc[~IN_temp.index.duplicated()]
                            if type(index_col) == list:
                                IN_temp = IN_temp.T.set_index(tuple(['Select time' for d in index_col])).T
                            else:
                                try:
                                    IN_temp = IN_temp.T.set_index('Select time').T
                                except KeyError:
                                    IN_temp = IN_temp.T.set_index('Time and frequency').T
                            INTLINE_temp = pd.concat([INTLINE_temp, IN_temp])
                else:
                    IN_t = pd.read_html(target.get_attribute('outerHTML'), skiprows=skiprows[:-2], header=header, index_col=index_col)[0]
                    ActionChains(chrome).move_to_element(WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'export-icon')))).perform()
                    WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.ID, 'export-excel-icon'))).click()
                    export = WebDriverWait(chrome, 5).until(EC.frame_to_be_available_and_switch_to_it((By.ID, 'DialogFrame')))
                    WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'btnExportToExcel'))).click()
                link_found = True
            elif address.find('BOI') >= 0:
                timeStart = time.time()
                while True:
                    try:
                        target = WebDriverWait(chrome, 60).until(EC.visibility_of_element_located((By.XPATH, './/div[@data-idx="2"]/button[@title="Enable multiple selection"]')))
                        ActionChains(chrome).click(WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, 'headerApp')))).perform()
                        for i in range(100):
                            ActionChains(chrome).send_keys(Keys.UP).perform()
                        target.click()
                    except TimeoutException:
                        if int(time.time()-timeStart) >= 300:
                            ERROR('The website is not correctly loaded: '+str(fname))
                        chrome.refresh()
                    else:
                        break
                for tab in re.split(r', ', str(file_name)):
                    while WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[@id="tree_2"]//li[contains(@id, "'+tab+'")]'))).get_attribute('aria-selected') == 'false':
                        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[@id="tree_2"]//li[contains(@id, "'+tab+'")]'))).click()
                ActionChains(chrome).click(WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, 'headerApp')))).perform()
                for i in range(100):
                    ActionChains(chrome).send_keys(Keys.UP).perform()
                WebDriverWait(chrome, 3).until(EC.visibility_of_element_located((By.XPATH, './/div[@data-idx="2"]/button[@id="esportaTaxo"]'))).click()
                time.sleep(1)
                WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'excel'))).click()
                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, 'alldateExport'))).click()
                #WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, 'expData'))).click()
                while WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, 'joinTimeseries'))).get_attribute('checked') != 'true':
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, 'joinTimeseries'))).click()
                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, 'descrizioniExport'))).click()
                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, 'applicaExport'))).click()
                timeStart = time.time()
                while True:
                    try:
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.ID, 'descrizioniExport'))).click()
                    except ElementClickInterceptedException:
                        if int(time.time()-timeStart) >= 300:
                            ERROR('The file was not properly downloaded')
                        time.sleep(0)
                    else:
                        break
                link_found = True
            elif address.find('ANFIA') >= 0:
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Passenger cars, press release tables', text_match=True)
            elif address.find('SIDRA') >= 0:
                WebDriverWait(chrome, 50).until(EC.element_to_be_clickable((By.XPATH, './/button[contains(., "Funções")]'))).click()
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='XLSX', text_match=True)
                time.sleep(20)
            elif address.find('BCB') >= 0:
                if str(sname).find('general government debt') >= 0:
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Divggni.xls')
                else:
                    try:
                        WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/span[contains(., "English")]'))).click()
                    except TimeoutException:
                        time.sleep(0)
                    for code in re.split(r', ', str(file_name).replace('.0','')):
                        target = WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'txCodigo')))
                        ActionChains(chrome).click(target).key_down(Keys.CONTROL).send_keys('A').key_up(Keys.CONTROL).send_keys(Keys.BACKSPACE).perform()
                        target.send_keys(code)
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/td[span[contains(., "By code")]]//img'))).click()
                        WebDriverWait(chrome, 3).until(EC.frame_to_be_available_and_switch_to_it((By.ID, 'iCorpo')))
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="'+code+'"]'))).click()
                        try:
                            ActionChains(chrome).send_keys(Keys.ENTER).perform()
                        except UnexpectedAlertPresentException:
                            ActionChains(chrome).send_keys(Keys.ENTER).perform()
                        chrome.switch_to.default_content()
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="Add series"]'))).click()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="Search series"]'))).click()
                    Select(WebDriverWait(chrome, 5).until(EC.visibility_of_element_located((By.ID, 'lbTipoArq')))).select_by_visible_text('CSV in english')
                    target = chrome.find_element_by_id('dataInicio')
                    chrome.execute_script("arguments[0].setAttribute('value', '01/01/1900')", target)
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="View values"]'))).click()
                    link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='CSV file', text_match=True)
                    chrome.back()
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="Remove"]'))).click()
            elif address.find('COMEX') >= 0:
                chrome.refresh()
                time.sleep(2)
                Selections = {'Initial year':'1997', 'Final year': str(datetime.today().year), 'Initial Month':'January', 'Final month':'December'}
                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/label[contains(., "Exports")]'))).click()
                for item in Selections:
                    if WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[label[contains(., "'+item+'")]]//div[@class="item"]'))).text != Selections[item]:
                        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[label[contains(., "'+item+'")]]//div[@data-dropdown-direction="down"]'))).click()
                        if item.lower() == 'final year':
                            try:
                                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[label[contains(., "'+item+'")]]//div[text()="'+Selections[item]+'"]'))).click()
                            except TimeoutException:
                                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[label[contains(., "'+item+'")]]//div[text()="'+str(datetime.today().year-1)+'"]'))).click()
                        else:
                            WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/div[label[contains(., "'+item+'")]]//div[text()="'+Selections[item]+'"]'))).click()
                if WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/label[contains(., "Detailing by month")]'))).get_attribute('class').find('active') < 0:
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/label[contains(., "Detailing by month")]'))).click()
                if WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/label[contains(., "US$ FOB")]'))).get_attribute('class').find('active') < 0:
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/label[contains(., "US$ FOB")]'))).click()
                if WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/label[contains(., "Net Weight")]'))).get_attribute('class').find('active') >= 0:
                    WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/label[contains(., "Net Weight")]'))).click()
                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/label[contains(., "Values")]'))).click()
                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[contains(., "Query")]'))).click()
                WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/label[contains(., "Export data ")]')))
                time.sleep(2)
                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/label[contains(., "Vertical")]'))).click()
                time.sleep(2)
                WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/button[contains(., "CSV")]'))).click()
                time.sleep(2)
                link_found = True
            elif address.find('FGV') >= 0:
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='SÉRIES INSTITUCIONAIS', text_match=True)
                for item in re.split(r', ', str(file_name)):
                    listed = False
                    while True:
                        try:
                            target = WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/table[@id="cphConsulta_dlsSerie"]')))
                            if target.text.find(item) >= 0:
                                listed = True
                        except TimeoutException:
                            time.sleep(0)
                        if listed == False:
                            WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'txtBuscarSeries'))).click()
                            ActionChains(chrome).key_down(Keys.CONTROL).send_keys('A').key_up(Keys.CONTROL).send_keys(Keys.BACKSPACE).send_keys(item).send_keys(Keys.ENTER).perform()
                            WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.ID, 'btnSelecionarTodas'))).click()
                            time.sleep(3)
                            WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'butBuscarSeriesOK'))).click()
                            time.sleep(2)
                            if WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/table[@id="cphConsulta_dlsSerie"]'))).text.find(item) >= 0:
                                break
                        else:
                            break
                while WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'cphConsulta_chkFerramenta2'))).get_attribute('checked') != 'true':
                    WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'cphConsulta_chkFerramenta2'))).click()
                    time.sleep(2)
                while WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'cphConsulta_rbtSerieHistorica'))).get_attribute('checked') != 'true':
                    WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'cphConsulta_rbtSerieHistorica'))).click()
                    time.sleep(2)
                WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.ID, 'cphConsulta_butVisualizarResultado'))).click()
                WebDriverWait(chrome, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, 'cphConsulta_ifrVisualizaConsulta')))
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Salvar XLS', text_match=True)
            elif address.find('CNI') >= 0:
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='recentseries')
            elif address.find('STANOR') >= 0:
                WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.ID, 'SaveAsHeaderButton'))).click()
                WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/label[contains(., "Excel")]'))).click()
                WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/input[@type="submit"][@value="Save"]'))).click()
                link_found = True
            elif address.find('NORGES') >= 0:
                chrome.execute_script("window.scrollTo(0,2500)")
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='xlsx')
            elif address.find('NIMA') >= 0:
                WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/article[@class="article-card-overlay"]'))).click()
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='xlsm')
            elif address.find('NAV') >= 0:
                WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/li[contains(., "Registered Unemployed")]/a[contains(., "xls")]'))).click()
                link_found = True
            if link_found == False:
                print(link_message)
                raise FileNotFoundError
        except (FileNotFoundError, TimeoutException, StaleElementReferenceException, ElementClickInterceptedException) as e:
            sys.stdout.write('\n')
            #if str(e.__class__.__name__) != 'ElementClickInterceptedException':
            #   print(traceback.format_exc())
            print(str(traceback.format_exc())[:1000])
            y+=500
            if y > height and link_found == False:
                print(traceback.format_exc())
                print(y, height)
                if link_message != None:
                    ERROR(link_message)
                else:
                    ERROR('Download File Not Found.')
        except Exception as e:
            print(traceback.format_exc())
            error_class = e.__class__.__name__ #取得錯誤類型
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            errMsg = "line {} inside {}: [{}] ".format(lineNum, funcName, error_class)
            ERROR('Error in '+str(errMsg)+str(e))
        else:
            done = True
            break
    time.sleep(3)
    if output == True:
        if INTLINE_temp.empty:
            ERROR('Table was not correctly loaded from the web.')
        INTLINE_temp.to_excel(data_path+str(country)+'/'+address+sname+'.xlsx', sheet_name=address[:3])
        print('Download Complete\n')
    elif INTLINE_temp == None:
        if address.find('RBI') >= 0 and str(sname).find('WPI') >= 0:
            time.sleep(100)
        elif address.find('RBI') >= 0 or address.find('DEUSTATIS') >= 0:
            time.sleep(12)
        else:
            time.sleep(3)
        INTLINE_temp = INTLINE_WEBDRIVER(chrome, country, address, sname, tables, header=header, index_col=index_col, skiprows=skiprows, usecols=usecols, nrows=nrows, csv=csv, Zip=Zip, US_address=US_address, encode=encode, specific_sheet=specific_sheet)
        if address.find('RBI') >= 0:
            chrome.refresh()
        elif address.find('ISTAT') >= 0:
            if INTLINE_temp.columns[-1] != IN_t.columns[-1]:
                ERROR('下載下來的Excel檔因時間範圍過長而未能載入完整時間序列，請在tablesINT中修改output參數為TRUE並重新嘗試')
    if len(chrome.window_handles) > 1:
        nohandles = len(chrome.window_handles)-1
        for window in range(nohandles):
            chrome.switch_to.window(chrome.window_handles[-1])
            chrome.close()
            chrome.switch_to.window(chrome.window_handles[0])
    if renote == True:
        if not not note:
            #if address.find('BOE') >= 0:
            #    pd.DataFrame.from_dict(note, orient='index').to_csv(data_path+str(country)+'/'+address+str(sname)+'_Label.csv', header=False)
            #else:
            pd.DataFrame(note).to_csv(data_path+str(country)+'/'+address+str(sname)+'_Notes.csv', header=False, index=False)
        return INTLINE_temp, note
    else:
        return INTLINE_temp

def INTLINE_WEB_TRADE(chrome, country, address, fname, sname, freq=None, header=None, index_col=None, skiprows=None, start_year=None):
    
    chrome.get(fname)
    try:
        xl = win32.gencache.EnsureDispatch('Excel.Application')
    except:
        xl = win32.DispatchEx('Excel.Application')
    xl.DisplayAlerts=False
    xl.Visible = 1
    path = data_path+str(country)+'/'+address+sname+'.xlsx'
    archive_path = data_path+str(country)+'/'+address+'old/'+sname+'.xlsx'
    old_base_year = readExcelFile(data_path+str(country)+'/'+address+sname+'.xlsx', sheet_name_=0, acceptNoFile=False).iloc[0].iloc[0][:4]
    shutil.copy(path, archive_path)
    ExcelFile = xl.Workbooks.Open(Filename=os.path.realpath(path))
    if sname.find('Index') < 0:
        start_year = datetime.today().year - 1
    
    link_list_temp = chrome.find_elements_by_xpath('.//div[@class="stat-cycle_sheet"]/ul')
    for l in range(len(link_list_temp)):
        link_list = chrome.find_elements_by_xpath('.//div[@class="stat-cycle_sheet"]/ul')
        link = link_list[l]
        year = link.text[:4]
        if start_year != None and int(year) < start_year:
            break
        sys.stdout.write("\rGetting Data From Year "+year+" ")
        sys.stdout.flush()
        link.find_element_by_xpath('.//*[@href]').click()
        chrome.find_element_by_xpath('.//a[@data-file_type="CSV"]').click()
        time.sleep(3)
        chrome.execute_script("window.open()")
        chrome.switch_to.window(chrome.window_handles[-1])
        chrome.get('chrome://downloads')
        time.sleep(3)
        excel_file = chrome.execute_script("return document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('div#content  #file-link').text")
        chrome.close()
        chrome.switch_to.window(chrome.window_handles[0])
        chrome.back()
        while True:
            try:
                CSVFILE = xl.Workbooks.Open(Filename=(Path.home() / "Downloads" / excel_file).as_posix())
            except com_error:
                time.sleep(1)
            else:
                break
        new_base_year = re.sub(r'.*?([0-9]{4}).*', r"\1", str(readFile((Path.home() / "Downloads" / excel_file).as_posix(), acceptNoFile=False).iloc[0]).replace('\n',''))
        CSVSHEET = CSVFILE.Worksheets(1)
        CSVSHEET.Name = year
        position = ExcelFile.Sheets.Count+1
        sheet_exist = False
        for sh in range(1, ExcelFile.Sheets.Count+1):
            if ExcelFile.Worksheets(sh).Name == CSVSHEET.Name:
                sheet_exist = True
                position = sh
        if sheet_exist == True:
            ExcelFile.Worksheets(position).Delete()
        if position > ExcelFile.Sheets.Count:
            CSVSHEET.Copy(After=ExcelFile.Worksheets(ExcelFile.Sheets.Count))
        else:
            CSVSHEET.Copy(Before=ExcelFile.Worksheets(position))
        CSVFILE.Close()
        os.remove((Path.home() / "Downloads" / excel_file).as_posix())
        if new_base_year == old_base_year:
            start_year = datetime.today().year - 1
    sys.stdout.write("\n\n")
    ExcelFile.Save()
    ExcelFile.Close()
    xl.Quit()
    
    INTLINE_temp = readExcelFile(path, header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=None, acceptNoFile=False)
    
    return INTLINE_temp

def INTLINE_SINGLEKEY(INTLINE_temp, data_path, country, address, fname, sname, Series, Countries, freq, head=None, index_col=None, transpose=True, Table=None, base_year=0, INTLINE_previous=pd.DataFrame(), FREQLISTW=None, find_unknown=True, note=[], footnote=[]):
    QUAR = ['03','06','09','12']
    if type(INTLINE_temp) != dict and INTLINE_temp.empty == True:
        ERROR('Sheet Not Found: '+data_path+str(country)+'/'+address+fname+', sheet name: '+str(sname))
    if type(INTLINE_temp) != dict and transpose == True:
        INTLINE_temp = INTLINE_temp.T
    elif transpose == True:
        for t in INTLINE_temp:
            INTLINE_temp[t] = INTLINE_temp[t].T
    INTLINE_t = pd.DataFrame()
    key_sum = False
    new_code_t = []
    new_label_t = []
    new_unit_t = []
    new_type_t = []
    new_form_c = []
    new_note_t = []
    dataset = fname

    new_columns = []
    if address.find('BOJ') >= 0:
        for col in INTLINE_temp.columns:
            try:
                if freq == 'A':
                    new_columns.append(str(col))
                elif freq == 'Q':
                    if str(col)[-2:] in QUAR:
                        new_columns.append(pd.Period(str(col), freq='Q').strftime('%Y-Q%q'))
                    else:
                        new_columns.append(str(col))
                elif freq == 'M':
                    new_columns.append(datetime.strptime(str(col), '%Y/%m').strftime('%Y-%m'))
                elif freq == 'D':
                    new_columns.append(datetime.strptime(str(col), '%Y/%m/%d').strftime('%Y-%m-%d'))
            except ValueError:
                new_columns.append(str(col))
        INTLINE_temp.columns = new_columns
    elif address.find('JGBY') >= 0 or address.find('JBA') >= 0:
        INTLINE_temp = INTLINE_temp[Series['M'].loc[Series['M']['DataSet'] == str(fname)]['keyword'].to_list()].copy(deep=True)
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna()]
        fillinNaN = [0 for col in range(INTLINE_temp.shape[1])]
        for dex in range(INTLINE_temp.shape[0]):
            try:
                if freq == 'A':
                    new_columns.append(datetime.strptime(str(INTLINE_temp.index[dex]), '%Y/%m/%d').strftime('%Y'))
                elif freq == 'M':
                    new_columns.append(datetime.strptime(str(INTLINE_temp.index[dex]), '%Y/%m/%d').strftime('%Y-%m'))
            except ValueError:
                ERROR('Incorrect date format: '+str(INTLINE_temp.index[dex]))
            else:
                for col in range(INTLINE_temp.shape[1]):
                    try:
                        fillinNaN[col] = float(INTLINE_temp.loc[INTLINE_temp.index[dex], INTLINE_temp.columns[col]])
                    except ValueError:
                        INTLINE_temp.loc[INTLINE_temp.index[dex], INTLINE_temp.columns[col]] = fillinNaN[col]
        INTLINE_temp['group'] = new_columns
        INTLINE_temp = INTLINE_temp.set_index('group', append=True)
        INTLINE_temp = INTLINE_temp.apply(pd.to_numeric).mean(level='group').T
    elif address.find('ITIA') >= 0:
        if INTLINE_temp.index.name != 'Item_Number':
            print(INTLINE_temp.index)
            ERROR('Index Error: '+str(fname))
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_previous], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp.columns = [datetime.strptime(re.sub(r'^[a-z]\s*', "", str(col).strip()), '%Y%m').strftime('%Y-%m') if re.sub(r'^[a-z]\s*', "", str(col).strip()).isnumeric() else str(col) for col in INTLINE_temp.columns]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        try:
            INTLINE_temp = INTLINE_temp.reset_index().set_index('Item_Name')
        except KeyError:
            ERROR('Index Key Not Found: "Item_Name"')
        INTLINE_temp.index = [str(dex).replace(' ','') for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated()]
    elif address.find('WTRS') >= 0:
        dataset = str(sname)
        INTLINE_temp.columns = [str(col)[:4]+'-'+str(col)[-2:] for col in INTLINE_temp.columns]
    elif address.find('EMPL') >= 0:
        year = 'nan'
        for c in range(INTLINE_temp.shape[1]):
            if c < INTLINE_temp.shape[1]-1 and str(INTLINE_temp.columns[c+1][0]).isnumeric():
                year = str(int(INTLINE_temp.columns[c+1][0]))
            try:
                mth = year+'-'+datetime.strptime(str(INTLINE_temp.columns[c][1]).strip()[:3], '%b').strftime('%m')
                if mth not in new_columns:
                    new_columns.append(mth)
                else:
                    new_columns.append(None)
            except ValueError:
                new_columns.append(None)
        INTLINE_temp.columns = new_columns
        INTLINE_temp.index = [str(dex).replace(' ','') for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated()]
    elif address.find('RPKT') >= 0:
        file_path = data_path+str(country)+'/'+address+'RPKT_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        KEYS = list(Series[freq].loc[Series[freq]['DataSet']==dataset]['keyword'])
        INTLINE_temp.columns = INTLINE_temp.iloc[8]
        INTLINE_temp = INTLINE_temp.loc[:, INTLINE_temp.columns.dropna()]
        drop_list = []
        for dex in INTLINE_temp.index:
            if str(dex).isnumeric() == False or int(dex) not in KEYS:
                drop_list.append(dex)
        INTLINE_temp = INTLINE_temp.drop(index=drop_list)
        if INTLINE_temp.empty:
            ERROR('Empty Data File: '+str(fname))
        new_columns = []
        yr = ''
        for col in INTLINE_temp.columns:
            if bool(re.search(r'[0-9]{4}', str(col))):
                yr = re.sub(r'.*?([0-9]{4}).*', r"\1", str(col))
            try:
                new_columns.append(yr+'-'+datetime.strptime(str(col).strip()[:3], '%b').strftime('%m'))
            except ValueError:
                new_columns.append(None)
        INTLINE_temp.columns = new_columns
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp.to_excel(file_path, sheet_name='RPKT')
    elif address.find('NIKK') >= 0:
        INTLINE_temp.index = [str(dex).strip() for dex in INTLINE_temp.index]
        if freq == 'W':
            new_columns = []
            if INTLINE_temp.columns[-1] > FREQLISTW[-1]:
                new_columns.append(None)
                week = -1
            else:
                delta = 5-datetime.strptime(INTLINE_temp.columns[-1], '%Y-%m-%d').weekday()
                if delta < 0:
                    delta+=7
                coldate = (datetime.strptime(INTLINE_temp.columns[-1], '%Y-%m-%d')+timedelta(days=delta)).strftime('%Y-%m-%d')
                new_columns.append(FREQLISTW[list(FREQLISTW).index(coldate)-len(FREQLISTW)])
                week = list(FREQLISTW).index(coldate)-len(FREQLISTW)-1
            for i in reversed(range(INTLINE_temp.shape[1]-1)):
                if INTLINE_temp.columns[i] <= FREQLISTW[week] and FREQLISTW[week] < INTLINE_temp.columns[i+1]:
                    while INTLINE_temp.columns[i] <= FREQLISTW[week-1]:
                        week -= 1
                    new_columns.append(FREQLISTW[week])
                    week -= 1
                else:
                    new_columns.append(None)
            INTLINE_temp.columns = reversed(new_columns)
            INTLINE_temp = INTLINE_temp.loc[:, INTLINE_temp.columns.dropna()]
    elif address.find('PPI') >= 0:
        if dataset.find('Rate') >= 0:
            INTLINE_temp = INTLINE_temp.loc[Series['M'].loc[Series['M']['DataSet'] == str(fname)]['keyword'].to_list()]
            freqlist = pd.date_range(start=INTLINE_temp.columns[-1],end=datetime.today(),freq='M').strftime('%Y-%m-%d').tolist()
            IN_t = pd.DataFrame(index=INTLINE_temp.index, columns=[datetime.strptime(i, '%Y-%m-%d').strftime('%Y-%m') for i in freqlist])
            freqlist.reverse()
            num = 0
            indexerr = False
            num = pd.Series(index=INTLINE_temp.index, dtype=int)
            standard = pd.Series(index=INTLINE_temp.index, dtype=str)
            for s in INTLINE_temp.index:
                for n in range(len(INTLINE_temp.columns)):
                    try:
                        float(INTLINE_temp.loc[s, INTLINE_temp.columns[n]])
                    except ValueError:
                        continue
                    else:
                        num.loc[s] = n
                        standard.loc[s] = INTLINE_temp.columns[num.loc[s]]
                        break
            for day in freqlist:
                for ind in IN_t.index:
                    if day >= standard.loc[ind]:
                        IN_t.loc[ind, datetime.strptime(day, '%Y-%m-%d').strftime('%Y-%m')] = float(str(INTLINE_temp.loc[ind, standard.loc[ind]]))
                    else:
                        while True:
                            try:
                                num.loc[ind] += 1
                                standard.loc[ind] = INTLINE_temp.columns[num.loc[ind]]
                                value = float(str(INTLINE_temp.loc[ind, standard.loc[ind]]))
                            except IndexError:
                                indexerr = True
                                break
                            except ValueError:
                                continue
                            else:
                                if day >= standard.loc[ind]:
                                    break
                        if indexerr == True:
                            break
                        IN_t.loc[ind, datetime.strptime(day, '%Y-%m-%d').strftime('%Y-%m')] = value
            if freq == 'A' or freq == 'Q':
                if transpose == True:
                    IN_t = IN_t.T
                for dex in IN_t.index:
                    if freq == 'A':
                        new_columns.append(datetime.strptime(dex, '%Y-%m').strftime('%Y'))
                    elif freq == 'Q':
                        new_columns.append(pd.Period(dex, freq='Q').strftime('%Y-Q%q'))
                IN_t['group'] = new_columns
                IN_t = IN_t.set_index('group', append=True)
                IN_t = IN_t.apply(pd.to_numeric).mean(level='group')
                if transpose == True:
                    IN_t = IN_t.T
            INTLINE_temp = IN_t
        if freq == 'Q':
            INTLINE_temp.columns = [str(col)[:4]+'-Q'+str(col)[-1:] for col in INTLINE_temp.columns]
    elif address.find('SIPR') >= 0 or address.find('GACC/SUM') >= 0:
        file_path = data_path+str(country)+'/'+address+str(fname)+' - Historical.xlsx'
        IN_his = readExcelFile(file_path, header_=0, index_col_=0, sheet_name_=sname)
        IN_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in IN_his.columns]
        if INTLINE_PRESENT(file_path) == True:
            INTLINE_temp = IN_his
        else:
            INTLINE_temp.columns = [str(col).strip()[:4]+'-'+str(col).strip()[-2:] if bool(re.match(r'[0-9]{4}\.[0-9]{2}$', str(col).strip())) else None for col in INTLINE_temp.columns]
            INTLINE_temp = INTLINE_temp.loc[:, INTLINE_temp.columns.dropna()]
            if address.find('GACC/SUM') >= 0:
                INTLINE_temp = pd.concat([INTLINE_temp.loc[['Balance']], INTLINE_temp.loc[['Export','Import']].applymap(lambda x: float(x)/100)])
            INTLINE_temp = pd.concat([IN_his, INTLINE_temp], axis=1)
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
            INTLINE_temp.to_excel(file_path, sheet_name='Monthly')
    elif address.find('HKMA') >= 0:
        if str(fname).find('Money supply') >= 0 or str(fname).find('Residential mortgage survey results') >= 0:
            INTLINE_previous.columns = [str(col[0])+'-'+datetime.strptime(str(col[1]), '%b').strftime('%m') for col in INTLINE_previous.columns]
            INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_previous], axis=1)
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
            INTLINE_temp = INTLINE_temp.sort_index(axis=1)
    elif address.find('MAS') >= 0:
        if address.find('SGSY') >= 0:
            new_columns = []
            for col in INTLINE_temp.columns:
                if str(col[0]).strip().isnumeric():
                    year = str(col[0]).strip()
                try:
                    new_columns.append(year+'-'+datetime.strptime(str(col[1]), '%b').strftime('%m'))
                except ValueError:
                    new_columns.append(None)
            INTLINE_temp.columns = new_columns
            INTLINE_temp = INTLINE_temp.loc[:, INTLINE_temp.columns.dropna()]
    elif address.find('BOK') >= 0:
        INTLINE_temp.columns = [str(col).strip() for col in INTLINE_temp.columns]
        if freq == 'Q':
            INTLINE_temp.columns = [col[:4]+'-Q'+col[-1:] if col[:4].isnumeric() else col for col in INTLINE_temp.columns]
            new_index = []
            for dex in INTLINE_temp.index:
                if str(dex).strip() == 'Construction':
                    new_index.append(re.sub(r'[^A-Z]+', "", str(INTLINE_temp.loc[dex, 'StatisticalTable']))[0]+str(dex))
                elif str(dex).find('F.O.B') >= 0:
                    new_index.append(re.sub(r'[^A-Z]+', "", str(previous_index))[0]+str(dex))
                else:
                    new_index.append(re.sub(r'\s+', "" , str(dex)).strip())
                previous_index = dex
            INTLINE_temp.index = new_index
        if freq == 'M':
            INTLINE_temp.columns = [col[:4]+'-'+col[-2:] if col[:4].isnumeric() else col for col in INTLINE_temp.columns]
        if type(INTLINE_temp.index[0]) == tuple:
            INTLINE_temp.index = [re.sub(r'\s+', "" , str(''.join(dex))).strip() for dex in INTLINE_temp.index]
        else:
            INTLINE_temp.index = [re.sub(r'\s+', "" , str(dex)).strip() for dex in INTLINE_temp.index]
        new_note = []
        for i in range(INTLINE_temp.shape[0]):
            if bool(re.search(r'\(.*?year.*?\)', str(INTLINE_temp.iloc[i]['StatisticalTable']))):
                new_note.append(re.sub(r'.*?\(.*?([^,]*?year[^,]*).*?\).*', r"\1", str(INTLINE_temp.iloc[i]['StatisticalTable'])).strip().title())
            else:
                new_note.append('nan')
        INTLINE_temp['Notes'] = new_note
    elif address.find('RBA') >= 0 or address.find('ABS') >= 0 or address.find('DEMP') >= 0:
        if str(fname) == '5368031':
            INTLINE_previous = INTLINE_previous.applymap(lambda x: float(x)*(-1))
            INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_previous], axis=0)
            key = str(Series[freq].loc[Series[freq]['DataSet'] == dataset]['keyword'].item())
            Keywords = re.split(r'-', key)
            INTLINE_temp.index = [None if str(dex).strip() not in Keywords else str(dex).strip() for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
            INTLINE_temp = pd.DataFrame(INTLINE_temp.sum(axis=0), columns=[key])
            INTLINE_temp['quarter'] = [str(dex.year)+'-Q'+str(dex.quarter) if str(dex)[:4].isnumeric() else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.set_index('quarter', append=True)
            INTLINE_temp = INTLINE_temp.apply(pd.to_numeric).sum(level='quarter').T
        else:
            if str(fname).find('Industry_GVA') >= 0:
                dataset = str(fname)+str(sname)
            if INTLINE_previous.empty == False:
                INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_previous], axis=1)
            if freq == 'A':
                INTLINE_temp = INTLINE_temp.T
                INTLINE_temp['year'] = [str(dex.year) if str(dex)[:4].isnumeric() else None for dex in INTLINE_temp.index]
                INTLINE_temp = INTLINE_temp.set_index('year', append=True)
                INTLINE_temp = INTLINE_temp.apply(pd.to_numeric).mean(level='year').T
            elif freq == 'S':
                INTLINE_temp.columns = [str(col.year)+'-S'+str(int(col.quarter/2)) if str(col)[:4].isnumeric() else None for col in INTLINE_temp.columns]
            elif freq == 'Q':
                INTLINE_temp.columns = [str(col.year)+'-Q'+str(col.quarter) if str(col)[:4].isnumeric() else None for col in INTLINE_temp.columns]
            elif freq == 'M':
                INTLINE_temp.columns = [str(col.year)+'-'+str(col.month).rjust(2,'0') if str(col)[:4].isnumeric() else None for col in INTLINE_temp.columns]
    elif address.find('SNMO') >= 0:
        INTLINE_temp.columns = [int(col.strftime('%Y')) if type(col) != int else col for col in INTLINE_temp.columns]
        INTLINE_temp.to_excel(data_path+str(country)+'/'+address+fname+'.xlsx', sheet_name=fname)
    elif address.find('RKB') >= 0:
        file_path = data_path+str(country)+'/'+address+'RKB_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        INTLINE_his.columns = [datetime.strptime(str(col), '%Y %B').strftime('%Y-%m') if str(col).strip()[-2:].isnumeric() == False else col for col in INTLINE_his.columns]
        INTLINE_his.index = [str(dex).replace(' ','') for dex in INTLINE_his.index]
        INTLINE_temp.columns = [datetime.strptime(str(col), '%Y %B').strftime('%Y-%m') if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp = INTLINE_temp.loc[:, INTLINE_temp.columns.dropna()]
        INTLINE_temp.index = [str(dex).replace(' ','') for dex in INTLINE_temp.index]
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp.to_excel(file_path, sheet_name='RKB_historical')
    elif address.find('MOSPI') >= 0 or address.find('MOCI') >= 0:
        file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
        if freq == 'A':
            INTLINE_his.columns = [int(str(col).strip()[:4]) if str(col).strip()[:4].isnumeric() else col for col in INTLINE_his.columns]
        elif freq == 'Q':
            INTLINE_his.columns = [pd.Period(col, freq='Q').strftime('%Y-Q%q') if type(col) != str else col for col in INTLINE_his.columns]
        elif freq == 'M':
            INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
        if address.find('KAPSARC') >= 0:
            Table = Table.reset_index().set_index('File or Sheet')
            if freq == 'A':
                file_date = 'Year'
                if str(fname).find('Population') >= 0:
                    file_date = 'DATE'
                    INTLINE_temp = INTLINE_temp.loc[INTLINE_temp['INDICATOR_NAME']=='Projected Population']
                elif str(fname).find('Mortality') >= 0:
                    file_date = 'DATE'
            if INTLINE_previous.empty == False:
                INTLINE_his = INTLINE_KAPSARC(INTLINE_his, INTLINE_previous, data_path, country, address, Table.loc[fname, 'previous_data'], Series, KEYS, freq, keyword=Table.loc[Table.loc[fname, 'previous_data'], 'keyword'], file_date=file_date)
            INTLINE_temp = INTLINE_KAPSARC(INTLINE_his, INTLINE_temp, data_path, country, address, fname, Series, KEYS, freq, keyword=Table.loc[fname, 'keyword'], file_date=file_date)
        elif address.find('NAD') >= 0 or address.find('NAS') >= 0:
            using_columns = False
            for col in INTLINE_temp.columns:
                if re.sub(r'.*?([0-9]{4}).*', r"\1", str(col)).isnumeric():
                    year = re.sub(r'.*?([0-9]{4}).*', r"\1", str(col))
                if str(col).find('Rupees in crore') >= 0 or str(col).find('Current Price') >= 0 or str(fname).find('PCIPFC') >= 0:
                    using_columns = True
                elif str(col).find('GROWTH RATE') >= 0 or str(col).find('constant') >= 0:
                    using_columns = False
                if using_columns == True and freq == 'A' and re.sub(r'.*?([0-9]{4}).*', r"\1", str(col)).isnumeric():
                    if int(re.sub(r'.*?([0-9]{4}).*', r"\1", str(col))) not in new_columns:
                        new_columns.append(int(re.sub(r'.*?([0-9]{4}).*', r"\1", str(col))))
                    else:
                        new_columns.append(None)
                elif using_columns == True and freq == 'Q' and bool(re.search(r'Q[1-4]', str(col))):
                    if year+'-'+re.sub(r'.*?(Q[1-4]).*', r"\1", str(col)) not in new_columns:
                        new_columns.append(year+'-'+re.sub(r'.*?(Q[1-4]).*', r"\1", str(col)))
                    else:
                        new_columns.append(None)
                elif str(col).upper().find('ITEM') >= 0:
                    new_columns.append('Item')
                else:
                    new_columns.append(None)
            INTLINE_temp.columns = new_columns
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
            try:
                INTLINE_temp = INTLINE_temp.set_index('Item')
            except KeyError:
                ERROR('Item Index Not Found in file: '+str(fname))
            if str(fname).find('PCIPFC') >= 0:
                new_index = []
                using_index = False
                for dex in INTLINE_temp.index:
                    if str(dex).find('current') >= 0 or str(dex).find('change') >= 0:
                        using_index = False
                    elif str(dex).find('constant') >= 0:
                        using_index = True
                        note_content = str(dex).strip()
                    if using_index:
                        new_index.append(dex)
                    else:
                        new_index.append(None)
                INTLINE_temp.index = new_index
                INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna()]
            INTLINE_temp.index = [re.sub(r'\s+', " ", re.sub(r'[^A-Za-z\s,&]+', "", str(dex))).strip() for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated(), INTLINE_temp.columns.dropna()]
            INTLINE_temp.index = [dex if dex in KEYS else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna()]
        elif address.find('STATYB') >= 0:
            CONTINENT = ['NORTH AMERICA','CENTRAL AND SOUTH AMERICA','WESTERN EUROPE','EASTERN EUROPE','AFRICA','WEST ASIA','SOUTH ASIA','SOUTH EAST ASIA','EAST ASIA','AUSTRALASIA','STATELESS']
            INTLINE_temp.columns = [int(str(col).strip()[:4]) if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
            new_index = []
            continent = ''
            for dex in INTLINE_temp.index:
                if str(dex).strip() in CONTINENT:
                    continent = str(dex).strip()
                if str(dex).find('Others') >= 0 or str(dex).find('Total') >= 0:
                    new_index.append(continent+', '+str(dex).strip())
                elif str(dex) != 'nan' and bool(re.search(r'[0-9]+', str(dex))) == False:
                    new_index.append(str(dex).strip())
                else:
                    new_index.append(None)
            INTLINE_temp.index = new_index
            INTLINE_temp.index = [dex if dex in KEYS else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
            if True in INTLINE_temp.index.duplicated():
                print(list(INTLINE_temp.index))
                ERROR('Duplicated Index Found in file: '+str(fname))
            INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x))
        elif address.find('IIP') >= 0:
            INTLINE_temp.columns = [col.strftime('%Y-%m') if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
            new_index = []
            using_index = True
            for dex in INTLINE_temp.index:
                if str(dex).find('growth rate') >= 0:
                    using_index = False
                elif str(dex).find('indice') >= 0:
                    using_index = True
                if using_index:
                    if str(fname).find('NIC') >= 0: 
                        if str(dex[0]).strip().isnumeric():
                            new_index.append(str(dex[1]).strip())
                        else:
                            new_index.append(str(dex[0]).strip())
                    else:
                        new_index.append(str(dex).strip())
                else:
                    new_index.append(None)
            INTLINE_temp.index = new_index
            INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated()]
            INTLINE_temp.index = [dex if dex in KEYS else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        elif address.find('APEDA') >= 0:
            multiplier = float(Series[freq].loc[(Series[freq]['DataSet']==str(fname)) & (Series[freq]['keyword']==KEYS[0])]['UnitChange'].item())
            INTLINE_temp.columns = [int(str(col[0]).strip()[:4]) if (type(col) == tuple and str(col[1]).strip() == 'Production') else None for col in INTLINE_temp.columns]
            INTLINE_temp.index = [str(dex).strip() if str(dex).find('Page Total') >= 0 else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()].applymap(lambda x: float(x)*multiplier)
        elif address.find('MOCI') >= 0:
            INTLINE_temp.index = [str(dex).strip() if str(dex).strip() in KEYS else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna()]
        if address.find('KAPSARC') < 0:
            INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp.to_excel(file_path, sheet_name=fname)
        if freq == 'A':
            INTLINE_temp.columns = [str(col).replace('.0','') for col in INTLINE_temp.columns]
        if str(fname).find('PCIPFC') >= 0:
            INTLINE_temp['Notes'] = [note_content for i in range(INTLINE_temp.shape[0])]
    elif address.find('RBI') >= 0:
        if str(fname).find('Yield_of_SGL') >= 0:
            Table = Table.reset_index().set_index('File or Sheet')
            INTLINE_temp.columns = [str(col).strip() if str(col).find('Unnamed') < 0 else None for col in INTLINE_temp.columns]
            INTLINE_temp = INTLINE_temp.loc[:, INTLINE_temp.columns.dropna()]
            IN_t = pd.DataFrame()
            for h in range(INTLINE_temp.shape[0]):
                if bool(re.search(r'[0-9]{4}\-[0-9]{2}', str(INTLINE_temp.index[h]))):
                    table_head = h+1
                    for i in range(h+2, INTLINE_temp.shape[0]):
                        if bool(re.search(r'[0-9]{4}\-[0-9]{2}', str(INTLINE_temp.index[i]))) or str(INTLINE_temp.index[i]) == 'nan':
                            table_tail = i
                            break
                    IN = readExcelFile(data_path+str(country)+'/'+address+fname+'.xlsx', header_=head, index_col_=index_col, skiprows_=list(range(table_head+int(Table['skip'][fname]))), nrows_=table_tail-table_head, sheet_name_=sname)
                    IN = IN.dropna(axis=1, how='all')
                    IN.columns = [str(INTLINE_temp.index[h])[:4]+'-'+datetime.strptime(str(col), '%b').strftime('%m') if datetime.strptime(str(col), '%b').strftime('%m') >= datetime.strptime(str(INTLINE_temp.columns[0]), '%b').strftime('%m') else str(int(str(INTLINE_temp.index[h])[:4])+1)+'-'+datetime.strptime(str(col), '%b').strftime('%m') for col in INTLINE_temp.columns]
                    IN_t = pd.concat([IN_t, IN], axis=1)
            INTLINE_temp = IN_t
            INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        elif str(fname).find('Ratios_and_Rates') >= 0:
            for i in range(INTLINE_temp.shape[0]):
                for j in range(INTLINE_temp.shape[1]):
                    if str(INTLINE_temp.iloc[i].iloc[j]) == '-':
                        INTLINE_temp.loc[INTLINE_temp.index[i], INTLINE_temp.columns[j]] = INTLINE_temp.iloc[i].iloc[j+1]
                    elif str(INTLINE_temp.iloc[i].iloc[j]).find('/') >= 0:
                        if str(INTLINE_temp.index[i]).find('Base Rate') >= 0:
                            INTLINE_temp.loc[INTLINE_temp.index[i], INTLINE_temp.columns[j]] = (float(re.split('/', str(INTLINE_temp.iloc[i].iloc[j]))[0])+float(re.split('/', str(INTLINE_temp.iloc[i].iloc[j]))[1]))/2
                        elif str(INTLINE_temp.index[i]).find('Term Deposit Rate >1 Year') >= 0:
                            INTLINE_temp.loc[INTLINE_temp.index[i], INTLINE_temp.columns[j]] = float(re.split('/', str(INTLINE_temp.iloc[i].iloc[j]))[1])
        elif str(fname).find('CPI_for_Industrial_Worker') >= 0 or str(fname).find('WPI') >= 0:
            file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
            INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
            KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
            INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
            base_path = data_path+str(country)+'/'+address+'base_year.csv'
            base_year_list = readFile(base_path, header_=[0], index_col_=0, acceptNoFile=False)
            if str(base_year_list.loc[fname, 'base year']) != str(base_year):
                print('Modifying Data with new base year')
                base_period = str(base_year)[:4]+'-12'
                for ind in INTLINE_his.index:
                    multiplier = 100/INTLINE_his.loc[ind, base_period]
                    for col in INTLINE_his.columns:
                        INTLINE_his.loc[ind, col] = float(INTLINE_his.loc[ind, col])*multiplier
                base_year_list.loc[fname, 'base year'] = base_year
                base_year_list.to_csv(base_path)
            if str(fname).find('WPI') >= 0:
                for col in INTLINE_temp.columns:
                    if pycnnum.cn2num(str(col).strip()[:2]) != 0 and str(col).strip()[-4:].isnumeric():
                        new_columns.append(str(col).strip()[-4:]+'-'+str(pycnnum.cn2num(str(col).strip()[:2])).rjust(2, '0'))
                    else:
                        new_columns.append(None)
                INTLINE_temp.columns = new_columns
            else:
                INTLINE_temp.columns = [str(col)[:7] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
            INTLINE_temp.index = [re.sub(r'\(.*?\+.*?\)', "", str(dex)).strip() if re.sub(r'\(.*?\+.*?\)', "", str(dex)).strip() in KEYS else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
            INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
            INTLINE_temp = INTLINE_temp.sort_index(axis=1)
            INTLINE_temp.to_excel(file_path, sheet_name=fname)
        if freq == 'A':
            INTLINE_temp.columns = [str(col).strip()[:4] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'Q' and str(fname).find('External_Debt') >= 0:
            INTLINE_temp.columns = [datetime.strptime(str(col).replace('PR','').strip()[-2:], '%y').strftime('%Y')+'-Q'+str(pd.Period(str(col).replace('PR','').strip(), freq='Q').quarter) if str(col).replace('PR','').strip()[-2:].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'Q' and str(fname).find('International_Investment_Position') >= 0:
            INTLINE_temp.columns = [re.sub(r'.*?([0-9]{4}).*?(Q[1-4]).*', r"\1-\2", str(col).strip()) if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'Q' and str(fname).find('Standard_Presentation_of_BoP_in_India_as_per_BPM6') >= 0:
            QUAR = {'01':'1','04':'2','07':'3','10':'4'}
            quarter_date = 'nan'
            for col in INTLINE_temp.columns:
                if str(col[0]) != 'nan':
                    new_columns.append(col)
                    quarter_date = str(col[0]).strip()
                else:
                    new_columns.append([quarter_date, str(col[1])])
            INTLINE_temp.columns = pd.MultiIndex.from_tuples(new_columns)
            if str(fname).find('Net') >= 0:
                INTLINE_temp.columns = [re.sub(r'.*?([0-9]{4}).*', r"\1", str(col[0]))+'-Q'+QUAR[datetime.strptime(str(col[0]).strip()[:3], '%b').strftime('%m')] if str(col[1]).find('Net') >= 0 else None for col in INTLINE_temp.columns]
            elif str(fname).find('Debit') >= 0:
                INTLINE_temp.columns = [re.sub(r'.*?([0-9]{4}).*', r"\1", str(col[0]))+'-Q'+QUAR[datetime.strptime(str(col[0]).strip()[:3], '%b').strftime('%m')] if str(col[1]).find('Debit') >= 0 else None for col in INTLINE_temp.columns]
            elif str(fname).find('Credit') >= 0:
                INTLINE_temp.columns = [re.sub(r'.*?([0-9]{4}).*', r"\1", str(col[0]))+'-Q'+QUAR[datetime.strptime(str(col[0]).strip()[:3], '%b').strftime('%m')] if str(col[1]).find('Credit') >= 0 else None for col in INTLINE_temp.columns]
        elif freq == 'M' and str(fname).find('Scheduled_Commercial_Banks') >= 0:
            dataset = fname+', '+sname
            INTLINE_temp.columns = [str(col)[:7] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'M' and str(fname).find('Business_of_Scheduled_Banks_in_India') >= 0:
            dataset = fname+', '+sname
            INTLINE_temp.columns = [str(col[1])+'-'+datetime.strptime(str(col[0]), '%b').strftime('%m') if str(col[1]).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'M' and str(fname).find('Auctions_of_Treasury_Bills') >= 0:
            INTLINE_temp.columns = [str(col[0])[:7] if str(col[0]).strip()[8:10].isnumeric() else None for col in INTLINE_temp.columns]
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        elif freq == 'M' and str(fname).find('Foreign_Exchange_Reserves') >= 0:
            INTLINE_temp.columns = [str(col[1])[:7] if str(col[1]).strip()[8:10].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'M' and str(fname).find('Ratios_and_Rates') >= 0:
            for col in INTLINE_temp.columns:
                if pycnnum.cn2num(str(col).strip()[:2]) != 0 and str(col).strip()[-4:].isnumeric():
                    new_columns.append(str(col).strip()[-4:]+'-'+str(pycnnum.cn2num(str(col).strip()[:2])).rjust(2, '0'))
                else:
                    new_columns.append(None)
            INTLINE_temp.columns = new_columns
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        elif freq == 'M' and str(fname).find('NRI_Deposits') >= 0:
            year = ''
            for col in INTLINE_temp.columns:
                if bool(re.search(r'[0-9]{4}\-[0-9]{2}', str(col[0]))):
                    year = str(col[0]).strip()[:4]
                try:
                    month = datetime.strptime(str(col[0])[:3], '%b').strftime('%m')
                except ValueError:
                    new_columns.append(None)
                else:
                    if str(col[1]) != 'nan':
                        new_columns.append(None)
                    elif int(month) < 4:
                        new_columns.append(str(int(year)+1)+'-'+month)
                    else:
                        new_columns.append(year+'-'+month)
            INTLINE_temp.columns = new_columns
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        elif freq == 'M' and str(fname).find("Trade") >= 0:
            year = ''
            for col in INTLINE_temp.columns:
                if bool(re.search(r'[0-9]{4}\-[0-9]{2}', str(col[0]))):
                    year = str(col[0]).strip()[:4]
                try:
                    month = datetime.strptime(str(col[1])[:3], '%b').strftime('%m')
                except ValueError:
                    new_columns.append(None)
                else:
                    if int(month) < 4:
                        new_columns.append(str(int(year)+1)+'-'+month)
                    else:
                        new_columns.append(year+'-'+month)
            INTLINE_temp.columns = new_columns
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        elif freq == 'M':
            INTLINE_temp.columns = [str(col)[:7] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        if freq == 'Q' and str(fname).find('External_Debt') < 0:
            INTLINE_temp.index = [str(int(dex)) if str(dex)[:1].isnumeric() else None for dex in INTLINE_temp.index]
        elif freq == 'M' and str(fname).find('Liabilities & Assets') >= 0:
            INTLINE_temp.index = [str(dex[1]).strip() if str(dex[1]).strip() != '' else str(dex[0]).strip() for dex in INTLINE_temp.index]
        elif freq == 'M' and str(fname).find('Foreign_Exchange_Reserves') >= 0:
            new_index = []
            currency = ''
            for dex in INTLINE_temp.index:
                if str(dex[0]).find('Unnamed') < 0 and str(dex[0]) != 'nan':
                    currency = str(dex[0]).strip()
                new_index.append(currency+str(dex[1]).strip())
            INTLINE_temp.index = new_index
        elif freq == 'M' and str(fname).find('Merchandise_Trade') >= 0:
            new_index = []
            trade = ''
            for dex in INTLINE_temp.index:
                if str(dex[0]).find('Unnamed') < 0 and str(dex[0]) != 'nan':
                    trade = str(dex[0]).strip()
                new_index.append(trade+str(dex[1]).strip())
            INTLINE_temp.index = new_index
        else:
            INTLINE_temp.index = [str(dex).strip() if str(dex).find('Unnamed') < 0 else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        if freq == 'M':
            INTLINE_temp = INTLINE_temp.sort_index(axis=1)
    elif address.find('CGB') >= 0:
        INTLINE_temp.columns = [pd.Period(col, freq='Q').strftime('%Y-Q%q') if type(col) != str else col for col in INTLINE_temp.columns]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
    elif address.find('CFIB') >= 0:
        INTLINE_temp.columns = [str(col)[:7] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated(keep='last')]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
    elif address.find('DEUSTATIS') >= 0:
        if type(INTLINE_temp) == dict:
            IN_temp = pd.DataFrame()
            if type(INTLINE_previous) == dict:
                for yr in INTLINE_temp:
                    IN = INTLINE_DEUSTATIS(INTLINE_temp[yr], data_path, country, address, fname, sname, Series, Countries, freq, head, index_col, transpose, Table, base_year, INTLINE_previous[yr])
                    IN_temp = pd.concat([IN_temp, IN], axis=1)
            else:
                for yr in INTLINE_temp:
                    IN = INTLINE_DEUSTATIS(INTLINE_temp[yr], data_path, country, address, fname, sname, Series, Countries, freq, head, index_col, transpose, Table, base_year, INTLINE_previous)
                    IN_temp = pd.concat([IN_temp, IN], axis=1)
            IN_temp = IN_temp.loc[~IN_temp.index.duplicated(), ~IN_temp.columns.duplicated()]
            IN_temp = IN_temp.sort_index(axis=1)
            INTLINE_temp = IN_temp
        else:
            INTLINE_temp = INTLINE_DEUSTATIS(INTLINE_temp, data_path, country, address, fname, sname, Series, Countries, freq, head, index_col, transpose, Table, base_year, INTLINE_previous)
    elif address.find('BUNDES') >= 0:
        INTLINE_temp.columns = ['Unit' if str(col).strip() == 'unit' else col for col in INTLINE_temp.columns]
        if freq == 'A' and str(fname).find('monthly average') >= 0:
            INTLINE_temp.columns = pd.MultiIndex.from_tuples([re.split(r'\-', str(col).strip()) if str(col).strip()[:4].isnumeric() else [None,None] for col in INTLINE_temp.columns])
            INTLINE_temp.columns.names = ['year','month']
            INTLINE_temp = INTLINE_temp.loc[:, INTLINE_temp.columns.dropna()].applymap(lambda x: float(x) if str(x)[-1].isnumeric() else np.nan).T
            INTLINE_temp = INTLINE_temp.apply(pd.to_numeric).mean(level='year').T
        elif freq == 'A' and str(fname).find('monthly end') >= 0:
            INTLINE_temp.columns = [None if (str(col) != 'Unit' and str(col).strip()[-2:] != '12') else col for col in INTLINE_temp.columns]
            INTLINE_temp.columns = [str(col).strip()[:4] if str(col).strip()[:4].isnumeric() else col for col in INTLINE_temp.columns]
        elif freq == 'Q' and str(fname).find('monthly end') >= 0:
            INTLINE_temp.columns = [None if (str(col) != 'Unit' and str(col).strip()[-2:] not in ['03','06','09','12']) else col for col in INTLINE_temp.columns]
            INTLINE_temp.columns = [str(col).strip()[:4]+'-Q'+str(int(int(str(col).strip()[-2:])/3)) if str(col).strip()[:4].isnumeric() else col for col in INTLINE_temp.columns]
        elif freq == 'A':
            INTLINE_temp.columns = [None if str(col) == 'nan' else col for col in INTLINE_temp.columns]
        elif freq == 'M':
            INTLINE_temp.columns = [None if str(col) == 'nan' else col for col in INTLINE_temp.columns]
        elif freq == 'Q':
            INTLINE_temp.columns = [str(col).strip()[:4]+'-Q'+str(int(int(str(col).strip()[-2:])/3)+1) if str(col).strip()[:4].isnumeric() else col for col in INTLINE_temp.columns]
            INTLINE_temp.columns = [None if str(col) == 'nan' else col for col in INTLINE_temp.columns]
        if str(fname).find('Consumer price index Long term time series') >= 0:
            INFLATION = {'N':'NSA','Y':'SA'}
            INTLINE_temp = INTLINE_temp.dropna(how='all').applymap(lambda x: float(x) if str(x).replace('.','',1).replace('-','',1).isdigit() else x)
            for dex in INTLINE_temp.index:
                if str(dex).find('FLAGS') < 0:
                    IN_temp = pd.DataFrame([(INTLINE_temp.loc[dex, col]-INTLINE_temp.loc[dex, str(int(str(col)[:4])-1)+str(col)[-3:]])*100/INTLINE_temp.loc[dex, str(int(str(col)[:4])-1)+str(col)[-3:]] if (str(col)[:4].isnumeric() and str(int(str(col)[:4])-1)+str(col)[-3:] in INTLINE_temp.columns) else None for col in INTLINE_temp.columns]).T
                    IN_temp.columns = INTLINE_temp.columns
                    IN_temp.index = ['CPIIR'+INFLATION[str(dex)[11]]]
                    INTLINE_temp = pd.concat([INTLINE_temp, IN_temp])
        INTLINE_temp.index = [str(dex).strip() for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
    elif address.find('ARBEIT') >= 0:
        dataset = sname
        INTLINE_temp.columns = [str(col).strip()[:7] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
    elif address.find('BPS') >= 0:
        file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
        if freq == 'A':
            INTLINE_his.columns = [int(str(col).strip()[:4]) if str(col).strip()[:4].isnumeric() else col for col in INTLINE_his.columns]
        elif freq == 'S':
            INTLINE_his.columns = [pd.Period(col, freq='Q').strftime('%Y-Q%q').replace('Q1','S1').replace('Q3','S2') if type(col) != str else col for col in INTLINE_his.columns]
        elif freq == 'Q':
            INTLINE_his.columns = [pd.Period(col, freq='Q').strftime('%Y-Q%q') if type(col) != str else col for col in INTLINE_his.columns]
        elif freq == 'M':
            INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
        base_path = data_path+str(country)+'/'+address+'base_year.csv'
        base_year_list = readFile(base_path, header_=[0], index_col_=0, acceptNoFile=False)
        if str(fname).find('ENTRY') < 0 and str(fname).find('ROOM') < 0 and str(fname).find('GOODS') < 0 and str(fname).find('TRADE') < 0 and str(fname).find('LABOR') < 0:
            if str(base_year_list.loc[fname, 'base year']) != str(base_year):
                if str(base_year) == '0':
                    ERROR('Incorrect Base Year for file: '+str(fname))
                print('Modifying Data with new base year')
                for ind in INTLINE_his.index:
                    if freq == 'Q':
                        new_base = sum([INTLINE_his.loc[ind, str(base_year)+'-Q'+str(num)] for num in [1,2,3,4]])/4
                    elif freq == 'M':
                        new_base = sum([INTLINE_his.loc[ind, str(base_year)+'-'+str(num).rjust(2,'0')] for num in range(1,13)])/12
                    multiplier = 100/new_base
                    for col in INTLINE_his.columns:
                        INTLINE_his.loc[ind, col] = float(INTLINE_his.loc[ind, col])*multiplier
                base_year_list.loc[fname, 'base year'] = base_year
                base_year_list.to_csv(base_path)
        yr = ''
        if str(fname).find('CPI') >= 0:
            Route_keys = [re.split(r'//', str(route))[-1] for route in list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['Routes'])]
            IN_temp = pd.DataFrame()
            for key in Route_keys:
                try:
                    IN_t = INTLINE_temp.xs(key, axis=1, level=0)
                except KeyError:
                    IN_t = INTLINE_temp.xs(key, axis=1, level=1)
                IN_t.columns = [str(col[1]).strip()+'-'+datetime.strptime(str(col[2]).strip(), '%B').strftime('%m') if str(col[1]).strip().isnumeric() else None for col in IN_t.columns]
                IN_t.index = [re.sub(r'(.+?),.+', r"\1", key)]
                IN_temp = pd.concat([IN_temp, IN_t])
            INTLINE_temp = IN_temp
        elif str(fname).find('TRADE') >= 0:
            Route_keys = [re.split(r'//', str(route))[-1] for route in list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['Routes'])]
            Route_keys = list(set(Route_keys))
            IN_temp = pd.DataFrame()
            for key in Route_keys:
                IN_t = INTLINE_temp.xs(key, axis=1, level=0)
                IN_t.columns = [str(col[0]).strip()+'-'+datetime.strptime(str(col[1]).strip(), '%B').strftime('%m') if str(col[0]).strip().isnumeric() else None for col in IN_t.columns]
                IN_t.index = [re.sub(r'.*?([EI][xm]port).*', r"\1", key)+'//'+str(dex).strip() for dex in IN_t.index]
                IN_temp = pd.concat([IN_temp, IN_t])
            INTLINE_temp = IN_temp
            INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x)[0].isnumeric() else np.nan)
            IN_sub = INTLINE_temp.loc['Export//Total'].sub(INTLINE_temp.loc['Import//Total'])
            IN_sub.name = 'Balance'
            INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_sub).T])
        elif freq == 'A':
            INTLINE_temp.columns = [int(str(col).strip()) if str(col).strip().isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'S':
            for col in INTLINE_temp.columns:
                if str(col[0]).strip().isnumeric():
                    yr = str(col[0]).strip()
                try:
                    new_columns.append(yr+'-S'+str(int(int(datetime.strptime(str(col[1]).strip(), '%B').strftime('%m'))/6)+1))
                except ValueError:
                    new_columns.append(None)
            INTLINE_temp.columns = new_columns
        elif freq == 'Q':
            for col in INTLINE_temp.columns:
                if str(col[0]).strip().isnumeric():
                    yr = str(col[0]).strip()
                if str(col[1]).strip()[-1].isnumeric():
                    new_columns.append(yr+'-Q'+str(col[1]).strip()[-1])
                else:
                    quar = re.sub(r'.*?Q[a-z]+\s+([IV]+).*', r"\1", str(col[1]))
                    new_columns.append(yr+'-Q'+str(roman.fromRoman(quar)))
            INTLINE_temp.columns = new_columns
        elif freq == 'M':
            for col in INTLINE_temp.columns:
                if str(col[0]).strip().isnumeric():
                    yr = str(col[0]).strip()
                try:
                    new_columns.append(yr+'-'+datetime.strptime(str(col[1]).strip(), '%B').strftime('%m'))
                except ValueError:
                    new_columns.append(None)
            INTLINE_temp.columns = new_columns
        INTLINE_temp.index = [re.sub(r'[0-9]+\.', "", str(dex)).strip() if re.sub(r'[0-9]+\.', "", str(dex)).strip() in KEYS else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x)[-1].isnumeric() else None)
        INTLINE_temp = INTLINE_temp.dropna(axis=1)
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp.to_excel(file_path, sheet_name=fname)
        if freq == 'A':
            INTLINE_temp.columns = [str(col) for col in INTLINE_temp.columns]
    elif address.find('SEKI') >= 0:
        FREQ = {'A':'Annually', 'Q':'Quarterly', 'M':'Monthly'}
        INDEX_NAME = ['ITEMS','TYPE OF DEPOSITS AND MATURITY','GROUP OF BANKS AND TYPE OF LOANS']
        file_path = data_path+str(country)+'/'+address+str(fname)+'_historical - '+FREQ[freq]+'.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
        if freq == 'A':
            INTLINE_his.columns = [int(str(col).strip()[:4]) if str(col).strip()[:4].isnumeric() else col for col in INTLINE_his.columns]
        elif freq == 'Q':
            INTLINE_his.columns = [pd.Period(col, freq='Q').strftime('%Y-Q%q') if type(col) != str else col for col in INTLINE_his.columns]
        elif freq == 'M':
            INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
        if freq == 'A':
            for col in INTLINE_temp.columns:
                if str(col[0]).strip().isnumeric():
                    yr = int(str(col[0]).strip())
                if str(col[1]).strip()[:3] == 'Dec':
                    new_columns.append(yr)
                elif str(col[0]).strip() in INDEX_NAME:
                    new_columns.append('index_name')
                else:
                    new_columns.append(None)
            INTLINE_temp.columns = new_columns
        elif freq == 'Q' and isinstance(INTLINE_temp.columns, pd.MultiIndex):
            yr = ''
            for col in INTLINE_temp.columns:
                if str(col[0]).strip().isnumeric():
                    yr = str(col[0]).strip()
                if str(col[1]).find('Total') >= 0 or str(col[1]).find('Unnamed') >= 0 or str(col[1]) == 'nan':
                    new_columns.append(None)
                else:
                    new_columns.append(yr+'-'+str(col[1]).strip()[:2])
            INTLINE_temp.columns = new_columns
        elif freq == 'Q' and isinstance(INTLINE_temp.columns, pd.Index):
            for col in INTLINE_temp.columns:
                if str(col).find('Unnamed') >= 0 or str(col) == 'nan':
                    new_columns.append(None)
                elif type(col) == datetime:
                    new_columns.append(pd.Period(col, freq='Q').strftime('%Y-Q%q'))
                else:
                    new_columns.append(datetime.strptime(str(col).strip(' *')[-2:], '%y').strftime('%Y')+pd.Period(str(col).strip(' *')[:3].replace('Des','Dec'), freq='Q').strftime('-Q%q'))
            INTLINE_temp.columns = new_columns
        elif freq == 'M':
            yr = ''
            for col in INTLINE_temp.columns:
                if str(col[0]).strip().isnumeric():
                    yr = str(col[0]).strip()
                if str(col[0]).strip() in INDEX_NAME:
                    new_columns.append('index_name')
                else:
                    try:
                        new_columns.append(yr+'-'+datetime.strptime(str(col[1]).strip()[:3], '%b').strftime('%m'))
                    except ValueError:
                        new_columns.append(None)
            INTLINE_temp.columns = new_columns
        if index_col == None:
            INTLINE_temp = INTLINE_temp.set_index('index_name')
        if str(fname).find('BPM6') >= 0:
            KEYS.append('Goods')
            INTLINE_temp.index = [re.sub(r'.*?(([A-Za-z][a-z]+\s*)+).*', r"\1", str(dex[1])).strip() if re.sub(r'.*?(([A-Za-z][a-z]+\s*)+).*', r"\1", str(dex[1])).strip() in KEYS else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated()]
            INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x)[-1].isnumeric() else np.nan)
            IN_sum = INTLINE_temp.loc['Goods'].add(INTLINE_temp.loc['Services'])
            IN_sum.name = 'Goods and Services'
            INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_sum).T])
            INTLINE_temp = INTLINE_temp.drop(['Goods'])
        elif str(fname).find('EXDB') >= 0:
            KEYS = re.split(r'&', KEYS[0])
            INTLINE_temp.index = [re.sub(r'.*?(([A-Za-z][a-z]+\s*)+).*', r"\1", str(dex[1])).strip() if re.sub(r'.*?(([A-Za-z][a-z]+\s*)+).*', r"\1", str(dex[1])).strip() in KEYS else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x).replace('.','').replace('-','').isnumeric() else np.nan)
            IN_sum = INTLINE_temp.loc['General Government'].add(INTLINE_temp.loc['Monetary Authorities'])
            IN_sum.name = 'General Government&Monetary Authorities'
            INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_sum).T])
            INTLINE_temp = INTLINE_temp.drop(['General Government', 'Monetary Authorities'])
        elif str(fname).find('Official Reserve Assets') >= 0:
            KEYS.append('Other Reserve Assets')
            INTLINE_temp.index = [re.sub(r'.*?(([A-Za-z][a-z]+\s*)+).*', r"\1", str(dex)).strip() if re.sub(r'.*?(([A-Za-z][a-z]+\s*)+).*', r"\1", str(dex)).strip() in KEYS else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated()]
            INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x)[-1].isnumeric() else np.nan)
            IN_sub = INTLINE_temp.loc['Other Reserve Assets'].sub(INTLINE_temp.loc['Other Claims'])
            IN_sub.name = 'Other Reserve Assets except Other Claims'
            INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_sub).T])
            INTLINE_temp = INTLINE_temp.drop(['Other Reserve Assets'])
            INTLINE_temp = INTLINE_temp.dropna(axis=1, how='all')
        elif str(fname).find('Interbank Call Money') >= 0 or str(fname).find('Interest Rate of Rupiah Loans') >= 0:
            new_index = []
            keywords = ['Call Money','JIBOR','Interest Rate','Mudharabah','Banks']
            subject = ''
            for dex in INTLINE_temp.index:
                if True in [str(dex).find(key) >= 0 for key in keywords]:
                    subject = str(dex).strip()
                    new_index.append(subject)
                    continue
                else:
                    new_index.append(subject+'//'+str(dex).strip())
            INTLINE_temp.index = new_index
            INTLINE_temp.index = [str(dex).strip() if str(dex).strip() in KEYS else None for dex in INTLINE_temp.index]
        else:
            INTLINE_temp.index = [re.sub(r'.*?(([A-Za-z][a-z]+\s*)+).*', r"\1", str(dex)).strip() if re.sub(r'.*?(([A-Za-z][a-z]+\s*)+).*', r"\1", str(dex)).strip() in KEYS else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp.to_excel(file_path, sheet_name=fname)
        if freq == 'A':
            INTLINE_temp.columns = [str(col) for col in INTLINE_temp.columns]
    elif address.find('SDDS') >= 0:
        INDEX_NAME = ['End of Period','PERIOD']
        if index_col == None and transpose == True:
            INTLINE_temp.index = ['index_name' if str(dex).strip() in INDEX_NAME else dex for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.T.set_index('index_name').T
        if str(fname).find('IndonesianGDP') >= 0:
            dataset = sname
            yr = ''
            if freq == 'A':
                for col in INTLINE_temp.columns:
                    if str(col[0]).strip()[:4].isnumeric():
                        yr = str(col[0]).strip()[:4]
                    if str(col[1]).strip() == 'Jumlah':
                        new_columns.append(yr)
                    else:
                        new_columns.append(None)
                INTLINE_temp.columns = new_columns
            elif freq == 'Q':
                quarter_dex = 1
                if str(fname).find('Exp') >= 0:
                    quarter_dex = 2
                for col in INTLINE_temp.columns:
                    if str(col[0]).strip()[:4].isnumeric():
                        yr = str(col[0]).strip()[:4]
                    if str(col[1]).strip() != 'Jumlah':
                        try:
                            new_columns.append(yr+'-Q'+str(roman.fromRoman(str(col[quarter_dex]).strip())))
                        except InvalidRomanNumeralError:
                            new_columns.append(None)
                    else:
                        new_columns.append(None)
                INTLINE_temp.columns = new_columns
        elif freq == 'A':
            INTLINE_temp.columns = [str(col).strip()[:4] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'Q':
            yr = ''
            for col in INTLINE_temp.columns:
                if str(col).strip().isnumeric():
                    yr = str(col).strip()
                    new_columns.append(None)
                else:
                    try:
                        new_columns.append(yr+pd.Period(str(col).strip(), freq='Q').strftime('-Q%q'))
                    except ValueError:
                        new_columns.append(None)
            INTLINE_temp.columns = new_columns
            INTLINE_temp = INTLINE_temp.dropna(axis=1, how='all')
        elif freq == 'M':
            yr = ''
            for col in INTLINE_temp.columns:
                if str(col).strip()[:4].isnumeric():
                    yr = str(col).strip()[:4]
                    new_columns.append(None)
                else:
                    try:
                        new_columns.append(yr+'-'+datetime.strptime(re.sub(r'\([A-Z]+\)', "", str(col)).strip().replace('Des','Dec'), '%B').strftime('%m'))
                    except ValueError:
                        new_columns.append(None)
            INTLINE_temp.columns = new_columns
            INTLINE_temp = INTLINE_temp.dropna(axis=1, how='all')
        if isinstance(INTLINE_temp.index, pd.MultiIndex):
            INTLINE_temp.index = [str(dex[0]).strip() if str(dex[0]) != 'nan' and str(dex[0])[-1].isnumeric() == False else str(dex[1]).strip() for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated()]
        else:
            INTLINE_temp.index = [str(dex).strip() if str(dex).find('Unnamed') < 0 and str(dex) != 'nan' else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        if str(fname).find('Outstanding') >= 0 or str(fname).find('CGOVOP') >= 0:
            INTLINE_temp = INTLINE_temp.applymap(lambda x: str(x).replace(',',''))
    elif address.find('BKPM') >= 0:
        PMDN = False
        yr = ''
        for col in INTLINE_temp.columns:
            if str(col[0]).find('PMDN') >= 0:
                PMDN = True
            if str(fname).find('DDI') >= 0 and PMDN == False:
                new_columns.append(None)
                continue
            else:
                if str(col[0]).strip().isnumeric():
                    yr = str(col[0]).strip()
                if str(col[1]).find('Investment') < 0:
                    new_columns.append(None)
                else:
                    new_columns.append(yr)
        INTLINE_temp.columns = new_columns
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
    elif address.find('GSO') >= 0:
        INTLINE_temp.columns = [str(col)[:4]+'-'+str(col)[-2:] for col in INTLINE_temp.columns]
        INTLINE_temp.index = ['CPI' if str(dex).find('Consumer Price Index, All items') >= 0 else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_previous], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
    elif address.find('INSEE') >= 0:
        if address.find('SERIE') >= 0:
            Table = Table.reset_index().set_index('File or Sheet')
            if str(fname).find('Hourly wage rate indice for labourers') >= 0:
                file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
                INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
                KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
                INTLINE_his.columns = [pd.Period(col, freq='Q').strftime('%Y-Q%q') if type(col) != str else col for col in INTLINE_his.columns]
                base_path = data_path+str(country)+'/'+address+'base_year_archive.csv'
                base_year_list = readFile(base_path, header_=[0], index_col_=0, acceptNoFile=False)
                if str(base_year_list.loc[fname, 'base year']) != str(base_year):
                    print('Modifying Data with new base year')
                    for ind in INTLINE_his.index:
                        new_base = INTLINE_his.loc[ind, str(base_year)]
                        multiplier = 100/new_base
                        for col in INTLINE_his.columns:
                            INTLINE_his.loc[ind, col] = float(INTLINE_his.loc[ind, col])*multiplier
                    base_year_list.loc[fname, 'base year'] = base_year
                    base_year_list.to_csv(base_path)
            if Table.loc[fname, 'zip'] == False:
                INTLINE_temp = INTLINE_temp.applymap(lambda x: float(re.sub(r'\([a-z]+\)|[,]', "", str(x))) if str(x).find('/') < 0 and str(x).find('na') < 0 else np.nan)
            if freq == 'A':
                INTLINE_temp = INTLINE_temp.groupby(axis=1, level=0).sum()
            elif freq == 'Q':
                if isinstance(INTLINE_temp.columns, pd.MultiIndex):
                    if str(fname).find('All households - France - Services') >= 0:
                        INTLINE_temp.columns = [str(col[0]).strip()+'-Q'+str(pd.Period(str(col[1]).strip(), freq='Q').quarter) if str(col[0]).strip().isnumeric() else None for col in INTLINE_temp.columns]
                        INTLINE_temp = INTLINE_temp.groupby(axis=1, level=0).mean()
                    else:
                        INTLINE_temp.columns = [str(col[0]).strip()+'-'+str(col[1]).strip() if str(col[0]).strip().isnumeric() else None for col in INTLINE_temp.columns]
                else:
                    INTLINE_temp.columns = [str(col).replace('T','Q').strip() if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
            elif freq == 'M' and Table.loc[fname, 'zip'] == True:
                INTLINE_temp['Index'] = INTLINE_temp.index
                if INTLINE_previous.empty:
                    ERROR('The characteristic table was not correctly read for file: '+str(fname))
                if str(fname).find('Industrial production index') >= 0:
                    key_characteristics = ['Frequency','Indicator','Activity','Other groupings','Nature','Correction']
                elif str(fname).find('Industrial producer and import price indices') >= 0:
                    key_characteristics = ['Indicator','Product']
                elif str(fname).find('Consumer price index') >= 0:
                    INTLINE_previous.columns = ['COICOP classification' if str(col).find('COICOP classification') >= 0 else col for col in INTLINE_previous.columns]
                    INTLINE_previous.columns = ['Products' if str(col).find('Products') >= 0 else col for col in INTLINE_previous.columns]
                    key_characteristics = ['Frequency','COICOP classification','Products','Households','Nature','Reference area']
                elif str(fname).find("Households' consumption expenditure on goods") >= 0:
                    key_characteristics = ['Product type']
                elif str(fname).find('Turnover index in wholesale and retail trade') >= 0:
                    key_characteristics = ['Activity','Nature','Correction']
                INTLINE_temp.index = [re.sub(r'\(\'|\',*\)', "", re.sub(r'\',\s\'', "//", str(tuple(INTLINE_previous.loc[dex, key_characteristics])))) for dex in INTLINE_temp.index]
                INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated()]
            elif freq == 'M':
                INTLINE_temp.columns = [str(col[0]).strip()+'-'+datetime.strptime(str(col[1]).strip(), '%B').strftime('%m') if str(col[0]).strip().isnumeric() else None for col in INTLINE_temp.columns]
        if freq == 'A':
            INTLINE_temp.columns = [str(col).strip() if str(col).strip().isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'Q':
            INTLINE_temp.columns = [str(col).strip()[:4]+'-'+str(col).strip()[-2:] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'M':
            INTLINE_temp.columns = [str(col).strip() if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        if str(fname).find('Households account') >= 0:
            new_index = []
            new_note = []
            Notes = {}
            Account = ''
            subject = ''
            for dex in INTLINE_temp.index:
                if str(dex[0]).strip() == '' and str(dex[1]).find('account') >= 0:
                    Account = str(dex[1]).strip()
                    new_index.append(Account)
                    new_note.append(None)
                    continue
                elif str(dex[0]).strip() == '' and (str(dex[1]).find('Resource') >= 0 or str(dex[1]).find('Use') >= 0):
                    subject = str(dex[1]).strip()
                    new_index.append(Account+'//'+subject)
                    new_note.append(None)
                    continue
                if str(dex[0]).strip() != '':
                    if bool(re.match(r'\(\*+\)', str(dex[1]).strip())):
                        if re.sub(r'(\(\*+\)).+', r"\1", str(dex[1]).strip()) not in Notes:
                            Notes[re.sub(r'(\(\*+\)).+', r"\1", str(dex[1]).strip())] = re.sub(r'\(\*+\)(.+)', r"\1", str(dex[1]).strip()).strip()
                        new_index.append(None)
                        new_note.append(None)
                    elif bool(re.search(r'\(\*+\)', str(dex[1]))):
                        new_index.append(Account+'//'+subject+'//'+re.sub(r'\(\*+\)', "", str(dex[1])).strip())
                        new_note.append(re.sub(r'.+?(\(\*+\))', r"\1", str(dex[1]).strip()))
                    else:
                        new_index.append(Account+'//'+subject+'//'+re.sub(r'\s+', " ", re.sub(r'\(\*+\)|[\(\+\-\)]', "", str(dex[1]))).strip())
                        new_note.append(None)
                else:
                    new_index.append(None)
                    new_note.append(None)
            INTLINE_temp.index = new_index
            new_note = [Notes[n] if n in Notes else None for n in new_note]
            INTLINE_temp['Notes'] = new_note
            INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x)[-1].isnumeric() else np.nan)
            IN_temp1 = INTLINE_temp.loc['Allocation of primary income account//Resources//Wages and salaries'].sub(INTLINE_temp.loc['Secondary distribution of income account//Uses//Employees actual social contributions'])
            IN_temp1.name = 'Net Wages and Salaries'
            INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_temp1).T])
            IN_temp2 = INTLINE_temp.loc['Net Wages and Salaries'].add(INTLINE_temp.loc['Secondary distribution of income account//Resources//Social benefits other than social transfers in kind'])
            IN_temp2.name = 'Salaries and Social Benefits'
            INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_temp2).T])
        elif isinstance(INTLINE_temp.index, pd.MultiIndex):
            new_index = []
            subject = ''
            for dex in INTLINE_temp.index:
                if str(dex[0]) != 'nan' and str(dex[0]).find('Unnamed') < 0:
                    subject = str(dex[0]).replace(':','').strip()
                if str(dex[1]) != 'nan' and str(dex[1]).find('Unnamed') < 0:
                    new_index.append(subject+'//'+str(dex[1]).replace(':','').strip())
                else:
                    new_index.append(subject)
            INTLINE_temp.index = new_index
            if str(fname).find('Supply and use in chain-linked volumes GDP') >= 0:
                INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x).replace('.','').isnumeric() else np.nan)
                IN_temp = INTLINE_temp.loc['Final consumption expenditure//General government (individual)'].add(INTLINE_temp.loc['Final consumption expenditure//General government (collective)'])
                IN_temp.name = 'Final consumption expenditure//General government'
                INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_temp).T])
        else:
            INTLINE_temp.index = [re.sub(r'[\+\-]', "", str(dex)).strip() if str(dex) != 'nan' and str(dex).strip() != '' else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        if str(fname).find('Hourly wage rate indice for labourers') >= 0:
            INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
            INTLINE_temp = INTLINE_temp.sort_index(axis=1)
            INTLINE_temp.to_excel(file_path, sheet_name=fname[:30])
    elif address.find('BOF') >= 0:
        if freq == 'Q':
            INTLINE_temp.columns = [str(pd.Period(datetime.strptime(str(col).strip(), '%d-%m-%Y'), freq='Q')).replace('Q','-Q') if str(col).strip()[3:5] in ['01','04','07','10'] else None for col in INTLINE_temp.columns]
        elif freq == 'M':
            INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%d-%m-%Y').strftime('%Y-%m') if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
    elif address.find('DOUANES') >= 0:
        file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
        INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
        INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%m.%Y').strftime('%Y-%m') if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp.index = [str(dex[0])+'//'+str(dex[1]) if str(dex[0])+'//'+str(dex[1]) in KEYS else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp.to_excel(file_path, sheet_name=fname)
    elif address.find('MEASTF') >= 0:
        file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
        INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
        INTLINE_temp.columns = [dateparser.parse(str(col).strip()).strftime('%Y-%m') if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x)[-1].isnumeric() else np.nan)
        if str(fname).find('authorized') >= 0:
            IN_temp = INTLINE_temp.loc['Nombre de logements autorisés individuels purs'].add(INTLINE_temp.loc['Nombre de logements autorisés individuels groupés'])
            IN_temp.name = 'Nombre de logements autorisés individuels'
        elif str(fname).find('started') >= 0:
            IN_temp = INTLINE_temp.loc['Nombre de logements commencés individuels purs'].add(INTLINE_temp.loc['Nombre de logements commencés individuels groupés'])
            IN_temp.name = 'Nombre de logements commencés individuels'
        INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_temp).T])
        INTLINE_temp.index = [str(dex).strip() if str(dex).strip() in KEYS else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp.to_excel(file_path, sheet_name=fname[:30])
    elif address.find('MLF') >= 0:
        if str(fname).find('Offers') >= 0:
            dataset = sname
        INTLINE_temp.columns = [col.strftime('%Y-%m') if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        new_index = []
        sex = ''
        for dex in INTLINE_temp.index:
            if str(dex[0]) != 'nan' and str(dex[0]).find('Unnamed') < 0:
                sex = str(dex[0]).strip()
            new_index.append(sex+'//'+str(dex[1]).strip())
        INTLINE_temp.index = new_index
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
    elif address.find('ISTAT') >= 0:
        INTLINE_his = pd.DataFrame()
        if (freq == 'A' and str(fname).find('Population') >= 0) or (freq == 'M' and str(fname).find('Index') >= 0):
            file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
            INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
            KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
            if freq == 'A':
                INTLINE_his.columns = [int(str(col).strip()[:4]) if str(col).strip()[:4].isnumeric() else col for col in INTLINE_his.columns]
                INTLINE_temp.columns = [int(str(col).strip()) if str(col).strip().isnumeric() else None for col in INTLINE_temp.columns]
            elif freq == 'M':
                INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
                INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%b-%Y').strftime('%Y-%m') if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
                base_path = data_path+str(country)+'/'+address+'base_year_archive.csv'
                base_year_list = readFile(base_path, header_=[0], index_col_=0, acceptNoFile=False)
                if re.sub(r'\.0$', "", str(base_year_list.loc[fname, 'base year'])) != str(base_year):
                    print('Modifying Data with new base year')
                    for ind in INTLINE_his.index:
                        if str(base_year).isnumeric():
                            new_base = sum([INTLINE_his.loc[ind, str(base_year)+'-'+str(num).rjust(2,'0')] for num in range(1,13)])/12
                        else:
                            new_base = INTLINE_his.loc[ind, str(base_year).replace('.','-')]
                        multiplier = 100/new_base
                        for col in INTLINE_his.columns:
                            INTLINE_his.loc[ind, col] = float(INTLINE_his.loc[ind, col])*multiplier
                    base_year_list.loc[fname, 'base year'] = base_year
                    base_year_list.to_csv(base_path)
        elif freq == 'Q' and str(fname).find('Index of production in construction') >= 0:
            INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%b-%Y').strftime('%Y-%m') if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
            INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
            INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x)[-1].isnumeric() and str(x)[0].isnumeric() else np.nan)
            INTLINE_temp = INTLINE_temp.T
            INTLINE_temp['quarter'] = [pd.Period(str(dex).strip(), freq='Q').strftime('%Y-Q%q') if str(dex)[:4].isnumeric() else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.set_index('quarter', append=True)
            INTLINE_temp = INTLINE_temp.groupby(axis=0, level=1).mean().T
        elif freq == 'M' and str(fname).find('discontinued') >= 0:
            INTLINE_temp.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_temp.columns]
        elif freq == 'A':
            INTLINE_temp.columns = [str(col).strip() if str(col).strip().isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'Q':
            INTLINE_temp.columns = [str(col).strip()[-4:]+'-'+str(col).strip()[:2] if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
            if INTLINE_previous.empty == False:
                INTLINE_previous.columns = [str(col).strip()[-4:]+'-'+str(col).strip()[:2] if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_previous.columns]
        elif freq == 'M':
            INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%b-%Y').strftime('%Y-%m') if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
        if str(fname).find('discontinued') < 0:
            INTLINE_temp = INTLINE_temp.applymap(lambda x: float(re.sub(r'[^0-9\.\-]', "", str(x))) if str(x)[-1].isnumeric() else np.nan)
        if INTLINE_his.empty == False:
            if isinstance(INTLINE_temp.index, pd.MultiIndex):
                new_index = []
                for dex in INTLINE_temp.index:
                    new_dex = ''
                    for d in dex:
                        new_dex = new_dex + re.sub(r'\+|\-+\s|(\(|base).+?=\s*100\s*\)*\s*', "", str(d)).strip()+'//'
                    new_index.append(new_dex.strip('//'))
                INTLINE_temp.index = new_index
            INTLINE_temp.index = [re.sub(r'\+|\-+\s|(\(|base).+?=\s*100\s*\)*\s*', "", str(dex)).strip() if re.sub(r'\+|\-+\s|\(.+?=\s*100\s*\)\s*', "", str(dex)).strip() in KEYS else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
            INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated()]
            not_found = []
            for dex in INTLINE_his.index:
                if dex not in list(INTLINE_temp.index):
                    not_found.append(dex)
            if not not not_found:
                ERROR(str(file_path))
            INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
            INTLINE_temp = INTLINE_temp.sort_index(axis=1)
            INTLINE_temp.to_excel(file_path, sheet_name=fname[:30])
            INTLINE_temp.columns = [str(col).replace('.0','') for col in INTLINE_temp.columns]
        elif str(fname).find('Economic account') >= 0 or str(fname).find('Social protection account') >= 0:
            new_index = []
            index_list = ['' for i in range(9)]
            for dex in INTLINE_temp.index:
                found = False
                for i in range(9):
                    if bool(re.match(r'\s{'+str(i)+'}[^\s]', str(dex))):
                        index_list[i] = str(dex).strip()+'//'
                        prefix = ''
                        for j in range(i):
                            prefix = prefix + index_list[j]
                        new_index.append(prefix+str(dex).strip())
                        found = True
                        break
                if found == False:
                    new_index.append(None)
            INTLINE_temp.index = new_index
            if str(fname).find('Economic account') >= 0:
                INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x)[-1].isnumeric() else np.nan)
                IN_sub = INTLINE_temp.loc['total Government expenditure'].sub(INTLINE_temp.loc['total Government expenditure//total capital expenditure'])
                IN_sub.name = 'total Government expenditure//total current expenditure'
                INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_sub).T])
        elif str(fname).find('non-financial assets by institutional sector') >= 0:
            new_index = []
            index_list = ['' for i in range(10)]
            for dex in INTLINE_temp.index:
                found = False
                for i in range(10):
                    if bool(re.match(r'\s{'+str(i)+'}[^\s]', str(dex[1]))):
                        index_list[i] = str(dex[1]).strip()+'//'
                        prefix = str(dex[0]).strip()+'//'
                        for j in range(i):
                            prefix = prefix + index_list[j]
                        new_index.append(prefix+str(dex[1]).strip())
                        found = True
                        break
                if found == False:
                    new_index.append(None)
            INTLINE_temp.index = new_index
        elif str(fname).find('Labour positions') >= 0:
            INTLINE_temp.index = [re.sub(r'(.+?\)).*', r"\1", str(dex)).strip() if str(dex) != 'nan' and str(dex).strip() != '' else None for dex in INTLINE_temp.index]
        elif isinstance(INTLINE_temp.index, pd.MultiIndex):
            new_index = []
            for dex in INTLINE_temp.index:
                new_dex = ''
                for d in dex:
                    new_dex = new_dex + re.sub(r'\+|\-\s|(\(|base).+?=\s*100\s*\)*\s*', "", str(d)).strip()+'//'
                new_index.append(new_dex.strip('//'))
            INTLINE_temp.index = new_index
            if str(fname).find('Labour force') >= 0:
                INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x)[-1].isnumeric() else np.nan)
                for dex in INTLINE_temp.index:
                    if str(dex).find('35-44') >= 0:
                        IN_sum = INTLINE_temp.loc[dex].add(INTLINE_temp.loc[dex.replace('35-44','45-54')])
                        IN_sum.name = dex.replace('35-44','35-54')
                        INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_sum).T])
            elif str(fname).find('Population by labour status') >= 0:
                INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x)[-1].isnumeric() else np.nan)
                for dex in INTLINE_temp.index:
                    if str(dex).find('15-19') >= 0:
                        IN_sum = INTLINE_temp.loc[dex].add(INTLINE_temp.loc[dex.replace('15-19','20-24')])
                        IN_sum.name = dex.replace('15-19','15-24')
                        INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_sum).T])
                    elif str(dex).find('25-29') >= 0:
                        IN_sum = INTLINE_temp.loc[dex].add(INTLINE_temp.loc[dex.replace('25-29','30-34')])
                        IN_sum.name = dex.replace('25-29','25-34')
                        INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_sum).T])
                    elif str(dex).find('15 years and over') >= 0 and str(dex).find('Data extracted') < 0:
                        IN_sum = INTLINE_temp.loc[dex].sub(INTLINE_temp.loc[dex.replace('15 years and over','15-64 years')]).add(INTLINE_temp.loc[dex.replace('15 years and over','55-59 years')]).add(INTLINE_temp.loc[dex.replace('15 years and over','60-64 years')])
                        IN_sum.name = dex.replace('15 years and over','55 years and over')
                        INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_sum).T])
        else:
            INTLINE_temp.index = [re.sub(r'\(*\+\)*|\(\-\)|\-{2,}\s|(\(|base).+?=\s*100\s*\)*\s*', "", str(dex)).strip() if str(dex) != 'nan' and str(dex).strip() != '' else None for dex in INTLINE_temp.index]
            if freq == 'Q' and (str(fname).find('Value added') >= 0 or str(fname).find('Gross') >= 0):
                INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x)[-1].isnumeric() else np.nan)
                INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
                INTLINE_previous.index = [re.sub(r'\(*\+\)*|\(\-\)|\-{2,}\s', "", str(dex)).strip() if str(dex) != 'nan' and str(dex).strip() != '' else None for dex in INTLINE_previous.index]
                INTLINE_previous = INTLINE_previous.applymap(lambda x: float(x) if str(x)[-1].isnumeric() else np.nan)
                INTLINE_previous = INTLINE_previous.loc[INTLINE_previous.index.dropna(), INTLINE_previous.columns.dropna()]
                IN_div = INTLINE_previous.mul(100).div(INTLINE_temp)
                IN_div = IN_div.loc[~IN_div.index.duplicated(), IN_div.columns.dropna()]
                IN_div.index = [str(dex).strip()+'//Deflator' for dex in IN_div.index]
                INTLINE_temp = pd.concat([INTLINE_temp, IN_div])
        INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated(), INTLINE_temp.columns.dropna()]
    elif address.find('BOI') >= 0:
        INTLINE_his = pd.DataFrame()
        file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
        if os.path.isfile(file_path):
            INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
            KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
            if freq == 'Q':
                INTLINE_his.columns = [pd.Period(col, freq='Q').strftime('%Y-Q%q') if type(col) != str else col for col in INTLINE_his.columns]
            elif freq == 'M':
                INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
        if freq == 'Q':
            INTLINE_temp.columns = [pd.Period(str(col), freq='Q').strftime('%Y-Q%q') if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'M':
            INTLINE_temp.columns = [str(col).strip()[:7] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        if INTLINE_his.empty == False:
            INTLINE_temp.index = [re.sub(r'\+', "", str(dex)).strip() if re.sub(r'\+', "", str(dex)).strip() in KEYS else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
            INTLINE_temp = INTLINE_temp.dropna(axis=1)
            if str(fname).find('External Debt') >= 0:
                INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x)*0.001 if str(x)[-1].isnumeric() else np.nan)
            INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
            INTLINE_temp = INTLINE_temp.sort_index(axis=1)
            INTLINE_temp.to_excel(file_path, sheet_name=fname[:30])
        else:
            INTLINE_temp.index = [re.sub(r'\+', "", str(dex)).strip() if str(dex) != 'nan' else None for dex in INTLINE_temp.index]
    elif address.find('ECB') >= 0:
        file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
        INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%Y%b').strftime('%Y-%m') if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp.to_excel(file_path, sheet_name=fname[:30])
    elif address.find('ANFIA') >= 0:
        MONTH = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        if re.sub(r'.*?([0-9]{4}).*', r"\1", sname) != str(datetime.today().year):
            present_year = str(datetime.today().year-1)
            previous_year = str(datetime.today().year-2)
        else:
            present_year = str(datetime.today().year)
            previous_year = str(datetime.today().year-1)
        file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
        INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
        INTLINE_temp.columns = [present_year+datetime.strptime(str(col).strip(), '%B').strftime('-%m') if str(col).strip()[:3] in MONTH else None for col in INTLINE_temp.columns]
        INTLINE_previous.columns = [previous_year+datetime.strptime(str(col).strip(), '%B').strftime('-%m') if str(col).strip()[:3] in MONTH else None for col in INTLINE_previous.columns]
        INTLINE_temp = pd.concat([INTLINE_previous, INTLINE_temp], axis=1)
        INTLINE_temp.index = [re.sub(r'\+', "", str(dex)).strip() if re.sub(r'\+', "", str(dex)).strip() in KEYS else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated()]
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp.to_excel(file_path, sheet_name=fname[:30])
    elif address.find('SIDRA') >= 0:
        FULL_MONTHS = {'janeiro':'01','fevereiro':'02','março': '03','abril':'04','maio':'05','junho':'06','julho':'07','agosto':'08','setembro':'09','outubro':'10','novembro':'11','dezembro':'12'}
        if freq == 'A':
            INTLINE_temp.columns = [str(col).strip() if str(col).strip().isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'M':
            INTLINE_temp.columns = [str(col).strip()[-4:]+'-'+FULL_MONTHS[str(col).strip()[:-4].replace(' ','')] if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
        if isinstance(INTLINE_temp.index, pd.MultiIndex):
            INTLINE_temp.index = [re.sub(r'\(.*?\)', "", str(dex[0])).strip()+'//'+str(dex[1]).strip() for dex in INTLINE_temp.index]
        else:
            INTLINE_temp.index = [str(dex).strip() for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        if str(fname).find('Table') >= 0 and str(fname).find('indice') < 0:
            file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
            INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
            KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
            INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
            INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x) if str(x).strip()[-1].isnumeric() else np.nan)
            INTLINE_temp = INTLINE_temp.dropna(axis=1, how='all')
            INTLINE_temp.index = [str(dex).strip() if str(dex).strip() in KEYS else None for dex in INTLINE_temp.index]
            INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
            INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
            INTLINE_temp = INTLINE_temp.sort_index(axis=1)
            INTLINE_temp.to_excel(file_path, sheet_name=fname)
    elif address.find('BCB') >= 0:
        if str(fname).find('monthly') >= 0 and freq != 'M':
            INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%m/%Y').strftime('%Y-%m') if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
            INTLINE_temp = INTLINE_temp.loc[:, INTLINE_temp.columns.dropna()]
            INTLINE_temp = INTLINE_temp.applymap(lambda x: float(str(x).replace(',','')) if str(x).strip()[-1].isnumeric() else np.nan)
            INTLINE_temp = INTLINE_temp.T
            if freq == 'A':
                INTLINE_temp['annual'] = [pd.Period(str(dex).strip(), freq='A').strftime('%Y') for dex in INTLINE_temp.index]
                INTLINE_temp = INTLINE_temp.set_index('annual', append=True)
            elif freq == 'Q':
                INTLINE_temp['quarter'] = [pd.Period(str(dex).strip(), freq='Q').strftime('%Y-Q%q') for dex in INTLINE_temp.index]
                INTLINE_temp = INTLINE_temp.set_index('quarter', append=True)
            INTLINE_temp = INTLINE_temp.groupby(axis=0, level=1).mean().T
        elif str(fname).find('daily') >= 0:
            INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%d/%m/%Y').strftime('%Y-%m-%d') if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
            INTLINE_temp = INTLINE_temp.loc[:, INTLINE_temp.columns.dropna()]
            if freq == 'A':
                annual_end_list = pd.date_range(start=INTLINE_temp.columns[0],end=datetime.today(),freq='A').strftime('%Y-%m-%d').tolist()
                INTLINE_temp.columns = [datetime.strptime(col, '%Y-%m-%d').strftime('%Y') if col in annual_end_list else None for col in INTLINE_temp.columns]
            elif freq == 'M':
                month_end_list = pd.date_range(start=INTLINE_temp.columns[0],end=datetime.today(),freq='M').strftime('%Y-%m-%d').tolist()
                INTLINE_temp.columns = [datetime.strptime(col, '%Y-%m-%d').strftime('%Y-%m') if col in month_end_list else None for col in INTLINE_temp.columns]
        elif str(fname).find('general government debt') >= 0:
            dataset = sname
            yr = ''
            for col in INTLINE_temp.columns:
                if str(col[0]).strip().isnumeric():
                    yr = str(col[0]).strip()
                try:
                    new_columns.append(yr+'-'+datetime.strptime(str(col[1]).strip(), '%B').strftime('%m'))
                except ValueError:
                    new_columns.append(None)
            INTLINE_temp.columns = new_columns
            INTLINE_temp.index = [re.sub(r'\+|\(.*?\)|[0-9]+/', "", str(dex)).strip() for dex in INTLINE_temp.index]
        elif freq == 'A':
            INTLINE_temp.columns = [str(col).strip() if str(col).strip().isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'M':
            INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%m/%Y').strftime('%Y-%m') if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp.index = [str(dex).strip() for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        INTLINE_temp = INTLINE_temp.applymap(lambda x: float(str(x).replace(',','')) if str(x).strip()[-1].isnumeric() else np.nan)
    elif address.find('COMEX') >= 0:
        file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
        INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
        INTLINE_temp.columns = [str(col[0]).strip()+'-'+str(col[1]).strip().rjust(2,'0') if str(col[0]).strip().isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        INTLINE_temp = INTLINE_temp.applymap(lambda x: float(x)*0.000001 if str(x).strip()[-1].isnumeric() else np.nan)
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp.to_excel(file_path, sheet_name=fname)
    elif address.find('FGV') >= 0:
        try:
            index_table = INTLINE_temp.loc[:, INTLINE_temp.columns[INTLINE_temp.columns.get_loc('Série'):INTLINE_temp.columns.get_loc('Série')+11]].set_index('Série').T
            base_year = datetime.strptime(str(index_table.iloc[0]['Base do No. índice']).strip(), '%m/%d/%Y').strftime('%Y.%m')
        except KeyError:
            ERROR('Index of index_table Not Found: '+str(fname))
        try:
            INTLINE_temp = INTLINE_temp.set_index('Data')
        except KeyError:
            ERROR('Data Index Not Found: '+str(fname))
        INTLINE_temp.index = [str(index_table.loc[dex,'Título']) for dex in INTLINE_temp.index]
        INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%m/%Y').strftime('%Y-%m') if (len(str(col).strip())>4 and str(col).strip()[-4:].isnumeric()) else None for col in INTLINE_temp.columns]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
    elif address.find('CNI') >= 0:
        file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
        INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
        INTLINE_temp.columns = [str(col).strip()[:7] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp.index = [str(dex[1]).strip() if str(dex[1]).strip() in KEYS else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated()]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp.to_excel(file_path, sheet_name=fname)
    elif address.find('STANOR') >= 0:
        if freq == 'A':
            INTLINE_temp.columns = [str(col).strip() if str(col).strip().isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'Q':
            INTLINE_temp.columns = [str(col).replace('K','-Q').strip() if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'M':
            INTLINE_temp.columns = [str(col).replace('M','-').strip() if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        if isinstance(INTLINE_temp.index, pd.MultiIndex):
            INTLINE_temp.index = [re.sub(r'Constant.+?prices', "Constant prices", str(dex[0])).strip()+'//'+re.sub(r'\(.+?=.*100.*\)', "", str(dex[1])).replace('+',' plus ').strip() if str(dex[0]) != 'nan' else None for dex in INTLINE_temp.index]
            if INTLINE_previous.empty == False:
                INTLINE_previous.index = [str(dex[0]).strip()+'//'+str(dex[1]).replace('+',' plus ').strip() if str(dex[0]) != 'nan' else None for dex in INTLINE_previous.index]
        else:
            INTLINE_temp.index = [re.sub(r'\(.+?=.*100.*\)', "", str(dex)).replace('+',' plus ').strip() if str(dex) != 'nan' else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.applymap(lambda x: float(str(x).replace(' ','')) if str(x).strip()[-1].isnumeric() else np.nan)
        INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated()]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        if str(fname).find('10799') >= 0:
            IN_temp = INTLINE_temp.loc['Resident sectors, total//Balance of primary income'].add(INTLINE_previous.loc['Total industry//Consumption of fixed capital. Current prices (NOK million)'])
            IN_temp.name = 'GROSS NATIONAL INCOME'
            INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_temp).T])
        elif str(fname).find('08864') >= 0:
            IN_temp = INTLINE_temp.loc['Exports excl. ships and oil platforms//Value'].sub(INTLINE_temp.loc['Imports excl. ships and oil platforms//Value'])
            IN_temp.name = 'Trade balance excl. ships and oil platforms//Value'
            INTLINE_temp = pd.concat([INTLINE_temp, pd.DataFrame(IN_temp).T])
    elif address.find('NORGES') >= 0:
        INTLINE_temp.columns = [col.strftime('%Y-%m') if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp.index = [str(dex).replace('\n','//').strip() if str(dex) != 'nan' else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
    elif address.find('NIMA') >= 0:
        INTLINE_temp.columns = [col.strftime('%Y-%m') if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        new_index = []
        subject = ''
        for dex in INTLINE_temp.index:
            if str(dex[0]) != 'nan':
                subject = str(dex[0]).strip()+'//'
            if str(dex[1]) != 'nan':
                new_index.append(subject+str(dex[1]).strip())
            else:
                new_index.append(None)
        INTLINE_temp.index = new_index
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
    elif address.find('NAV') >= 0:
        dataset = sname
        file_path = data_path+str(country)+'/'+address+str(dataset)+'_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(dataset)]['keyword'])
        INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
        IN_temp = pd.DataFrame(index=[str(sname)])
        for dex in INTLINE_temp.index:
            if str(dex).strip().isnumeric():
                for col in INTLINE_temp.columns:
                    try:
                        IN_temp.insert(loc=len(IN_temp.columns), column=str(dex).strip()+'-'+datetime.strptime(str(col).strip(), '%B').strftime('%m'), value=[INTLINE_temp.loc[dex,col]])
                    except ValueError:
                        continue
        INTLINE_temp = IN_temp
        INTLINE_temp.index = [dex if dex in KEYS else None for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_his], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        INTLINE_temp.to_excel(file_path, sheet_name=fname)  
    # INTLINE_keywords(INTLINE_temp, data_path, country, address, fname, freq, data_key='', data_year=2018, multiplier=1, check_long_label=False, allow_duplicates=False, multiple=True)
    # return 'testing', False, False, False
    print(INTLINE_temp)
    #ERROR('')
    
    #################################################################################################################################################################################################################
    for ind in range(Series[freq].shape[0]):
        sys.stdout.write("\rLoading...("+str(round((ind+1)*100/Series[freq].shape[0], 1))+"%)*")
        sys.stdout.flush()
        if Series[freq].iloc[ind]['DataSet'] == dataset:
            code = re.sub(r'[0_&]+|\.[A-Z]', "", str(Series[freq].index[ind])).replace(Countries.loc[country, 'location'],'').replace(Countries.loc[country, 'location'][:2],'')
            #if base_year != 0:
            #    code = code+base_year[-2:]
            if str(Series[freq].iloc[ind]['keyword']).find('+') >= 0:
                key_sum = True
                IN_t = pd.DataFrame()
                keys = re.split(r'\+', str(Series[freq].iloc[ind]['keyword']))
            elif address.find('RPKT') >= 0:
                key = int(Series[freq].iloc[ind]['keyword'])
            else:
                key = str(Series[freq].iloc[ind]['keyword'])
            if key_sum == True:
                found = False
                for dex in INTLINE_temp.index:
                    item_found = [False for k in keys]
                    for k in keys:
                        if str(dex).find(k) == 0:
                            item_found[keys.index(k)] = True
                    if True in item_found:
                        pdSeries = INTLINE_temp.loc[dex]
                        if pdSeries.shape[0] == 1:
                            pdSeries = pdSeries.T.squeeze()
                        IN_t = pd.concat([IN_t, pd.DataFrame(pdSeries).T])
                if IN_t.shape[0] == len(keys):
                    found = True
                IN_t = IN_t.applymap(lambda x: float(str(x).replace(',','').replace('nan','0')))
                IN_t = IN_t.sum(axis=0)
                INTLINE_t = pd.concat([INTLINE_t, pd.DataFrame(IN_t).T])
                if found == False:
                    logging.info(list(INTLINE_temp.index))
                    ERROR('Item not found: '+str(keys))
            else:
                try:
                    pdSeries = INTLINE_temp.loc[key]
                    if pdSeries.shape[0] == 1:
                        pdSeries = pdSeries.T.squeeze()
                    INTLINE_t = pd.concat([INTLINE_t, pd.DataFrame(pdSeries).T])
                except KeyError:
                    logging.info(list(INTLINE_temp.index))
                    ERROR('Item not found: '+str(key))
            suffix = ''
            if address.find('ISTAT') >= 0 and str(Series[freq].iloc[ind]['Short Label']).find('(PY)') >= 0:
                suffix = ' (In Previous Year Prices)'
            lab = re.sub(r'(\([ESNA0-9]+\)\s*)*(\(([0-9]{4}(\-[0-9]{2,4})*|PY)\))*[,\s]*([NSWCDA]|Trend)*\s+\-\s*(\(.+?\)\s*|SAR)*', "",\
                 str(Series[freq].iloc[ind]['Short Label']).replace(Countries.loc[country, 'Country_Name'],'')).strip(' ,').replace(Countries.loc[country, 'Country_Name'].lower(),Countries.loc[country, 'Country_Name'])+suffix
            if str(Series[freq].iloc[ind]['Scale']) != 'nan' and str(Series[freq].iloc[ind]['Scale']) != 'Unit':
                unit = str(Series[freq].iloc[ind]['Scale'])+' of '+str(Series[freq].iloc[ind]['Unit'])
            elif str(Series[freq].iloc[ind]['Unit']) == 'Index' and (address.find('PPI') < 0 and address.find('CFIB') < 0 and str(dataset).find('5206') != 0 and address.find('CNI') < 0):
                if base_year != 0 :
                    base = base_year
                elif address.find('NIKK') >= 0:
                    base = datetime.strptime(str(Series[freq].iloc[ind]['Base Period']), '%Y/%m/%d').strftime('%Y.%m.%d')
                elif address.find('DEUSTATIS') >= 0 and base == 0:
                    ERROR('Index base not found: '+str(code))
                else:
                    unit_name = 'Unit'
                    if address.find('BUNDES') >= 0 and str(fname).find('Effective exchange rates of the euro') >= 0:
                        base_t = re.sub(r'.*?([0-9]{1})Q([0-9]{2}).*', r"\2Q\1", str(INTLINE_temp.loc[key, unit_name]))
                        base = str(datetime.strptime(base_t[:2],'%y').year)+base_t[-2:]
                    elif address.find('BUNDES') >= 0 and str(fname).find('Securities issues') >= 0:
                        base = re.sub(r'.*?([0-9]{4}).*', r"\1", str(INTLINE_temp.loc['BBK01.WU001A', unit_name]))
                    else:
                        base = re.sub(r'.*?([0-9]{4}).*', r"\1", str(INTLINE_temp.loc[key, unit_name]))
                unit = str(Series[freq].iloc[ind]['Unit'])+': '+base+'=100'
            else:
                unit = str(Series[freq].iloc[ind]['Unit'])
            concept = str(Series[freq].iloc[ind]['Concept'])
            if str(Series[freq].iloc[ind]['Seasonal Adjustment']) != 'nan':
                form = str(Series[freq].iloc[ind]['Seasonal Adjustment'])
            else:
                form = 'Not Seasonally Adjusted'
            new_code_t.append(address[:3]+code)
            new_label_t.append(lab)
            new_unit_t.append(unit)
            new_type_t.append(concept)
            new_form_c.append(form)
            try:
                if str(INTLINE_temp.loc[key, 'Notes']) != 'nan':
                    new_note_t.append(str(INTLINE_temp.loc[key, 'Notes']))
                else:
                    new_note_t.append('nan')
            except KeyError:
                new_note_t.append('nan')
    sys.stdout.write("\n\n")
    
    INTLINE_t = INTLINE_t.sort_index(axis=1)
    INTLINE_t.insert(loc=0, column='Index', value=new_code_t)
    INTLINE_t.insert(loc=1, column='Label', value=new_label_t)
    INTLINE_t.insert(loc=2, column='unit', value=new_unit_t)
    INTLINE_t.insert(loc=3, column='type', value=new_type_t)
    INTLINE_t.insert(loc=4, column='form_c', value=new_form_c)
    INTLINE_t.insert(loc=5, column='note', value=new_note_t)
    INTLINE_t = INTLINE_t.set_index('Index', drop=False)
    INTLINE_t = INTLINE_t.loc[:, INTLINE_t.columns.dropna()]
    label = INTLINE_t['Label']
    
    return INTLINE_t, label, note, footnote

def INTLINE_MULTIKEYS(INTLINE_temp, data_path, country, address, fname, sname, Series, Countries, freq, head=None, index_col=None, transpose=True, base_year=0, INTLINE_previous=pd.DataFrame(), is_period=False, note=[], footnote=[]):
    QUAR = {'1-3.':'Q1','4-6.':'Q2','7-9.':'Q3','10-12.':'Q4'}
    if type(INTLINE_temp) != dict and INTLINE_temp.empty == True:
        ERROR('Sheet Not Found: '+data_path+str(country)+'/'+address+fname+', sheet name: '+str(sname))
    if type(INTLINE_temp) != dict and transpose == True:
        INTLINE_temp = INTLINE_temp.T
    elif transpose == True:
        for t in INTLINE_temp:
            INTLINE_temp[t] = INTLINE_temp[t].T
    INTLINE_t = pd.DataFrame()
    key_sum = False
    trade_sum = False
    new_code_t = []
    new_label_t = []
    new_unit_t = []
    new_type_t = []
    new_form_c = []
    dataset = str(fname)
    
    if address.find('COJ') >= 0:
        new_index = []
        if len(head) > 1:
            dex_t = ['nan' for d in range(len(head))]
            for dex in INTLINE_temp.index:
                dex_temp = ['nan' for d in range(len(head))]
                isNaN = [str(d).find('Unnamed') >= 0 for d in dex]
                if False in isNaN:
                    renew = False
                    for d in range(len(head)):
                        if str(dex[d]).find('Unnamed') < 0:
                            renew = True
                            item = dex[d]
                            if freq == 'M':
                                item = str(item).replace('(','')
                            dex_temp[d] = re.sub(r'^[a-z0-9]+\.|（.+?\)|^\(.+?\)', "", str(item).replace(' ','').replace('Goods&Services','Trade')).strip()
                            dex_t[d] = dex_temp[d]
                        elif renew == False and freq != 'M':
                            dex_temp[d] = dex_t[d]
                new_index.append(dex_temp)
            INTLINE_temp.index = pd.MultiIndex.from_tuples(new_index)
        else:
            for dex in INTLINE_temp.index:
                new_index.append(re.sub(r'^[a-z0-9]+\.|\(.+?\)', "", str(dex).replace(' ','')).strip())
            INTLINE_temp.index = new_index
        new_columns = []
        if str(fname) == 'kshotoku' and freq == 'Q':
            INTLINE_temp.columns = pd.MultiIndex.from_frame(INTLINE_temp.iloc[:2].T)
        if str(fname) == 'kshotoku' and freq == 'A':
            keyword_list = Series[freq].loc[Series[freq]['DataSet'] == str(fname)]['keyword'].to_list()
            INTLINE_temp2 = pd.DataFrame()
            for keyword in keyword_list:
                for dex in range(INTLINE_temp.shape[0]):
                    if INTLINE_temp.index[dex][0].find(re.split(r', ', keyword)[0]) >= 0 and INTLINE_temp.index[dex][1].find(re.split(r', ', keyword)[1]) >= 0:
                        INTLINE_tem = INTLINE_temp.iloc[dex:dex+2]
                        key_index = [INTLINE_tem.iloc[0].name]
                        INTLINE_tem.columns = INTLINE_tem.iloc[0]
                        INTLINE_tem = INTLINE_tem.iloc[1].to_frame().T
                        INTLINE_tem.index = pd.MultiIndex.from_tuples(key_index)
                        INTLINE_temp2 = pd.concat([INTLINE_temp2, INTLINE_tem])
                        break
            INTLINE_temp = INTLINE_temp2
        for col in INTLINE_temp.columns:
            if freq == 'A':
                new_columns.append(re.sub(r'/1-12.|\.0', "", str(col).replace(' ','')).strip())
            elif freq == 'Q' and index_col != None:
                if bool(re.search(r'[^0-9/\.\-\s]', str(col))):
                    new_columns.append('nan')
                else:
                    if str(col).find('/') >= 0:
                        year = re.split(r'/', str(col).replace(' ',''))[0]
                        quar = QUAR[re.split(r'/', str(col).replace(' ',''))[1]]
                    else:
                        quar = QUAR[str(col).replace(' ','')]
                    new_columns.append(year+'-'+quar)
            elif freq == 'Q' and index_col == None:
                if bool(re.search(r'[^na0-9/\.\-\s]', str(col[0]))) or str(col[1]) == 'nan':
                    new_columns.append('nan')
                else:
                    if str(col[0]).find('/') >= 0:
                        year = str(col[0]).replace('/','').strip()
                    quar = QUAR[col[1].strip()]
                    new_columns.append(year+'-'+quar)
            elif freq == 'M':
                yr = col[-2]
                mth = col[-1]
                if str(yr).isnumeric():
                    year = str(yr)
                month = str(mth).rjust(2,'0')
                new_columns.append(year+'-'+month)
            else:
                new_columns.append(None)
        INTLINE_temp.columns = new_columns
        INTLINE_temp = INTLINE_temp.sort_index().applymap(lambda x: str(x).replace(',',''))
    elif address.find('TRADE') >= 0:
        if address.find('COMM') >= 0:
            INTLINE_temp = INTLINE_TRADE(INTLINE_temp, fname, transpose=transpose)
        elif address.find('COUN') >= 0:
            INTLINE_temp = INTLINE_temp.loc[:,INTLINE_temp.columns.dropna()]
            if str(fname) == 'World':
                INTLINE_temp.index = pd.MultiIndex.from_tuples([[str(dex).strip(), 'Total'] for dex in INTLINE_temp.index])
            else:
                INTLINE_temp.index = pd.MultiIndex.from_tuples([[str(dex[0]).strip(), 'Total'] if str(dex[0]).find('Total') >= 0 else [str(dex[d]).strip() for d in range(len(dex))] for dex in INTLINE_temp.index])
            INTLINE_temp.columns = [datetime.strptime(str(col), '%Y/%m').strftime('%Y-%m') for col in INTLINE_temp.columns]
            INTLINE_temp = INTLINE_temp.apply(lambda x: x/1000)
    elif address.find('JPC') >= 0:
        new_columns = []
        for col in INTLINE_temp.columns:
            try:
                if str(col[0]) != 'nan':
                    year = re.sub(r'.*?([0-9]{4}).+', r"\1", str(col[0]))
                new_columns.append(year+'-'+datetime.strptime(re.sub(r'[^A-Za-z]+', "", str(col[1])), '%b').strftime('%m'))
            except ValueError:
                new_columns.append(None)
        INTLINE_temp.columns = new_columns
    elif address.find('FSA') >= 0:
        new_index = []
        previous = ['' for d in range(len(index_col))]
        for dex in INTLINE_temp.index:
            dex_temp = [None for d in range(len(dex))]
            for d in range(len(dex)):
                if d == 0:
                    if str(dex[d]) == 'nan' or str(dex[d]).isnumeric():
                        dex_temp[d] = previous[d]
                    else:
                        dex_temp[d] = str(dex[d]).replace(' ','')
                        previous[d] = dex_temp[d]
                elif str(dex[d]).replace(' ','') == previous[d]:
                    dex_temp[d] = 'nan'
                else:
                    dex_temp[d] = str(dex[d]).replace(' ','')
                    previous[d] = dex_temp[d]
            new_index.append(dex_temp)
        INTLINE_temp.index = pd.MultiIndex.from_tuples(new_index)
        new_columns = []
        for col in INTLINE_temp.columns:
            try:
                new_columns.append(datetime.strptime(str(col).strip(), '%B-%y').strftime('%Y'))
            except ValueError:
                new_columns.append(None)
        INTLINE_temp.columns = new_columns
        INTLINE_temp = INTLINE_temp.sort_index()
    elif address.find('WKHH') >= 0:
        INTLINE_temp.columns = pd.MultiIndex.from_frame(INTLINE_temp.iloc[3:5].T)
        new_index = []
        previous = ['NaN' for d in range(len(head))]
        for dex in INTLINE_temp.index:
            dex_temp = [None for d in range(len(dex))]
            for d in range(len(dex)):
                if d == 0 and str(dex[d]).find('Unnamed') >= 0:
                    dex_temp[d] = previous[d]
                else:
                    dex_temp[d] = re.sub(r'[^A-Za-z]', "", str(dex[d])).strip()
                    previous[d] = dex_temp[d]
            new_index.append(dex_temp)
        INTLINE_temp.index = pd.MultiIndex.from_tuples(new_index)
        new_columns = []
        year = ''
        for col in INTLINE_temp.columns:
            if str(col[0])[:4].isnumeric():
                year = str(col[0])[:4]
            try:
                mth = year+'-'+datetime.strptime(str(col[1]).strip()[:3], '%b').strftime('%m')
                if mth not in new_columns:
                    new_columns.append(mth)
                else:
                    new_columns.append(None)
            except ValueError:
                new_columns.append(None)
        INTLINE_temp.columns = new_columns
        INTLINE_temp = INTLINE_temp.loc[:, INTLINE_temp.columns.dropna()]
        INTLINE_temp = INTLINE_temp.sort_index()
    elif address.find('NBS') >= 0:
        if freq == 'Q':
            if str(fname) == 'Money Supply':
                QUAR = {'Mar':'1','Jun':'2','Sep':'3','Dec':'4'}
                INTLINE_temp.columns = [str(col)[-4:]+'-Q'+QUAR[str(col)[:3]] if str(col)[:3] in QUAR else None for col in INTLINE_temp.columns]
                INTLINE_temp = INTLINE_temp.loc[:, INTLINE_temp.columns.dropna()]
            else:
                INTLINE_temp.columns = [str(col)[-4:]+'-Q'+str(col)[:1] for col in INTLINE_temp.columns]
        elif freq == 'M':
            INTLINE_temp.columns = [str(col)[-4:]+'-'+datetime.strptime(str(col).strip()[:3], '%b').strftime('%m') for col in INTLINE_temp.columns]
            if INTLINE_previous.empty == False:
                INTLINE_previous.columns = [str(col)[-4:]+'-'+datetime.strptime(str(col).strip()[:3], '%b').strftime('%m') for col in INTLINE_previous.columns]
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
        if INTLINE_previous.empty == False:
            key_list = list(Series[freq].loc[Series[freq]['DataSet'] == str(fname)]['keyword'])
            INTLINE_previous = NEW_INDEX(INTLINE_previous, key_list, Series, freq, fname)
            INTLINE_temp = NEW_INDEX(INTLINE_temp, key_list, Series, freq, fname)
            INTLINE_temp = INTLINE_temp.dropna(axis=1, how='all')
            INTLINE_previous = INTLINE_previous.sort_index(axis=1)
            INTLINE_previous = INTLINE_previous.dropna(axis=1, how='all')
            if len(INTLINE_temp.index) != len(INTLINE_previous.index):
                print(INTLINE_temp.index)
                print(INTLINE_previous.index)
                ERROR('The Indices of Two DataFrames are not identical.')
            INTLINE_temp = pd.concat([INTLINE_previous, INTLINE_temp], axis=1)
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        else:
            INTLINE_temp.index = pd.MultiIndex.from_tuples([[str(d).replace(' ','') for d in re.split(r', ', str(dex), 1)] if str(dex).find(', ') >= 0 else [str(dex).replace(' ',''), ''] for dex in INTLINE_temp.index])
        if str(fname) == 'Gross Domestic Product by Expenditure Approach':
            FCGR = tuple(['FinalConsumptionGrowthRate',''])
            INTLINE_temp = INTLINE_temp.append(pd.DataFrame(index=[FCGR]))
            FCE = tuple(['FinalConsumptionExpenditure(100millionyuan)',''])
            for col in range(1, INTLINE_temp.shape[1]):
                if str(INTLINE_temp.loc[FCE, INTLINE_temp.columns[col-1]]) != 'nan' and str(INTLINE_temp.loc[FCE, INTLINE_temp.columns[col]]) != 'nan':
                    INTLINE_temp.loc[FCGR, INTLINE_temp.columns[col]] = round((INTLINE_temp.loc[FCE, INTLINE_temp.columns[col]]-INTLINE_temp.loc[FCE, INTLINE_temp.columns[col-1]])*100/INTLINE_temp.loc[FCE, INTLINE_temp.columns[col-1]], 2)
        elif str(fname) == 'Indices of Gross Domestic Product':
            targets = ['GrossDomesticProduct','PrimaryIndustry','SecondaryIndustry','TertiaryIndustry']
            for dex in range(INTLINE_temp.shape[0]):
                if str(INTLINE_temp.index[dex][1]).find('Accumulated') >= 0 and True in [str(INTLINE_temp.index[dex][0]).find(t) >= 0 for t in targets]:
                    for col in range(1, INTLINE_temp.shape[1]):
                        if str(INTLINE_temp.loc[INTLINE_temp.index[dex], INTLINE_temp.columns[col]]) != 'nan':
                            INTLINE_temp.loc[INTLINE_temp.index[dex], INTLINE_temp.columns[col]] = INTLINE_temp.loc[INTLINE_temp.index[dex], INTLINE_temp.columns[col]]-100
    elif address.find('GACC/CAT') >= 0 and str(fname).find('Export') >= 0 and str(fname).find('Import') >= 0:
        time.sleep(0)
    elif address.find('HKCSD') >= 0:
        if freq == 'A':
            if str(fname).find('Gross Value Added') >= 0:
                INTLINE_temp.columns = [str(col).strip()[:4] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
            elif str(fname).find('Gross Domestic Fixed Capital Formation') >= 0:
                INTLINE_temp.columns = [str(col[0]).strip()[:4] if str(col[0]).strip()[:4].isnumeric() and str(col[1]).strip()[0].isnumeric() == False else None for col in INTLINE_temp.columns]
            else:
                INTLINE_temp.columns = [str(col[1]).strip()[:4] if str(col[1]).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'Q':
            if str(fname).find('Gross Domestic Fixed Capital Formation') >= 0:
                new_columns = []
                for col in INTLINE_temp.columns:
                    if str(col[0]).strip()[:4].isnumeric():
                        year = str(col[0]).strip()[:4]
                    if str(col[1]).strip()[0].isnumeric():
                        new_columns.append(year+'-Q'+str(col[1]).strip()[0])
                    else:
                        new_columns.append(None)
                INTLINE_temp.columns = new_columns
            else:
                INTLINE_temp.columns = [str(col[0]).strip()[:4]+'-'+str(col[1]).strip()[:2] if str(col[1]).strip()[0] == 'Q' else None for col in INTLINE_temp.columns]
        elif freq == 'M':
            INTLINE_temp.columns = [str(col[0]).strip()[:4]+'-'+datetime.strptime(str(col[1]).strip()[:3], '%b').strftime('%m') if str(col[1]).strip().isnumeric() == False else None for col in INTLINE_temp.columns]
        INTLINE_temp = INTLINE_temp.loc[:, INTLINE_temp.columns.dropna()]
        INTLINE_temp.index = pd.MultiIndex.from_tuples([[str(ind).strip() if str(ind).find('professional and business services') < 0 else 'nan' for ind in dex] for dex in INTLINE_temp.index])
        INTLINE_temp = INTLINE_temp.sort_index(axis=0)
        note_temp = []
        for dex in INTLINE_temp.index:
            note_found = False
            for d in dex:
                if d.find('chained') >= 0:
                    note_found = True
                    note_temp.append(re.sub(r'[\(\)]+', "", re.sub(r'.*?([Ii]n chained.+?dollars).*', r"\1", d)).title())
                    break
            if note_found == False:
                note_temp.append('nan')
        INTLINE_temp['note'] = note_temp
    elif address.find('HKCPI') >= 0:
        dataset = str(sname)
        new_columns = []
        for col in INTLINE_temp.columns:
            if str(col[0]).strip()[:4].isnumeric():
                year = str(col[0]).strip()[:4]
            if str(col[1]).strip().isnumeric():
                new_columns.append(year+'-'+str(col[1]).strip().rjust(2,'0'))
            else:
                new_columns.append(None)
        INTLINE_temp.columns = new_columns
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        new_index = []
        for dex in INTLINE_temp.index:
            if str(dex[0]).find('Unnamed') < 0:
                item = str(dex[0]).strip()
            if str(dex[1]).find('Unnamed') < 0:
                new_index.append([item, str(dex[1]).strip()])
            else:
                new_index.append([None, None])
        INTLINE_temp.index = pd.MultiIndex.from_tuples(new_index)
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
        INTLINE_temp = INTLINE_temp.sort_index(axis=0)
        INTLINE_temp['note'] = ['nan' for dex in INTLINE_temp.index]
    elif address.find('DOS') >= 0:
        if freq == 'Q':
            INTLINE_temp.columns = [str(col)[:4]+'-'+str(col)[-1]+str(col)[-2] for col in INTLINE_temp.columns]
        elif freq == 'M':
            INTLINE_temp.columns = [datetime.strptime(str(col), '%Y %b').strftime('%Y-%m') for col in INTLINE_temp.columns]
        INTLINE_temp.index = [re.sub(r'\s+|.+?:', "", str(dex)) for dex in INTLINE_temp.index]
        ADDPREFIX = ['M060171','M212881','M212882','M451001','M451002','M451391']
        if str(fname) in ADDPREFIX:
            new_index = []
            prefix = ''
            for dex in INTLINE_temp.index:
                if str(dex).find('OfServices') >= 0:
                    prefix = str(dex)
                elif str(dex).find('Total') == 0:
                    prefix = str(dex)[:str(dex).find(',')]
                elif str(dex).find('Food') == 0 and str(dex) != 'Food' and str(fname).find('M21288') == 0:
                    prefix = str(dex).replace('Food','')
                elif str(dex).find('OtherBusinessServices') >= 0 or str(fname).find('M21288') == 0:
                    prefix = ''
                new_index.append(prefix+str(dex))
            INTLINE_temp.index = new_index
        if INTLINE_previous.empty == False:
            INTLINE_previous.columns = [str(col) for col in INTLINE_previous.columns]
            INTLINE_temp = pd.concat([INTLINE_previous, INTLINE_temp], axis=1)
            INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
            INTLINE_temp = INTLINE_temp.sort_index(axis=1)
    elif address.find('KOSTAT') >= 0:
        INTLINE_temp.columns = [re.sub(r'[^0-9\s\./]+', "", str(col)).strip() for col in INTLINE_temp.columns]
        if freq == 'Q':
            INTLINE_temp.columns = [col[:4]+'-Q'+col[-3] if col[:4].isnumeric() else col for col in INTLINE_temp.columns]
            INTLINE_temp.index = pd.MultiIndex.from_tuples([[re.sub(r'\(.*?\)', "", str(d)).strip() for d in dex] for dex in INTLINE_temp.index])
            INTLINE_previous.columns = [col[:4]+'-Q'+col[-3] if col[:4].isnumeric() else col for col in INTLINE_previous.columns]
            INTLINE_previous.index = pd.MultiIndex.from_tuples([[re.sub(r'\(.*?\)', "", str(d)).strip() for d in dex] for dex in INTLINE_previous.index])
            INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_previous], axis=1)
            INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated(), ~INTLINE_temp.columns.duplicated()]
        elif freq == 'M':
            INTLINE_temp.columns = [col[:4]+'-'+col[-2:] if col[:4].isnumeric() else col for col in INTLINE_temp.columns]
            INTLINE_temp.index = pd.MultiIndex.from_tuples([[str(d).strip() for d in dex] for dex in INTLINE_temp.index])
        INTLINE_temp = INTLINE_temp.sort_index(axis=1)
    elif address.find('SNDO') >= 0:
        INTLINE_temp.columns = [str(col).strip()[:4] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated(), INTLINE_temp.columns.dropna()]
    elif address.find('SCB') >= 0:
        if index_col == 0 or index_col == None:
            if index_col == None:
                INTLINE_temp.index = [fname]
        if freq == 'Q':
            INTLINE_temp.columns = [str(col).strip()[:4]+'-Q'+str(col).strip()[-1:] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'M':
            INTLINE_temp.columns = [str(col).strip()[:4]+'-'+str(col).strip()[-2:] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
        if isinstance(INTLINE_temp.index, pd.MultiIndex):
            INTLINE_temp.index = pd.MultiIndex.from_tuples([[re.sub(r'[^A-Za-z\s=]+', "", str(d)).strip() for d in dex] for dex in INTLINE_temp.index])
        else:
            INTLINE_temp.index = [re.sub(r'[^A-Za-z\s=]+', "", str(dex)).strip() for dex in INTLINE_temp.index]
        INTLINE_temp = INTLINE_temp.loc[~INTLINE_temp.index.duplicated(), INTLINE_temp.columns.dropna()]
    elif address.find('CANSIMS') >= 0:
        if index_col == 0:
            if str(fname).find('Financial') >= 0:
                INTLINE_temp.index = [re.sub(r',+', "", str(dex)).strip() if str(dex) != 'nan' else None for dex in INTLINE_temp.index]
            else:
                INTLINE_temp.index = [re.sub(r'[0-9]+', "", str(dex)).strip() if str(dex) != 'nan' else None for dex in INTLINE_temp.index]
        else:
            INTLINE_temp.index = pd.MultiIndex.from_tuples([[str(d).strip().replace('residential and','Residential and') if str(d) != 'nan' else None for d in dex] for dex in INTLINE_temp.index])
        if freq == 'M':
            INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%B %Y').strftime('%Y-%m') if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
        elif freq == 'W':
            INTLINE_temp.columns = [(datetime.strptime(str(col).strip(), '%B %d, %Y')+timedelta(days=3)).strftime('%Y-%m-%d') if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp.index.dropna(), INTLINE_temp.columns.dropna()]
    elif address.find('IFO') >= 0:
        if str(fname).find('Employment Barometer') >= 0 or str(fname).find('Export Expectations') >= 0:
            INTLINE_temp.columns = [col.strftime('%Y-%m') if str(col).strip()[:4].isnumeric() else None for col in INTLINE_temp.columns]
            INTLINE_temp.index = ['Total' if str(dex).find('Unnamed') >= 0 else dex for dex in INTLINE_temp.index]
        else:
            INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%m/%Y').strftime('%Y-%m') if str(col).strip()[-4:].isnumeric() else None for col in INTLINE_temp.columns]
            new_index = []
            sector = None
            adjusted = None
            for dex in INTLINE_temp.index:
                if str(dex[0]).find('Unnamed') < 0:
                    sector = str(dex[0]).strip()
                if str(dex[1]).find('Unnamed') < 0:
                    adjusted = re.split(r', ', str(dex[1]))[0].strip()
                new_index.append([sector, adjusted, str(dex[2]).strip()])
            INTLINE_temp.index = pd.MultiIndex.from_tuples(new_index)
            IN_temp = pd.DataFrame()
            for dex in INTLINE_temp.index:
                if str(dex[1]).find('Balances') >= 0 and tuple([dex[0], 'Index', dex[2]]) not in INTLINE_temp.index:
                    average_base = float(INTLINE_temp.loc[dex].loc[[str(base_year)+'-'+str(k).rjust(2, '0') for k in range(1,13)]].mean())
                    IN_t = pd.DataFrame([(float(INTLINE_temp.loc[dex, col])+200)*100/(average_base+200) for col in INTLINE_temp.columns]).T
                    IN_t.index = pd.MultiIndex.from_tuples([tuple([dex[0], 'Index', dex[2]])])
                    IN_t.columns = INTLINE_temp.columns
                    IN_temp = pd.concat([IN_temp, IN_t])
            INTLINE_temp = pd.concat([INTLINE_temp, IN_temp])
    elif address.find('HWWI') >= 0:
        INTLINE_temp.columns = [str(col).strip()[:3]+str(col).strip()[-2:] if str(col).strip()[-2:].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp.columns = [datetime.strptime(col, '%b%y').strftime('%Y-%m') if str(col)[-2:].isnumeric() else None for col in INTLINE_temp.columns]
        INTLINE_temp.index = pd.MultiIndex.from_tuples([[str(d).strip() if str(d).find('excluding energy') < 0 else 'Excluding energy' for d in dex] for dex in INTLINE_temp.index])
    
    #################################################################################################################################################################################################################
    for ind in range(Series[freq].shape[0]):
        sys.stdout.write("\rLoading...("+str(round((ind+1)*100/Series[freq].shape[0], 1))+"%)*")
        sys.stdout.flush()
        if Series[freq].iloc[ind]['DataSet'] == dataset:
            code = re.sub(r'[_]+|\.[A-Z]', "", str(Series[freq].index[ind])).replace(Countries.loc[country, 'location'],'').replace(Countries.loc[country, 'location'][:2],'')
            #if base_year != 0:
            #    if is_period == True:
            #        code = code+base_year[2:4]
            #    else:
            #        code = code+base_year[-2:]
            if str(Series[freq].iloc[ind]['keyword']).find('+') >= 0:
                key_sum = True
                IN_t = pd.DataFrame()
                keys = re.split(r'\+', str(Series[freq].iloc[ind]['keyword']))
            elif address.find('COMM') >= 0 and str(fname).find('Index') >= 0:
                keys = [str(Series[freq].iloc[ind]['Exp or Imp']), str(Series[freq].iloc[ind]['Commodity or Country']), str(Series[freq].iloc[ind]['Unit Type'])]
            elif address.find('COMM') >= 0 and str(fname).find('Index') < 0:
                keys = [str(Series[freq].iloc[ind]['Commodity or Country']), str(Series[freq].iloc[ind]['Unit Type'])]
            elif address.find('COUN') >= 0:
                if bool(re.search(r'\+', str(Series[freq].iloc[ind]['Exp or Imp']))):
                    trade_sum = True
                    IN_t = pd.DataFrame()
                    target = re.split(r'\+', str(Series[freq].iloc[ind]['Exp or Imp']))
                    minus = [True if t.find('-') == 0 else False for t in target]+[False]
                    target = [t.replace('-','') for t in target]
                    keys = target+[str(Series[freq].iloc[ind]['Commodity or Country'])]
                elif bool(re.search(r'\+', str(Series[freq].iloc[ind]['Commodity or Country']))):
                    trade_sum = True
                    IN_t = pd.DataFrame()
                    target = re.split(r'\+', str(Series[freq].iloc[ind]['Commodity or Country']))
                    minus = [True if t.find('-') == 0 else False for t in target]+[False]
                    target = [t.replace('-','') for t in target]
                    keys = target+[str(Series[freq].iloc[ind]['Exp or Imp'])]
                else:
                    keys = [str(Series[freq].iloc[ind]['Exp or Imp']), str(Series[freq].iloc[ind]['Commodity or Country'])]
            elif address.find('GACC/CAT') >= 0 and str(fname).find('Export') >= 0 and str(fname).find('Import') >= 0:
                keys = [str(Series[freq].iloc[ind]['Trade']), str(Series[freq].iloc[ind]['keyword'])]
            else:
                keys = re.split(r', ', str(Series[freq].iloc[ind]['keyword']))
            found = False
            for dex in INTLINE_temp.index:
                item_found = [False for k in keys]
                minus_count = False
                for k in keys:
                    if (isinstance(INTLINE_temp.index, pd.MultiIndex) and True in [str(d).find(k) == 0 for d in dex]) or (isinstance(INTLINE_temp.index, pd.Index) and str(dex).find(k) == 0):
                        item_found[keys.index(k)] = True
                        if trade_sum == True and minus[keys.index(k)] == True:
                            minus_count = True
                    elif key_sum == True or trade_sum == True:
                        continue
                    else:
                        break
                if key_sum == True and True in item_found:
                    pdSeries = INTLINE_temp.loc[dex]
                    if pdSeries.shape[0] == 1:
                        pdSeries = pdSeries.T.squeeze()
                    IN_t = pd.concat([IN_t, pd.DataFrame(pdSeries).T])
                elif trade_sum == True and item_found.count(True) >= 2:
                    pdSeries = INTLINE_temp.loc[dex]
                    if pdSeries.shape[0] == 1:
                        pdSeries = pdSeries.T.squeeze()
                    if minus_count == True:
                        pdSeries = pdSeries.apply(lambda x: x*(-1))
                    IN_t = pd.concat([IN_t, pd.DataFrame(pdSeries).T])
                elif key_sum == False and trade_sum == False and False not in item_found:
                    found = True
                    pdSeries = INTLINE_temp.loc[dex]
                    if pdSeries.shape[0] == 1:
                        pdSeries = pdSeries.T.squeeze()
                    if address.find('JPC') >= 0:
                        pdSeries = pdSeries.apply(lambda x: float(re.sub(r'[^0-9\.]+', "", str(x))))
                    INTLINE_t = pd.concat([INTLINE_t, pd.DataFrame(pdSeries).T])
                    break
                else:
                    continue
            if key_sum == True or trade_sum == True:
                if (key_sum == True and IN_t.shape[0] == len(keys)) or (trade_sum == True and IN_t.shape[0] == len(target)):
                    found = True
                IN_t = IN_t.applymap(lambda x: float(str(x).replace(',','')))
                IN_t = IN_t.sum(axis=0)
                INTLINE_t = pd.concat([INTLINE_t, pd.DataFrame(IN_t).T])
            if found == False:
                logging.info(list(INTLINE_temp.index))
                ERROR('Item not found: '+str(keys))
            lab = re.sub(r'(\([ESNA0-9]+\)\s*)*(\(([0-9]{4}|PY)\))*[,\s]*[NSWDA]*\s+\-\s*(\(.+?\)\s*|SAR)*', "", \
                str(Series[freq].iloc[ind]['Short Label']).replace(Countries.loc[country, 'Country_Name'],'')).strip(' ,')
            if str(Series[freq].iloc[ind]['Scale']) != 'nan' and str(Series[freq].iloc[ind]['Scale']) != 'Unit':
                unit = str(Series[freq].iloc[ind]['Scale'])+' of '+str(Series[freq].iloc[ind]['Unit'])
            elif str(Series[freq].iloc[ind]['Unit']) == 'Index':
                base = 0
                if address.find('NBS') >= 0:
                    if freq == 'M':
                        base = 'The Same Month of Last Year'
                    else:
                        base = 'Previous Year'
                elif freq == 'A' or freq == 'Q' or (freq == 'M' and address.find('COJ') < 0):
                    base = base_year
                elif freq == 'M' and address.find('COJ') >= 0:
                    base = re.sub(r'.*?([0-9]{4}).+', r"\1", str(pdSeries.name[-1]))
                if base == 0:
                    ERROR('Index base not found: '+str(code))
                if address.find('CANSIMS') >= 0 and str(fname).find('Stock Exchange') >= 0:
                    unit = str(Series[freq].iloc[ind]['Unit'])+': '+base+'=1000'
                else:
                    unit = str(Series[freq].iloc[ind]['Unit'])+': '+base+'=100'
            else:
                unit = str(Series[freq].iloc[ind]['Unit'])
            concept = str(Series[freq].iloc[ind]['Concept'])
            if str(Series[freq].iloc[ind]['Seasonal Adjustment']) != 'nan':
                form = str(Series[freq].iloc[ind]['Seasonal Adjustment'])
            else:
                form = 'Not Seasonally Adjusted'
            if key_sum != False:
                key_sum = False
            #if base_year != 0:
            #    new_code_t.append(code)
            #else:
            new_code_t.append(address[:3]+code)
            new_label_t.append(lab)
            new_unit_t.append(unit)
            new_type_t.append(concept)
            new_form_c.append(form)
    sys.stdout.write("\n\n")

    INTLINE_t = INTLINE_t.sort_index(axis=1)
    INTLINE_t.insert(loc=0, column='Index', value=new_code_t)
    INTLINE_t.insert(loc=1, column='Label', value=new_label_t)
    INTLINE_t.insert(loc=2, column='unit', value=new_unit_t)
    INTLINE_t.insert(loc=3, column='type', value=new_type_t)
    INTLINE_t.insert(loc=4, column='form_c', value=new_form_c)
    INTLINE_t = INTLINE_t.set_index('Index', drop=False)
    INTLINE_t = INTLINE_t.loc[:, INTLINE_t.columns.dropna()]
    label = INTLINE_t['Label']

    return INTLINE_t, label, note, footnote

def INTLINE_ONS(INTLINE_temp, data_path, country, address, fname, sname, Series, Countries, freq, x='x', transpose=True, CDID=True, table=None, note=[], footnote=[]):
    #INTLINE_temp = readExcelFile(data_path+str(country)+'/'+address+fname+'.xls'+x, header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=sname, acceptNoFile=False)
    if type(INTLINE_temp) != dict and INTLINE_temp.empty == True:
        ERROR('Sheet Not Found: '+data_path+str(country)+'/'+address+fname+'.xls'+x+', sheet name: '+str(sname))
    if transpose == True:
        INTLINE_temp = INTLINE_temp.T
    INTLINE_t = pd.DataFrame()
    code_sum = None
    new_code_t = []
    new_label_t = []
    new_unit_t = []
    new_type_t = []
    new_form_c = []

    if CDID == True:
        INTLINE_temp = INTLINE_temp.set_index('CDID')
    else:
        new_columns = []
        for col in INTLINE_temp.columns:
            if bool(re.match(r'[0-9]{4}', str(col[0]))):
                year = str(int(col[0]))
            if str(col[1]) != 'nan':
                new_columns.append(year+'-'+str(col[1]).strip())
            elif bool(re.match(r'[0-9]{4}', str(col[0]))):
                new_columns.append(str(int(col[0])))
            else:
                new_columns.append(None)
        INTLINE_temp.columns = new_columns
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        if type(INTLINE_temp.index) == pd.core.indexes.multi.MultiIndex:
            new_index = []
            sector = ''
            SeriesID = ''
            for dex in INTLINE_temp.index:
                if str(dex[0]).find('Unnamed') < 0 and str(dex[0]).find('nan') < 0:
                    sector = str(dex[0]).strip()
                if str(dex[1]).find('Unnamed') < 0 and str(dex[1]).find('nan') < 0 and str(dex[1]).find('Excluding') < 0:
                    SeriesID = str(dex[1]).strip()
                elif str(dex[2]).find('Unnamed') < 0 and str(dex[2]).find('nan') < 0:
                    SeriesID = str(dex[2]).strip()
                else:
                    SeriesID = str(dex[0]).strip()
                new_index.append(tuple([sector, SeriesID]))
            INTLINE_temp.index = pd.MultiIndex.from_tuples(new_index)
            INTLINE_temp = INTLINE_temp.sort_index()
    for ind in range(Series[freq].shape[0]):
        sys.stdout.write("\rLoading...("+str(round((ind+1)*100/Series[freq].shape[0], 1))+"%)*")
        sys.stdout.flush()
        if Series[freq].iloc[ind]['DataSet'] == str(fname):
            if CDID == True or str(fname).find('opci') >= 0:
                if str(Series[freq].iloc[ind]['SeriesID']).find('+') >= 0:
                    code_sum = re.split(r'\+', str(Series[freq].iloc[ind]['SeriesID']))
                else:
                    code = str(Series[freq].iloc[ind]['SeriesID'])
            else:
                if Series[freq].iloc[ind]['Table'] == table:
                    code = tuple([Series[freq].iloc[ind]['Sector'], Series[freq].iloc[ind]['SeriesID']])
                else:
                    continue
            try:
                if code_sum != None:
                    try:
                        IN_t = pd.concat([INTLINE_temp.loc[c] for c in code_sum], axis=1, keys=[re.sub(r'\.[A-Z]', "", str(Series[freq].index[ind])[3:]) for c in code_sum], names=['code']).T
                        IN_t = IN_t.sum(level='code')
                        for col in IN_t.columns:
                            if IN_t.iloc[0][col] == 0:
                                IN_t.loc[IN_t.index[0], col] = None
                        code = code_sum[0]
                        INTLINE_t = INTLINE_t.append(IN_t)
                    except KeyError:
                        ERROR('SeriesID not found: '+str(Series[freq].iloc[ind]['SeriesID']))
                else:
                    pdSeries = INTLINE_temp.loc[code]
                    if pdSeries.shape[0] == 1:
                        pdSeries = pdSeries.T.squeeze()
                    INTLINE_t = pd.concat([INTLINE_t, pd.DataFrame(pdSeries).T])
            except KeyError:
                if str(fname).find('opci') >= 0:
                    continue
                ERROR('SeriesID not found: '+str(code))
            lab = re.sub(r'((\([0-9=]+\))*|(\([0-9]{4}\))*)[,\s]*[NSA]*\s+\-\s*', "", re.sub(r'(\([0-9]+)([a-z]+)([0-9]+\))', r"\1 \2 \3", \
                str(Series[freq].iloc[ind]['Short Label']).replace(Countries.loc[country, 'Country_Name'],''))).strip(' ,').replace('&','and').replace('Qtly','Quarterly')
            base = ''
            if str(Series[freq].iloc[ind]['Scale']) != 'nan' and str(Series[freq].iloc[ind]['Scale']) != 'Unit':
                unit = str(Series[freq].iloc[ind]['Scale'])+' of '+str(Series[freq].iloc[ind]['Unit'])
            elif str(Series[freq].iloc[ind]['Unit']) == 'Index':
                for period in range(pdSeries.shape[0]):
                    if str(pdSeries.index[period]).isnumeric() and pdSeries.iloc[period] == 100:
                        base = str(pdSeries.index[period])
                if base == '':
                    ERROR('Index base not found: '+str(code))
                unit = str(Series[freq].iloc[ind]['Unit'])+': '+base+'=100'
            else:
                unit = str(Series[freq].iloc[ind]['Unit'])
            concept = str(Series[freq].iloc[ind]['Concept'])
            if str(Series[freq].iloc[ind]['Seasonal Adjustment']) != 'nan':
                form = str(Series[freq].iloc[ind]['Seasonal Adjustment'])
            else:
                #print(code)
                try:
                    form = Series['Q'].loc[Series['Q']['SeriesID'] == code]['Seasonal Adjustment'].item()
                except:
                    form = Series['M'].loc[Series['M']['SeriesID'] == code]['Seasonal Adjustment'].item()
            if code_sum != None or CDID == False:
                code = re.sub(r'\.[A-Z]', "", str(Series[freq].index[ind])[3:])
                code_sum = None
            new_code_t.append('ONS'+code)#+base[-2:]
            new_label_t.append(lab)
            new_unit_t.append(unit)
            new_type_t.append(concept)
            new_form_c.append(form)
    sys.stdout.write("\n\n")

    new_columns = []
    for col in INTLINE_t.columns:
        if bool(re.match(r'[0-9]{4}\sQ[1-4]', str(col))):
            new_columns.append(re.sub(r'\s', "-", str(col)))
        elif bool(re.match(r'[0-9]{4}\s[A-z]{3}', str(col))):
            new_columns.append(datetime.strptime(str(col), '%Y %b').strftime('%Y-%m'))
        else:
            new_columns.append(col)
    INTLINE_t.columns = new_columns
    INTLINE_t = INTLINE_t.sort_index(axis=1)
    INTLINE_t.insert(loc=0, column='Index', value=new_code_t)
    INTLINE_t.insert(loc=1, column='Label', value=new_label_t)
    INTLINE_t.insert(loc=2, column='unit', value=new_unit_t)
    INTLINE_t.insert(loc=3, column='type', value=new_type_t)
    INTLINE_t.insert(loc=4, column='form_c', value=new_form_c)
    INTLINE_t = INTLINE_t.set_index('Index', drop=False)
    INTLINE_t = INTLINE_t.loc[:, INTLINE_t.columns.dropna()]
    label = INTLINE_t['Label']

    return INTLINE_t, label, note, footnote

def INTLINE_BOE(INTLINE_t, note_temp, address, sname, freq):
    
    #note, footnote = INTLINE_NOTE(INTLINE_t.index, sname, address=address)
    note = note_temp
    footnote = []
    INTLINE_t = INTLINE_t.T
    new_columns = []
    new_code_t = []
    new_label_t = []
    new_note_t = []
    new_unit_t = []
    is_adj_t = []
    
    for col in INTLINE_t.columns:
        try:
            if freq == 'A':
                new_columns.append(datetime.strptime(str(col), '%d %b %y').strftime('%Y'))
            elif freq == 'Q':
                new_columns.append(pd.Period(str(col), freq='Q').strftime('%Y-Q%q'))
            elif freq == 'M':
                new_columns.append(datetime.strptime(str(col), '%d %b %y').strftime('%Y-%m'))
            elif freq == 'D':
                new_columns.append(datetime.strptime(str(col), '%d %b %y').strftime('%Y-%m-%d'))
        except:
            new_columns.append(None)
    INTLINE_t.columns = new_columns
    INTLINE_t = INTLINE_t.sort_index(axis=1)
    for ind in range(INTLINE_t.shape[0]):
        """if type(INTLINE_t.index) == pd.core.indexes.multi.MultiIndex:
            code = str(INTLINE_t.index[ind][0]).strip()
            note_index = re.split(r'\s+', str(INTLINE_t.index[ind][1]).strip())
        else:
            code = str(INTLINE_t.index[ind]).strip()
            note_index = None"""
        code = str(INTLINE_t.index[ind][0]).strip()
        lab = str(INTLINE_t.index[ind][1]).strip()#label_temp[code]#chrome.find_element_by_xpath('.//tr[td[@width="12%"]/label/b[text()="'+code+'"]]/td[@width="78%"]/b').text
        if len(INTLINE_t.index[ind]) == 3:
            note_index = re.split(r'\s+', str(INTLINE_t.index[ind][2]).strip())
        else:
            note_index = None
        is_adj = 'Not Seasonally Adjusted'
        base_year = ''
        if bool(re.search(r'\(.+?[0-9]{4}\s*=\s*100\s*\)', lab)):
            unit = 'Index: '+re.sub(r'.+?([0-9]{4})\s*(=)\s*(100).*', r"\1\2\3", lab)
            base_year = re.sub(r'.*?([0-9]{4}).+', r"\1", unit)
            lab = re.sub(r'(.+?)\(.+?[0-9]{4}\s*=\s*100\s*\)', r"\1", lab).strip()
        elif bool(re.search(r'amount[s]* outstanding', lab)):
            unit = 'UK Sterling Pound'
            is_adj = re.sub(r'.+?\(in sterling millions\)\s*(.+)', r"\1", lab).title().strip()
            lab = re.sub(r'\s*\(in sterling millions\).+', "", lab).strip()
        else:
            unit = 'Percentage'
        new_code_t.append(code)#+base_year[-2:]
        new_unit_t.append(unit)
        new_label_t.append(lab)
        new_note_t.append(note_index)
        is_adj_t.append(is_adj)

    INTLINE_t.insert(loc=0, column='Index', value=new_code_t)
    INTLINE_t.insert(loc=1, column='Label', value=new_label_t)
    INTLINE_t.insert(loc=2, column='note', value=new_note_t)
    INTLINE_t.insert(loc=3, column='unit', value=new_unit_t)
    INTLINE_t.insert(loc=4, column='is_adj', value=is_adj_t)
    INTLINE_t = INTLINE_t.set_index('Index', drop=False)
    label = INTLINE_t['Label']

    return INTLINE_t, label, note, footnote

def INTLINE_EUC(INTLINE_temp, data_path, country, address, fname, sname, Series, Table, Countries, freq, transpose=True, keyword='keyword', note=[], footnote=[]):
    if type(INTLINE_temp) != dict and INTLINE_temp.empty == True:
        ERROR('Sheet Not Found: '+fname+'.xlsx, sheet name: '+str(sname))
    if transpose == True:
        INTLINE_temp = INTLINE_temp.T
    INTLINE_t = pd.DataFrame()
    new_code_t = []
    new_label_t = []
    new_unit_t = []
    new_type_t = []
    new_form_c = []
    
    new_columns = []
    for col in INTLINE_temp.columns:
        if freq == 'M':
            try:
                new_columns.append(col.strftime('%Y-%m'))
            except:
                new_columns.append(None)
        else:
            new_columns.append(str(col))
    INTLINE_temp.columns = new_columns
    for ind in range(Series[freq].shape[0]):
        sys.stdout.write("\rLoading...("+str(round((ind+1)*100/Series[freq].shape[0], 1))+"%)*")
        sys.stdout.flush()
        if Series[freq].iloc[ind]['DataSet'] == str(fname):
            #series_code = str(Series[freq].iloc[ind][keyword])
            code = str(Series[freq].iloc[ind][keyword])
            coun = Table[Table == Countries.loc[country, 'Country_Name']].index[0]
            """try:
                coun = Table[Table == Countries.loc[country, 'Country_Name']].index[0]
                if str(fname) == 'main_indicators_nace2':
                    survey = Table[Table == str(Series[freq].iloc[ind]['Concept'])].index[0]
                    code = coun+'.'+survey
                else:
                    sector = str(sname)[:4]
                    question = Table[Table == str(Series[freq].iloc[ind]['Concept'])].index[0]
                    answer = Table[Table == str(Series[freq].iloc[ind]['Scale'])].index[0]
                    code = sector+'.'+coun+'.TOT.'+question+'.'+answer+'.'+freq
            except IndexError:
                if str(traceback.format_exc()).find('Concept') >= 0:
                    print('\nQuestions Error')
                elif str(traceback.format_exc()).find('Scale') >= 0:
                    print('\nAnswers Error')
                else:
                    print(traceback.format_exc())
                ERROR(keyword+' components not found: '+series_code)
            if code != series_code:
                ERROR(keyword+' components have been modified, please do the corresponding modification on the Series Excel: '+series_code)"""
            try:
                pdSeries = INTLINE_temp.loc[code]
                if INTLINE_temp.loc[code].shape[0] == 1:
                    pdSeries = pdSeries.T.squeeze()
                INTLINE_t = pd.concat([INTLINE_t, pd.DataFrame(pdSeries).T])
            except KeyError:
                ERROR(keyword+' not found in Series Excel: '+str(code))
            lab = re.sub(r'[,\s]*[NSA]*\s+\-\s*', "", str(Series[freq].iloc[ind]['Short Label']).replace(Countries.loc[country, 'Country_Name'],'')).strip(' ,')
            unit = str(Series[freq].iloc[ind]['Unit'])
            concept = str(Series[freq].iloc[ind]['Concept'])
            form = str(Series[freq].iloc[ind]['Seasonal Adjustment'])
            code = code.replace('.', '').replace(coun+'TOT', '')[:-1]
            new_code_t.append(code)
            new_label_t.append(lab)
            new_unit_t.append(unit)
            new_type_t.append(concept)
            new_form_c.append(form)
    sys.stdout.write("\n\n")

    INTLINE_t = INTLINE_t.sort_index(axis=1)
    INTLINE_t.insert(loc=0, column='Index', value=new_code_t)
    INTLINE_t.insert(loc=1, column='Label', value=new_label_t)
    INTLINE_t.insert(loc=2, column='unit', value=new_unit_t)
    INTLINE_t.insert(loc=3, column='type', value=new_type_t)
    INTLINE_t.insert(loc=4, column='form_c', value=new_form_c)
    INTLINE_t = INTLINE_t.set_index('Index', drop=False)
    INTLINE_t = INTLINE_t.loc[:, INTLINE_t.columns.dropna()]
    label = INTLINE_t['Label']

    return INTLINE_t, label, note, footnote

def INTLINE_EST(INTLINE_temp, data_path, country, address, fname, Series, freq, Countries, keyword='keyword', note=[], footnote=[]):
    for i in range(INTLINE_temp.shape[0]):
        INTLINE_temp.loc[INTLINE_temp.index[i], 'GEO_LABEL'] = re.sub(r'\(.+?\)', "", str(INTLINE_temp.iloc[i]['GEO_LABEL'])).strip()
    INTLINE_temp = INTLINE_temp.loc[INTLINE_temp['GEO_LABEL'] == Countries.loc[country, 'Country_Name']]
    if INTLINE_temp.empty:
        ERROR('Country Not Found in file '+str(fname)+': '+str(Countries.loc[country, 'Country_Name']))
    INTLINE_temp = INTLINE_temp.rename(columns={'INDIC_BT':'INDIC'})
    INTLINE_temp = INTLINE_temp.sort_values(by=['GEO','S_ADJ','UNIT','INDIC','TIME'], ignore_index=True)
       
    INTLINE_t = pd.DataFrame()
    new_item_t = []
    new_index_t = []
    new_code_t = []
    new_label_t = []
    new_unit_t = []
    new_type_t = []
    new_form_c = []
    new_dataframe = []
    ATTR = {'Label':'','Type':''}
    firstfound = False
    code = ''
    for i in range(INTLINE_temp.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/INTLINE_temp.shape[0], 1))+"%)*")
        sys.stdout.flush()
        indic = str(INTLINE_temp.iloc[i]['INDIC']).replace('-','')
        adj = str(INTLINE_temp.iloc[i]['S_ADJ']).replace('-','')
        try:
            nace = str(INTLINE_temp.iloc[i]['NACE_R2']).replace('-','')
        except KeyError:
            nace = ''
        if str(fname).find('cphi') >= 0:
            code_t = indic[:2]+indic[6:]+nace+adj
        else:
            code_t = indic[:2]+nace+adj
        if code_t != code and code_t in list(Series[freq].loc[Series[freq]['DataSet']==str(fname)][keyword]):
            if firstfound == True:
                new_dataframe.append(new_item_t)
                INTLINE_new = pd.DataFrame(new_dataframe, columns=new_index_t)
                if INTLINE_new.empty == False:
                    new_code_t.append(code)#+base_year[-2:]
                    new_label_t.append(ATTR['Label'])
                    new_unit_t.append(unit)
                    new_type_t.append(ATTR['Type'])
                    new_form_c.append(form)
                    INTLINE_t = pd.concat([INTLINE_t, INTLINE_new], ignore_index=True)
                new_dataframe = []
                new_item_t = []
                new_index_t = []
            code = code_t
            unit = str(INTLINE_temp.iloc[i]['UNIT_LABEL'])
            base_year = re.sub(r'.*?([0-9]{4}).+', r"\1", unit)
            form = str(INTLINE_temp.iloc[i]['S_ADJ_LABEL'])
            for key in ATTR:
                ATTR[key] = ''
            for ind in range(Series[freq].shape[0]):
                if Series[freq].iloc[ind]['DataSet'] == str(fname) and code == str(Series[freq].iloc[ind][keyword]):
                    ATTR['Label'] = re.sub(r'(\([0-9]{4}\))*[,\s]*[NSCA]*\s+\-\s*', "", str(Series[freq].iloc[ind]['Short Label']).replace(Countries.loc[country, 'Country_Name'],'')).strip(' ,')
                    ATTR['Type'] = str(Series[freq].iloc[ind]['Concept'])
            for key in ATTR:
                if ATTR[key] == '':
                    ERROR(key+' not found: '+code)
            firstfound = True
        new_item_t.append(INTLINE_temp.iloc[i]['Value'])
        if freq == 'M':
            period_index = str(INTLINE_temp.iloc[i]['TIME']).replace('M','-')
        new_index_t.append(period_index)  
    sys.stdout.write("\n\n")
    new_dataframe.append(new_item_t)
    INTLINE_new = pd.DataFrame(new_dataframe, columns=new_index_t)
    if INTLINE_new.empty == False:
        new_code_t.append(code)#+base_year[-2:]
        new_label_t.append(ATTR['Label'])
        new_unit_t.append(unit)
        new_type_t.append(ATTR['Type'])
        new_form_c.append(form)
        INTLINE_t = pd.concat([INTLINE_t, INTLINE_new], ignore_index=True)
    INTLINE_t = INTLINE_t.sort_index(axis=1)
    INTLINE_t.insert(loc=0, column='Index', value=new_code_t)
    INTLINE_t.insert(loc=1, column='Label', value=new_label_t)
    INTLINE_t.insert(loc=2, column='unit', value=new_unit_t)
    INTLINE_t.insert(loc=3, column='type', value=new_type_t)
    INTLINE_t.insert(loc=4, column='form_c', value=new_form_c)
    INTLINE_t = INTLINE_t.set_index('Index', drop=False)
    label = INTLINE_t['Label']

    return INTLINE_t, label, note, footnote

def INTLINE_BEIS(INTLINE_temp, data_path, country, address, fname, Series, freq, Countries, note=[], footnote=[]):
    VALUE = {'Index':['Index','HPI'], 'Average price':['Average_Price','APP']}
    GEO = {'United Kingdom':'K02','England':'E92','Wales':'W92','Scotland':'S92','Northern Ireland':'N92'}
    INTLINE_temp = INTLINE_temp.sort_values(by=['Region_Name','Date'], ignore_index=True)
    INTLINE_temp = INTLINE_temp.loc[INTLINE_temp['Region_Name'].isin(GEO)]
    if VALUE[fname][0] not in INTLINE_temp.columns:
        ERROR('Item not found in file: '+str(fname))
    
    INTLINE_t = pd.DataFrame()
    new_item_t = []
    new_index_t = []
    new_code_t = []
    new_label_t = []
    new_unit_t = []
    new_type_t = []
    new_form_c = []
    new_dataframe = []
    ATTR = {'Label':'','Unit':'','Type':'','Form':''}
    firstfound = False
    code = ''
    for i in range(INTLINE_temp.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/INTLINE_temp.shape[0], 1))+"%)*")
        sys.stdout.flush()
        #if str(INTLINE_temp.iloc[i]['Region_Name']) not in GEO:
        #    continue
        code_t = VALUE[fname][1]+GEO[str(INTLINE_temp.iloc[i]['Region_Name'])]
        if code_t != code:
            if firstfound == True:
                new_dataframe.append(new_item_t)
                US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
                if US_new.empty == False:
                    new_code_t.append(code)#+base[2:4]
                    new_label_t.append(ATTR['Label'])
                    new_unit_t.append(ATTR['Unit'])
                    new_type_t.append(ATTR['Type'])
                    new_form_c.append(ATTR['Form'])
                    INTLINE_t = pd.concat([INTLINE_t, US_new], ignore_index=True)
                new_dataframe = []
                new_item_t = []
                new_index_t = []
            code = code_t
            for key in ATTR:
                ATTR[key] = ''
            for ind in range(Series[freq].shape[0]):
                if Series[freq].iloc[ind]['DataSet'] == str(fname) and code == str(Series[freq].iloc[ind]['SeriesID']):
                    ATTR['Label'] = re.sub(r'[,\s]*[NSA]*\s+\-\s*', "", str(Series[freq].iloc[ind]['Short Label']).replace(Countries.loc[country, 'Country_Name'],'')).strip(' ,')
                    base = ''
                    if str(Series[freq].iloc[ind]['Unit']) == 'Index':
                        pdSeries = INTLINE_temp.loc[INTLINE_temp['Region_Name'] == str(INTLINE_temp.iloc[i]['Region_Name'])]
                        for period in range(pdSeries.shape[0]):
                            if pdSeries.iloc[period][VALUE[fname][0]] == 100:
                                base = datetime.strptime(str(pdSeries.iloc[period]['Date']), '%Y-%m-%d').strftime('%Y.%m')
                                break
                        if base == '':
                            ERROR('Index base not found: '+str(code))
                        ATTR['Unit'] = str(Series[freq].iloc[ind]['Unit'])+': '+base+'=100'
                    else:
                        ATTR['Unit'] = str(Series[freq].iloc[ind]['Unit'])
                    ATTR['Type'] = str(Series[freq].iloc[ind]['Concept'])
                    ATTR['Form'] = str(Series[freq].iloc[ind]['Seasonal Adjustment'])
            for key in ATTR:
                if ATTR[key] == '':
                    ERROR(key+' not found: '+code)
            firstfound = True
        
        new_item_t.append(INTLINE_temp.iloc[i][VALUE[fname][0]])
        if freq == 'M':
            period_index = datetime.strptime(str(INTLINE_temp.iloc[i]['Date']), '%Y-%m-%d').strftime('%Y-%m')
        new_index_t.append(period_index)  
    sys.stdout.write("\n\n")
    new_dataframe.append(new_item_t)
    US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
    if US_new.empty == False:
        new_code_t.append(code)#+base[2:4]
        new_label_t.append(ATTR['Label'])
        new_unit_t.append(ATTR['Unit'])
        new_type_t.append(ATTR['Type'])
        new_form_c.append(ATTR['Form'])
        INTLINE_t = pd.concat([INTLINE_t, US_new], ignore_index=True)
    INTLINE_t = INTLINE_t.sort_index(axis=1)
    INTLINE_t.insert(loc=0, column='Index', value=new_code_t)
    INTLINE_t.insert(loc=1, column='Label', value=new_label_t)
    INTLINE_t.insert(loc=2, column='unit', value=new_unit_t)
    INTLINE_t.insert(loc=3, column='type', value=new_type_t)
    INTLINE_t.insert(loc=4, column='form_c', value=new_form_c)
    INTLINE_t = INTLINE_t.set_index('Index', drop=False)
    label = INTLINE_t['Label']

    return INTLINE_t, label, note, footnote

def INTLINE_LTPLR(chrome, data_path, country, address, fname, sname, freq, start_year, update, Countries, note=[], footnote=[]):
    file_path = data_path+str(country)+'/'+address+sname+'.xlsx'
    IHS = readExcelFile(file_path, header_=0, index_col_=0, sheet_name_=0)
    IHS.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in IHS.columns]
    if INTLINE_PRESENT(file_path) == True:
        label = IHS['Label']
        return IHS, label, note, footnote
    INTLINE_t = pd.DataFrame()
    link_list = []
    
    chrome.get(fname)
    if str(sname).find('LTPLR') >= 0:
        link_list_temp = chrome.find_elements_by_xpath('.//*[@href]')
        for link in link_list_temp:
            if link.get_attribute("href").find('statistics/dl/loan/prime/prime') >= 0:
                link_list.append(link.get_attribute("href"))
        for url in link_list:
            chrome.get(url)
            INTLINE_temp = pd.read_html(chrome.page_source, header=0, index_col=0)[0]
            drop_list = []
            for col in INTLINE_temp.columns:
                if str(col).find('Long-term') < 0:
                    drop_list.append(col)
            INTLINE_temp = INTLINE_temp.drop(columns=drop_list).squeeze()
            new_index = []
            year = ''
            for dex in INTLINE_temp.index:
                dex = re.sub(r'\s+', "", str(dex).replace('.','')).strip()
                if bool(re.match(r'[0-9]{4}', dex)):
                    year = dex[:4]
                    dex = re.sub(r'\s+', "", dex[4:].replace('.','')).strip()
                try:
                    new_index.append(year+'-'+datetime.strptime(dex, '%b%d').strftime('%m-%d'))
                except ValueError:
                    try:
                        new_index.append(year+'-'+datetime.strptime(dex, '%B%d').strftime('%m-%d'))
                    except ValueError:
                        new_index.append(dex)
            INTLINE_temp.index = new_index
            for dex in INTLINE_temp.index:
                try:
                    float(INTLINE_temp.loc[dex])
                except ValueError:
                    INTLINE_temp = INTLINE_temp.drop(index=dex)
                    continue
            INTLINE_t = pd.concat([INTLINE_t, INTLINE_temp])
    elif str(sname).find('PR') >= 0:
        y = 0
        height = chrome.execute_script("return document.documentElement.scrollHeight")
        while True:
            try:
                chrome.execute_script("window.scrollTo(0,"+str(y)+")")
                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.ID, 'showMoreHistory165'))).click()
            except:
                y+=500
                if y > height:
                    break
        chrome.find_element_by_id('eventHistoryTable165').get_attribute("outerHTML")
        INTLINE_t = pd.DataFrame(pd.read_html(chrome.page_source, header=[0], index_col=0)[0]['Actual'])
        INTLINE_t.index = [datetime.strptime(dex, '%b %d, %Y').strftime('%Y-%m-%d') for dex in INTLINE_t.index]
    INTLINE_t = INTLINE_t.sort_index(ascending=False)
    num = 0
    indexerr = False
    standard = INTLINE_t.index[num]
    freqlist = pd.date_range(start = str(start_year)+'-01-01',end=update,freq='M').strftime('%Y-%m-%d').tolist()
    freqlist.reverse()
    for day in freqlist:
        if day >= standard:
            IHS.loc[IHS.index[0], datetime.strptime(day, '%Y-%m-%d').strftime('%Y-%m')] = float(str(INTLINE_t.loc[standard].item()).replace('%',''))
        else:
            while True:
                try:
                    num += 1
                    standard = INTLINE_t.index[num]
                except IndexError:
                    indexerr = True
                    break
                else:
                    if day >= standard:
                        break
            if indexerr == True:
                break
            IHS.loc[IHS.index[0], datetime.strptime(day, '%Y-%m-%d').strftime('%Y-%m')] = float(str(INTLINE_t.loc[standard].item()).replace('%',''))
    
    IHS = IHS.sort_index(axis=1)
    label = IHS['Label']
    
    IHS.to_excel(data_path+str(country)+'/'+address+sname+'.xlsx', sheet_name=sname)
    return IHS, label, note, footnote

def INTLINE_TRADE(INTLINE_temp, fname, transpose=True):
    INTLINE_t = pd.DataFrame()
    for year in INTLINE_temp:
        sys.stdout.write("\rLoading Data From Year "+year+" ")
        sys.stdout.flush()
        new_columns = []
        new_index = []
        IN = INTLINE_temp[year]
        #if transpose == True:
        #    IN = IN.T
        if fname.find('Index') >= 0:
            IN = IN.loc[IN['Indexes Area'] == 'WORLD']
            for col in IN.columns:
                try:
                    new_columns.append(year+'-'+datetime.strptime(str(col), '%b').strftime('%m'))
                except ValueError:
                    new_columns.append('drop')
            IN.columns = new_columns
            IN = IN.drop(columns=['drop'])
            for dex in IN.index:
                new_index.append([int(dex[0]), str(dex[1]).strip(' *()'), str(dex[2]).strip(' *()')])
            IN.index = pd.MultiIndex.from_tuples(new_index)
        else:
            for col in IN.columns:
                try:
                    new_col = datetime.strptime(str(col[1]), '%Y %b.').strftime('%Y-%m')
                    if new_col[:4] != year:
                        new_columns.append('drop')
                    else:
                        new_columns.append(new_col)
                except ValueError:
                    new_columns.append('drop')
            IN.columns = new_columns
            IN = IN.drop(columns=['drop'])
            for dex in IN.index:
                Unit = str(dex[1]).strip(' *()')
                if len(Unit) >= 3 and Unit[0] == 'T':
                    Unit = 'Thousand Volume'
                elif len(Unit) >= 3 and Unit[0] == 'M':
                    Unit = 'Million Volume'
                elif Unit != 'Value':
                    Unit = 'Volume'
                new_index.append([str(dex[0]).strip(' *()'), Unit])
            IN.index = pd.MultiIndex.from_tuples(new_index)
        INTLINE_t = pd.concat([INTLINE_t, IN], axis=1)
    sys.stdout.write("\n\n")
    if fname.find('Index') < 0:
        INTLINE_t.loc[pd.IndexSlice[:,['Value']], :] = INTLINE_t.loc[pd.IndexSlice[:,['Value']], :].apply(lambda x: x/1000000)
        INTLINE_t.loc[pd.IndexSlice[:,['Volume']], :] = INTLINE_t.loc[pd.IndexSlice[:,['Volume']], :].apply(lambda x: x/1000)
    if INTLINE_t.empty:
        print(INTLINE_t)
        ERROR('Empty Table')

    return INTLINE_t

def INTLINE_JREI(INTLINE_temp, data_path, country, address, fname, sname, freq='S', note=[], footnote=[]):
    SEMI = {'01':'1', '03':'1', '07':'2', '09':'2'}
    IHS = readExcelFile(data_path+str(country)+'/'+address+'ULPI.xlsx', header_=0, index_col_=0, sheet_name_=0)
    IHS.columns = [col.strftime('%Y-S')+SEMI[col.strftime('%m')] if type(col) != str else col for col in IHS.columns]
    
    INTLINE_temp.index = [datetime.strptime(str(dex).strip(), '%b.%Y').strftime('%Y-S')+SEMI[datetime.strptime(str(dex).strip(), '%b.%Y').strftime('%m')] for dex in INTLINE_temp.index]
    base_period = None
    for ind in range(INTLINE_temp.shape[0]):
        if False not in [i == 100 for i in list(INTLINE_temp.iloc[ind])]:
            base_period = INTLINE_temp.index[ind]
    if base_period == None:
        ERROR('Base Period Not Found: '+str(fname))
    for ind in IHS.index:
        if str(IHS.loc[ind, 'DataSet']) == str(fname):
            IHS.loc[ind, 'Index'] = str(IHS.loc[ind, 'Index'])#[:-2]+base_period[2:4]
            IHS.loc[ind, 'unit'] = 'Index: '+base_period+'=100'
            multiplier = 100/IHS.loc[ind, base_period]
            for col in IHS.columns:
                try:
                    if col in INTLINE_temp.index:
                        IHS.loc[ind, col] = float(INTLINE_temp.loc[col, IHS.loc[ind, 'keyword']])
                    else:
                        IHS.loc[ind, col] = round(float(IHS.loc[ind, col])*multiplier, 2)
                except ValueError:
                    continue
    for ind in INTLINE_temp.index:
        if ind not in IHS.columns:
            for col in INTLINE_temp.columns:
                IHS.loc[IHS.loc[(IHS['DataSet'] == str(fname)) & (IHS['keyword'] == str(col))].index[0], ind] = float(INTLINE_temp.loc[ind, col])
    
    IHS = IHS.set_index('Index', drop=False)
    IHS.index.name = 'index'
    IHS = IHS.sort_index(axis=1)
    IN = IHS.loc[IHS['DataSet'] == str(fname)]
    label = IN['Label']
    
    IHS.to_excel(data_path+str(country)+'/'+address+'ULPI.xlsx', sheet_name='ULPI')
    return IN, label, note, footnote

def INTLINE_METI(INTLINE_temp, INTLINE_previous, data_path, country, address, fname, sname, Table, freq, transpose=True, base_year=0, note=[], footnote=[]):
    CONTENT = {0: '', 'Production': 'P','Shipments': 'S', 'Inventory': 'I', 'Operating Ratio': 'O'}
    ISADJUSTED = {True: ['S', 'Seasonally Adjusted'], False: ['U', 'Not Seasonally Adjusted']}
    AREA = {'JP': 'Japan Total', 'KT': 'Ku-area of Tokyo'}
    if type(INTLINE_temp) != dict and INTLINE_temp.empty == True:
        ERROR('Sheet Not Found: '+data_path+str(country)+'/'+address+fname+', sheet name: '+str(sname))
    if transpose == True:
        INTLINE_temp = INTLINE_temp.T
    label_level = None
    new_code_t = []
    new_label_t = []
    new_unit_t = []
    new_type_t = []
    
    if address.find('IIPD') >= 0:
        lab_keyword = 'Item_Name'
        if INTLINE_temp.index.name != 'Item_Number':
            print(INTLINE_temp.index)
            ERROR('Index Error: '+str(fname))
        try:
            INTLINE_previous = INTLINE_previous.set_index('ITEM NO.')
        except KeyError:
            ERROR('Index Key Not Found: "ITEM NO."')
        INTLINE_temp = pd.concat([INTLINE_temp, INTLINE_previous], axis=1)
        INTLINE_temp = INTLINE_temp.loc[:, ~INTLINE_temp.columns.duplicated()]
        INTLINE_temp.columns = [datetime.strptime(re.sub(r'^[a-z]\s*', "", str(col).strip()), '%Y%m').strftime('%Y-%m') if re.sub(r'^[a-z]\s*', "", str(col).strip()).isnumeric() else str(col) for col in INTLINE_temp.columns]
        INTLINE_t = INTLINE_temp.sort_index(axis=1)
        if str(sname) != 'Production':
            try:
                INTLINE_tem = INTLINE_t.loc[INTLINE_t['Item_Name'] == 'Mining and manufacturing']
                if INTLINE_tem.empty:
                    INTLINE_tem = INTLINE_t.loc[INTLINE_t['Item_Name'] == 'Manufacturing']
                    if INTLINE_tem.empty:
                        print(INTLINE_t['Item_Name'])
                        ERROR('Item Not Found: "Manufacturing"')
            except KeyError:
                ERROR('Index Key Not Found: "Item_Name"')
            INTLINE_t = INTLINE_tem
    elif address.find('MCPI') >= 0:
        lab_keyword = 'Group/Item'
        try:
            INTLINE_temp = INTLINE_temp.set_index('類・品目符号(Group/Item code)')
        except KeyError:
            ERROR('Index Key Not Found: "類・品目符号(Group/Item code)"')
        INTLINE_temp.columns = [datetime.strptime(str(col).strip(), '%Y%m').strftime('%Y-%m') if str(col).strip().isnumeric() else str(col) for col in INTLINE_temp.columns]
        INTLINE_t = INTLINE_temp.sort_index(axis=1)
    
    prefix = str(Table['prefix'][fname])
    content = CONTENT[sname]
    is_adj = ISADJUSTED[Table['Seasonally Adjusted'][fname]][0]
    for ind in range(INTLINE_t.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((ind+1)*100/INTLINE_t.shape[0], 1))+"%)*")
        sys.stdout.flush()
        lab = str(INTLINE_t.iloc[ind][lab_keyword]).strip().title().replace('And','and').replace('&','and').replace("'S","'s").replace('Excl.','excl.')
        if address.find('IIPD') >= 0:
            lab = lab.replace(', ',',')
            code = prefix+content+is_adj+re.sub(r'0{3}', "", str(INTLINE_t.index[ind]), 1)
            concept = str(sname)
        elif address.find('MCPI') >= 0:
            code = 'CPI'+prefix+is_adj+re.sub(r'^0+', "", str(INTLINE_t.index[ind])).strip()#+base_year[-2:]
            concept = AREA[prefix]
            lab = re.sub(r'(,\s+)*[Ss]easonally\s*[Aa]djusted', "", lab).strip()
        if len(code) > 11:
            ERROR('Code Length is too long: '+code)
        unit = 'Index: '+base_year+'=100'
        new_code_t.append(code)
        new_label_t.append(lab)
        new_unit_t.append(unit)
        new_type_t.append(concept)
    sys.stdout.write("\n\n")

    INTLINE_t.insert(loc=0, column='Index', value=new_code_t)
    INTLINE_t.insert(loc=1, column='Label', value=new_label_t)
    INTLINE_t.insert(loc=2, column='unit', value=new_unit_t)
    INTLINE_t.insert(loc=3, column='type', value=new_type_t)
    INTLINE_t = INTLINE_t.reset_index()
    INTLINE_t = INTLINE_t.set_index('Index', drop=False)
    INTLINE_t = INTLINE_t.loc[:, INTLINE_t.columns.dropna()]
    label = INTLINE_t['Label']
    if address.find('IIPD') >= 0:
        label_level = INTLINE_t['index']
    elif address.find('MCPI') >= 0:
        label_level_t = INTLINE_t['類・品目符号(Group/Item code)']
        label_level = label_level_t.copy()
        PARENT_LEVEL = [221, 228, 232]
        child = False
        for l in range(label_level_t.shape[0]):
            if int(label_level_t.iloc[l]) in PARENT_LEVEL:
                label_level.iloc[l] = 0
                child = True
                continue
            elif int(label_level_t.iloc[l]) == 237:
                child = False
            if child == True:
                label_level.iloc[l] = 1
            else:
                label_level.iloc[l] = 0
    
    return INTLINE_t, label, label_level, note, footnote

def INTLINE_MHLW(INTLINE_temp, data_path, country, address, fname, sname, Series, Countries, freq, transpose=True, base_year=0, note=[], footnote=[]):
    ATTRIBUTES = ['種別', '産業分類', '規模', '就業形態']
    if type(INTLINE_temp) != dict and INTLINE_temp.empty == True:
        ERROR('Sheet Not Found: '+data_path+str(country)+'/'+address+fname+', sheet name: '+str(sname))
    if transpose == True:
        INTLINE_temp = INTLINE_temp.T
    INTLINE_t = pd.DataFrame()
    new_code_t = []
    new_label_t = []
    new_unit_t = []
    new_type_t = []
    new_form_c = []
    dataset = str(fname)
    INTLINE_temp.loc[:, ATTRIBUTES] = INTLINE_temp.loc[:, ATTRIBUTES].applymap(lambda x: str(x).strip())
    
    for ind in range(Series[freq].shape[0]):
        sys.stdout.write("\rLoading...("+str(round((ind+1)*100/Series[freq].shape[0], 1))+"%)*")
        sys.stdout.flush()
        if Series[freq].iloc[ind]['DataSet'] == dataset:
            code = re.sub(r'[0_]+|JP[N]*|\.[A-Z]', "", str(Series[freq].index[ind]))
            #if base_year != 0:
            #    code = code+base_year[-2:]
            key = str(Series[freq].iloc[ind]['keyword'])
            IN_t = INTLINE_temp.copy()
            for attr in ATTRIBUTES:
                IN_t = IN_t.loc[IN_t[attr] == str(Series[freq].iloc[ind][attr]).replace('.0','')]
            IN_t = IN_t.sort_values(by=['年','月'], ignore_index=True)
            IN_t.index = [str(IN_t.iloc[date]['年'])+'-'+str(IN_t.iloc[date]['月']).rjust(2,'0') for date in range(IN_t.shape[0])]
            IN_t = IN_t.T
            try:
                pdSeries = IN_t.loc[key]
                if pdSeries.shape[0] == 1:
                    pdSeries = pdSeries.T.squeeze()
                INTLINE_t = pd.concat([INTLINE_t, pd.DataFrame(pdSeries).T])
            except KeyError:
                ERROR('Item not found: '+str(key))
            lab = re.sub(r'(\([SNA0-9]+\)\s*)*(\([0-9]{4}\))*[,\s]*[NSA]*\s+\-\s*', "", str(Series[freq].iloc[ind]['Short Label']).replace(Countries.loc[country, 'Country_Name'],'')).strip(' ,')
            if str(Series[freq].iloc[ind]['Scale']) != 'nan' and str(Series[freq].iloc[ind]['Scale']) != 'Unit':
                unit = str(Series[freq].iloc[ind]['Scale'])+' of '+str(Series[freq].iloc[ind]['Unit'])
            elif str(Series[freq].iloc[ind]['Unit']) == 'Index':
                unit = str(Series[freq].iloc[ind]['Unit'])+': '+base_year+'=100'
            else:
                unit = str(Series[freq].iloc[ind]['Unit'])
            concept = str(Series[freq].iloc[ind]['Concept'])
            if str(Series[freq].iloc[ind]['Seasonal Adjustment']) != 'nan':
                form = str(Series[freq].iloc[ind]['Seasonal Adjustment'])
            else:
                form = 'Not Seasonally Adjusted'
            new_code_t.append(address[:3]+code)
            new_label_t.append(lab)
            new_unit_t.append(unit)
            new_type_t.append(concept)
            new_form_c.append(form)
    sys.stdout.write("\n\n")

    INTLINE_t = INTLINE_t.sort_index(axis=1)
    INTLINE_t.insert(loc=0, column='Index', value=new_code_t)
    INTLINE_t.insert(loc=1, column='Label', value=new_label_t)
    INTLINE_t.insert(loc=2, column='unit', value=new_unit_t)
    INTLINE_t.insert(loc=3, column='type', value=new_type_t)
    INTLINE_t.insert(loc=4, column='form_c', value=new_form_c)
    INTLINE_t = INTLINE_t.set_index('Index', drop=False)
    INTLINE_t = INTLINE_t.loc[:, INTLINE_t.columns.dropna()]
    label = INTLINE_t['Label']
    
    return INTLINE_t, label, note, footnote

def INTLINE_CBFI(chrome, data_path, country, address, fname, sname, freq, update, skip=None, head=None, index_col=None, note=[], footnote=[]):
    file_path = data_path+str(country)+'/'+address+str(sname)+' - Monthly.xlsx'
    IHS = readExcelFile(file_path, header_=0, index_col_=0, sheet_name_=0)
    IHS.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in IHS.columns]
    if freq == 'M' and INTLINE_PRESENT(file_path, discontinued=True) == True:
        label = IHS['Label']
        return IHS, label, note, footnote
    elif freq == 'Q':
        file_path_Q = data_path+str(country)+'/'+address+str(sname)+' - Quarterly.xlsx'
        IHS_Q = readExcelFile(file_path_Q, header_=0, index_col_=0, sheet_name_=0)
        IHS_Q.columns = [pd.Period(col.strftime('%Y-%m'), freq='Q').strftime('%Y-Q%q') if type(col) != str else col for col in IHS_Q.columns]
        if INTLINE_PRESENT(file_path_Q) == True:
            label = IHS_Q['Label']
            return IHS_Q, label, note, footnote
    new_index = []
    
    chrome.get(fname)
    if address.find('CBFI') >= 0:
        chrome.find_element_by_xpath('.//a[contains(., "人民银行对金融机构贷款利率")]').click()
        chrome.switch_to.window(chrome.window_handles[-1])
        target = chrome.find_element_by_xpath('.//table[contains(., "人民银行对金融机构贷款利率")]')
        IN_t = pd.read_html(target.get_attribute('outerHTML'), skiprows=skip, header=head, index_col=index_col)[0]
        chrome.close()
        chrome.switch_to.window(chrome.window_handles[0])
        target_range = False
        for dex in IN_t.index:
            if str(dex).find('人民银行对金融机构贷款利率') >= 0:
                target_range = True
            elif str(dex).find('再贴现') >= 0:
                target_range = False
            if target_range == True:
                new_index.append(dex)
            else:
                new_index.append(None)
        IN_t.index = new_index
        IN_t = IN_t.loc[IN_t.index.dropna()]
        standard = datetime.strptime(IN_t.loc['人民银行对金融机构贷款利率','调整日期'], '%Y.%m.%d').strftime('%Y-%m-%d')
        until = update
    elif address.find('CPF') >= 0:
        target = chrome.find_element_by_xpath('.//h4[text()="Interest Rate for Ordinary Account"]/following-sibling::p').text
        standard = datetime.strptime(re.sub(r'.*?from\s(.+?)\sto.*', r"\1", target), '%d %B %Y').strftime('%Y-%m-%d')
        until = datetime.strptime(re.sub(r'.*?to\s(.+?):.*', r"\1", target), '%d %B %Y').strftime('%Y-%m-%d')
    freqlist = pd.date_range(start=standard, end=until, freq='M').strftime('%Y-%m-%d').tolist()
    freqlist.reverse()
    for ind in IHS.index:
        for day in freqlist:
            if day >= standard:
                if address.find('CBFI') >= 0:
                    IHS.loc[ind, datetime.strptime(day, '%Y-%m-%d').strftime('%Y-%m')] = float(str(IN_t.loc[IHS.loc[ind, 'keyword'], '利率水平']))
                elif address.find('CPF') >= 0:
                    IHS.loc[ind, datetime.strptime(day, '%Y-%m-%d').strftime('%Y-%m')] = float(re.sub(r'.*?:\s(.+?)%.*', r"\1", target))
            else:
                break
    IHS = IHS.sort_index(axis=1)
    IHS.to_excel(data_path+str(country)+'/'+address+str(sname)+' - Monthly.xlsx', sheet_name='Monthly')
    if freq == 'M':
        label = IHS['Label']
        return IHS, label, note, footnote
    elif freq == 'Q':
        new_columns = []
        IHS = IHS.T
        for dex in IHS.index:
            try:
                new_columns.append(pd.Period(dex, freq='Q').strftime('%Y-Q%q'))
            except:
                new_columns.append(None)
        IHS['group'] = new_columns
        IHS = IHS.set_index('group', append=True)
        IHS = IHS.loc[IHS.index.dropna()]
        IHS = IHS.apply(pd.to_numeric).mean(level='group')
        IHS = IHS.T
        for ind in IHS_Q.index:
            for col in IHS.columns:
                IHS_Q.loc[ind, col] = IHS.loc[ind, col]
        IHS_Q = IHS_Q.sort_index(axis=1)
        label = IHS_Q['Label']
        IHS_Q.to_excel(data_path+str(country)+'/'+address+str(sname)+' - Quarterly.xlsx', sheet_name='Quarterly')
        return IHS_Q, label, note, footnote

def INTLINE_GACC(chrome, data_path, country, address, fname, sname, freq, skip=None, head=None, index_col=None):
    file_path = data_path+str(country)+'/'+address+str(sname)+' - Historical.xlsx'
    if str(sname).find('Export') >= 0 and str(sname).find('Import') >= 0:
        IHS = readExcelFile(file_path, header_=0, index_col_=[0,1], sheet_name_=0)
        INTLINE_temp = {'Exports': pd.DataFrame(), 'Imports': pd.DataFrame()}
    else:
        IHS = readExcelFile(file_path, header_=0, index_col_=0, sheet_name_=0)
        INTLINE_temp = pd.DataFrame()
    IHS.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in IHS.columns]
    if INTLINE_PRESENT(file_path) == True:
        return IHS
    start_year = datetime.today().year - 1
    
    chrome.get(fname)
    for yr in range(start_year, datetime.today().year+1):
        Select(chrome.find_element_by_id("monthlysel")).select_by_value(str(yr))
        target = chrome.find_element_by_xpath('.//tr[contains(., "'+str(sname)+'")]')
        month_list_temp = target.find_elements_by_xpath('.//*[@href]')
        for mth in range(len(month_list_temp)):
            target = chrome.find_element_by_xpath('.//tr[contains(., "'+str(sname)+'")]')
            month_list = target.find_elements_by_xpath('.//*[@href]')
            month = month_list[mth].text.strip()
            month_path = data_path+str(country)+'/'+address+'historical data/'+sname+' - '+str(yr)+datetime.strptime(month[:3],'%b').strftime('%m')+'.xls'
            if INTLINE_PRESENT(month_path):
                IN_t = readExcelFile(month_path, skiprows_=skip, header_=head, index_col_=index_col, sheet_name_=0)
            else:
                sys.stdout.write("\rDownloading historical data: "+str(yr)+"-"+datetime.strptime(month[:3],'%b').strftime('%m')+" ")
                sys.stdout.flush()
                month_list[mth].click()
                link_found, link_meassage = INTLINE_WEB_LINK(chrome, fname, keyword='Excel')
                if link_found == False:
                    ERROR(link_meassage)
                time.sleep(5)
                IN_t = INTLINE_WEBDRIVER(chrome, country, address+'historical data/', sname+' - '+str(yr)+datetime.strptime(month[:3],'%b').strftime('%m'), tables=[0], header=head, index_col=index_col, skiprows=skip, csv=False)
                time.sleep(1)
                chrome.back()
                time.sleep(1)
            if str(sname).find('Export') >= 0 and str(sname).find('Import') >= 0:
                IN_t.index = [re.sub(r'[:，]|China|[0-9]+\.*', "", str(dex[1])).replace(', ',',').strip() for dex in IN_t.index]
                if str(IN_t.columns[0][0]).strip() != 'Exports' and str(IN_t.columns[0][0]).strip() != 'Imports' and str(IN_t.columns[0][0]).strip() != 'Total':
                    IN_t = readExcelFile(month_path, skiprows_=skip, header_=head, index_col_=0, sheet_name_=0)
                    if str(IN_t.columns[0][0]).strip() != 'Exports' and str(IN_t.columns[0][0]).strip() != 'Imports' and str(IN_t.columns[0][0]).strip() != 'Total':
                        ERROR('Incorrect Excel Format: '+sname+' - '+str(yr)+datetime.strptime(month[:3],'%b').strftime('%m')+'.xls')
                    IN_t.index = [re.sub(r'[:，]|China|[0-9]+\.*', "", str(dex)).replace(', ',',').strip() for dex in IN_t.index]
                for trade in ['Exports','Imports']:
                    IN = IN_t.copy()
                    new_columns = []
                    for col in IN.columns:
                        if str(col[0]).find('Unnamed') < 0:
                            previous = str(col[0]).strip()
                        if previous == trade and str(col[1]).strip() == str(datetime.strptime(month[:3],'%b').month):
                            new_columns.append(str(yr)+'-'+datetime.strptime(month[:3],'%b').strftime('%m'))
                        else:
                            new_columns.append(None)
                    IN.columns = new_columns
                    IN = IN.loc[:, IN.columns.dropna()]
                    INTLINE_temp[trade] = pd.concat([INTLINE_temp[trade], IN], axis=1)
            else:
                new_columns = []
                for col in IN_t.columns:
                    if str(col[0]).find('Unnamed') < 0:
                        previous = str(col[0]).strip()
                    if previous == str(datetime.strptime(month[:3],'%b').month) and str(col[1]).find('Value') >= 0:
                        new_columns.append(str(yr)+'-'+datetime.strptime(month[:3],'%b').strftime('%m'))
                    else:
                        new_columns.append(None)
                IN_t.columns = new_columns
                IN_t = IN_t.loc[:, IN_t.columns.dropna()]
                IN_t.index = [str(dex[1]).strip() for dex in IN_t.index]
                INTLINE_temp = pd.concat([INTLINE_temp, IN_t], axis=1)
    sys.stdout.write("\n\n")
    sys.stdout.write('\nDownload Complete\n\n')
    
    if str(sname).find('Export') >= 0 and str(sname).find('Import') >= 0:
        for trade in ['Exports','Imports']:
            for ind in IHS.index:
                if ind[0] == trade:
                    if ind[1] not in INTLINE_temp[trade].index:
                        ERROR('Item not found in Downloaded File: '+str(ind[0])+', '+str(ind[1]))
                    for col in INTLINE_temp[trade].columns:
                        if str(sname).find('Section') >= 0:
                            IHS.loc[ind, col] = float(str(INTLINE_temp[trade].loc[ind[1], col]).replace(',',''))/1000
                        else:
                            IHS.loc[ind, col] = float(str(INTLINE_temp[trade].loc[ind[1], col]).replace(',',''))
    else:
        for ind in IHS.index:
            if ind not in INTLINE_temp.index:
                ERROR('Item not found in Downloaded File: '+str(ind))
            for col in INTLINE_temp.columns:
                IHS.loc[ind, col] = float(str(INTLINE_temp.loc[ind, col]).replace(',',''))
    IHS = IHS.sort_index(axis=1)
    IHS.to_excel(data_path+str(country)+'/'+address+str(sname)+' - Historical.xlsx', sheet_name='Monthly')

    return IHS

def INTLINE_KERI(chrome, data_path, country, address, fname, sname, tables, freq, skiprows=None, header=None, index_col=None, note=[], footnote=[]):
    file_path = data_path+str(country)+'/'+address+sname+'.xlsx'
    IHS = readExcelFile(file_path, header_=0, index_col_=0, sheet_name_=0)
    IHS.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in IHS.columns]
    if INTLINE_PRESENT(file_path) == True:
        label = IHS['Label']
        return IHS, label, note, footnote

    for period in pd.date_range(start=datetime.today()-timedelta(days=330), periods=12, freq='M').strftime('%Y-%m'):
        logging.info('Reading file: '+sname+'_'+str(period[2:4]+period[-2:])+'\n')
        INTLINE_t = pd.DataFrame()
        historical_path = data_path+str(country)+'/'+address+'historical_data/'+sname+'_'+str(period[2:4]+period[-2:])+'.xlsx'
        if INTLINE_PRESENT(historical_path):
            INTLINE_temp = readExcelFile(historical_path, skiprows_=skiprows, header_=header, index_col_=index_col, sheet_name_=tables[0])
        else:
            key_period = period
            if key_period[-2:] == '01':
                key_period = str(int(key_period[:4])-1)+key_period[4:]
            INTLINE_temp = INTLINE_WEB(chrome, country, address+'historical_data/', fname, sname+'_'+str(period[2:4]+period[-2:]), freq=freq, tables=tables, header=header, index_col=index_col, skiprows=skiprows, file_name=key_period, specific_sheet=True)
        start = 3
        while True:
            table_found = False
            for i in range(start, INTLINE_temp.shape[0]):
                if str(INTLINE_temp.iloc[i].iloc[0]).find('1. 종합경기 BSI') >= 0:
                    ERROR('Incorrect sheet was gotten.')
                if bool(re.search(r'\([0-9]+\)', str(INTLINE_temp.iloc[i].iloc[0]))):
                    table_found = True
                    table_head = i
                    for j in range(i+3, INTLINE_temp.shape[0]):
                        if str(INTLINE_temp.iloc[j].iloc[0]) == 'nan' or bool(re.search(r'\([0-9]+\)', str(INTLINE_temp.iloc[j].iloc[0]))):
                            table_tail = j-1
                            break
                        elif j == INTLINE_temp.shape[0]-1:
                            table_tail = j
                            break
                    IN = readExcelFile(historical_path, skiprows_=list(range(table_head)), header_=[0,1,2], index_col_=None, sheet_name_=tables[0], nrows_=table_tail-table_head-2)
                    IN = IN.set_index(IN.columns[0], drop=False)
                    INTLINE_t = pd.concat([INTLINE_t, IN], axis=1)
                    start = table_tail+1
                    break
            if table_found == False:
                break
        INTLINE_t.index = [str(dex).replace(' ','').strip() for dex in INTLINE_t.index]
        new_columns = []
        for col in INTLINE_t.columns:
            if False not in [str(c).find('Unnamed') >= 0 for c in col]:
                new_columns.append([None, None, None])
            else:
                if str(col[0]).find('Unnamed') < 0:
                    sector = re.sub(r'\([0-9]+\)', "", str(col[0])).replace(' ','').strip()
                if str(col[1]).find('Unnamed') < 0:
                    isadjusted = str(col[1]).replace(' ','').strip()
                if str(col[2]).find('Unnamed') >= 0:
                    time_value = None
                else:
                    time_value = str(col[2]).replace(' ','').strip()
                    if time_value == '현황':
                        time_value = '실적'
                new_columns.append([sector, isadjusted, time_value])
        INTLINE_t.columns = pd.MultiIndex.from_tuples(new_columns)
        INTLINE_t = INTLINE_t.loc[INTLINE_t.index.dropna(), INTLINE_t.columns.dropna()]
        
        for ihs in range(IHS.shape[0]):
            indexes = re.split(r', ', str(IHS.iloc[ihs]['keyword']))
            ind = indexes[0]
            col = tuple(indexes[1:])
            if col[-1] != '전망':
                IHS.loc[IHS.index[ihs], (datetime.strptime(period, '%Y-%m')-relativedelta(months=1)).strftime('%Y-%m')] = INTLINE_t.loc[ind, col]
            else:
                IHS.loc[IHS.index[ihs], period] = INTLINE_t.loc[ind, col]

    IHS = IHS.sort_index(axis=1)
    label = IHS['Label']
    
    IHS.to_excel(data_path+str(country)+'/'+address+sname+'.xlsx', sheet_name=sname)
    return IHS, label, note, footnote

def INTLINE_KAPSARC(INTLINE_his, INTLINE_temp, data_path, country, address, fname, Series, KEYS, freq, keyword='keyword', file_date='DATE', recursion=False):
    INTLINE_temp = INTLINE_temp.rename(columns={'VALUE':'Value'})
    if fname == 'TCPP' and keyword == 'Energy Source':
        INTLINE_his = INTLINE_KAPSARC(INTLINE_his, INTLINE_temp, data_path, country, address, fname, Series, KEYS, freq, keyword='Indicator', file_date=file_date, recursion=True)
        INTLINE_temp = INTLINE_temp.loc[INTLINE_temp['Indicator']=='Trends in Consumption of Petroleum Products']

    INTLINE_temp = INTLINE_temp.sort_values(by=[file_date], ignore_index=True)
    INTLINE_temp = INTLINE_temp.set_index([keyword, file_date])
    for ind in range(INTLINE_temp.shape[0]):
        key = str(INTLINE_temp.index[ind][0]).replace('&','and').replace('Orissa','Odisha').replace('NCT of Delhi','Delhi').replace('Pondicherry','Puducherry')
        if str(INTLINE_temp.iloc[ind]['Value']) != 'nan' and key in KEYS:
            if fname == 'TPPSCE' and int(str(INTLINE_temp.index[ind][1])[:4]) < datetime.today().year-10:
                continue
            try:
                multiplier = float(Series[freq].loc[(Series[freq]['DataSet']==str(fname)) & (Series[freq]['keyword']==str(key))]['UnitChange'].item())
            except ValueError:
                ERROR('Keyword is not unique: '+str(fname)+', '+str(key))
            INTLINE_his.loc[key, INTLINE_temp.index[ind][1]] = float(INTLINE_temp.iloc[ind]['Value'])*multiplier
    
    return INTLINE_his

def INTLINE_DEUSTATIS(INTLINE_tem, data_path, country, address, fname, sname, Series, Countries, freq, head, index_col, transpose, Table, base_year, INTLINE_previous):
    new_columns = []
    def NA_index(IN_t, fname, freq):
        new_index = []
        if freq == 'Q':
            seasonally = str(IN_t.index.names[0]).replace('+','').strip()+'//'
        else:
            seasonally = ''
        adjusted = ''
        for dex in IN_t.index:
            if str(dex[0]).find('adjusted') >= 0 and str(dex[0]).find('(') < 0:
                seasonally = str(dex[0]).replace('+','').strip()+'//'
                new_index.append(seasonally)
                continue
            elif ((str(dex[0]).find('At current') >= 0 or str(dex[0]).find('adjusted') >= 0) and str(dex[0]).find('(') >= 0) or ((str(fname).find('Employment quarters') >= 0 or str(fname).find('Productivity') >= 0) and str(dex[0]).find('(dom') < 0):
                adjusted = re.sub(r'(.+?)\(.+?\).*', r"\1", str(dex[0])).strip()+'//'
                new_index.append(adjusted)
                continue
            if str(fname).find('Gross value added') >= 0 or str(fname).find('Growth contributions') >= 0 or str(fname).find('Deflators') >= 0:
                new_index.append(seasonally+adjusted+str(dex[1]).replace('=','').replace('- ','').replace('+ ','').strip())
            else:
                new_index.append(seasonally+adjusted+str(dex[0]).replace('=','').replace('- ','').replace('+ ','').strip())
        return new_index
    def FT_index(IN_t, EU=False):
        IN_temp = pd.DataFrame()
        new_columns = []
        yr = ''
        end = False
        for col in IN_t.columns:
            if str(col[0]).strip().isnumeric():
                yr = str(col[0]).strip()
                new_columns.append([None,None])
                continue
            elif str(col[0]).find('_') >= 0:
                end = True
            if end == True:
                new_columns.append([None,None])
            else:
                new_columns.append([yr+'-'+datetime.strptime(str(col[0]).strip(), '%B').strftime('%m'), str(col[1]).strip()])
        IN_t.columns = pd.MultiIndex.from_tuples(new_columns)
        IN_t = IN_t.loc[:, IN_t.columns.dropna()]
        IN_t = IN_t.applymap(lambda x: float(x) if str(x)[0].isnumeric() else np.nan)
        for item in ['Exports: Value','Imports: Value']:
            IN = IN_t.xs(item, level=1, axis=1)
            if EU == True:
                IN = pd.DataFrame(IN.sum(axis=0)).T
                IN.index = ['EU']
            IN.index = [str(dex).strip()+'//'+item for dex in IN.index]
            IN_temp = pd.concat([IN_temp, IN])
        
        return IN_temp
    if INTLINE_previous.empty == False and freq == 'A':
        INTLINE_previous.index = [str(dex).replace('=','').replace('- ','').strip() if str(dex) != 'nan' else None for dex in INTLINE_previous.index]
    if str(fname).find('National accounts - Employment years') >= 0:
        Table = Table.reset_index().set_index('File or Sheet')
        IN_temp = pd.DataFrame()
        for h in range(INTLINE_tem.shape[0]):
            if bool(re.match(r'[0-9]{4}', str(INTLINE_tem.index[h]))):
                table_head = h+1
                for i in range(h+2, INTLINE_tem.shape[0]):
                    if bool(re.match(r'[0-9]{4}', str(INTLINE_tem.index[i]))) or str(INTLINE_tem.index[i]).find('_') >= 0:
                        table_tail = i
                        break
                IN = readExcelFile(data_path+str(country)+'/'+address+fname+'.xlsx', index_col_=index_col, skiprows_=list(range(table_head+int(Table['skip'][fname])+len(head))), nrows_=table_tail-table_head, sheet_name_=sname, names_=INTLINE_tem.columns)
                IN.index = [re.sub(r'(.+?)\(.+?\).*', r"\1", str(dex)).strip() for dex in IN.index]
                IN.columns = [str(col).strip() if str(col).find('Unnamed') < 0 else None for col in IN.columns]
                IN = IN.loc[:, IN.columns.dropna()]
                IN_t = pd.DataFrame(index=pd.MultiIndex.from_product([IN.index, IN.columns]), columns=[INTLINE_tem.index[h]])
                for dex in IN_t.index:
                    IN_t.loc[dex, INTLINE_tem.index[h]] = float(IN.loc[dex[0], dex[1]])
                IN_t.index = [dex[0]+'//'+dex[1] for dex in IN_t.index]
                IN_temp = pd.concat([IN_temp, IN_t], axis=1)
        INTLINE_tem = IN_temp
        INTLINE_tem = INTLINE_tem.sort_index(axis=1)
    elif freq == 'Q' and str(fname).find('Construction price indices') >= 0:
        FREQ = {'February':'1','May':'2','August':'3','November':'4'}
        Table = Table.reset_index().set_index('File or Sheet')
        IN_temp = {}
        year = ''
        for h in range(INTLINE_tem.shape[0]):
            sys.stdout.write("\rModifying the table...("+str(round((h+1)*100/INTLINE_tem.shape[0], 1))+"%)*")
            sys.stdout.flush()
            if bool(re.match(r'[0-9]{4}', str(INTLINE_tem.index[h]))):
                year = str(INTLINE_tem.index[h]).strip()
            elif str(INTLINE_tem.index[h]).strip() not in FREQ:
                if h+2 >= INTLINE_tem.shape[0]:
                    break
                table_head = h+1
                for i in range(h+2, INTLINE_tem.shape[0]):
                    if str(INTLINE_tem.index[i]).strip() not in FREQ or str(INTLINE_tem.index[i]).find('_') >= 0:
                        table_tail = i
                        break
                IN = readExcelFile(data_path+str(country)+'/'+address+fname+'.xlsx', index_col_=index_col, skiprows_=list(range(table_head+int(Table['skip'][fname])+len(head))), nrows_=table_tail-table_head, sheet_name_=sname, names_=INTLINE_tem.columns)
                IN.index = [year+'-Q'+FREQ[str(dex).strip()] if str(dex).strip() in FREQ else None for dex in IN.index]
                IN.columns = [str(INTLINE_tem.index[h]).strip()+'//'+str(col).strip() if str(col).find('Unnamed') < 0 else None for col in IN.columns]
                IN = IN.loc[IN.index.dropna(), IN.columns.dropna()].T
                if str(INTLINE_tem.index[h]).strip() not in IN_temp:
                    IN_temp[str(INTLINE_tem.index[h]).strip()] = IN
                else:
                    IN_temp[str(INTLINE_tem.index[h]).strip()] = pd.concat([IN_temp[str(INTLINE_tem.index[h]).strip()], IN], axis=1)
        sys.stdout.write("\n\n")
        INTLINE_tem = pd.concat([IN_temp[keys] for keys in IN_temp])
        INTLINE_tem = INTLINE_tem.sort_index(axis=1)
    elif str(fname).find('Volume index of stock of orders') >= 0:
        file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
        INTLINE_his.columns = [pd.Period(col, freq='Q').strftime('%Y-Q%q') if type(col) != str else col for col in INTLINE_his.columns]
        base_path = data_path+str(country)+'/'+address+'base_year.csv'
        base_year_list = readFile(base_path, header_=[0], index_col_=0, acceptNoFile=False)
        if str(base_year_list.loc[fname, 'base year']) != str(base_year):
            print('Modifying Data with new base year')
            for ind in INTLINE_his.index:
                new_base = sum([INTLINE_his.loc[ind, str(base_year)+'-Q'+str(num)] for num in [1,2,3,4]])/4
                multiplier = 100/new_base
                for col in INTLINE_his.columns:
                    INTLINE_his.loc[ind, col] = float(INTLINE_his.loc[ind, col])*multiplier
            base_year_list.loc[fname, 'base year'] = base_year
            base_year_list.to_csv(base_path)
        INTLINE_tem.columns = [str(col[0]).strip()+'-Q'+str(roman.fromRoman(str(col[1]).strip())) if str(col[0]).strip().isnumeric() else None for col in INTLINE_tem.columns]
        INTLINE_tem.index = [str(dex[0]).strip()+'//'+str(dex[1]).strip() if str(dex[0]).strip()+'//'+str(dex[1]).strip() in KEYS else None for dex in INTLINE_tem.index]
        INTLINE_tem = INTLINE_tem.loc[INTLINE_tem.index.dropna(), INTLINE_tem.columns.dropna()]
        INTLINE_tem = pd.concat([INTLINE_tem, INTLINE_his], axis=1)
        INTLINE_tem = INTLINE_tem.loc[:, ~INTLINE_tem.columns.duplicated()]
        INTLINE_tem = INTLINE_tem.sort_index(axis=1)
        INTLINE_tem.to_excel(file_path, sheet_name=fname)
    elif str(fname).find('Foreign trade') >= 0 or str(fname).find('Local units') >= 0:
        file_path = data_path+str(country)+'/'+address+str(fname)+'_historical.xlsx'
        INTLINE_his = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0)
        KEYS = list(Series[freq].loc[Series[freq]['DataSet']==str(fname)]['keyword'])
        INTLINE_his.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in INTLINE_his.columns]
        if str(fname).find('Index') >= 0:
            base_path = data_path+str(country)+'/'+address+'base_year.csv'
            base_year_list = readFile(base_path, header_=[0], index_col_=0, acceptNoFile=False)
            if str(base_year_list.loc[fname, 'base year']) != str(base_year):
                print('Modifying Data with new base year')
                for ind in INTLINE_his.index:
                    new_base = sum([INTLINE_his.loc[ind, str(base_year)+'-'+str(num).rjust(2,'0')] for num in range(1,13)])/12
                    multiplier = 100/new_base
                    for col in INTLINE_his.columns:
                        INTLINE_his.loc[ind, col] = float(INTLINE_his.loc[ind, col])*multiplier
                base_year_list.loc[fname, 'base year'] = base_year
                base_year_list.to_csv(base_path)
            INTLINE_tem.columns = [str(col[0]).strip()+'-'+datetime.strptime(str(col[1]).strip(), '%b').strftime('%m') if str(col[0]).strip().isnumeric() else None for col in INTLINE_tem.columns]
            INTLINE_tem.index = [str(dex[0]).strip()+'//'+str(dex[1]).strip() if str(dex[0]).strip()+'//'+str(dex[1]).strip() in KEYS else None for dex in INTLINE_tem.index]
        elif str(fname).find('total') >= 0:
            year = ''
            for col in INTLINE_tem.columns:
                if str(col[0]).strip()[:4].isnumeric():
                    year = str(col[0]).strip()[:4]
                if str(col[1]).find('Unnamed') >= 0:
                    new_columns.append(None)
                else:
                    new_columns.append(year+'-'+datetime.strptime(str(col[1]).strip(), '%B').strftime('%m'))
            INTLINE_tem.columns = new_columns
            INTLINE_tem.index = ['Total//'+str(dex).strip() if 'Total//'+str(dex).strip() in KEYS else None for dex in INTLINE_tem.index]
            INTLINE_tem = INTLINE_tem.loc[INTLINE_tem.index.dropna(), INTLINE_tem.columns.dropna()]
            INTLINE_tem = INTLINE_tem.applymap(lambda x: float(x) if str(x)[0].isnumeric() else np.nan)
            INTLINE_prev = FT_index(INTLINE_previous, EU=True)
            for item in ['Exports: Value','Imports: Value']:
                IN_sub = INTLINE_tem.loc['Total//'+item].sub(INTLINE_prev.loc['EU//'+item])
                IN_sub.name = 'Non-EU//'+item
                INTLINE_tem = pd.concat([INTLINE_tem, pd.DataFrame(IN_sub).T])
            INTLINE_tem = INTLINE_tem.applymap(lambda x: float(x)/1000 if str(x)[0].isnumeric() else np.nan)
        elif str(fname).find('Local units') >= 0:
            year = ''
            for col in INTLINE_tem.columns:
                if str(col[0]).strip()[:4].isnumeric():
                    year = str(col[0]).strip()[:4]
                if str(col[1]).find('Unnamed') >= 0:
                    new_columns.append(None)
                else:
                    new_columns.append(year+'-'+datetime.strptime(str(col[1]).strip(), '%B').strftime('%m'))
            INTLINE_tem.columns = new_columns
            subject = str(INTLINE_tem.index.name).strip()+'//'
            new_index = []
            end = False
            for dex in INTLINE_tem.index:
                if str(dex).find('WZ') >= 0:
                    subject = str(dex).replace('+','').strip()+'//'
                    new_index.append(subject)
                    continue
                elif str(dex).find('_') >= 0:
                    end = True
                if end == True:
                    new_index.append(None)
                else:
                    new_index.append(subject+str(dex).replace('+','').strip())
            INTLINE_tem.index = new_index
        else:
            INTLINE_tem = FT_index(INTLINE_tem)
            INTLINE_tem.index = [dex if dex in KEYS else None for dex in INTLINE_tem.index]
            INTLINE_tem = INTLINE_tem.loc[INTLINE_tem.index.dropna()]
            INTLINE_tem = INTLINE_tem.applymap(lambda x: float(x)/1000 if str(x)[0].isnumeric() else np.nan)
        INTLINE_tem = INTLINE_tem.loc[INTLINE_tem.index.dropna(), INTLINE_tem.columns.dropna()]
        INTLINE_tem = pd.concat([INTLINE_tem, INTLINE_his], axis=1)
        INTLINE_tem = INTLINE_tem.loc[:, ~INTLINE_tem.columns.duplicated()]
        INTLINE_tem = INTLINE_tem.sort_index(axis=1)
        INTLINE_tem.to_excel(file_path, sheet_name=fname)
    elif freq == 'A':
        INTLINE_tem.columns = [str(col).strip()[:4] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_tem.columns]
        if INTLINE_previous.empty == False:
            INTLINE_previous.columns = [str(col).strip()[:4] if str(col).strip()[:4].isnumeric() else None for col in INTLINE_previous.columns]
            INTLINE_previous = INTLINE_previous.loc[INTLINE_previous.index.dropna(), INTLINE_previous.columns.dropna()]
    elif freq == 'Q':
        year = ''
        for col in INTLINE_tem.columns:
            if str(col[0]).strip()[:4].isnumeric():
                year = str(col[0]).strip()[:4]
            if str(col[1]).find('Unnamed') >= 0:
                new_columns.append(None)
            else:
                new_columns.append(year+'-Q'+str(col[1]).strip()[-1])
        INTLINE_tem.columns = new_columns
        if INTLINE_previous.empty == False:
            INTLINE_previous.columns = new_columns
            INTLINE_previous = INTLINE_previous.loc[:, INTLINE_previous.columns.dropna()]
    elif freq == 'M':
        year = ''
        for col in INTLINE_tem.columns:
            if isinstance(INTLINE_tem.columns, pd.MultiIndex) and str(col[0]).strip()[:4].isnumeric():
                year = str(col[0]).strip()[:4]
            elif isinstance(INTLINE_tem.columns, pd.Index) and str(col).strip()[:4].isnumeric():
                year = str(col).strip()[:4]
                new_columns.append(None)
                continue
            if isinstance(INTLINE_tem.columns, pd.MultiIndex) and (str(col[0]).find('__') >= 0 or str(col[1]).find('Unnamed') >= 0 or str(col[1]).find('nan') >= 0):
                new_columns.append(None)
            elif isinstance(INTLINE_tem.columns, pd.MultiIndex):
                new_columns.append(year+'-'+datetime.strptime(str(col[1]).strip(), '%B').strftime('%m'))
            else:
                try:
                    new_columns.append(year+'-'+datetime.strptime(str(col).strip(), '%B').strftime('%m'))
                except ValueError:
                    new_columns.append(None)
        INTLINE_tem.columns = new_columns
    if index_col != 0 and index_col != 1 and str(fname).find('National accounts') >= 0:
        INTLINE_tem.index = NA_index(INTLINE_tem, fname, freq)
        if INTLINE_previous.index.nlevels > 1:
            INTLINE_previous.index = NA_index(INTLINE_previous, fname, freq)
        if str(fname).find('National accounts - GDP') >= 0:
            if freq == 'Q':
                seasonally = 'Unadjusted values//'
            else:
                seasonally = ''
            INTLINE_tem = INTLINE_tem.applymap(lambda x: float(x) if str(x)[0].isnumeric() else np.nan)
            INTLINE_previous = INTLINE_previous.applymap(lambda x: float(x) if str(x)[0].isnumeric() else np.nan)
            IN_div = INTLINE_tem.loc[seasonally+'At current prices//Gross domestic product'].div(INTLINE_tem.loc[seasonally+'At current prices//Mem. item: Gross domestic product per inhabitant'])
            IN_temp = INTLINE_previous.loc[seasonally+'Gross national income'].div(IN_div)
            IN_temp.name = seasonally+'At current prices//Gross national income per inhabitant'
            INTLINE_tem = pd.concat([INTLINE_tem, pd.DataFrame(IN_temp).T])
    elif index_col != 0 and index_col != 1 and transpose == False:
        if str(fname).find('Construction work completed') >= 0:
            keywords = ['residential']
        elif str(fname).find('Indices of labour costs') >= 0 or str(fname).find('main construction industry') >= 0:
            keywords = ['adjusted','trend']
        elif str(fname).find('manufacturing') >= 0:
            keywords = ['mining','manuf','goods','energy','product']
        elif str(fname).find('Turnover') >= 0:
            keywords = ['wz']
        elif str(fname).find('Unemployment as a percentage of the civilian labour force') >= 0:
            keywords = ['total','federal','new']
        elif str(fname).find('First registrations and changes in ownership') >= 0:
            keywords = ['motor','car','truck','tractor']
        if str(fname).find('New orders in manufacturing') >= 0:
            keywords2 = ['adjusted','trend']
        elif str(fname).find('main construction industry') >= 0:
            keywords2 = ['construction','engineering']
        elif str(fname).find('Turnover') >= 0:
            keywords2 = ['price']
        else:
            keywords2 = []
        new_index = []
        subject = str(INTLINE_tem.index.names[0]).strip()+'//'
        subject2 = ''
        end = False
        for dex in INTLINE_tem.index:
            if True in [str(dex[0]).lower().find(key) >= 0 for key in keywords]:
                subject = str(dex[0]).replace('+','').strip()+'//'
                if str(fname).find('First registrations and changes in ownership') < 0:
                    new_index.append(subject)
                    continue
            elif True in [str(dex[0]).lower().find(key) >= 0 for key in keywords2]:
                subject2 = str(dex[0]).replace('+','').strip()+'//'
            elif str(dex[0]).find('_') >= 0:
                end = True
            if end == True:
                new_index.append(None)
            elif str(fname).find('Persons employed and turnover') >= 0:
                new_index.append(subject+subject2+str(dex[0]).replace('+','').strip())
            else:
                new_index.append(subject+subject2+str(dex[1]).replace('+','').strip())
        INTLINE_tem.index = new_index
    elif index_col == 0 and head == [0,2] and str(fname).find('Foreign trade') < 0 and str(fname).find('Local units') < 0:
        subject = str(INTLINE_tem.index.name).strip()+'//'
        if str(fname).find('Indices of agreed earnings') >= 0 or str(fname).find('Persons employed') >= 0 or str(fname).find('WZ') >= 0:
            keywords = ['WZ']
        elif str(fname).find('Index of production in manufacturing') >= 0:
            keywords = ['Industry','Mining','Manuf','goods','Energy','Electricity','Construction','industry','engineering','Building','product']
        else:
            keywords = []
            subject = ''
        new_index = []
        end = False
        for dex in INTLINE_tem.index:
            if True in [str(dex).find(key) >= 0 for key in keywords]:
                subject = str(dex).replace('+','').strip()+'//'
                new_index.append(subject)
                continue
            elif str(dex).find('_') >= 0:
                end = True
            if end == True:
                new_index.append(None)
            else:
                new_index.append(subject+str(dex).replace('+','').strip())
        INTLINE_tem.index = new_index
    else:
        INTLINE_tem.index = [str(dex).replace('=','').replace('- ','').replace('+ ','').strip() if str(dex) != 'nan' else None for dex in INTLINE_tem.index]
    INTLINE_tem = INTLINE_tem.loc[INTLINE_tem.index.dropna(), INTLINE_tem.columns.dropna()]
    INTLINE_tem = INTLINE_tem.loc[~INTLINE_tem.index.duplicated()]

    return INTLINE_tem
#INTLINE_keywords(INTLINE_temp, data_path, country, address, fname, freq, data_key='Index of Export Price', data_year=2015, multiplier=1, check_long_label=False)
def INTLINE_keywords(INTLINE_temp, data_path, country, address, fname, freq, data_key, data_year, multiplier=1, check_long_label=False, allow_duplicates=True, multiple=True):
    FREQ = {'A':'Annual','Q':'Quarterly','M':'Monthly'}
    print(INTLINE_temp)
    index_selected = [False for dex in INTLINE_temp.index]
    index_duplicated = []
    file_path = data_path+str(country)+'/IHS'+str(country)+'.xlsx'
    IHS = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=FREQ[freq])
    if freq == 'A':
        time_range = [str(yr) for yr in range(int(data_year), int(data_year)+11)]
        IHS.columns = [str(col).strip()[:4] if str(col).strip()[:4].isnumeric() else col for col in IHS.columns]
    elif freq == 'Q':
        time_range = [str(yr)+'-Q'+str(k) for yr in range(int(data_year), int(data_year)+3) for k in range(1,5)]
        IHS.columns = [pd.Period(col, freq='Q').strftime('%Y-Q%q') if type(col) != str else col for col in IHS.columns]
    elif freq == 'M':
        time_range = [str(data_year)+'-'+str(k).rjust(2, '0') for k in range(1,13)]
        IHS.columns = [col.strftime('%Y-%m') if type(col) != str else col for col in IHS.columns]
    INDEX_path = data_path+str(country)+'/'+address+'INDEX.xlsx'
    if os.path.isfile(INDEX_path):
        INDEX = readExcelFile(INDEX_path, header_=[0], index_col_=0, sheet_name_=0)
        INDEX_temp = pd.DataFrame(index=IHS.index, columns=['DataSet','keyword'])
        INDEX = pd.concat([INDEX, INDEX_temp])
        INDEX = INDEX.loc[~INDEX.index.duplicated()]
    else:
        INDEX = pd.DataFrame(index=IHS.index, columns=['DataSet','keyword'])
    round_num = 3
    for ind in range(IHS.shape[0]):
        sys.stdout.write("\rFinding keyword...("+str(round((ind+1)*100/IHS.shape[0], 1))+"%)*")
        sys.stdout.flush()
        if str(IHS.index[ind]).find(data_key) >= 0:
            found = False
            target = [round(float(i),round_num) if str(i).replace('.','',1).replace(',','',1).replace('-','',1).isdigit() else i for i in list(IHS.iloc[ind][time_range])]
            for dex in range(INTLINE_temp.shape[0]):
                key_label = INTLINE_temp.index[dex][:7]
                try:
                    data = [round(float(i)*multiplier,round_num) if str(i).replace('.','',1).replace(',','',1).replace('-','',1).isdigit() else i for i in list(INTLINE_temp.iloc[dex][time_range])]
                except KeyError:
                    ERROR('Incorrect year was given: '+str(data_year))
                if data == target and allow_duplicates == False and index_selected[dex] == True:
                    continue
                elif data == target:
                    if (check_long_label and str(IHS.loc[IHS.index[ind], 'Long Label']).find(str(key_label)) >= 0) or check_long_label == False:
                        INDEX.loc[IHS.index[ind], 'DataSet'] = str(fname)
                        INDEX.loc[IHS.index[ind], 'keyword'] = INTLINE_temp.index[dex]
                        if index_selected[dex] == True:
                            index_duplicated.append(INTLINE_temp.index[dex])
                        index_selected[dex] = True
                        found = True
                        break
                    elif check_long_label:
                        print(str(IHS.loc[IHS.index[ind], 'Long Label']))
                        print(str(INTLINE_temp.index[dex]))
                """elif check_long_label and str(IHS.loc[IHS.index[ind], 'Long Label']).find(str(INTLINE_temp.index[dex])) >= 0:
                    INDEX.loc[IHS.index[ind], 'DataSet'] = str(fname)
                    INDEX.loc[IHS.index[ind], 'keyword'] = INTLINE_temp.index[dex]
                    index_selected[dex] = True
                    found = True
                    break"""
        else:
            try:
                INDEX = INDEX.drop([IHS.index[ind]])
            except KeyError:
                time.sleep(0)
    sys.stdout.write("\n\n")
    print(INDEX)
    if not not index_duplicated:
        print('Duplicates:', index_duplicated)
    INDEX.to_excel(INDEX_path, sheet_name=fname[:30])
    if multiple == False:
        ERROR('')

def INTLINE_combine(data_path, country, address, INDEX_list):
    INDEX = readExcelFile(data_path+str(country)+'/'+address+'INDEX'+INDEX_list[0]+'.xlsx', header_=[0], index_col_=0, sheet_name_=0)
    IN_t = pd.DataFrame(index=INDEX.index, columns=INDEX.columns)
    IN_t = IN_t.reset_index()
    for i in INDEX_list:
        file_path = data_path+str(country)+'/'+address+'INDEX'+i+'.xlsx'
        INDEX_t = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=0).reset_index()
        for dex in IN_t.index:
            if str(IN_t.loc[dex, 'DataSet']) == 'nan' and str(INDEX_t.loc[dex, 'DataSet']) != 'nan':
                IN_t.loc[dex, 'DataSet'] = INDEX_t.loc[dex, 'DataSet']
                IN_t.loc[dex, 'keyword'] = INDEX_t.loc[dex, 'keyword']
    IN_t = IN_t.set_index('Short Label')
    print(IN_t)
    IN_t.to_excel(data_path+str(country)+'/'+address+'INDEX_final.xlsx', sheet_name='final')
    ERROR('')

#INTLINE_combine(data_path, country=142, address='STANOR/', INDEX_list=[str(i) for i in range(1,6)]+[])
