# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=E1101
# pylint: disable=unbalanced-tuple-unpacking
import math, re, sys, calendar, os, copy, time, shutil
import pandas as pd
import numpy as np
import requests as rq
from datetime import datetime, date
from urllib.error import HTTPError
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
import webdriver_manager
from webdriver_manager.chrome import ChromeDriverManager

NAME = 'FOREX_'
databank = 'FOREX'
base_year = ['1999','2010','2015']
ENCODING = 'utf-8-sig'
data_path = "./data/"
out_path = "./output/"
DB_TABLE = 'DB_'
DB_CODE = 'data'
START_YEAR = input('Output file suffix (If test identity press 0): ')
find_unknown = False
main_suf = '?'
merge_suf = '?'
dealing_start_year = 1999
start_year = 1999
if START_YEAR != '0':
    merging = bool(int(input('Merging data file (1/0): ')))
    updating = bool(int(input('Updating TOT file (1/0): ')))
    if merging and updating:
        ERROR('Cannot do merging and updating at the same time.')
    elif merging or updating:
        merge_suf = input('Be Merged(Original) data suffix: ')
        main_suf = input('Main(Updated) data suffix: ')
    else:
        find_unknown = bool(int(input('Check if new items exist (1/0): ')))
        if find_unknown == False:
            dealing_start_year = int(input("Dealing with data from year: "))
            start_year = dealing_start_year-5
update = datetime.today()
tStart = time.time()

this_year = datetime.now().year + 1
FREQNAME = {'A':'annual','M':'month','Q':'quarter','S':'semiannual','W':'week'}
FREQLIST = {}
FREQLIST['A'] = [tmp for tmp in range(start_year,this_year)]
FREQLIST['S'] = []
for y in range(start_year,this_year):
    for s in range(1,3):
        FREQLIST['S'].append(str(y)+'-S'+str(s))
#print(FREQLIST['S'])
FREQLIST['Q'] = []
for q in range(start_year,this_year):
    for r in range(1,5):
        FREQLIST['Q'].append(str(q)+'-Q'+str(r))
#print(FREQLIST['Q'])
FREQLIST['M'] = []
for y in range(start_year,this_year):
    for m in range(1,13):
        FREQLIST['M'].append(str(y)+'-'+str(m).rjust(2,'0'))
#print(FREQLIST['M'])
calendar.setfirstweekday(calendar.SATURDAY)
FREQLIST['W'] = pd.date_range(start = str(start_year)+'-01-01',end=update,freq='W-SAT')
FREQLIST['W_s'] = pd.date_range(start = str(start_year)+'-01-01',end=update,freq='W-SAT').strftime('%Y-%m-%d')

def ERROR(error_text, waiting=False):
    if waiting == True:
        sys.stdout.write("\r"+error_text)
        sys.stdout.flush()
    else:
        print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()

def readFile(dir, default=pd.DataFrame(), acceptNoFile=False,header_=None,names_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,nrows_=None,encoding_=ENCODING,engine_='python',sep_=None, wait=False):
    try:
        t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        names=names_,usecols=usecols_,nrows=nrows_,encoding=encoding_,engine=engine_,sep=sep_)
        #print(t)
        return t
    except FileNotFoundError:
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
    except:
        try: #檔案編碼格式不同
            t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        names=names_,usecols=usecols_,nrows=nrows_,engine=engine_,sep=sep_)
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了

def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, na_filter_=True, \
             header_=None,names_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,nrows_=None,sheet_name_=None, wait=False):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,names=names_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_,nrows=nrows_,na_filter=na_filter_)
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            if wait == True:
                ERROR('Waiting for Download...', waiting=True)
            else:
                ERROR('找不到檔案：'+dir)
    except:
        try: #檔案編碼格式不同
            t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,names=names_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_,nrows=nrows_,na_filter=na_filter_)
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

Base = readExcelFile(data_path+'base_year.xlsx', header_ = [0],index_col_=0)
Country = readFile(data_path+'Country.csv', header_ = 0)
ECB = Country.set_index('Currency_Code').to_dict()
IMF = Country.set_index('IMF_country').to_dict()
CRC = Country.set_index('Country_Code').to_dict()
OLC = Country.set_index('Country_Code').to_dict()
CCOFER = Country.set_index('Country_Name').to_dict()
def COUNTRY(code):
    if code in ECB['Country_Code']:
        return str(ECB['Country_Code'][code])
    elif code in IMF['Country_Code']:
        return str(IMF['Country_Code'][code])
    elif code in CCOFER['Country_Code']:
        return str(CCOFER['Country_Code'][code])
    elif code in CRC['Country_Name']:
        return str(code)
    else:
        ERROR('國家代碼錯誤: '+code)
def CURRENCY(code):
    if code in ECB['Currency_Name']:
        return str(ECB['Currency_Name'][code])
    elif code in IMF['Currency_Name']:
        return str(IMF['Currency_Name'][code])
    elif code in CRC['Currency_Name']:
        return str(CRC['Currency_Name'][code])
    else:
        ERROR('貨幣代碼錯誤: '+code)
      
def OLD_LEGACY(code):
    if code in OLC['Old_legacy_currency']:
        return str(OLC['Old_legacy_currency'][code])
    else:
        return code

def LOCKING(code):
    if code in OLC['locking rate']:
        return float(OLC['locking rate'][code])
    else:
        ERROR('LOCKING國家代碼錯誤: '+code)

def INDEXBASE(nominal_year, code, index_item, NonValue):
    try:
        BaseEX = Base[nominal_year+'=100'].loc[str(OLC['IMF_country'][code]), nominal_year]
        return float(BaseEX)
    except KeyError:
        try:
            BaseEX = Base[nominal_year+'=100'].loc[str(OLC['Country_Name'][code]), nominal_year]
            return float(BaseEX)
        except KeyError:
            BaseEX = Base[nominal_year+'=100'].loc[index_item, nominal_year]
            return float(BaseEX)
        except ValueError:
            return NonValue
        except:
            ERROR('INDEXBASE國家代碼錯誤: '+code)
    except ValueError:
        return NonValue
    #except:
    #    ERROR('INDEXBASE國家代碼錯誤2: '+code)

CONTINUE = []
REPLICATED = []

before1 = ['FOREIGN EXCHANGE',') PER','DATA)',')FROM','SOURCE','NOTE','RATESDR','RATESEMI','RATEEND','RATES','MARKET RATE','OFFICIAL RATE','PRINCIPAL RATE','USING','ONWARDD','WEDOLLAR','ESOFFICIAL','MILLIONS','NSAINTERNATIONAL','aA','aE','ReservesClaims','DollarsUnit','DollarSource','www.imf.org','FUNDCURRENCY','DATAU.S.','ORLUXEMBOURG','EMUEURO','Y DATA',' AS','HOUSEHOLDSCANNOT','NACIONALWHICH','WITH ',"#IES",'#']
after1 = [' FOREIGN EXCHANGE ',') PER ','DATA): ',') FROM',', SOURCE',', NOTE','RATE SDR','RATE SEMI','RATE END','RATES ','MARKET RATE ','OFFICIAL RATE ','PRINCIPAL RATE ','USING ','ONWARD D','WE DOLLAR','ES OFFICIAL',' MILLIONS','NSA INTERNATIONAL','a A','a E','Reserves, Claims','Dollars; Unit','Dollar; Source','','FUND CURRENCY','DATA U.S.','OR LUXEMBOURG','EMU EURO','Y DATA ',' AS ','HOUSEHOLDS CANNOT','NACIONAL WHICH',' WITH ','IES',' ']
before2 = ['Ecb','1 Ecu','Sdr','Ifs','Ihs','Imf','Iso','Exchange S ','Rate S ','Am','Pm','Of ',"People S","People'S",'Usd','Us ','Name?eekly','Name?','Cfa','Cfp','Fx','Rate,,','Rate,','Nsa','Cofer','And ', 'In ',')Total','Or ','Luf','Emu ','Rexa','Rexeurd','Rexe','Rexeure','Rexi','Rexeuri','Subsidizedby']
after2 = ['ECB','1 ECU','SDR','IFS','IHS','IMF','ISO','Exchanges ','Rates ','am','pm','of ',"People's","People's",'USD','US ','weekly','','CFA','CFP','Foreign Exchange','Rate,','Rate.','NSA','COFER','and ','in ','): Total','or ','LUF','EMU ','REXA','REXEURD','REXE','REXEURE','REXI','REXEURI','Subsidized by']
before3 = ['CYPrus','EURo']
after3 = ['Cyprus','Euro']

def FOREX_WEBDRIVER(chrome, suffix, header=None, index_col=None, skiprows=None, usecols=None, names=None, csv=True):

    chrome.execute_script("window.open()")
    chrome.switch_to.window(chrome.window_handles[-1])
    chrome.get('chrome://downloads')
    time.sleep(5)
    excel_file = chrome.execute_script("return document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('div#content  #file-link').text")
    new_file_name = 'GERFIN'+str(suffix)+re.sub(r'.+?(\..+)$', r"\1", excel_file)
    chrome.close()
    chrome.switch_to.window(chrome.window_handles[0])
    while True:
        try:            
            if csv == True:
                FOREX_t = readFile((Path.home() / "Downloads" / excel_file).as_posix(), header_=header, index_col_=index_col, skiprows_=skiprows, acceptNoFile=False, usecols_=usecols, names_=names, wait=True)
            else:
                FOREX_t = readExcelFile((Path.home() / "Downloads" / excel_file).as_posix(), header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=0, acceptNoFile=False, usecols_=usecols, names_=names, wait=True)
        except:
            time.sleep(1)
        else:
            if os.path.isfile((Path.home() / "Downloads" / new_file_name)) and excel_file != new_file_name:
                os.remove((Path.home() / "Downloads" / new_file_name))
            os.rename((Path.home() / "Downloads" / excel_file), (Path.home() / "Downloads" / new_file_name))
            if os.path.isfile(data_path+new_file_name):
                if os.path.isfile(data_path+'old/'+new_file_name):
                    os.remove(data_path+'old/'+new_file_name)
                shutil.move(data_path+new_file_name, data_path+'old/'+new_file_name)
            shutil.move((Path.home() / "Downloads" / new_file_name), data_path+new_file_name)
            break

    return FOREX_t

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
    db_table_t[db_code] = DATA_BASE[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
    df_key.loc[f, 'db_table'] = db_table
    df_key.loc[f, 'db_code'] = db_code
    start_code += 1
    db_table_new = db_table
    db_code_new = db_code
    
    return df_key, DATA_BASE_new, DB_name_new, db_table_t, start_table, start_code, db_table_new, db_code_new

def CONCATE(NAME, suf, data_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, KEY_DATA_t, DB_dict, DB_name_dict, find_unknown=True):
    if find_unknown == True:
        repeated_standard = 'start'
    else:
        repeated_standard = 'last'
    #print('Reading file: '+NAME+'key'+suf+', Time: ', int(time.time() - tStart),'s'+'\n')
    #KEY_DATA_t = readExcelFile(data_path+NAME+'key'+suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
    print('Reading file: '+NAME+'database'+suf+', Time: ', int(time.time() - tStart),'s'+'\n')
    DATA_BASE_t = readExcelFile(data_path+NAME+'database'+suf+'.xlsx', header_ = 0, index_col_=0)
    if type(DATA_BASE_t) != dict:
        DATA_BASE_t = {}
    
    print('Concating file: '+NAME+'key'+suf+', Time: ', int(time.time() - tStart),'s'+'\n')
    KEY_DATA_t = pd.concat([KEY_DATA_t, df_key], ignore_index=True)
    
    print('Concating file: '+NAME+'database'+suf+', Time: ', int(time.time() - tStart),'s'+'\n')
    for f in FREQNAME:
        for d in DB_name_dict[f]:
            sys.stdout.write("\rConcating sheet: "+str(d))
            sys.stdout.flush()
            if d in DATA_BASE_t.keys():
                DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_dict[f][d])
            else:
                DATA_BASE_t[d] = DB_dict[f][d]
        sys.stdout.write("\n")

    print('Time: ', int(time.time() - tStart),'s'+'\n')
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
        sys.stdout.write("\r"+str(repeated)+" repeated data key(s) found")
        sys.stdout.flush()
    sys.stdout.write("\n")
    for target in repeated_index:
        sys.stdout.write("\rDropping repeated database column(s)...("+str(round((repeated_index.index(target)+1)*100/len(repeated_index), 1))+"%)*")
        sys.stdout.flush()
        try:
            DATA_BASE_t[KEY_DATA_t.iloc[target]['db_table']] = DATA_BASE_t[KEY_DATA_t.iloc[target]['db_table']].drop(columns = KEY_DATA_t.iloc[target]['db_code'])
        except:
            continue
    sys.stdout.write("\n")
    KEY_DATA_t = KEY_DATA_t.drop(repeated_index)
    KEY_DATA_t.reset_index(drop=True, inplace=True)
    #print(KEY_DATA_t)
    print('Time: ', int(time.time() - tStart),'s'+'\n')
    for s in range(KEY_DATA_t.shape[0]):
        sys.stdout.write("\rSetting new snls: "+str(s+1))
        sys.stdout.flush()
        KEY_DATA_t.loc[s, 'snl'] = s+1
    sys.stdout.write("\n")
    print('Setting new files, Time: ', int(time.time() - tStart),'s'+'\n')
    
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
            DB_new_dict[f][DB_TABLE+f+'_'+str(start_table_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
            DB_name_new_dict[f].append(DB_TABLE+f+'_'+str(start_table_dict[f]).rjust(4,'0'))
        if not not DB_name_new_dict[f]:
            DB_dict[f] = DB_new_dict[f]
            DB_name_dict[f] = DB_name_new_dict[f]

    print('Concating new files: '+NAME+'database, Time: ', int(time.time() - tStart),'s'+'\n')
    DATA_BASE_dict = {}
    for f in FREQNAME:
        if not DB_name_dict[f]:
            DATA_BASE_dict[f] = DB_dict[f]
            continue
        DATA_BASE_dict[f] = {}
        for d in DB_name_dict[f]:
            sys.stdout.write("\rConcating sheet: "+str(d))
            sys.stdout.flush()
            DATA_BASE_dict[f][d] = DB_dict[f][d]
        sys.stdout.write("\n")
    
    #print(KEY_DATA_t)
    print('Time: ', int(time.time() - tStart),'s'+'\n')

    return KEY_DATA_t, DATA_BASE_dict

def UPDATE(original_file, updated_file, key_list, NAME, data_path, orig_suf, up_suf):
    updated = 0
    tStart = time.time()
    print('Updating file: ', int(time.time() - tStart),'s'+'\n')
    print('Reading original database: '+NAME+'database'+orig_suf+', Time: ', int(time.time() - tStart),'s'+'\n')
    original_database = readExcelFile(data_path+NAME+'database'+orig_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
    print('Reading updated database: '+NAME+'database'+up_suf+'.xlsx, Time: ', int(time.time() - tStart),'s'+'\n')
    updated_database = readExcelFile(data_path+NAME+'database'+up_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
    
    original_file = original_file.set_index('name')
    updated_file = updated_file.set_index('name')
    for ind in updated_file.index:
        sys.stdout.write("\rUpdating latest data time: "+ind+" ")
        sys.stdout.flush()

        if ind in original_file.index:
            original_file.loc[ind, 'desc_e'] = updated_file.loc[ind, 'desc_e']
            if updated_file.loc[ind, 'last'] == 'Nan':
                continue
            elif (original_file.loc[ind, 'last'] == 'Nan' and updated_file.loc[ind, 'last'] != 'Nan') or updated_file.loc[ind, 'last'] > original_file.loc[ind, 'last']:
                updated+=1
            if updated_file.loc[ind, 'last'] != 'Nan':
                original_file.loc[ind, 'last'] = updated_file.loc[ind, 'last']
            for period in updated_database[updated_file.loc[ind, 'db_table']].index:
                if updated_file.loc[ind, 'db_table'][3] == 'W' and type(period) != str:
                    period = period.strftime('%Y-%m-%d')
                if str(updated_database[updated_file.loc[ind, 'db_table']].loc[period, updated_file.loc[ind, 'db_code']]) != 'nan':
                    original_database[original_file.loc[ind, 'db_table']].loc[period, original_file.loc[ind, 'db_code']] = updated_database[updated_file.loc[ind, 'db_table']].loc[period, updated_file.loc[ind, 'db_code']]
        else:
            ERROR('Updated file index does not belongs to the original file index list: '+ind)
    sys.stdout.write("\n\n")
    for key in original_database.keys():
        original_database[key] = original_database[key].sort_index(axis=0)
    original_file = original_file.reset_index()
    original_file = original_file.reindex(key_list, axis='columns')
    print('updated:', updated, '\n')

    return original_file, original_database

def FOREX_NAME(source, frequency, form_e, FOREXcurrency, ind, FOREX_t, SORT_DATA, opp=False, suffix='', replicate='', df_key=None, db_table_t=None, associate=False, ECU=False):
    name_replicate = []
    done = False
    
    if source == 'Official ECB & EUROSTAT Reference':
        if associate == False:
            loc1 = str(FOREX_t.columns[ind][0]).find('.EUR')
            code = str(FOREX_t.columns[ind][0])[loc1-3:loc1]
        else:
            code = replicate[1:4]
        if replicate == '':
            if form_e == 'Average of observations through period (A)':
                if opp == False:
                    name = frequency+COUNTRY(code)+'REXEURDECB'+suffix
                    name_currency = CURRENCY(code)
                    for i in range(Country.shape[0]):
                        if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != COUNTRY(code):
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXEURDECB'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                        if COUNTRY(code) == '111' and str(Country.iloc[i]['Old_legacy_currency']) == 'Y':
                            replicate_name = [frequency+str(Country.iloc[i]['Country_Code'])+'REXEURDECB'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REXEURD'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REXELOCK'+suffix]
                            name_replicate.extend(replicate_name)
                            if str(Country.iloc[i]['Country_Code']) == '253':
                                replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXUSLOCK'+suffix
                                name_replicate.append(replicate_name)
                            if frequency == 'W':
                                replicate_name = [frequency+str(Country.iloc[i]['Country_Code'])+'REXEUREECB'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REXEURE'+suffix]
                                name_replicate.extend(replicate_name)
                    if COUNTRY(code) == '111':
                        replicate_name = [frequency+'163REXEURDECB'+suffix, frequency+'ECUREXELOCK'+suffix]
                        name_replicate.extend(replicate_name)
                        name_currency = 'Euro'
                        for i in range(Country.shape[0]):
                            if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != '163':
                                replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXEURDECB'+suffix
                                for key in SORT_DATA:
                                    if key[0] == replicate_name:
                                        done = True
                                        break
                                if done == False:
                                    name_replicate.append(replicate_name)
                    if frequency == 'W':
                        replicate_name = frequency+COUNTRY(code)+'REXEUREECB'+suffix
                        name_replicate.append(replicate_name)
                else:
                    name = frequency+COUNTRY(code)+'REXEURECB'+suffix
                    name_currency = CURRENCY(code)
                    for i in range(Country.shape[0]):
                        if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != COUNTRY(code):
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXEURECB'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                        if COUNTRY(code) == '111' and str(Country.iloc[i]['Old_legacy_currency']) == 'Y':
                            replicate_name = [frequency+str(Country.iloc[i]['Country_Code'])+'REXEURECB'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REXEUR'+suffix]
                            name_replicate.extend(replicate_name)
                            if frequency == 'W':
                                replicate_name = [frequency+str(Country.iloc[i]['Country_Code'])+'REXEURIECB'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REXEURI'+suffix]
                                name_replicate.extend(replicate_name)
                    if COUNTRY(code) == '111':
                        replicate_name = frequency+'163REXEURECB'+suffix
                        name_replicate.append(replicate_name)
                        name_currency = 'Euro'
                        for i in range(Country.shape[0]):
                            if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != '163':
                                replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXEURECB'+suffix
                                for key in SORT_DATA:
                                    if key[0] == replicate_name:
                                        done = True
                                        break
                                if done == False:
                                    name_replicate.append(replicate_name)
                    if frequency == 'W':
                        replicate_name = frequency+COUNTRY(code)+'REXEURIECB'+suffix
                        name_replicate.append(replicate_name)
            elif form_e == 'End of period (E)':
                if opp == False:
                    name = frequency+COUNTRY(code)+'REXEUREECB'+suffix
                    name_currency = CURRENCY(code)
                    for i in range(Country.shape[0]):
                        if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != COUNTRY(code):
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXEUREECB'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                        if COUNTRY(code) == '111' and str(Country.iloc[i]['Old_legacy_currency']) == 'Y':
                            replicate_name = [frequency+str(Country.iloc[i]['Country_Code'])+'REXEUREECB'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REXEURE'+suffix]
                            name_replicate.extend(replicate_name)
                    if COUNTRY(code) == '111':
                        replicate_name = frequency+'163REXEUREECB'+suffix
                        name_replicate.append(replicate_name)
                        name_currency = 'Euro'
                        for i in range(Country.shape[0]):
                            if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != '163':
                                replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXEUREECB'+suffix
                                for key in SORT_DATA:
                                    if key[0] == replicate_name:
                                        done = True
                                        break
                                if done == False:
                                    name_replicate.append(replicate_name)
                else:
                    name = frequency+COUNTRY(code)+'REXEURIECB'+suffix
                    name_currency = CURRENCY(code)
                    for i in range(Country.shape[0]):
                        if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != COUNTRY(code):
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXEURIECB'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                        if COUNTRY(code) == '111' and str(Country.iloc[i]['Old_legacy_currency']) == 'Y':
                            replicate_name = [frequency+str(Country.iloc[i]['Country_Code'])+'REXEURIECB'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REXEURI'+suffix]
                            name_replicate.extend(replicate_name)
                    if COUNTRY(code) == '111':
                        replicate_name = frequency+'163REXEURIECB'+suffix
                        name_replicate.append(replicate_name)
                        name_currency = 'Euro'
                        for i in range(Country.shape[0]):
                            if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != '163':
                                replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXEURIECB'+suffix
                                for key in SORT_DATA:
                                    if key[0] == replicate_name:
                                        done = True
                                        break
                                if done == False:
                                    name_replicate.append(replicate_name)
            else:
                ERROR('form not found: '+str(FOREX_t.columns[ind][0]))
        
        if ECU == False:
            value = list(FOREX_t[FOREX_t.columns[ind]])
        else:
            value = [1 for tmp in range(len(list(FOREX_t[FOREX_t.columns[ind]])))]
        if associate == False:
            index_item = FOREX_t.columns[ind]
        else:
            index_item = code
        index = FOREX_t[FOREX_t.columns[ind]].index
        roundnum = 10

    elif source == 'International Financial Statistics (IFS)' and FOREXcurrency == 'Special Drawing Rights (SDR)':
        if replicate == '':
            if form_e == 'End of period (E)':
                if opp == False:
                    name = frequency+COUNTRY(FOREX_t.index[ind])+'REXSDRE'+suffix
                    name_currency = CURRENCY(FOREX_t.index[ind])
                    for i in range(Country.shape[0]):
                        if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != COUNTRY(FOREX_t.index[ind]):# and str(Country.iloc[i]['IMF_country']) == 'nan':
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXSDRE'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                        if COUNTRY(FOREX_t.index[ind]) == '163' and str(Country.iloc[i]['Old_legacy_currency']) == 'Y':
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXSDRE'+suffix
                            name_replicate.append(replicate_name)
                    if COUNTRY(FOREX_t.index[ind]) == '111':
                        replicate_name = frequency+'001REXI'+suffix
                        name_replicate.append(replicate_name)
                else:
                    name = frequency+COUNTRY(FOREX_t.index[ind])+'REXSDRDE'+suffix
                    name_currency = CURRENCY(FOREX_t.index[ind])
                    for i in range(Country.shape[0]):
                        if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != COUNTRY(FOREX_t.index[ind]):# and str(Country.iloc[i]['IMF_country']) == 'nan':
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXSDRDE'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                        if COUNTRY(FOREX_t.index[ind]) == '163' and str(Country.iloc[i]['Old_legacy_currency']) == 'Y':
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXSDRDE'+suffix
                            name_replicate.append(replicate_name)
                    if COUNTRY(FOREX_t.index[ind]) == '111':
                        replicate_name = frequency+'001REXE'+suffix
                        name_replicate.append(replicate_name)
            elif form_e == 'Average of observations through period (A)':
                if opp == False:
                    name = frequency+COUNTRY(FOREX_t.index[ind])+'REXSDRA'+suffix
                    name_currency = CURRENCY(FOREX_t.index[ind])
                    for i in range(Country.shape[0]):
                        if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != COUNTRY(FOREX_t.index[ind]):# and str(Country.iloc[i]['IMF_country']) == 'nan':
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXSDRA'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                        if COUNTRY(FOREX_t.index[ind]) == '163' and str(Country.iloc[i]['Old_legacy_currency']) == 'Y':
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXSDRA'+suffix
                            name_replicate.append(replicate_name)
                    if COUNTRY(FOREX_t.index[ind]) == '111':
                        replicate_name = frequency+'001REXD'+suffix
                        name_replicate.append(replicate_name)
                else:
                    name = frequency+COUNTRY(FOREX_t.index[ind])+'REXSDRDA'+suffix
                    name_currency = CURRENCY(FOREX_t.index[ind])
                    for i in range(Country.shape[0]):
                        if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != COUNTRY(FOREX_t.index[ind]):# and str(Country.iloc[i]['IMF_country']) == 'nan':
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXSDRDA'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                        if COUNTRY(FOREX_t.index[ind]) == '163' and str(Country.iloc[i]['Old_legacy_currency']) == 'Y':
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXSDRDA'+suffix
                            name_replicate.append(replicate_name)
                    if COUNTRY(FOREX_t.index[ind]) == '111':
                        replicate_name = [frequency+'001REXA'+suffix, frequency+'001REX'+suffix, frequency+'001REXW'+suffix]
                        name_replicate.extend(replicate_name)
        
        value = list(FOREX_t.loc[FOREX_t.index[ind]])
        index = FOREX_t.loc[FOREX_t.index[ind]].index
        if associate == False:
            index_item = FOREX_t.index[ind]
        else:
            index_item = replicate[1:4]
        roundnum = 10
        code = index_item

    elif source == 'International Financial Statistics (IFS)' and FOREXcurrency == 'United States Dollar (USD)':
        if replicate == '':
            if form_e == 'End of period (E)':
                if opp == False:
                    name = frequency+COUNTRY(FOREX_t.index[ind])+'REXE'+suffix
                    name_currency = CURRENCY(FOREX_t.index[ind])
                    for i in range(Country.shape[0]):
                        if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != COUNTRY(FOREX_t.index[ind]):# and str(Country.iloc[i]['IMF_country']) == 'nan':
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXE'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                        if COUNTRY(FOREX_t.index[ind]) == '163' and str(Country.iloc[i]['Old_legacy_currency']) == 'Y':
                            replicate_name = [frequency+str(Country.iloc[i]['Country_Code'])+'REXE'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REXUSDEE'+suffix]
                            name_replicate.extend(replicate_name)
                    if COUNTRY(FOREX_t.index[ind]) == '163' or COUNTRY(FOREX_t.index[ind]) == '248':
                        replicate_name = [frequency+'111REXEURI'+suffix, frequency+COUNTRY(FOREX_t.index[ind])+'REXUSDEE'+suffix]
                        name_replicate.extend(replicate_name)
                else:
                    name = frequency+COUNTRY(FOREX_t.index[ind])+'REXI'+suffix
                    name_currency = CURRENCY(FOREX_t.index[ind])
                    for i in range(Country.shape[0]):
                        if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != COUNTRY(FOREX_t.index[ind]):# and str(Country.iloc[i]['IMF_country']) == 'nan':
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXI'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                        if COUNTRY(FOREX_t.index[ind]) == '163' and str(Country.iloc[i]['Old_legacy_currency']) == 'Y':
                            replicate_name = [frequency+str(Country.iloc[i]['Country_Code'])+'REXI'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REXUSDEI'+suffix]
                            name_replicate.extend(replicate_name)
                    if COUNTRY(FOREX_t.index[ind]) == '163' or COUNTRY(FOREX_t.index[ind]) == '248':
                        replicate_name = [frequency+'111REXEURE'+suffix, frequency+COUNTRY(FOREX_t.index[ind])+'REXUSDEI'+suffix]
                        name_replicate.extend(replicate_name)
            elif form_e == 'Average of observations through period (A)':
                if opp == False:
                    name = frequency+COUNTRY(FOREX_t.index[ind])+'REXA'+suffix
                    name_replicate.append(frequency+COUNTRY(FOREX_t.index[ind])+'REX'+suffix)
                    name_replicate.append(frequency+COUNTRY(FOREX_t.index[ind])+'REXW'+suffix)
                    name_currency = CURRENCY(FOREX_t.index[ind])
                    for i in range(Country.shape[0]):
                        if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != COUNTRY(FOREX_t.index[ind]):# and str(Country.iloc[i]['IMF_country']) == 'nan':
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXA'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                            done = False
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REX'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                            done = False
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXW'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                        if COUNTRY(FOREX_t.index[ind]) == '163' and str(Country.iloc[i]['Old_legacy_currency']) == 'Y':
                            replicate_name = [frequency+str(Country.iloc[i]['Country_Code'])+'REXA'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REX'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REXW'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REXUSDE'+suffix]
                            name_replicate.extend(replicate_name)
                    if COUNTRY(FOREX_t.index[ind]) == '163' or COUNTRY(FOREX_t.index[ind]) == '248':
                        replicate_name = [frequency+'111REXEUR'+suffix, frequency+COUNTRY(FOREX_t.index[ind])+'REXUSDE'+suffix]
                        name_replicate.extend(replicate_name)
                else:
                    name = frequency+COUNTRY(FOREX_t.index[ind])+'REXD'+suffix
                    name_currency = CURRENCY(FOREX_t.index[ind])
                    for i in range(Country.shape[0]):
                        if str(Country.iloc[i]['Currency_Name']) == name_currency and str(Country.iloc[i]['Country_Code']) != COUNTRY(FOREX_t.index[ind]):# and str(Country.iloc[i]['IMF_country']) == 'nan':
                            replicate_name = frequency+str(Country.iloc[i]['Country_Code'])+'REXD'+suffix
                            for key in SORT_DATA:
                                if key[0] == replicate_name:
                                    done = True
                                    break
                            if done == False:
                                name_replicate.append(replicate_name)
                        if COUNTRY(FOREX_t.index[ind]) == '163' and str(Country.iloc[i]['Old_legacy_currency']) == 'Y':
                            replicate_name = [frequency+str(Country.iloc[i]['Country_Code'])+'REXD'+suffix, frequency+str(Country.iloc[i]['Country_Code'])+'REXUSDED'+suffix]
                            name_replicate.extend(replicate_name)
                    if COUNTRY(FOREX_t.index[ind]) == '163' or COUNTRY(FOREX_t.index[ind]) == '248':
                        replicate_name = [frequency+'111REXEURD'+suffix, frequency+COUNTRY(FOREX_t.index[ind])+'REXUSDED'+suffix]
                        name_replicate.extend(replicate_name)
        
        value = list(FOREX_t.loc[FOREX_t.index[ind]])
        index = FOREX_t.loc[FOREX_t.index[ind]].index
        index_item = FOREX_t.index[ind]
        roundnum = 10
        if associate == False:
            code = index_item
        else:
            code = replicate[1:4]

    elif source == 'International Financial Statistics (IFS)' and FOREXcurrency == 'Euro':
        if form_e == 'End of period (E)':
            if opp == False:
                name = frequency+str(df_key.iloc[ind]['name'])[1:4]+'REXEURE'+suffix
            else:
                name = frequency+str(df_key.iloc[ind]['name'])[1:4]+'REXEURI'+suffix
        elif form_e == 'Average of observations through period (A)':
            if opp == False:
                name = frequency+str(df_key.iloc[ind]['name'])[1:4]+'REXEURD'+suffix
            else:
                name = frequency+str(df_key.iloc[ind]['name'])[1:4]+'REXEUR'+suffix
        
        #try:
        value = list(FOREX_t[df_key.iloc[ind]['db_table']][df_key.iloc[ind]['db_code']])
        index = FOREX_t[df_key.iloc[ind]['db_table']][df_key.iloc[ind]['db_code']].index
        #except KeyError:
        #    value = list(db_table_t[df_key.iloc[ind]['db_code']])
        #    index = db_table_t[df_key.iloc[ind]['db_code']].index
        code = str(df_key.iloc[ind]['name'])[1:4]
        roundnum = 10

        return name, value, index, code, roundnum
    elif source == 'International Financial Statistics (IFS)' and FOREXcurrency == 'United States Dollar (USD) (Millions of)':
        if form_e == 'World Currency Composition of Official Foreign Exchange Reserves':
            name = frequency+'010VRC'+COUNTRY(FOREX_t.index[ind])+suffix
        elif form_e == 'Advanced Economies Currency Composition of Official Foreign Exchange Reserves':
            name = frequency+'110VRC'+COUNTRY(FOREX_t.index[ind])+suffix
        elif form_e == 'Emerging and Developing Economies Currency Composition of Official Foreign Exchange Reserves':
            name = frequency+'200VRC'+COUNTRY(FOREX_t.index[ind])+suffix

        value = list(FOREX_t.loc[FOREX_t.index[ind]])
        index = FOREX_t.loc[FOREX_t.index[ind]].index
        index_item = FOREX_t.index[ind]
        roundnum = 10
        code = index_item
    else:
        ERROR('Source Error: '+str(source))    
    
    if replicate != '':
        name = replicate
    
    return name, value, index, index_item, roundnum, code, name_replicate

def FOREX_DATA(ind, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num, table_num, KEY_DATA, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, source, freqlist, frequency, form_e, FOREXcurrency, opp=False, suffix='', freqnum=None, freqsuffix=[], keysuffix=[], repl=None, again='', semiA=False, semi=False, weekA=False, weekE=False):
    freqlen = len(freqlist)
    name_replicate = []
    NonValue = '...'
    if code_num >= 200:
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        if frequency == 'W':
            db_table_t = db_table_t.reindex(FREQLIST['W_s'])
        DATA_BASE[db_table] = db_table_t
        DB_name.append(db_table)
        table_num += 1
        code_num = 1
        db_table_t = pd.DataFrame(index = freqlist, columns = [])

    nominal_index = False
    old_legacy = False
    ECU = False
    if again != '':
        if again.find('REX.') >= 0 or again.find('REXW') >= 0 or again[-1] == 'X':
            nominal_index = True
        if OLD_LEGACY(again[1:4]) == 'Y':
            old_legacy = True
        if OLD_LEGACY(again[1:4]) == 'Y' or (again.find('163') >= 0 and again.find('ECB') >= 0) or again.find('LOCK') >= 0:
            ECU = True
        name, value, index, index_item, roundnum, code, name_replicate = FOREX_NAME(source, frequency, form_e, FOREXcurrency, ind, FOREX_t, SORT_DATA, opp, suffix, replicate=again, associate=True, ECU=ECU)
    else:
        name, value, index, index_item, roundnum, code, name_replicate = FOREX_NAME(source, frequency, form_e, FOREXcurrency, ind, FOREX_t, SORT_DATA, opp, suffix)

    weekA2 = weekA
    weekE2 = weekE
    form_e2 = form_e
    if not not name_replicate:
        for other_name in name_replicate:
            if frequency == 'W':
                if other_name.find('I') >= 0 or other_name.find('EE') >= 0 or other_name.find('REXEURE'+suffix) >= 0:
                    weekA = False
                    weekE = True
                    form_e = 'End of period (E)'
                else:
                    weekA = True
                    weekE = False
                    form_e = 'Average of observations through period (A)' 
            code_num, table_num, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, new_item_counts = \
                FOREX_DATA(ind, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num, table_num, KEY_DATA, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, source, freqlist, frequency, form_e, FOREXcurrency, opp, suffix, freqnum, freqsuffix, keysuffix, repl, again=other_name, semiA=semiA, semi=semi, weekA=weekA, weekE=weekE)
    weekA = weekA2
    weekE = weekE2
    form_e = form_e2

    AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == name].to_dict('list')
    if pd.DataFrame(AREMOS_key).empty == True:
        if opp == False:
            if name.find('_') >= 0:
                AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == name.replace('_','')].to_dict('list')
                if pd.DataFrame(AREMOS_key).empty == True:
                    return code_num, table_num, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, new_item_counts
            else:
                CONTINUE.append(name)
                return code_num, table_num, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, new_item_counts
        elif source == 'International Financial Statistics (IFS)' and FOREXcurrency == 'Special Drawing Rights (SDR)':
            if form_e == 'End of period (E)':
                AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == frequency+COUNTRY(code)+'REXSDRE'+suffix].to_dict('list')
            elif form_e == 'Average of observations through period (A)':
                AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == frequency+COUNTRY(code)+'REXSDRA'+suffix].to_dict('list')
            if pd.DataFrame(AREMOS_key).empty == False:
                temp = AREMOS_key['base currency'][0]
                AREMOS_key['base currency'][0] = AREMOS_key['quote currency'][0]
                AREMOS_key['quote currency'][0] = temp
                AREMOS_key['description'][0] = AREMOS_key['description'][0].replace(str(AREMOS_key['base currency'][0]),'base currency').replace(str(AREMOS_key['quote currency'][0]),str(AREMOS_key['base currency'][0])).replace('base currency',str(AREMOS_key['quote currency'][0]))
            else:
                return code_num, table_num, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, new_item_counts
        elif source == 'Official ECB & EUROSTAT Reference':
            return code_num, table_num, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, new_item_counts
        elif source == 'International Financial Statistics (IFS)':
            return code_num, table_num, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, new_item_counts
        else:
            ERROR('Source Error: '+str(source))
    
    if (name in DF_KEY.index and find_unknown == True) or (name not in DF_KEY.index and find_unknown == False):
        return code_num, table_num, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, new_item_counts
    elif name not in DF_KEY.index and find_unknown == True:
        new_item_counts+=1

    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
    db_code = DB_CODE+str(code_num).rjust(3,'0')
    db_table_t[db_code] = ['' for tmp in range(freqlen)]
    desc_e = str(AREMOS_key['description'][0])
    #if desc_e.find('FOREIGN EXCHANGE') >= 0:
    for ph in range(len(before1)):
        desc_e = desc_e.replace(before1[ph],after1[ph])
    desc_e = desc_e.title()
    for ph in range(len(before2)):
        desc_e = desc_e.replace(before2[ph],after2[ph])
    loc2 = desc_e.find('ISO Code:')+10
    loc3 = loc2+3
    loc4 = desc_e.find('ISO Codes:')+11
    loc5 = loc4+3
    if loc2-10 >= 0:
        desc_e = desc_e.replace(desc_e[loc2:loc3],desc_e[loc2:loc3].upper())
    if loc4-11 >= 0:
        desc_e = desc_e.replace(desc_e[loc4:loc5],desc_e[loc4:loc5].upper())
    for ph in range(len(before3)):
        desc_e = desc_e.replace(before3[ph],after3[ph])
    if name.find('_') >= 0:
        desc_e = 'Share of Reserves: '+desc_e
    #base = str(AREMOS_key['base currency'][0])
    if code == '001':
        code = '1'
        FOREXcurrency = 'United States Dollar (USD)'
    #if base == 'nan':
    if opp == False:
        if code == '1':
            base = CURRENCY(code)
        else:
            base = FOREXcurrency
    else:
        if code == '1':
            base = FOREXcurrency
        else:
            base = CURRENCY(code)
    #quote = str(AREMOS_key['quote currency'][0])
    #if quote == 'nan':
    if opp == False:
        if FOREXcurrency == 'United States Dollar (USD) (Millions of)':
            NonValue = 'Nan'
            quote = ''
        else:
            if code == '1':
                quote = FOREXcurrency
            else:
                quote = CURRENCY(code)
    else:
        if code == '1':
            quote = CURRENCY(code)
        else:
            quote = FOREXcurrency
    desc_c = ''
    form_c = ''
    if str(desc_e) == 'Nan':
        if nominal_index == True:
            if name.find('REXW') >= 0:
                desc_e = 'Exchange Rate (Nominal Index 1999=100): '+quote+' per '+base+', '+source+', '+form_e
            else:
                desc_e = 'Exchange Rate (Nominal Index 2015=100): '+quote+' per '+base+', '+source+', '+form_e
        else:
            desc_e = 'Exchange Rate: '+quote+' per '+base+', '+source+', '+form_e
    
    start_found = False
    last_found = False
    found = False
    if weekA == False and weekE == False:
        for k in range(len(value)):
            if not not keysuffix:
                for word in range(len(keysuffix)):
                    if str(index[k]).find(keysuffix[word]) >= 0:
                        freq_index = str(index[k])[:freqnum]+freqsuffix[word]
                        if frequency == 'A':
                            freq_index = int(freq_index)
                        break
                    else:
                        freq_index = 'Nan'
            else:
                if str(index[k]).find(frequency) >= 0 or str(index[k]).isnumeric():
                    if frequency == 'A':
                        freq_index = int(index[k])
                    else:
                        freq_index = str(index[k]).replace(frequency,repl)
                else:
                    freq_index = 'Nan'
                #ERROR('Index Error: '+str(index[k]))
            if freq_index in db_table_t.index and ((find_unknown == False and int(str(freq_index)[:4]) >= dealing_start_year) or find_unknown == True):
                if str(value[k]) == NonValue:
                    db_table_t[db_code][freq_index] = ''
                else:
                    found = True
                    if opp == False:
                        if semiA == True:
                            if str(value[k-4]) == NonValue:
                                db_table_t[db_code][freq_index] = ''
                            elif nominal_index == True:
                                nominal_found = False
                                found = False
                                for nominal_year in base_year:
                                    if desc_e.find(nominal_year+'=100') >= 0:
                                        nominal_found = True
                                        if INDEXBASE(nominal_year, code, index_item, NonValue) == NonValue:
                                            db_table_t[db_code][freq_index] = ''
                                        else:
                                            found = True
                                            db_table_t[db_code][freq_index] = ((float(value[k])+float(value[k-4]))/2)*100/INDEXBASE(nominal_year, code, index_item, NonValue)
                                        break
                                if nominal_found == False:
                                    ERROR('Nominal Index Not Found: '+name)
                            else:
                                db_table_t[db_code][freq_index] = (float(value[k])+float(value[k-4]))/2
                        elif nominal_index == True:
                            nominal_found = False
                            found = False
                            for nominal_year in base_year:
                                if desc_e.find(nominal_year+'=100') >= 0:
                                    nominal_found = True
                                    if INDEXBASE(nominal_year, code, index_item, NonValue) == NonValue:
                                        db_table_t[db_code][freq_index] = ''
                                    else:
                                        found = True
                                        if old_legacy == True:
                                            db_table_t[db_code][freq_index] = float(value[k])*LOCKING(code)*100/INDEXBASE(nominal_year, code, index_item, NonValue)
                                        else:
                                            db_table_t[db_code][freq_index] = float(value[k])*100/INDEXBASE(nominal_year, code, index_item, NonValue)
                                    break
                            if nominal_found == False:
                                ERROR('Nominal Index Not Found: '+name)
                        elif old_legacy == True:
                            if name.find('USD') >= 0:
                                db_table_t[db_code][freq_index] = float(value[k])
                            else:
                                db_table_t[db_code][freq_index] = float(value[k])*LOCKING(code)
                        else:
                            db_table_t[db_code][freq_index] = float(value[k])
                    else:
                        if semiA == True:
                            if str(value[k-4]) == NonValue:
                                db_table_t[db_code][freq_index] = ''
                            else:
                                db_table_t[db_code][freq_index] = (round(1/float(value[k]), roundnum)+round(1/float(value[k-4]), roundnum))/2
                        elif old_legacy == True:
                            if name.find('USD') >= 0:
                                db_table_t[db_code][freq_index] = round(1/float(value[k]), roundnum)
                            else:
                                db_table_t[db_code][freq_index] = round(1/(float(value[k])*LOCKING(code)), roundnum)
                        else:
                            db_table_t[db_code][freq_index] = round(1/float(value[k]), roundnum)
                    if start_found == False and found == True:
                        if frequency == 'A':
                            start = int(freq_index)
                        else:
                            start = str(freq_index)
                        start_found = True
                    if start_found == True:
                        if k == len(value)-1:
                            if frequency == 'A':
                                last = int(freq_index)
                            else:
                                last = str(freq_index)
                            last_found = True
                        else:
                            for st in range(k+1, len(value)):
                                if not not keysuffix:
                                    for word in range(len(keysuffix)):
                                        if str(index[st]).find(keysuffix[word]) >= 0:
                                            if semi == True:
                                                if str(value[st]) != NonValue:
                                                    last_found = False
                                                    break
                                                else:
                                                    last_found = True
                                            else:
                                                if str(value[st]) != 'nan':
                                                    last_found = False
                                                    break
                                                else:
                                                    last_found = True
                                        else:
                                            last_found = True
                                else:
                                    if (str(index[st]).find(frequency) >= 0 or str(index[st]).isnumeric()) and str(value[st]) != NonValue:
                                        last_found = False
                                    else:
                                        last_found = True
                                if last_found == False:
                                    break
                            if last_found == True:
                                if frequency == 'A':
                                    last = int(freq_index)
                                else:
                                    last = str(freq_index)
            else:
                continue
    else:
        new_index = []
        for dex in index:
            if type(dex) != datetime and type(dex) != pd._libs.tslibs.timestamps.Timestamp:
                new_index.append(datetime.strptime(dex, '%Y-%m-%d'))
            else:
                new_index.append(dex)
        index = new_index
        head = 0
        for j in range(freqlen):
            if (find_unknown == False and int(str(db_table_t.index[j])[:4]) >= dealing_start_year) or find_unknown == True:
                weekdays = []
                for k in range(head, len(value)):
                    if (index[k]-db_table_t.index[j]).days < 7 and (index[k]-db_table_t.index[j]).days >= 0:
                        head = k
                        try:
                            weekdays.append(float(value[k]))
                        except ValueError:
                            continue
                    elif (index[k]-db_table_t.index[j]).days >= 7:
                        break
                if weekA == True:
                    if opp == False and len(weekdays) > 0:
                        if old_legacy == True and name.find('USD') < 0:
                            db_table_t[db_code][db_table_t.index[j]] = float(sum(weekdays)/len(weekdays))*LOCKING(code)
                        else:
                            db_table_t[db_code][db_table_t.index[j]] = float(sum(weekdays)/len(weekdays))
                        found = True
                    elif len(weekdays) > 0:
                        if old_legacy == True and name.find('USD') < 0:
                            db_table_t[db_code][db_table_t.index[j]] = round(1/float(sum(weekdays)/len(weekdays)), roundnum)*LOCKING(code)
                        else:
                            db_table_t[db_code][db_table_t.index[j]] = round(1/float(sum(weekdays)/len(weekdays)), roundnum)
                        found = True
                    else:
                        db_table_t[db_code][db_table_t.index[j]] = ''
                elif weekE == True:
                    if opp == False and len(weekdays) > 0:
                        if old_legacy == True and name.find('USD') < 0:
                            db_table_t[db_code][db_table_t.index[j]] = float(weekdays[-1])*LOCKING(code)
                        else:
                            db_table_t[db_code][db_table_t.index[j]] = float(weekdays[-1])
                        found = True
                    elif  len(weekdays) > 0:
                        if old_legacy == True and name.find('USD') < 0:
                            db_table_t[db_code][db_table_t.index[j]] = round(1/float(weekdays[-1]), roundnum)*LOCKING(code)
                        else:
                            db_table_t[db_code][db_table_t.index[j]] = round(1/float(weekdays[-1]), roundnum)
                        found = True
                    else:
                        db_table_t[db_code][db_table_t.index[j]] = ''
                if start_found == False and found == True:
                    start = str(db_table_t.index[j]).replace(' 00:00:00','')
                    start_found = True
                if start_found == True:
                    if (index[len(value)-1]-db_table_t.index[j]).days < 6:
                        untill = j
                        for l in list(reversed(range(untill))):
                            if db_table_t[db_code][db_table_t.index[l]] != '':
                                last = str(db_table_t.index[l]).replace(' 00:00:00','')
                                last_found = True
                                break
                        if last_found == True:
                            break

    if start_found == False:
        if found == True:
            ERROR('start not found: '+str(name))
    elif last_found == False:
        if found == True:
            ERROR('last not found: '+str(name))
    if found == False:
        start = 'Nan'
        last = 'Nan'               
    
    key_tmp= [databank, name, db_table, db_code, desc_e, desc_c, frequency, start, last, base, quote, snl, source, form_e, form_c]
    KEY_DATA.append(key_tmp)
    sort_tmp = [name, snl, db_table, db_code, start]
    SORT_DATA.append(sort_tmp)
    snl += 1

    code_num += 1
    
    return code_num, table_num, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, new_item_counts

def FOREX_CROSSRATE(g, new_item_counts, DF_KEY, df_key, AREMOS_forex, code_num, table_num, KEY_DATA, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, source, freqlist, frequency, form_e, FOREXcurrency, opp=False, suffix=''):
    freqlen = len(freqlist)
    print('Calculating Cross Rate: '+NAME+str(g)+', frequency = '+frequency+', opposite = '+str(opp)+' Time: ', int(time.time() - tStart),'s'+'\n')
    for ind in range(df_key.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((ind+1)*100/df_key.shape[0], 1))+"%)*")
        sys.stdout.flush()
        
        cross_rate = False
        if form_e == 'Average of observations through period (A)' and str(df_key.iloc[ind]['name']).find('REXA') >= 0 and str(df_key.iloc[ind]['freq']) == frequency and OLD_LEGACY(str(df_key.iloc[ind]['name'])[1:4]) != 'Y':
            USDPEREUR = DATA_BASE[df_key.iloc[df_key[df_key['name'] == frequency+'111REXEURD'+suffix].index[0]]['db_table']][df_key.iloc[df_key[df_key['name'] == frequency+'111REXEURD'+suffix].index[0]]['db_code']]
            cross_rate = True
        if form_e == 'End of period (E)' and str(df_key.iloc[ind]['name']).find('REXE') >= 0 and str(df_key.iloc[ind]['name']).find('REXEUR') < 0 and str(df_key.iloc[ind]['freq']) == frequency and OLD_LEGACY(str(df_key.iloc[ind]['name'])[1:4]) != 'Y':
            USDPEREUR = DATA_BASE[df_key.iloc[df_key[df_key['name'] == frequency+'111REXEURE'+suffix].index[0]]['db_table']][df_key.iloc[df_key[df_key['name'] == frequency+'111REXEURE'+suffix].index[0]]['db_code']]
            cross_rate = True
    
        if cross_rate == True:
            if code_num >= 200:
                db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
                DATA_BASE[db_table] = db_table_t
                DB_name.append(db_table)
                table_num += 1
                code_num = 1
                db_table_t = pd.DataFrame(index = freqlist, columns = [])
            
            name, value, index, code, roundnum = FOREX_NAME(source, frequency, form_e, FOREXcurrency, ind, DATA_BASE, SORT_DATA, opp, suffix, df_key=df_key, db_table_t=db_table_t)
            
            if code == '111':
                continue
            AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == name].to_dict('list')
            if pd.DataFrame(AREMOS_key).empty == True:
                if opp == False:
                    CONTINUE.append(name)
                    continue
                elif source == 'International Financial Statistics (IFS)':
                    if form_e == 'End of period (E)':
                        AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == frequency+COUNTRY(code)+'REXEURE'+suffix].to_dict('list')
                    elif form_e == 'Average of observations through period (A)':
                        AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == frequency+COUNTRY(code)+'REXEURD'+suffix].to_dict('list')
                    if pd.DataFrame(AREMOS_key).empty == False:
                        temp = AREMOS_key['base currency'][0]
                        AREMOS_key['base currency'][0] = AREMOS_key['quote currency'][0]
                        AREMOS_key['quote currency'][0] = temp
                        AREMOS_key['description'][0] = AREMOS_key['description'][0].replace(str(AREMOS_key['base currency'][0]),'base currency').replace(str(AREMOS_key['quote currency'][0]),str(AREMOS_key['base currency'][0])).replace('base currency',str(AREMOS_key['quote currency'][0]))
                    else:
                        continue
                else:
                    ERROR('Source Error: '+str(source))
            
            if (name in DF_KEY.index and find_unknown == True) or (name not in DF_KEY.index and find_unknown == False):
                continue
            elif name not in DF_KEY.index and find_unknown == True:
                new_item_counts+=1

            db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
            db_code = DB_CODE+str(code_num).rjust(3,'0')
            db_table_t[db_code] = ['' for tmp in range(freqlen)]
            
            start = df_key.iloc[ind]['start']
            last = df_key.iloc[ind]['last']
            for k in range(len(value)):
                if (find_unknown == False and int(str(index[k])[:4]) >= dealing_start_year) or find_unknown == True:
                    if str(value[k]) == '':
                        db_table_t[db_code][index[k]] = ''
                    else:
                        if opp == False:
                            db_table_t[db_code][index[k]] = float(value[k])*USDPEREUR[index[k]]
                        else:
                            db_table_t[db_code][index[k]] = round(1/(float(value[k])*USDPEREUR[index[k]]), roundnum)             

            desc_e = str(AREMOS_key['description'][0])
            #if desc_e.find('FOREIGN EXCHANGE') >= 0:
            for ph in range(len(before1)):
                desc_e = desc_e.replace(before1[ph],after1[ph])
            desc_e = desc_e.title()
            for ph in range(len(before2)):
                desc_e = desc_e.replace(before2[ph],after2[ph])
            loc2 = desc_e.find('ISO Code:')+10
            loc3 = loc2+3
            loc4 = desc_e.find('ISO Codes:')+11
            loc5 = loc4+3
            if loc2-10 >= 0:
                desc_e = desc_e.replace(desc_e[loc2:loc3],desc_e[loc2:loc3].upper())
            if loc4-11 >= 0:
                desc_e = desc_e.replace(desc_e[loc4:loc5],desc_e[loc4:loc5].upper())
            for ph in range(len(before3)):
                desc_e = desc_e.replace(before3[ph],after3[ph])
            #base = str(AREMOS_key['base currency'][0])
            if code == '001':
                code = 'XDR'
            #if base == 'nan':
            if opp == False:
                base = FOREXcurrency
            else:
                base = CURRENCY(code)
            #quote = str(AREMOS_key['quote currency'][0])
            #if quote == 'nan':
            if opp == False:
                quote = CURRENCY(code)
            else:
                quote = FOREXcurrency
            desc_c = ''
            form_c = ''
            if str(desc_e) == 'Nan':
                desc_e = 'Exchange Rate: '+quote+' per '+base+', '+source+', '+form_e
            
            key_tmp= [databank, name, db_table, db_code, desc_e, desc_c, frequency, start, last, base, quote, snl, source, form_e, form_c]
            KEY_DATA.append(key_tmp)
            sort_tmp = [name, snl, db_table, db_code, start]
            SORT_DATA.append(sort_tmp)
            snl += 1

            code_num += 1
        else:
            db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')

    sys.stdout.write("\n")

    return code_num, table_num, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, new_item_counts

def FOREX_CRAW(g, head, skip, url='https://data.imf.org/regular.aspx?key=61545862'):

    options = Options()
    options.add_argument("--disable-notifications")
    options.add_experimental_option("prefs", {"profile.default_content_setting_values.cookies": 2})
    chrome = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    chrome.get(url)
    time.sleep(5)
    WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[@id="DimCombo2552"]'))).click()
    WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[@id="ExportSplitButton"]'))).click()
    FOREX_t = FOREX_WEBDRIVER(chrome, g, header=head, index_col=0, skiprows=skip)

    return FOREX_t