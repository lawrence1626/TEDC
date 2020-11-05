# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date
#from FOREX_concat import CONCATE, readExcelFile

ENCODING = 'utf-8-sig'

NAME = 'FOREX_'
data_path = './data/'
out_path = "./output/"
databank = 'FOREX'
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'last', 'base', 'quote', 'snl', 'source', 'form_e', 'form_c']
#merge_file = readExcelFile(out_path+'FOREX_key.xlsx', header_ = 0, sheet_name_='FOREX_key')
base_year = ['1999','2010','2015']
start_year = 1999
start_file = 1
last_file = 10
update = datetime.today()
for i in range(len(key_list)):
    if key_list[i] == 'snl':
        snl_pos = i
        break

# 回報錯誤、儲存錯誤檔案並結束程式
def ERROR(error_text):
    print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()

def readFile(dir, default=pd.DataFrame(), acceptNoFile=False, \
             header_=None,skiprows_=None,index_col_=None,skipfooter_=0,encoding_=ENCODING):
    try:
        t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        encoding=encoding_,engine='python')
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            ERROR('找不到檔案：'+dir)
    except:
        try: #檔案編碼格式不同
            t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,\
                        engine='python')
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, \
             header_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,sheet_name_=None):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_)
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            ERROR('找不到檔案：'+dir)
    except:
        try: #檔案編碼格式不同
            t = pd.read_excel(dir, header=header_,skiprows=skiprows_,index_col=index_col_,sheet_name=sheet_name_)
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

def takeFirst(alist):
    return alist[0]

AREMOS_forex = readExcelFile(data_path+'forex2020.xlsx', header_ = [0], sheet_name_='forex')
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
            ERROR('INDEXBASE國家代碼錯誤1: '+code)
    except ValueError:
        return NonValue
    #except:
    #    ERROR('INDEXBASE國家代碼錯誤2: '+code)

this_year = datetime.now().year + 1
Year_list = [tmp for tmp in range(start_year,this_year)]
HalfYear_list = []
for y in range(start_year,this_year):
    for s in range(1,3):
        HalfYear_list.append(str(y)+'-S'+str(s))
#print(HalfYear_list)
Quarter_list = []
for q in range(start_year,this_year):
    for r in range(1,5):
        Quarter_list.append(str(q)+'-Q'+str(r))
#print(Quarter_list)
Month_list = []
for y in range(start_year,this_year):
    for m in range(1,13):
        Month_list.append(str(y)+'-'+str(m).rjust(2,'0'))
#print(Month_list)
calendar.setfirstweekday(calendar.SATURDAY)
Week_list = pd.date_range(start = str(start_year)+'-01-01',end=update,freq='W-SAT')
Week_list_s = pd.date_range(start = str(start_year)+'-01-01',end=update,freq='W-SAT').strftime('%Y-%m-%d')

KEY_DATA = []
SORT_DATA_A = []
SORT_DATA_S = []
SORT_DATA_Q = []
SORT_DATA_M = []
SORT_DATA_W = []
DATA_BASE_A = {}
DATA_BASE_S = {}
DATA_BASE_Q = {}
DATA_BASE_M = {}
DATA_BASE_W = {}
db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
db_table_S_t = pd.DataFrame(index = HalfYear_list, columns = [])
db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
db_table_W_t = pd.DataFrame(index = Week_list, columns = [])
DB_name_A = []
DB_name_S = []
DB_name_Q = []
DB_name_M = []
DB_name_W = []
DB_TABLE = 'DB_'
DB_CODE = 'data'
    
table_num_A = 1
table_num_S = 1
table_num_Q = 1
table_num_M = 1
table_num_W = 1
code_num_A = 1
code_num_S = 1
code_num_Q = 1
code_num_M = 1
code_num_W = 1
snl = 1
start_snl = snl
start_table_A = table_num_A
start_table_S = table_num_S
start_table_Q = table_num_Q
start_table_M = table_num_M
start_table_W = table_num_W
start_code_A = code_num_A
start_code_S = code_num_S
start_code_Q = code_num_Q
start_code_M = code_num_M
start_code_W = code_num_W
CONTINUE = []
REPLICATED = []

before1 = ['FOREIGN EXCHANGE',') PER','DATA)',')FROM','SOURCE','NOTE','RATESDR','RATESEMI','RATEEND','RATES','MARKET RATE','OFFICIAL RATE','PRINCIPAL RATE','USING','ONWARDD','WEDOLLAR','ESOFFICIAL','MILLIONS','NSAINTERNATIONAL','aA','aE','ReservesClaims','DollarsUnit','DollarSource','www.imf.org','FUNDCURRENCY','DATAU.S.','ORLUXEMBOURG','EMUEURO','Y DATA',' AS','HOUSEHOLDSCANNOT','NACIONALWHICH','WITH ',"#IES",'#']
after1 = [' FOREIGN EXCHANGE ',') PER ','DATA): ',') FROM',', SOURCE',', NOTE','RATE SDR','RATE SEMI','RATE END','RATES ','MARKET RATE ','OFFICIAL RATE ','PRINCIPAL RATE ','USING ','ONWARD D','WE DOLLAR','ES OFFICIAL',' MILLIONS','NSA INTERNATIONAL','a A','a E','Reserves, Claims','Dollars; Unit','Dollar; Source','','FUND CURRENCY','DATA U.S.','OR LUXEMBOURG','EMU EURO','Y DATA ',' AS ','HOUSEHOLDS CANNOT','NACIONAL WHICH',' WITH ','IES',' ']
before2 = ['Ecb','1 Ecu','Sdr','Ifs','Ihs','Imf','Iso','Exchange S ','Rate S ','Am','Pm','Of ',"People S","People'S",'Usd','Us ','#Name?eekly','#Name?','Cfa','Cfp','Fx','Rate,,','Rate,','Nsa','Cofer','And ', 'In ',')Total','Or ','Luf','Emu ','Rexa','Rexeurd','Rexe','Rexeure','Rexi','Rexeuri','Subsidizedby']
after2 = ['ECB','1 ECU','SDR','IFS','IHS','IMF','ISO','Exchanges ','Rates ','am','pm','of ',"People's","People's",'USD','US ','weekly','','CFA','CFP','Foreign Exchange','Rate,','Rate.','NSA','COFER','and ','in ','): Total','or ','LUF','EMU ','REXA','REXEURD','REXE','REXEURE','REXI','REXEURI','Subsidized by']
before3 = ['CYPrus','EURo']
after3 = ['Cyprus','Euro']

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

def FOREX_DATA(ind, FOREX_t, AREMOS_forex, code_num, table_num, KEY_DATA, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, source, freqlist, frequency, form_e, FOREXcurrency, opp=False, suffix='', freqnum=None, freqsuffix=[], keysuffix=[], repl=None, again='', semiA=False, semi=False, weekA=False, weekE=False):
    freqlen = len(freqlist)
    name_replicate = []
    NonValue = '...'
    if code_num >= 200:
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        if frequency == 'W':
            db_table_t = db_table_t.reindex(Week_list_s)
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
            code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name, snl = FOREX_DATA(ind, FOREX_t, AREMOS_forex, code_num, table_num, KEY_DATA, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, source, freqlist, frequency, form_e, FOREXcurrency, opp, suffix, freqnum, freqsuffix, keysuffix, repl, again=other_name, semiA=semiA, semi=semi, weekA=weekA, weekE=weekE)
    weekA = weekA2
    weekE = weekE2
    form_e = form_e2

    AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == name].to_dict('list')
    if pd.DataFrame(AREMOS_key).empty == True:
        if opp == False:
            if name.find('_') >= 0:
                AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == name.replace('_','')].to_dict('list')
                if pd.DataFrame(AREMOS_key).empty == True:
                    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
                    return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name, snl
            else:
                CONTINUE.append(name)
                db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
                return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name, snl
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
                db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
                return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name, snl
        elif source == 'Official ECB & EUROSTAT Reference':
            db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
            return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name, snl
        elif source == 'International Financial Statistics (IFS)':
            db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
            return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name, snl
        else:
            ERROR('Source Error: '+str(source))
    
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
            if freq_index in db_table_t.index:
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
        head = 0
        for j in range(freqlen):
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
    
    return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name, snl

def FOREX_CROSSRATE(g, df_key, AREMOS_forex, code_num, table_num, KEY_DATA, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, source, freqlist, frequency, form_e, FOREXcurrency, opp=False, suffix=''):
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
                    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
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
                        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
                        continue
                else:
                    ERROR('Source Error: '+str(source))
            
            db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
            db_code = DB_CODE+str(code_num).rjust(3,'0')
            db_table_t[db_code] = ['' for tmp in range(freqlen)]
            
            start = df_key.iloc[ind]['start']
            last = df_key.iloc[ind]['last']
            for k in range(len(value)):
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

    sys.stdout.write("\n\n")

    return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name, snl

#print(FOREX_t.head(10))
tStart = time.time()

for g in range(start_file,last_file+1):
    print('Reading file: '+NAME+str(g)+' Time: ', int(time.time() - tStart),'s'+'\n')
    if g == 1 or g == 2 or g == 10:############################################################ ECB ##################################################################
        FOREX_t = readFile(data_path+NAME+str(g)+'.csv', header_ = [0,1,2], index_col_=0, skiprows_=[0,4])
        if str(FOREX_t.index[0]).find('/') >= 0:
            new_index = []
            for ind in FOREX_t.index:
                new_index.append(pd.to_datetime(ind))
            FOREX_t = FOREX_t.reindex(new_index)
        if FOREX_t.index[0] > FOREX_t.index[1]:
            FOREX_t = FOREX_t[::-1]
        
        nG = FOREX_t.shape[1]
        print('Total Columns:',nG,'Time: ', int(time.time() - tStart),'s'+'\n')        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()

            source = 'Official ECB & EUROSTAT Reference'
            form_e = str(FOREX_t.columns[i][2])
            FOREXcurrency = 'Euro'
            if str(FOREX_t.columns[i][0]).find('EXR.A') >= 0:
                freqnum = 4
                freqsuffix = ['']
                frequency = 'A'
                keysuffix = ['-12-31']
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=False, freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=True, freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
            elif str(FOREX_t.columns[i][0]).find('EXR.H') >= 0:
                freqnum = 5
                freqsuffix = ['S1','S2']
                frequency = 'S'
                keysuffix = ['06-30','12-31']
                code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
            elif str(FOREX_t.columns[i][0]).find('EXR.M') >= 0:
                freqnum = 7
                freqsuffix = ['']
                frequency = 'M'
                keysuffix = ['-']
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.M', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.M', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                if str(FOREX_t.columns[i][0]).find('SP00.E') >= 0:
                    freqnum = 5
                    freqsuffix = ['S1','S2']
                    frequency = 'S'
                    keysuffix = ['06-30','12-31']
                    code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                    code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
            elif str(FOREX_t.columns[i][0]).find('EXR.Q') >= 0:
                freqnum = 5
                freqsuffix = ['Q1','Q2','Q3','Q4']
                frequency = 'Q'
                keysuffix = ['03-31','06-30','09-30','12-31']
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.Q', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.Q', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
            elif str(FOREX_t.columns[i][0]).find('EXR.D') >= 0:
                frequency = 'W'
                code_num_W, table_num_W, SORT_DATA_W, DATA_BASE_W, db_table_W, db_table_W_t, DB_name_W, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_W, table_num_W, KEY_DATA, SORT_DATA_W, DATA_BASE_W, db_table_W_t, DB_name_W, snl, source, Week_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.W', weekA=True)
                code_num_W, table_num_W, SORT_DATA_W, DATA_BASE_W, db_table_W, db_table_W_t, DB_name_W, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_W, table_num_W, KEY_DATA, SORT_DATA_W, DATA_BASE_W, db_table_W_t, DB_name_W, snl, source, Week_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.W', weekA=True)
                if str(FOREX_t.columns[i][0]).find('ISK') >= 0:
                    form_e = 'End of period (E)'
                    freqnum = 7
                    freqsuffix = ['','','','','','','']
                    frequency = 'M'
                    keysuffix = ['-25','-26','-27','-28','-29','-30','-31']
                    code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.M', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                    code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.M', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                    freqnum = 5
                    freqsuffix = ['Q1','Q1','Q1','Q1','Q2','Q2','Q2','Q2','Q3','Q3','Q3','Q3','Q4','Q4','Q4','Q4']
                    frequency = 'Q'
                    keysuffix = ['03-28','03-29','03-30','03-31','06-27','06-28','06-29','06-30','09-27','09-28','09-29','09-30','12-28','12-29','12-30','12-31']
                    code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.Q', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                    code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.Q', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                    freqnum = 5
                    freqsuffix = ['S1','S1','S1','S1','S2','S2','S2','S2']
                    frequency = 'S'
                    keysuffix = ['06-27','06-28','06-29','06-30','12-28','12-29','12-30','12-31']
                    code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                    code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                    freqnum = 4
                    freqsuffix = ['','','','']
                    frequency = 'A'
                    keysuffix = ['12-28','12-29','12-30','12-31']
                    code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=False, freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
                    code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=True, freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
            
    elif g >= 3 and g <= 6:############################################################ IMF ##################################################################
        FOREX_t = readExcelFile(data_path+NAME+str(g)+'.xlsx', header_ =0, index_col_=1, skiprows_=list(range(6)), sheet_name_=0)
        FOREX_t = FOREX_t.drop(columns=['Unnamed: 0', 'Scale', 'Base Year'])
        
        nG = FOREX_t.shape[0]
        print('Total Rows:',nG,'Time: ', int(time.time() - tStart),'s'+'\n')
        #print(FOREX_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()
            
            source = 'International Financial Statistics (IFS)'
            if g == 3:
                FOREXcurrency = 'Special Drawing Rights (SDR)'
                form_e = 'End of period (E)'
                frequency = 'A'
                repl = ''
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=False, repl=repl)
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=True, repl=repl)
                frequency = 'M'
                repl = '-'
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.M', repl=repl)
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.M', repl=repl)
                frequency = 'Q'
                repl = '-Q'
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.Q', repl=repl)
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.Q', repl=repl)
                frequency = 'S'
                freqnum = 4
                freqsuffix = ['-S1','-S2']
                keysuffix = ['M06','M12']
                code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, semi=True)
                code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, semi=True)
            elif g == 4:
                FOREXcurrency = 'Special Drawing Rights (SDR)'
                form_e = 'Average of observations through period (A)'
                frequency = 'A'
                repl = ''
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=False, repl=repl)
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=True, repl=repl)
                frequency = 'M'
                repl = '-'
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.M', repl=repl)
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.M', repl=repl)
                frequency = 'Q'
                repl = '-Q'
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.Q', repl=repl)
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.Q', repl=repl)
                frequency = 'S'
                freqnum = 4
                freqsuffix = ['-S1','-S2']
                keysuffix = ['Q2','Q4']
                code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, semiA=True, semi=True)
                code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, semiA=True, semi=True)
            elif g == 5:
                FOREXcurrency = 'United States Dollar (USD)'
                form_e = 'End of period (E)'
                frequency = 'A'
                repl = ''
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=False, repl=repl)
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=True, repl=repl)
                frequency = 'M'
                repl = '-'
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.M', repl=repl)
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.M', repl=repl)
                frequency = 'Q'
                repl = '-Q'
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.Q', repl=repl)
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.Q', repl=repl)
                frequency = 'S'
                freqnum = 4
                freqsuffix = ['-S1','-S2']
                keysuffix = ['M06','M12']
                code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, semi=True)
                code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, semi=True)
            elif g == 6:
                FOREXcurrency = 'United States Dollar (USD)'
                form_e = 'Average of observations through period (A)'
                frequency = 'A'
                repl = ''
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=False, repl=repl)
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=True, repl=repl)
                frequency = 'M'
                repl = '-'
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.M', repl=repl)
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.M', repl=repl)
                frequency = 'Q'
                repl = '-Q'
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.Q', repl=repl)
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.Q', repl=repl)
                frequency = 'S'
                freqnum = 4
                freqsuffix = ['-S1','-S2']
                keysuffix = ['Q2','Q4']
                code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, semiA=True, semi=True)
                code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.S', freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, semiA=True, semi=True)

        sys.stdout.write("\n\n") 
        
        df_key_temp = pd.DataFrame(KEY_DATA, columns = key_list)
        if g == 5:
            FOREXcurrency = 'Euro'
            form_e = 'End of period (E)'
            frequency = 'A'
            code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=False)
            code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=True)
            frequency = 'M'
            code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.M')
            code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.M')
            frequency = 'Q'
            code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.Q')
            code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.Q')
            frequency = 'S'
            code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.S')
            code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.S')
        elif g == 6:
            FOREXcurrency = 'Euro'
            form_e = 'Average of observations through period (A)'
            frequency = 'A'
            code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=False)
            code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, form_e, FOREXcurrency, opp=True)
            frequency = 'M'
            code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.M')
            code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.M')
            frequency = 'Q'
            code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.Q')
            code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.Q')
            frequency = 'S'
            code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.S')
            code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_CROSSRATE(g, df_key_temp, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, frequency, form_e, FOREXcurrency, opp=True, suffix='.S')
    
    elif g == 7:
        FOREX_t = readExcelFile(data_path+NAME+str(g)+'.xlsx', header_ =0, index_col_=1, skiprows_=list(range(4)), skipfooter_=3, sheet_name_=0)
        FOREX_t = FOREX_t.drop(columns=['Unnamed: 0'])
        
        nG = FOREX_t.shape[0]
        print('Total Rows:',nG,'Time: ', int(time.time() - tStart),'s'+'\n')
        #print(FOREX_t)      
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()
            
            source = 'International Financial Statistics (IFS)'
            FOREXcurrency = 'United States Dollar (USD) (Millions of)'
            form_e = 'World Currency Composition of Official Foreign Exchange Reserves'
            frequency = 'Q'
            repl = '-Q'
            code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.Q', repl=repl)
        
    elif g >= 8 and g <= 9:
        FOREX_t = readExcelFile(data_path+NAME+str(g)+'.xlsx', header_ =0, index_col_=1, skiprows_=list(range(6)), skipfooter_=3, sheet_name_=0)
        FOREX_t = FOREX_t.drop(columns=['Unnamed: 0'])
        
        nG = FOREX_t.shape[0]
        print('Total Rows:',nG,'Time: ', int(time.time() - tStart),'s'+'\n')
        #print(FOREX_t)       
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()
            
            source = 'International Financial Statistics (IFS)'
            FOREXcurrency = 'United States Dollar (USD) (Millions of)'
            if g == 8:
                form_e = 'Advanced Economies Currency Composition of Official Foreign Exchange Reserves'
                frequency = 'Q'
                repl = '-Q'
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.Q', repl=repl)
            if g == 9:
                form_e = 'Emerging and Developing Economies Currency Composition of Official Foreign Exchange Reserves'
                frequency = 'Q'
                repl = '-Q'
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, form_e, FOREXcurrency, opp=False, suffix='.Q', repl=repl)
                    
                
    sys.stdout.write("\n\n") 

if db_table_A_t.empty == False:
    DATA_BASE_A[db_table_A] = db_table_A_t
    DB_name_A.append(db_table_A)
if db_table_S_t.empty == False:
    DATA_BASE_S[db_table_S] = db_table_S_t
    DB_name_S.append(db_table_S)
if db_table_M_t.empty == False:
    DATA_BASE_M[db_table_M] = db_table_M_t
    DB_name_M.append(db_table_M)
if db_table_Q_t.empty == False:
    DATA_BASE_Q[db_table_Q] = db_table_Q_t
    DB_name_Q.append(db_table_Q)
if db_table_W_t.empty == False:
    db_table_W_t = db_table_W_t.reindex(Week_list_s)
    DATA_BASE_W[db_table_W] = db_table_W_t
    DB_name_W.append(db_table_W)       

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_A.sort(key=takeFirst)
repeated_A = 0
for i in range(1, len(SORT_DATA_A)):
    if SORT_DATA_A[i][0] == SORT_DATA_A[i-1][0]:
        repeated_A += 1
        if str(SORT_DATA_A[i-1][4]) == 'Nan':
            target = i-1
            try:
                DATA_BASE_A[SORT_DATA_A[target][2]].drop(columns = SORT_DATA_A[target][3])
            except KeyError:
                target = i
        else:
            target = i
        #print(SORT_DATA_A[i][0],' ',SORT_DATA_A[i-1][1],' ',SORT_DATA_A[i][1],' ',SORT_DATA_A[i][2],' ',SORT_DATA_A[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_A[target][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_A[SORT_DATA_A[target][2]] = DATA_BASE_A[SORT_DATA_A[target][2]].drop(columns = SORT_DATA_A[target][3])
        if DATA_BASE_A[SORT_DATA_A[target][2]].empty == True:
            DB_name_A.remove(SORT_DATA_A[target][2])
    sys.stdout.write("\r"+str(repeated_A)+" repeated annual data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_Q.sort(key=takeFirst)
repeated_Q = 0
for i in range(1, len(SORT_DATA_Q)):
    if SORT_DATA_Q[i][0] == SORT_DATA_Q[i-1][0]:
        repeated_Q += 1
        if str(SORT_DATA_Q[i-1][4]) == 'Nan':
            target = i-1
            try:
                DATA_BASE_Q[SORT_DATA_Q[target][2]].drop(columns = SORT_DATA_Q[target][3])
            except KeyError:
                target = i
        else:
            target = i
        #print(SORT_DATA_Q[i][0],' ',SORT_DATA_Q[i-1][1],' ',SORT_DATA_Q[i][1],' ',SORT_DATA_Q[i][2],' ',SORT_DATA_Q[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_Q[target][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_Q[SORT_DATA_Q[target][2]] = DATA_BASE_Q[SORT_DATA_Q[target][2]].drop(columns = SORT_DATA_Q[target][3])
        if DATA_BASE_Q[SORT_DATA_Q[target][2]].empty == True:
            DB_name_Q.remove(SORT_DATA_Q[target][2])
    sys.stdout.write("\r"+str(repeated_Q)+" repeated quarter data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_M.sort(key=takeFirst)
repeated_M = 0
for i in range(1, len(SORT_DATA_M)):
    if SORT_DATA_M[i][0] == SORT_DATA_M[i-1][0]:
        repeated_M += 1
        if str(SORT_DATA_M[i-1][4]) == 'Nan':
            target = i-1
            try:
                DATA_BASE_M[SORT_DATA_M[target][2]].drop(columns = SORT_DATA_M[target][3])
            except KeyError:
                target = i
        else:
            target = i
        #print(SORT_DATA_M[i][0],' ',SORT_DATA_M[i-1][1],' ',SORT_DATA_M[i][1],' ',SORT_DATA_M[i][2],' ',SORT_DATA_M[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_M[target][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_M[SORT_DATA_M[target][2]] = DATA_BASE_M[SORT_DATA_M[target][2]].drop(columns = SORT_DATA_M[target][3])
        if DATA_BASE_M[SORT_DATA_M[target][2]].empty == True:
            DB_name_M.remove(SORT_DATA_M[target][2])
    sys.stdout.write("\r"+str(repeated_M)+" repeated month data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_S.sort(key=takeFirst)
repeated_S = 0
for i in range(1, len(SORT_DATA_S)):
    if SORT_DATA_S[i][0] == SORT_DATA_S[i-1][0]:
        repeated_S += 1
        if str(SORT_DATA_S[i-1][4]) == 'Nan':
            target = i-1
            try:
                DATA_BASE_S[SORT_DATA_S[target][2]].drop(columns = SORT_DATA_S[target][3])
            except KeyError:
                target = i
        else:
            target = i
        #print(SORT_DATA_S[i][0],' ',SORT_DATA_S[i-1][1],' ',SORT_DATA_S[i][1],' ',SORT_DATA_S[i][2],' ',SORT_DATA_S[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_S[target][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_S[SORT_DATA_S[target][2]] = DATA_BASE_S[SORT_DATA_S[target][2]].drop(columns = SORT_DATA_S[target][3])
        if DATA_BASE_S[SORT_DATA_S[target][2]].empty == True:
            DB_name_S.remove(SORT_DATA_S[target][2])
    sys.stdout.write("\r"+str(repeated_S)+" repeated semiannual data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_W.sort(key=takeFirst)
repeated_W = 0
for i in range(1, len(SORT_DATA_W)):
    if SORT_DATA_W[i][0] == SORT_DATA_W[i-1][0]:
        repeated_W += 1
        if str(SORT_DATA_W[i-1][4]) == 'Nan':
            target = i-1
            try:
                DATA_BASE_W[SORT_DATA_W[target][2]].drop(columns = SORT_DATA_W[target][3])
            except KeyError:
                target = i
        else:
            target = i
        #print(SORT_DATA_W[i][0],' ',SORT_DATA_W[i-1][1],' ',SORT_DATA_W[i][1],' ',SORT_DATA_W[i][2],' ',SORT_DATA_W[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_W[target][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_W[SORT_DATA_W[target][2]] = DATA_BASE_W[SORT_DATA_W[target][2]].drop(columns = SORT_DATA_W[target][3])
        if DATA_BASE_W[SORT_DATA_W[target][2]].empty == True:
            DB_name_W.remove(SORT_DATA_W[target][2])
    sys.stdout.write("\r"+str(repeated_W)+" repeated week data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')
df_key = pd.DataFrame(KEY_DATA, columns = key_list)
df_key = df_key.sort_values(by=['name', 'db_table'], ignore_index=True)
if df_key.iloc[0]['snl'] != start_snl:
    df_key.loc[0, 'snl'] = start_snl
for s in range(1,df_key.shape[0]):
    sys.stdout.write("\rSetting new snls: "+str(s))
    sys.stdout.flush()
    df_key.loc[s, 'snl'] = df_key.loc[0, 'snl'] + s
sys.stdout.write("\n")
#if repeated_A > 0 or repeated_Q > 0 or repeated_M > 0:
print('Setting new files, Time: ', int(time.time() - tStart),'s'+'\n')

DATA_BASE_A_new = {}
DATA_BASE_Q_new = {}
DATA_BASE_M_new = {}
DATA_BASE_S_new = {}
DATA_BASE_W_new = {}
db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
db_table_S_t = pd.DataFrame(index = HalfYear_list, columns = [])
db_table_W_t = pd.DataFrame(index = Week_list_s, columns = [])
DB_name_A_new = []
DB_name_Q_new = []
DB_name_M_new = []
DB_name_S_new = []
DB_name_W_new = []
db_table_new = 0
db_code_new = 0
for f in range(df_key.shape[0]):
    sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
    sys.stdout.flush()
    if df_key.iloc[f]['freq'] == 'A':
        if start_code_A >= 200:
            DATA_BASE_A_new[db_table_A] = db_table_A_t
            DB_name_A_new.append(db_table_A)
            start_table_A += 1
            start_code_A = 1
            db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
        db_table_A = DB_TABLE+'A_'+str(start_table_A).rjust(4,'0')
        db_code_A = DB_CODE+str(start_code_A).rjust(3,'0')
        db_table_A_t[db_code_A] = DATA_BASE_A[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_A
        df_key.loc[f, 'db_code'] = db_code_A
        start_code_A += 1
        db_table_new = db_table_A
        db_code_new = db_code_A
    elif df_key.iloc[f]['freq'] == 'Q':
        if start_code_Q >= 200:
            DATA_BASE_Q_new[db_table_Q] = db_table_Q_t
            DB_name_Q_new.append(db_table_Q)
            start_table_Q += 1
            start_code_Q = 1
            db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
        db_table_Q = DB_TABLE+'Q_'+str(start_table_Q).rjust(4,'0')
        db_code_Q = DB_CODE+str(start_code_Q).rjust(3,'0')
        db_table_Q_t[db_code_Q] = DATA_BASE_Q[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_Q
        df_key.loc[f, 'db_code'] = db_code_Q
        start_code_Q += 1
        db_table_new = db_table_Q
        db_code_new = db_code_Q
    elif df_key.iloc[f]['freq'] == 'M':
        if start_code_M >= 200:
            DATA_BASE_M_new[db_table_M] = db_table_M_t
            DB_name_M_new.append(db_table_M)
            start_table_M += 1
            start_code_M = 1
            db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
        db_table_M = DB_TABLE+'M_'+str(start_table_M).rjust(4,'0')
        db_code_M = DB_CODE+str(start_code_M).rjust(3,'0')
        db_table_M_t[db_code_M] = DATA_BASE_M[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_M
        df_key.loc[f, 'db_code'] = db_code_M
        start_code_M += 1
        db_table_new = db_table_M
        db_code_new = db_code_M
    elif df_key.iloc[f]['freq'] == 'S':
        if start_code_S >= 200:
            DATA_BASE_S_new[db_table_S] = db_table_S_t
            DB_name_S_new.append(db_table_S)
            start_table_S += 1
            start_code_S = 1
            db_table_S_t = pd.DataFrame(index = HalfYear_list, columns = [])
        db_table_S = DB_TABLE+'S_'+str(start_table_S).rjust(4,'0')
        db_code_S = DB_CODE+str(start_code_S).rjust(3,'0')
        db_table_S_t[db_code_S] = DATA_BASE_S[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_S
        df_key.loc[f, 'db_code'] = db_code_S
        start_code_S += 1
        db_table_new = db_table_S
        db_code_new = db_code_S
    elif df_key.iloc[f]['freq'] == 'W':
        if start_code_W >= 200:
            DATA_BASE_W_new[db_table_W] = db_table_W_t
            DB_name_W_new.append(db_table_W)
            start_table_W += 1
            start_code_W = 1
            db_table_W_t = pd.DataFrame(index = Week_list_s, columns = [])
        db_table_W = DB_TABLE+'W_'+str(start_table_W).rjust(4,'0')
        db_code_W = DB_CODE+str(start_code_W).rjust(3,'0')
        db_table_W_t[db_code_W] = DATA_BASE_W[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_W
        df_key.loc[f, 'db_code'] = db_code_W
        start_code_W += 1
        db_table_new = db_table_W
        db_code_new = db_code_W
    
    if f == df_key.shape[0]-1:
        if db_table_A_t.empty == False:
            DATA_BASE_A_new[db_table_A] = db_table_A_t
            DB_name_A_new.append(db_table_A)
        if db_table_Q_t.empty == False:
            DATA_BASE_Q_new[db_table_Q] = db_table_Q_t
            DB_name_Q_new.append(db_table_Q)
        if db_table_M_t.empty == False:
            DATA_BASE_M_new[db_table_M] = db_table_M_t
            DB_name_M_new.append(db_table_M)
        if db_table_S_t.empty == False:
            DATA_BASE_S_new[db_table_S] = db_table_S_t
            DB_name_S_new.append(db_table_S)
        if db_table_W_t.empty == False:
            DATA_BASE_W_new[db_table_W] = db_table_W_t
            DB_name_W_new.append(db_table_W)
sys.stdout.write("\n")
DATA_BASE_A = DATA_BASE_A_new
DATA_BASE_Q = DATA_BASE_Q_new
DATA_BASE_M = DATA_BASE_M_new
DATA_BASE_S = DATA_BASE_S_new
DATA_BASE_W = DATA_BASE_W_new
DB_name_A = DB_name_A_new
DB_name_Q = DB_name_Q_new
DB_name_M = DB_name_M_new
DB_name_S = DB_name_S_new
DB_name_W = DB_name_W_new

print(df_key)
#print(DATA_BASE_t)

print('Time: ', int(time.time() - tStart),'s'+'\n')
"""if merge_file.empty == False:
    df_key, DATA_BASE = CONCATE(df_key, DATA_BASE_A, DB_name_A)
    df_key.to_excel(out_path+NAME+"key.xlsx", sheet_name=NAME+'key')
    with pd.ExcelWriter(out_path+NAME+"database.xlsx") as writer: # pylint: disable=abstract-class-instantiated
        for key in sorted(DATA_BASE.keys()):
            sys.stdout.write("\rOutputing sheet: "+str(key))
            sys.stdout.flush()
            DATA_BASE[key].to_excel(writer, sheet_name = key)
    sys.stdout.write("\n")"""
df_key.to_excel(out_path+NAME+"key.xlsx", sheet_name=NAME+'key')
with pd.ExcelWriter(out_path+NAME+"database.xlsx") as writer: # pylint: disable=abstract-class-instantiated
    for d in DB_name_A:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_A[d].empty == False:
            DATA_BASE_A[d].to_excel(writer, sheet_name = d)
    sys.stdout.write("\n")
    for d in DB_name_M:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_M[d].empty == False:
            DATA_BASE_M[d].to_excel(writer, sheet_name = d)
    sys.stdout.write("\n")
    for d in DB_name_Q:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_Q[d].empty == False:
            DATA_BASE_Q[d].to_excel(writer, sheet_name = d)
    sys.stdout.write("\n")
    for d in DB_name_S:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_S[d].empty == False:
            DATA_BASE_S[d].to_excel(writer, sheet_name = d)
    sys.stdout.write("\n")
    for d in DB_name_W:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_W[d].empty == False:
            DATA_BASE_W[d].to_excel(writer, sheet_name = d)
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')

#print('Total items not found: ',len(CONTINUE), '\n')

OLCurrency = []
SDR = []
LEFT = []
DF_NAME = list(df_key['name'])
freq_list = ['A','M','Q','S']
for i in range(AREMOS_forex.shape[0]):
    if str(AREMOS_forex.loc[i, 'code']) not in DF_NAME and str(AREMOS_forex.loc[i, 'code'])[:1] in freq_list and str(AREMOS_forex.loc[i, 'code']).find('REX') >= 0:
        LEFT.append(AREMOS_forex.loc[i, 'code'])
    if OLD_LEGACY(str(AREMOS_forex.loc[i, 'country_code'])) == 'Y' and str(AREMOS_forex.loc[i, 'code'])[:1] in freq_list and str(AREMOS_forex.loc[i, 'code']).find('REX') >= 0:
        if str(AREMOS_forex.loc[i, 'code']) not in DF_NAME:
            OLCurrency.append(AREMOS_forex.loc[i, 'code'])
    elif OLD_LEGACY(str(AREMOS_forex.loc[i, 'country_code'])) == 'S' and str(AREMOS_forex.loc[i, 'code'])[:1] in freq_list and str(AREMOS_forex.loc[i, 'code']).find('REX') >= 0:
        if str(AREMOS_forex.loc[i, 'code']) not in DF_NAME:
            SDR.append(AREMOS_forex.loc[i, 'code'])
print('Total Old Legacy Currency items not found: ', len(OLCurrency), '\n')
print('Total International Monetary Fund (IMF) SDRs items not found: ', len(SDR), '\n')
print('Items not found: ', len(LEFT), '\n')
print('Time: ', int(time.time() - tStart),'s'+'\n')
