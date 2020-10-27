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
start_year = 1999
start_file = 1
last_file = 4
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
             header_=None,skiprows_=None,index_col_=None,usecols_=None,sheet_name_=None):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,index_col=index_col_,skiprows=skiprows_,usecols=usecols_)
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
Country = readFile(data_path+'Country.csv', header_ = 0)
ECB = Country.set_index('Currency_Code').to_dict()
IMF = Country.set_index('IMF_country').to_dict()
def COUNTRY(code):
    if code in ECB['Country_Code']:
        return str(ECB['Country_Code'][code])
    elif code in IMF['Country_Code']:
        return str(IMF['Country_Code'][code])
    else:
        ERROR('國家代碼錯誤: '+code)
def CURRENCY(code):
    if code in ECB['Currency_Name']:
        return str(ECB['Currency_Name'][code])
    elif code in IMF['Currency_Name']:
        return str(IMF['Currency_Name'][code])
    else:
        ERROR('貨幣代碼錯誤: '+code)

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
Week_list = []
for y in range(start_year,this_year):
    for w in range(1,54):
        Week_list.append(str(y)+'-W'+str(w).rjust(2,'0'))

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
if code_num_A == 200:
    code_num_A = 1
if code_num_S == 200:
    code_num_S = 1
if code_num_Q == 200:
    code_num_Q = 1
if code_num_M == 200:
    code_num_M = 1
if code_num_W == 200:
    code_num_W  = 1
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

before1 = ['FOREIGN EXCHANGE',') PER','DATA)',')FROM','SOURCE','NOTE','RATESDR','RATES','MARKET RATE','OFFICIAL RATE','PRINCIPAL RATE','USING']
after1 = [' FOREIGN EXCHANGE ',') PER ','DATA): ',') FROM',', SOURCE',', NOTE','RATE SDR','RATES ','MARKET RATE ','OFFICIAL RATE ','PRINCIPAL RATE ','USING ']
before2 = ['Ecb','1 Ecu','Sdr','Ifs','Ihs','Imf','Iso','Exchange S ','Rate S ','Am','Pm','Of ',"People S"]
after2 = ['ECB','1 ECU','SDR','IFS','IHS','IMF','ISO','Exchanges ','Rates ','am','pm','of ',"People's"]
before3 = ['CYPrus']
after3 = ['Cyprus']

def FOREX_ECB(ind, FOREX_t, AREMOS_forex, code_num, table_num, KEY_DATA, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, source, freqlist, freqnum, freqsuffix, frequency, keysuffix, tab, form_e, FOREXcurrency, opp=False, suffix=''):
    freqlen = len(freqlist)
    if code_num >= 200:
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        DATA_BASE[db_table] = db_table_t
        DB_name.append(db_table)
        table_num += 1
        code_num = 1
        db_table_t = pd.DataFrame(index = freqlist, columns = [])
    
    if str(FOREX_t.columns[ind][0]).find('SP00.A') >= 0:
        loc1 = str(FOREX_t.columns[ind][0]).find('.EUR')
        code = str(FOREX_t.columns[ind][0])[loc1-3:loc1]
        if opp == False:
            name = frequency+COUNTRY(code)+'REXEURDECB'+suffix
        else:
            name = frequency+COUNTRY(code)+'REXEURECB'+suffix
    elif str(FOREX_t.columns[ind][0]).find('SP00.E') >= 0:
        loc1 = str(FOREX_t.columns[ind][0]).find('.EUR')
        code = str(FOREX_t.columns[ind][0])[loc1-3:loc1]
        if opp == False:
            name = frequency+COUNTRY(code)+'REXEUREECB'+suffix
        else:
            name = frequency+COUNTRY(code)+'REXEURIECB'+suffix
    
    AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == name].to_dict('list')
    if pd.DataFrame(AREMOS_key).empty == True:
        if opp == False:
            CONTINUE.append(name)
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name
    
    value = list(FOREX_t[FOREX_t.columns[ind]])
    index = FOREX_t[FOREX_t.columns[ind]].index
    new_table = False
    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
    db_code = DB_CODE+str(code_num).rjust(3,'0')
    db_table_t[db_code] = ['' for tmp in range(freqlen)]
    
    head = 0
    start_found = False
    last_found = False
    for j in range(freqlen):
        for k in range(head, len(value)):
            for word in range(len(keysuffix)):
                if str(index[k]).find(keysuffix[word]) >= 0:
                    freq_index = str(index[k])[:freqnum]+freqsuffix[word]
                    if frequency == 'A':
                        freq_index = int(freq_index)
                    break
                else:
                    freq_index = 'Nan'
            if freq_index in db_table_t.index:
                if str(value[k]) == 'nan':
                    db_table_t[db_code][freq_index] = ''
                else:
                    if opp == False:
                        db_table_t[db_code][freq_index] = float(value[k])
                    else:
                        db_table_t[db_code][freq_index] = round(1/float(value[k]), 4)
                    if start_found == False:
                        if frequency == 'A':
                            start = int(freq_index)
                        else:
                            start = str(freq_index)
                        start_found = True
                    if start_found == True:
                        if j == freqlen-1:
                            if frequency == 'A':
                                last = int(freq_index)
                            else:
                                last = str(freq_index)
                            last_found = True
                        try:
                            if str(value[k+tab]) == 'nan':
                                if frequency == 'A':
                                    last = int(freq_index)
                                else:
                                    last = str(freq_index)
                                last_found = True
                        except IndexError:
                            if frequency == 'A':
                                last = int(freq_index)
                            else:
                                last = str(freq_index)
                            last_found = True
                head = k+1
                break
            else:
                continue
    if start_found == False:
        ERROR('start not found:'+str(FOREX_t.columns[ind]))
    elif last_found == False:
        ERROR('last not found:'+str(FOREX_t.columns[ind]))                

    desc_e = str(AREMOS_key['description'][0])
    if desc_e.find('FOREIGN EXCHANGE') >= 0:
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
    base = str(AREMOS_key['base currency'][0])
    if base == 'nan':
        if opp == False:
            base = FOREXcurrency
        else:
            base = CURRENCY(code)
    quote = str(AREMOS_key['quote currency'][0])
    if quote == 'nan':
        if opp == False:
            quote = CURRENCY(code)
        else:
            quote = FOREXcurrency
    desc_c = ''
    form_c = ''
    if desc_e == 'nan':
        desc_e = 'Exchange Rate: '+quote+' per '+base+', '+source+', '+form_e
    
    key_tmp= [databank, name, db_table, db_code, desc_e, desc_c, frequency, start, last, base, quote, snl, source, form_e, form_c]
    KEY_DATA.append(key_tmp)
    sort_tmp = [name, snl, db_table, db_code]
    SORT_DATA.append(sort_tmp)
    snl += 1

    code_num += 1

    return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name

def FOREX_IMF(ind, FOREX_t, AREMOS_forex, code_num, table_num, KEY_DATA, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, source, freqlist, frequency, repl, form_e, FOREXcurrency, opp=False, suffix=''):
    freqlen = len(freqlist)
    if code_num >= 200:
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        DATA_BASE[db_table] = db_table_t
        DB_name.append(db_table)
        table_num += 1
        code_num = 1
        db_table_t = pd.DataFrame(index = freqlist, columns = [])
    
    if form_e == 'End of period (E)':
        if opp == False:
            name = frequency+COUNTRY(FOREX_t.index[ind])+'REXSDRE'+suffix
        else:
            name = frequency+COUNTRY(FOREX_t.index[ind])+'REXSDRDE'+suffix
    elif form_e == 'Average of observations through period (A)':
        if opp == False:
            name = frequency+COUNTRY(FOREX_t.index[ind])+'REXSDRA'+suffix
        else:
            name = frequency+COUNTRY(FOREX_t.index[ind])+'REXSDRDA'+suffix
    
    AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == name].to_dict('list')
    if pd.DataFrame(AREMOS_key).empty == True:
        if opp == False:
            CONTINUE.append(name)
            db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
            return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name
        else:
            if form_e == 'End of period (E)':
                AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == frequency+COUNTRY(FOREX_t.index[ind])+'REXSDRE'+suffix].to_dict('list')
            elif form_e == 'Average of observations through period (A)':
                AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == frequency+COUNTRY(FOREX_t.index[ind])+'REXSDRA'+suffix].to_dict('list')
            if pd.DataFrame(AREMOS_key).empty == False:
                temp = AREMOS_key['base currency'][0]
                AREMOS_key['base currency'][0] = AREMOS_key['quote currency'][0]
                AREMOS_key['quote currency'][0] = temp
                AREMOS_key['description'][0] = AREMOS_key['description'][0].replace(str(AREMOS_key['base currency'][0]),'base currency').replace(str(AREMOS_key['quote currency'][0]),str(AREMOS_key['base currency'][0])).replace('base currency',str(AREMOS_key['quote currency'][0]))
            else:
                db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
                return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name
    
    value = list(FOREX_t.loc[FOREX_t.index[ind]])
    index = FOREX_t.loc[FOREX_t.index[ind]].index
    new_table = False
    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
    db_code = DB_CODE+str(code_num).rjust(3,'0')
    db_table_t[db_code] = ['' for tmp in range(freqlen)]
    
    head = 0
    start_found = False
    last_found = False
    found = False
    for k in range(len(value)):
        if str(index[k]).find(frequency) >= 0 or str(index[k]).isnumeric():
            if frequency == 'A':
                freq_index = int(index[k])
            else:
                freq_index = str(index[k]).replace(frequency,repl)
            if freq_index in db_table_t.index:
                if str(value[k]) == '...':
                    db_table_t[db_code][freq_index] = ''
                else:
                    found = True
                    if opp == False:
                        db_table_t[db_code][freq_index] = float(value[k])
                    else:
                        db_table_t[db_code][freq_index] = round(1/float(value[k]), 10)
                    if start_found == False:
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
                        for st in range(k+1, len(value)):
                            try:
                                if (str(index[st]).find(frequency) >= 0 or str(index[st]).isnumeric()) and str(value[st]) != '...':
                                    last_found = False
                                    break
                                else:
                                    last_found = True
                            except IndexError:
                                if frequency == 'A':
                                    last = int(freq_index)
                                else:
                                    last = str(freq_index)
                                last_found = True
                                break
                        if last_found == True:
                            if frequency == 'A':
                                last = int(freq_index)
                            else:
                                last = str(freq_index)
            else:
                continue
        #else:
        #    ERROR('Index Error: '+str(index[k]))
    if start_found == False:
        if found == True:
            ERROR('start not found:'+str(FOREX_t.index[ind]))
    elif last_found == False:
        if found == True:
            ERROR('last not found:'+str(FOREX_t.index[ind]))
    if found == False:
        start = 'Nan'
        last = 'Nan'               

    desc_e = str(AREMOS_key['description'][0])
    if desc_e.find('FOREIGN EXCHANGE') >= 0:
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
    base = str(AREMOS_key['base currency'][0])
    if base == 'nan':
        if opp == False:
            base = FOREXcurrency
        else:
            base = CURRENCY(FOREX_t.index[ind])
    quote = str(AREMOS_key['quote currency'][0])
    if quote == 'nan':
        if opp == False:
            quote = CURRENCY(FOREX_t.index[ind])
        else:
            quote = FOREXcurrency
    desc_c = ''
    form_c = ''
    if desc_e == 'nan':
        desc_e = 'Exchange Rate: '+quote+' per '+base+', '+source+', '+form_e
    
    key_tmp= [databank, name, db_table, db_code, desc_e, desc_c, frequency, start, last, base, quote, snl, source, form_e, form_c]
    KEY_DATA.append(key_tmp)
    sort_tmp = [name, snl, db_table, db_code]
    SORT_DATA.append(sort_tmp)
    snl += 1

    code_num += 1

    return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name

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
                tab = 12
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A2, db_table_A_t, DB_name_A = FOREX_ECB(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, freqnum, freqsuffix, frequency, keysuffix, tab, form_e, FOREXcurrency, opp=False)
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A2, db_table_A_t, DB_name_A = FOREX_ECB(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, freqnum, freqsuffix, frequency, keysuffix, tab, form_e, FOREXcurrency, opp=True)
            elif str(FOREX_t.columns[i][0]).find('EXR.H') >= 0:
                freqnum = 5
                freqsuffix = ['S1','S2']
                frequency = 'S'
                keysuffix = ['06-30','12-31']
                tab = 6
                code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S2, db_table_S_t, DB_name_S = FOREX_ECB(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, freqnum, freqsuffix, frequency, keysuffix, tab, form_e, FOREXcurrency, opp=False, suffix='.S')
                code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S2, db_table_S_t, DB_name_S = FOREX_ECB(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, freqnum, freqsuffix, frequency, keysuffix, tab, form_e, FOREXcurrency, opp=True, suffix='.S')
            elif str(FOREX_t.columns[i][0]).find('EXR.M') >= 0:
                freqnum = 7
                freqsuffix = ['']
                frequency = 'M'
                keysuffix = ['-']
                tab = 1
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M2, db_table_M_t, DB_name_M = FOREX_ECB(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, freqnum, freqsuffix, frequency, keysuffix, tab, form_e, FOREXcurrency, opp=False, suffix='.M')
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M2, db_table_M_t, DB_name_M = FOREX_ECB(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, freqnum, freqsuffix, frequency, keysuffix, tab, form_e, FOREXcurrency, opp=True, suffix='.M')
                if str(FOREX_t.columns[i][0]).find('SP00.E') >= 0:
                    freqnum = 5
                    freqsuffix = ['S1','S2']
                    frequency = 'S'
                    keysuffix = ['06-30','12-31']
                    tab = 6
                    code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S2, db_table_S_t, DB_name_S = FOREX_ECB(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, freqnum, freqsuffix, frequency, keysuffix, tab, form_e, FOREXcurrency, opp=False, suffix='.S')
                    code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S2, db_table_S_t, DB_name_S = FOREX_ECB(i, FOREX_t, AREMOS_forex, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, source, HalfYear_list, freqnum, freqsuffix, frequency, keysuffix, tab, form_e, FOREXcurrency, opp=True, suffix='.S')
            elif str(FOREX_t.columns[i][0]).find('EXR.Q') >= 0:
                freqnum = 5
                freqsuffix = ['Q1','Q2','Q3','Q4']
                frequency = 'Q'
                keysuffix = ['03-31','06-30','09-30','12-31']
                tab = 3
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q2, db_table_Q_t, DB_name_Q = FOREX_ECB(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, freqnum, freqsuffix, frequency, keysuffix, tab, form_e, FOREXcurrency, opp=False, suffix='.Q')
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q2, db_table_Q_t, DB_name_Q = FOREX_ECB(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, freqnum, freqsuffix, frequency, keysuffix, tab, form_e, FOREXcurrency, opp=True, suffix='.Q')
            elif str(FOREX_t.columns[i][0]).find('EXR.D') >= 0:
                frequency = 'W'
            
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
            FOREXcurrency = 'Special Drawing Rights (SDR)'
            if g == 3:
                form_e = 'End of period (E)'
                frequency = 'A'
                repl = ''
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A2, db_table_A_t, DB_name_A = FOREX_IMF(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, repl, form_e, FOREXcurrency, opp=False)
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A2, db_table_A_t, DB_name_A = FOREX_IMF(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, repl, form_e, FOREXcurrency, opp=True)
                frequency = 'M'
                repl = '-'
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M2, db_table_M_t, DB_name_M = FOREX_IMF(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, repl, form_e, FOREXcurrency, opp=False, suffix='.M')
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M2, db_table_M_t, DB_name_M = FOREX_IMF(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, repl, form_e, FOREXcurrency, opp=True, suffix='.M')
                frequency = 'Q'
                repl = '-Q'
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q2, db_table_Q_t, DB_name_Q = FOREX_IMF(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, repl, form_e, FOREXcurrency, opp=False, suffix='.Q')
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q2, db_table_Q_t, DB_name_Q = FOREX_IMF(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, repl, form_e, FOREXcurrency, opp=True, suffix='.Q')
            if g == 4:
                form_e = 'Average of observations through period (A)'
                frequency = 'A'
                repl = ''
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A2, db_table_A_t, DB_name_A = FOREX_IMF(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, repl, form_e, FOREXcurrency, opp=False)
                code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A2, db_table_A_t, DB_name_A = FOREX_IMF(i, FOREX_t, AREMOS_forex, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, source, Year_list, frequency, repl, form_e, FOREXcurrency, opp=True)
                frequency = 'M'
                repl = '-'
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M2, db_table_M_t, DB_name_M = FOREX_IMF(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, repl, form_e, FOREXcurrency, opp=False, suffix='.M')
                code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M2, db_table_M_t, DB_name_M = FOREX_IMF(i, FOREX_t, AREMOS_forex, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, source, Month_list, frequency, repl, form_e, FOREXcurrency, opp=True, suffix='.M')
                frequency = 'Q'
                repl = '-Q'
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q2, db_table_Q_t, DB_name_Q = FOREX_IMF(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, repl, form_e, FOREXcurrency, opp=False, suffix='.Q')
                code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q2, db_table_Q_t, DB_name_Q = FOREX_IMF(i, FOREX_t, AREMOS_forex, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, source, Quarter_list, frequency, repl, form_e, FOREXcurrency, opp=True, suffix='.Q')
            
    elif g >= 7 and g <= 9:
        FOREX_t = readExcelFile(data_path+NAME+str(g)+'.xls', header_ =0, index_col_=0, sheet_name_='Daily')
        README_t = readExcelFile(data_path+NAME+str(g)+'.xls', sheet_name_='README')
        README = list(README_t[0])
        if FOREX_t.index[0] < FOREX_t.index[1]:
            FOREX_t = FOREX_t[::-1]
    
        nG = FOREX_t.shape[1]
        nR = len(README)
        #print(FOREX_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()
            if str(FOREX_t.columns[i]).find('DEX') < 0:
                continue
            for r in range(nR):
                if README[r] == FOREX_t.columns[i]:
                    for rr in range(r,nR):
                        if README[rr] == 'Units:':
                            if str(FOREX_t.columns[i]).find('DEXUS') >= 0:
                                loc1 = README[rr+1].find('One ')
                                currency = README[rr+1][loc1+4:]
                            else:
                                loc1 = README[rr+1].find(' to')
                                currency = README[rr+1][:loc1]
                            break
                    break
            
            if code_num_A >= 200:
                DATA_BASE_A[db_table_A2] = db_table_A_t
                DB_name_A.append(db_table_A2)
                table_num_A += 1
                code_num_A = 1
                db_table_A_t = pd.DataFrame(index = Day_list, columns = [])
            
            AREMOS_key = AREMOS_forex.loc[AREMOS_forex['source'] == 'FRB NY'].loc[AREMOS_forex['quote currency'] == currency].to_dict('list')
            if pd.DataFrame(AREMOS_key).empty == True:
                continue
            name = str(AREMOS_key['code'][0])
            
            value = list(FOREX_t[FOREX_t.columns[i]])
            index = FOREX_t[FOREX_t.columns[i]].index
            db_table_A = DB_TABLE+'D_'+str(table_num_A).rjust(4,'0')
            db_code_A = DB_CODE+str(code_num_A).rjust(3,'0')
            db_table_A_t[db_code_A] = ['' for tmp in range(nD)]
            head = 0
            start_found = False
            last = str(index[0]).replace(' 00:00:00','')
            for k in range(len(value)):
                find = False
                for j in range(head, nD):
                    if db_table_A_t.index[j] == str(index[k]).replace(' 00:00:00',''):
                        find = True
                        if value[k] == 0:
                            db_table_A_t[db_code_A][db_table_A_t.index[j]] = ''
                        else:
                            db_table_A_t[db_code_A][db_table_A_t.index[j]] = float(value[k])
                        head = j+1
                        break
                if start_found == False:
                    if k == len(value)-1:
                        start = str(index[k]).replace(' 00:00:00','')
                        start_found = True
                    elif str(value[k+1]) == 'nan':
                        start = str(index[k]).replace(' 00:00:00','')
                        start_found = True
                if find == False:
                    ERROR(str(FOREX_t.columns[i]))        
        
            desc_e = str(AREMOS_key['description'][0])
            base = str(AREMOS_key['base currency'][0])
            quote = str(AREMOS_key['quote currency'][0])
            desc_c = ''
            freq = frequency
            source = str(AREMOS_key['source'][0])
            form_e = str(AREMOS_key['attribute'][0])
            form_c = ''
            
            key_tmp= [databank, name, db_table_A, db_code_A, desc_e, desc_c, freq, start, last, base, quote, snl, source, form_e, form_c]
            KEY_DATA.append(key_tmp)
            sort_tmp_A = [name, snl, db_table_A, db_code_A]
            SORT_DATA_A.append(sort_tmp_A)
            snl += 1

            code_num_A += 1
                
    sys.stdout.write("\n\n") 

if db_table_A_t.empty == False:
    DATA_BASE_A[db_table_A2] = db_table_A_t
    DB_name_A.append(db_table_A2)
if db_table_S_t.empty == False:
    DATA_BASE_S[db_table_S2] = db_table_S_t
    DB_name_S.append(db_table_S2)
if db_table_M_t.empty == False:
    DATA_BASE_M[db_table_M2] = db_table_M_t
    DB_name_M.append(db_table_M2)
if db_table_Q_t.empty == False:
    DATA_BASE_Q[db_table_Q2] = db_table_Q_t
    DB_name_Q.append(db_table_Q2)
if db_table_W_t.empty == False:
    DATA_BASE_W[db_table_W2] = db_table_W_t
    DB_name_W.append(db_table_W2)       

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_A.sort(key=takeFirst)
repeated_A = 0
for i in range(1, len(SORT_DATA_A)):
    if SORT_DATA_A[i][0] == SORT_DATA_A[i-1][0]:
        repeated_A += 1
        #print(SORT_DATA_A[i][0],' ',SORT_DATA_A[i-1][1],' ',SORT_DATA_A[i][1],' ',SORT_DATA_A[i][2],' ',SORT_DATA_A[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_A[i][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_A[SORT_DATA_A[i][2]] = DATA_BASE_A[SORT_DATA_A[i][2]].drop(columns = SORT_DATA_A[i][3])
        if DATA_BASE_A[SORT_DATA_A[i][2]].empty == True:
            DB_name_A.remove(SORT_DATA_A[i][2])
    sys.stdout.write("\r"+str(repeated_A)+" repeated annual data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_Q.sort(key=takeFirst)
repeated_Q = 0
for i in range(1, len(SORT_DATA_Q)):
    if SORT_DATA_Q[i][0] == SORT_DATA_Q[i-1][0]:
        repeated_Q += 1
        #print(SORT_DATA_Q[i][0],' ',SORT_DATA_Q[i-1][1],' ',SORT_DATA_Q[i][1],' ',SORT_DATA_Q[i][2],' ',SORT_DATA_Q[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_Q[i][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_Q[SORT_DATA_Q[i][2]] = DATA_BASE_Q[SORT_DATA_Q[i][2]].drop(columns = SORT_DATA_Q[i][3])
        if DATA_BASE_Q[SORT_DATA_Q[i][2]].empty == True:
            DB_name_Q.remove(SORT_DATA_Q[i][2])
    sys.stdout.write("\r"+str(repeated_Q)+" repeated quarter data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_M.sort(key=takeFirst)
repeated_M = 0
for i in range(1, len(SORT_DATA_M)):
    if SORT_DATA_M[i][0] == SORT_DATA_M[i-1][0]:
        repeated_M += 1
        #print(SORT_DATA_M[i][0],' ',SORT_DATA_M[i-1][1],' ',SORT_DATA_M[i][1],' ',SORT_DATA_M[i][2],' ',SORT_DATA_M[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_M[i][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_M[SORT_DATA_M[i][2]] = DATA_BASE_M[SORT_DATA_M[i][2]].drop(columns = SORT_DATA_M[i][3])
        if DATA_BASE_M[SORT_DATA_M[i][2]].empty == True:
            DB_name_M.remove(SORT_DATA_M[i][2])
    sys.stdout.write("\r"+str(repeated_M)+" repeated month data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_S.sort(key=takeFirst)
repeated_S = 0
for i in range(1, len(SORT_DATA_S)):
    if SORT_DATA_S[i][0] == SORT_DATA_S[i-1][0]:
        repeated_S += 1
        #print(SORT_DATA_S[i][0],' ',SORT_DATA_S[i-1][1],' ',SORT_DATA_S[i][1],' ',SORT_DATA_S[i][2],' ',SORT_DATA_S[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_S[i][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_S[SORT_DATA_S[i][2]] = DATA_BASE_S[SORT_DATA_S[i][2]].drop(columns = SORT_DATA_S[i][3])
        if DATA_BASE_S[SORT_DATA_S[i][2]].empty == True:
            DB_name_S.remove(SORT_DATA_S[i][2])
    sys.stdout.write("\r"+str(repeated_S)+" repeated semiannual data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_W.sort(key=takeFirst)
repeated_W = 0
for i in range(1, len(SORT_DATA_W)):
    if SORT_DATA_W[i][0] == SORT_DATA_W[i-1][0]:
        repeated_W += 1
        #print(SORT_DATA_W[i][0],' ',SORT_DATA_W[i-1][1],' ',SORT_DATA_W[i][1],' ',SORT_DATA_W[i][2],' ',SORT_DATA_W[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_W[i][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_W[SORT_DATA_W[i][2]] = DATA_BASE_W[SORT_DATA_W[i][2]].drop(columns = SORT_DATA_W[i][3])
        if DATA_BASE_W[SORT_DATA_W[i][2]].empty == True:
            DB_name_W.remove(SORT_DATA_W[i][2])
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
db_table_W_t = pd.DataFrame(index = Week_list, columns = [])
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
            db_table_W_t = pd.DataFrame(index = Week_list, columns = [])
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
    for d in DB_name_Q:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_Q[d].empty == False:
            DATA_BASE_Q[d].to_excel(writer, sheet_name = d)
    sys.stdout.write("\n")
    for d in DB_name_M:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_M[d].empty == False:
            DATA_BASE_M[d].to_excel(writer, sheet_name = d)
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

print(CONTINUE)
print('Time: ', int(time.time() - tStart),'s'+'\n')
