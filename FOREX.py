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
#frequency = 'D'
start_file = 1
last_file = 1
maximum = 10
TO_EXCEL = True
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
             header_=None,skiprows_=None,index_col_=None,sheet_name_=None):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,index_col=index_col_,skiprows=skiprows_)
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
        HalfYear_list.append(str(y)+'-H'+str(s))
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
#print(Week_list)
Year_list.reverse()
HalfYear_list.reverse()
Quarter_list.reverse()
Month_list.reverse()
Week_list.reverse()
nY = len(Year_list)
nH = len(HalfYear_list)
nQ = len(Quarter_list)
nM = len(Month_list)
nW = len(Week_list)
KEY_DATA = []
SORT_DATA_A = []
SORT_DATA_H = []
SORT_DATA_Q = []
SORT_DATA_M = []
SORT_DATA_W = []
DATA_BASE_A = {}
DATA_BASE_H = {}
DATA_BASE_Q = {}
DATA_BASE_M = {}
DATA_BASE_W = {}
db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
db_table_H_t = pd.DataFrame(index = HalfYear_list, columns = [])
db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
db_table_W_t = pd.DataFrame(index = Week_list, columns = [])
DB_name_A = []
DB_name_H = []
DB_name_Q = []
DB_name_M = []
DB_name_W = []
DB_TABLE = 'DB_'
DB_CODE = 'data'
"""
if merge_file.empty == False:
    snl = int(merge_file['snl'][merge_file.shape[0]-1]+1)
    for d in range(1,10000):
        if DB_TABLE+'D_'+str(d).rjust(4,'0') not in list(merge_file['db_table']):
            table_num_A = d-1
            code_t = []
            for c in range(merge_file.shape[0]):
                if merge_file['db_table'][c] == DB_TABLE+'D_'+str(d-1).rjust(4,'0'):
                    code_t.append(merge_file['db_code'][c])
            for code in range(1,200):
                if max(code_t) == DB_CODE+str(code).rjust(3,'0'):
                    code_num_A = code+1
                    break
            break
"""    
table_num_A = 1
table_num_H = 1
table_num_Q = 1
table_num_M = 1
table_num_W = 1
code_num_A = 1
code_num_H = 1
code_num_Q = 1
code_num_M = 1
code_num_W = 1
snl = 1
if code_num_A == 200:
    code_num_A = 1
if code_num_H == 200:
    code_num_H = 1
if code_num_Q == 200:
    code_num_Q = 1
if code_num_M == 200:
    code_num_M = 1
if code_num_W == 200:
    code_num_W  = 1
start_snl = snl
start_table_A = table_num_A
start_table_H = table_num_H
start_table_Q = table_num_Q
start_table_M = table_num_M
start_table_W = table_num_W
start_code_A = code_num_A
start_code_H = code_num_H
start_code_Q = code_num_Q
start_code_M = code_num_M
start_code_W = code_num_W

#print(FOREX_t.head(10))
tStart = time.time()

for g in range(start_file,last_file+1):
    print('Reading file: '+NAME+str(g)+' Time: ', int(time.time() - tStart),'s'+'\n')
    if g == 1 or g == 2 or g == 10:
        FOREX_t = readFile(data_path+NAME+str(g)+'.csv', header_ = [0,1,2], index_col_=0, skiprows_=[0,4])
        if str(FOREX_t.index[0]).find('/') >= 0:
            new_index = []
            for ind in FOREX_t.index:
                new_index.append(pd.to_datetime(ind))
            FOREX_t = FOREX_t.reindex(new_index)
        
        nG = FOREX_t.shape[1]
        #print(FOREX_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()

            if str(FOREX_t.columns[i][0]).find('EXR.A') >= 0:
                frequency = 'A'
            elif str(FOREX_t.columns[i][0]).find('EXR.H') >= 0:
                frequency = 'S'
            elif str(FOREX_t.columns[i][0]).find('EXR.M') >= 0:
                frequency = 'M'
            elif str(FOREX_t.columns[i][0]).find('EXR.Q') >= 0:
                frequency = 'Q'
            elif str(FOREX_t.columns[i][0]).find('EXR.D') >= 0:
                frequency = 'W'
            
            if frequency == 'A':
                if code_num_A >= 200:
                    DATA_BASE_A[db_table_A2] = db_table_A_t
                    DB_name_A.append(db_table_A2)
                    table_num_A += 1
                    code_num_A = 1
                    db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
                
                if str(FOREX_t.columns[i][0]).find('SP00.A') >= 0:
                    loc1 = str(FOREX_t.columns[i][0]).find('.EUR')
                    code = str(FOREX_t.columns[i][0])[loc1-3:loc1]
                    name = frequency+COUNTRY(code)+'REXEURDECB'
                    name2 = frequency+COUNTRY(code)+'REXEURECB'
                elif str(FOREX_t.columns[i][0]).find('SP00.E') >= 0:
                    loc1 = str(FOREX_t.columns[i][0]).find('.EUR')
                    code = str(FOREX_t.columns[i][0])[loc1-3:loc1]
                    name = frequency+COUNTRY(code)+'REXEUREECB'
                    name2 = frequency+COUNTRY(code)+'REXEURIECB'

                AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == name].to_dict('list')
                AREMOS_key2 = AREMOS_forex.loc[AREMOS_forex['code'] == name2].to_dict('list')
                if pd.DataFrame(AREMOS_key).empty == True:
                    continue
                
                name = name+'.A'
                name2 = name2+'.A'

                value = list(FOREX_t[FOREX_t.columns[i]])
                index = FOREX_t[FOREX_t.columns[i]].index
                db_table_A = DB_TABLE+frequency+'_'+str(table_num_A).rjust(4,'0')
                db_code_A = DB_CODE+str(code_num_A).rjust(3,'0')
                db_table_A_t[db_code_A] = ['' for tmp in range(nY)]
                code_num_A += 1
                if code_num_A >= 200:
                    DATA_BASE_A[db_table_A] = db_table_A_t
                    DB_name_A.append(db_table_A)
                    table_num_A += 1
                    code_num_A = 1
                    db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
                db_table_A2 = DB_TABLE+frequency+'_'+str(table_num_A).rjust(4,'0')
                db_code_A2 = DB_CODE+str(code_num_A).rjust(3,'0')
                db_table_A_t[db_code_A2] = ['' for tmp in range(nY)]
                head = 0
                start_found = False
                last_found = False
                for j in range(nY):
                    for k in range(head, len(value)):
                        if db_table_A_t.index[j] == int(str(index[k])[:4]) and str(index[k]).find('-12-31') >= 0:
                            if value[k] == 'nan':
                                db_table_A_t[db_code_A][db_table_A_t.index[j]] = ''
                                db_table_A_t[db_code_A2][db_table_A_t.index[j]] = ''
                            else:
                                db_table_A_t[db_code_A][db_table_A_t.index[j]] = float(value[k])
                                db_table_A_t[db_code_A2][db_table_A_t.index[j]] = round(1/float(value[k]), 4)
                            if last_found == False:
                                last = int(db_table_A_t.index[j])
                                last2 = last
                                last_found = True
                            if last_found == True:
                                if k == len(value)-1:
                                    start = int(db_table_A_t.index[j])
                                    start2 = start
                                    start_found = True
                                elif str(value[k+1]) == 'nan':
                                    start = int(db_table_A_t.index[j])
                                    start2 = start
                                    start_found = True
                            head = k+1
                            break
                        else:
                            continue
                if last_found == False:
                    ERROR('last not found:'+str(FOREX_t.columns[i]))
                elif start_found == False:
                    ERROR('start not found:'+str(FOREX_t.columns[i]))                
            
                desc_e = str(AREMOS_key['description'][0])
                if desc_e.find('FOREIGN EXCHANGE') >= 0:
                    desc_e = desc_e.replace('FOREIGN EXCHANGE',' FOREIGN EXCHANGE ').title()
                base = str(AREMOS_key['base currency'][0])
                if base == 'nan':
                    base = 'Euro'
                quote = str(AREMOS_key['quote currency'][0])
                if quote == 'nan':
                    quote = CURRENCY(code)
                desc_c = ''
                freq = frequency
                source = 'Official ECB & EUROSTAT Reference'
                form_e = str(FOREX_t.columns[i][2])
                form_c = ''
                desc_e2 = str(AREMOS_key2['description'][0])
                if desc_e2.find('FOREIGN EXCHANGE') >= 0:
                    desc_e2 = desc_e2.replace('FOREIGN EXCHANGE',' FOREIGN EXCHANGE ').title()
                base2 = str(AREMOS_key2['base currency'][0])
                if base2 == 'nan':
                    base2 = CURRENCY(code)
                quote2 = str(AREMOS_key2['quote currency'][0])
                if quote2 == 'nan':
                    quote2 = 'Euro'
                desc_c2 = ''
                form_c2 = ''
                
                key_tmp= [databank, name, db_table_A, db_code_A, desc_e, desc_c, freq, start, last, base, quote, snl, source, form_e, form_c]
                KEY_DATA.append(key_tmp)
                sort_tmp_A = [name, snl, db_table_A, db_code_A]
                SORT_DATA_A.append(sort_tmp_A)
                snl += 1
                key_tmp2= [databank, name2, db_table_A2, db_code_A2, desc_e2, desc_c2, freq, start2, last2, base2, quote2, snl, source, form_e, form_c2]
                KEY_DATA.append(key_tmp2)
                sort_tmp_A2 = [name2, snl, db_table_A2, db_code_A2]
                SORT_DATA_A.append(sort_tmp_A2)
                snl += 1

                code_num_A += 1
            if frequency == 'M':
                if code_num_M >= 200:
                    DATA_BASE_M[db_table_M2] = db_table_M_t
                    DB_name_M.append(db_table_M2)
                    table_num_M += 1
                    code_num_M = 1
                    db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
                
                if str(FOREX_t.columns[i][0]).find('SP00.A') >= 0:
                    loc1 = str(FOREX_t.columns[i][0]).find('.EUR')
                    code = str(FOREX_t.columns[i][0])[loc1-3:loc1]
                    name = frequency+COUNTRY(code)+'REXEURDECB.'+frequency
                    name2 = frequency+COUNTRY(code)+'REXEURECB.'+frequency
                elif str(FOREX_t.columns[i][0]).find('SP00.E') >= 0:
                    loc1 = str(FOREX_t.columns[i][0]).find('.EUR')
                    code = str(FOREX_t.columns[i][0])[loc1-3:loc1]
                    name = frequency+COUNTRY(code)+'REXEUREECB.'+frequency
                    name2 = frequency+COUNTRY(code)+'REXEURIECB.'+frequency

                AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == name].to_dict('list')
                AREMOS_key2 = AREMOS_forex.loc[AREMOS_forex['code'] == name2].to_dict('list')
                if pd.DataFrame(AREMOS_key).empty == True:
                    continue

                value = list(FOREX_t[FOREX_t.columns[i]])
                index = FOREX_t[FOREX_t.columns[i]].index
                db_table_M = DB_TABLE+frequency+'_'+str(table_num_M).rjust(4,'0')
                db_code_M = DB_CODE+str(code_num_M).rjust(3,'0')
                db_table_M_t[db_code_M] = ['' for tmp in range(nM)]
                code_num_M += 1
                if code_num_M >= 200:
                    DATA_BASE_M[db_table_M] = db_table_M_t
                    DB_name_M.append(db_table_M)
                    table_num_M += 1
                    code_num_M = 1
                    db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
                db_table_M2 = DB_TABLE+frequency+'_'+str(table_num_M).rjust(4,'0')
                db_code_M2 = DB_CODE+str(code_num_M).rjust(3,'0')
                db_table_M_t[db_code_M2] = ['' for tmp in range(nM)]
                head = 0
                start_found = False
                last_found = False
                for j in range(nM):
                    for k in range(head, len(value)):
                        if db_table_M_t.index[j] == str(index[k])[:7]:
                            if value[k] == 'nan':
                                db_table_M_t[db_code_M][db_table_M_t.index[j]] = ''
                                db_table_M_t[db_code_M2][db_table_M_t.index[j]] = ''
                            else:
                                db_table_M_t[db_code_M][db_table_M_t.index[j]] = float(value[k])
                                db_table_M_t[db_code_M2][db_table_M_t.index[j]] = round(1/float(value[k]), 4)
                            if last_found == False:
                                last = str(db_table_M_t.index[j])
                                last2 = last
                                last_found = True
                            if last_found == True:
                                if k == len(value)-1:
                                    start = str(db_table_M_t.index[j])
                                    start2 = start
                                    start_found = True
                                elif str(value[k+1]) == 'nan':
                                    start = str(db_table_M_t.index[j])
                                    start2 = start
                                    start_found = True
                            head = k+1
                            break
                        else:
                            continue
                if last_found == False:
                    ERROR('last not found:'+str(FOREX_t.columns[i]))
                elif start_found == False:
                    ERROR('start not found:'+str(FOREX_t.columns[i]))                
            
                desc_e = str(AREMOS_key['description'][0])
                if desc_e.find('FOREIGN EXCHANGE') >= 0:
                    desc_e = desc_e.replace('FOREIGN EXCHANGE',' FOREIGN EXCHANGE ').title()
                base = str(AREMOS_key['base currency'][0])
                if base == 'nan':
                    base = 'Euro'
                quote = str(AREMOS_key['quote currency'][0])
                if quote == 'nan':
                    quote = CURRENCY(code)
                desc_c = ''
                freq = frequency
                source = 'Official ECB & EUROSTAT Reference'
                form_e = str(FOREX_t.columns[i][2])
                form_c = ''
                desc_e2 = str(AREMOS_key2['description'][0])
                if desc_e2.find('FOREIGN EXCHANGE') >= 0:
                    desc_e2 = desc_e2.replace('FOREIGN EXCHANGE',' FOREIGN EXCHANGE ').title()
                base2 = str(AREMOS_key2['base currency'][0])
                if base2 == 'nan':
                    base2 = CURRENCY(code)
                quote2 = str(AREMOS_key2['quote currency'][0])
                if quote2 == 'nan':
                    quote2 = 'Euro'
                desc_c2 = ''
                form_c2 = ''
                
                key_tmp= [databank, name, db_table_M, db_code_M, desc_e, desc_c, freq, start, last, base, quote, snl, source, form_e, form_c]
                KEY_DATA.append(key_tmp)
                sort_tmp_M = [name, snl, db_table_M, db_code_M]
                SORT_DATA_M.append(sort_tmp_M)
                snl += 1
                key_tmp2= [databank, name2, db_table_M2, db_code_M2, desc_e2, desc_c2, freq, start2, last2, base2, quote2, snl, source, form_e, form_c2]
                KEY_DATA.append(key_tmp2)
                sort_tmp_M2 = [name2, snl, db_table_M2, db_code_M2]
                SORT_DATA_M.append(sort_tmp_M2)
                snl += 1
                
                code_num_M += 1
    elif g >= 3 and g <= 6:
        FOREX_t = readFile(data_path+NAME+str(g)+'.csv', header_ = [0,1,2], index_col_=0, skiprows_=[3,4], skipfooter_=1)
        if FOREX_t.index[0] < FOREX_t.index[1]:
            FOREX_t = FOREX_t[::-1]
        
        nG = FOREX_t.shape[1]
        #print(FOREX_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()

            if str(FOREX_t.columns[i][0]).find('FLAGS') >= 0:
                continue
            
            if code_num_A >= 200:
                DATA_BASE_A[db_table_A2] = db_table_A_t
                DB_name_A.append(db_table_A2)
                table_num_A += 1
                code_num_A = 1
                db_table_A_t = pd.DataFrame(index = Day_list, columns = [])
            
            AREMOS_key = AREMOS_forex.loc[AREMOS_forex['source'] == 'Fin. Market Indicative Reference'].loc[AREMOS_forex['quote currency'] == CURRENCY(FOREX_t.columns[i][2])].to_dict('list')
            AREMOS_key2 = AREMOS_forex.loc[AREMOS_forex['source'] == 'Fin. Market Indicative Reference'].loc[AREMOS_forex['base currency'] == CURRENCY(FOREX_t.columns[i][2])].to_dict('list')
            if pd.DataFrame(AREMOS_key).empty == True:
                continue
            name = str(AREMOS_key['code'][0])
            name2 = str(AREMOS_key2['code'][0])
            
            value = list(FOREX_t[FOREX_t.columns[i]])
            index = FOREX_t[FOREX_t.columns[i]].index
            db_table_A = DB_TABLE+frequency+'_'+str(table_num_A).rjust(4,'0')
            db_code_A = DB_CODE+str(code_num_A).rjust(3,'0')
            db_table_A_t[db_code_A] = ['' for tmp in range(nD)]
            code_num_A += 1
            if code_num_A >= 200:
                DATA_BASE_A[db_table_A] = db_table_A_t
                DB_name_A.append(db_table_A)
                table_num_A += 1
                code_num_A = 1
                db_table_A_t = pd.DataFrame(index = Day_list, columns = [])
            db_table_A2 = DB_TABLE+'D_'+str(table_num_A).rjust(4,'0')
            db_code_A2 = DB_CODE+str(code_num_A).rjust(3,'0')
            db_table_A_t[db_code_A2] = ['' for tmp in range(nD)]
            head = 0
            start_found = False
            last = str(index[0]).replace(' 00:00:00','')
            last2 = last
            for k in range(len(value)):
                find = False
                for j in range(head, nD):
                    if db_table_A_t.index[j] == str(index[k]).replace(' 00:00:00',''):
                        find = True
                        if value[k] == '.':
                            db_table_A_t[db_code_A][db_table_A_t.index[j]] = ''
                            db_table_A_t[db_code_A2][db_table_A_t.index[j]] = ''
                        else:
                            db_table_A_t[db_code_A][db_table_A_t.index[j]] = float(value[k])
                            db_table_A_t[db_code_A2][db_table_A_t.index[j]] = round(1/float(value[k]), 4)
                        head = j+1
                        break
                if start_found == False:
                    if k == len(value)-1:
                        start = str(index[k]).replace(' 00:00:00','')
                        start2 = start
                        start_found = True
                    elif str(value[k+1]) == 'nan':
                        start = str(index[k]).replace(' 00:00:00','')
                        start2 = start
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
            desc_e2 = str(AREMOS_key2['description'][0])
            base2 = str(AREMOS_key2['base currency'][0])
            quote2 = str(AREMOS_key2['quote currency'][0])
            desc_c2 = ''
            form_c2 = ''
            
            key_tmp= [databank, name, db_table_A, db_code_A, desc_e, desc_c, freq, start, last, base, quote, snl, source, form_e, form_c]
            KEY_DATA.append(key_tmp)
            sort_tmp_A = [name, snl, db_table_A, db_code_A]
            SORT_DATA_A.append(sort_tmp_A)
            snl += 1
            key_tmp2= [databank, name2, db_table_A2, db_code_A2, desc_e2, desc_c2, freq, start2, last2, base2, quote2, snl, source, form_e, form_c2]
            KEY_DATA.append(key_tmp2)
            sort_tmp_A2 = [name2, snl, db_table_A2, db_code_A2]
            SORT_DATA_A.append(sort_tmp_A2)
            snl += 1

            code_num_A += 1
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
    DATA_BASE_A[db_table_A] = db_table_A_t
    DB_name_A.append(db_table_A)
if db_table_H_t.empty == False:
    DATA_BASE_H[db_table_H] = db_table_H_t
    DB_name_H.append(db_table_H)
if db_table_M_t.empty == False:
    DATA_BASE_M[db_table_M] = db_table_M_t
    DB_name_M.append(db_table_M)
if db_table_Q_t.empty == False:
    DATA_BASE_Q[db_table_Q] = db_table_Q_t
    DB_name_Q.append(db_table_Q)
if db_table_W_t.empty == False:
    DATA_BASE_W[db_table_W] = db_table_W_t
    DB_name_W.append(db_table_W)       

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
df_key = pd.DataFrame(KEY_DATA, columns = key_list)
ERROR('')
df_key = df_key.sort_values(by=['name', 'db_table'], ignore_index=True)
if df_key.iloc[0]['snl'] != start_snl:
    df_key.loc[0, 'snl'] = start_snl
for s in range(1,df_key.shape[0]):
    sys.stdout.write("\rSetting new snls: "+str(s))
    sys.stdout.flush()
    df_key.loc[s, 'snl'] = df_key.loc[0, 'snl'] + s
sys.stdout.write("\n")
#if repeated_A > 0 or repeated_Q > 0:
print('Setting new files, Time: ', int(time.time() - tStart),'s'+'\n')

DATA_BASE_A_new = {}
db_table_A_t = pd.DataFrame(index = Day_list, columns = [])
DB_name_A_new = []
db_table_new = 0
db_code_new = 0
for f in range(df_key.shape[0]):
    sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
    sys.stdout.flush()
    if df_key.iloc[f]['freq'] == 'D':
        if start_code_A >= 200:
            DATA_BASE_A_new[db_table_A] = db_table_A_t
            DB_name_A_new.append(db_table_A)
            start_table_A += 1
            start_code_A = 1
            db_table_A_t = pd.DataFrame(index = Day_list, columns = [])
        db_table_A = DB_TABLE+'D_'+str(start_table_A).rjust(4,'0')
        db_code_A = DB_CODE+str(start_code_A).rjust(3,'0')
        db_table_A_t[db_code_A] = DATA_BASE_A[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_A
        df_key.loc[f, 'db_code'] = db_code_A
        start_code_A += 1
        db_table_new = db_table_A
        db_code_new = db_code_A
    
    if f == df_key.shape[0]-1:
        if db_table_A_t.empty == False:
            DATA_BASE_A_new[db_table_A] = db_table_A_t
            DB_name_A_new.append(db_table_A)

sys.stdout.write("\n")
DATA_BASE_A = DATA_BASE_A_new
DB_name_A = DB_name_A_new

print(df_key)
#print(DATA_BASE_t)

print('Time: ', int(time.time() - tStart),'s'+'\n')
if TO_EXCEL == True:
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

    print('Time: ', int(time.time() - tStart),'s'+'\n')
"""
def FREQUENCY(freq):
    if freq == 'D':
        return 'Daily'
    elif freq == 'W':
        return 'Weekly'
    elif freq == 'M':
        return 'Monthly'
    elif freq == 'Q':
        return 'Quarterly'
    elif freq == 'S':
        return 'Semiannual'
    elif freq == 'A':
        return 'Annual'

AREMOS = []
AREMOS_DATA = []
print('Outputing AREMOS files:'+'\n')
for key in range(df_key.shape[0]):
    sys.stdout.write("\rLoading...("+str(round((key+1)*100/df_key.shape[0], 1))+"%)*")
    sys.stdout.flush()
    SERIES = 'SERIES<FREQ '+FREQUENCY(frequency)+' >'+df_key.loc[key,'name']+'!'
    SERIES_DATA = 'SERIES<FREQ '+frequency+' PER '+str(date.fromisoformat(df_key.loc[key,'start']).year)+'D'+date.fromisoformat(df_key.loc[key,'start']).strftime('%j')+\
        ' TO '+str(date.fromisoformat(df_key.loc[key,'last']).year)+'D'+date.fromisoformat(df_key.loc[key,'last']).strftime('%j')+'>!'
    DESC = "'"+df_key.loc[key,'desc_e']+"'"+'!'
    DATA = df_key.loc[key,'name']+'='
    nA = DATA_BASE_A[df_key.loc[key,'db_table']].shape[0]
    found = False
    for ar in reversed(range(nA)):
        if DATA_BASE_A[df_key.loc[key,'db_table']].index[ar] >= df_key.loc[key,'start']:
            if found == True:
                DATA = DATA + ',' 
            if str(DATA_BASE_A[df_key.loc[key,'db_table']].loc[DATA_BASE_A[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) == 'nan':
                DATA = DATA + 'M'
            else:
                DATA = DATA + str(DATA_BASE_A[df_key.loc[key,'db_table']].loc[DATA_BASE_A[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']])
            found = True
    end = ';'
    DATA = DATA + end
    #DATA = DATA.replace('"','')
    AREMOS.append(SERIES)
    AREMOS.append(DESC)
    AREMOS.append(end)
    AREMOS_DATA.append(SERIES_DATA)
    AREMOS_DATA.append(DATA)
sys.stdout.write("\n\n")

aremos = pd.DataFrame(AREMOS)
aremos_data = pd.DataFrame(AREMOS_DATA)
aremos.to_csv(out_path+NAME+"doc.txt", header=False, index=False, sep='|', quoting=csv.QUOTE_NONE, quotechar='')
aremos_data.to_csv(out_path+NAME+"data.txt", header=False, index=False, sep='|', quoting=csv.QUOTE_NONE, quotechar='')
"""
print('Time: ', int(time.time() - tStart),'s'+'\n')