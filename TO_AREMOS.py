# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time, csv
import pandas as pd
import numpy as np
from datetime import datetime, date

ENCODING = 'utf-8-sig'
data_path = './output/'
out_path = './output/'
NAME = 'EIKON_'
from_year = '2001'
to_year = '2011'
part_file = True
make_doc = False

def SPECIAL(special_text):
    print('\n= ! = '+special_text+'\n\n')
    #with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
    #    f.write(special_text)
    sys.exit()
def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=False, \
             header_=None,skiprows_=None,index_col_=None,sheet_name_=None):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,index_col=index_col_,skiprows=skiprows_)
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            SPECIAL('Several files input')
    except:
        try: #檔案編碼格式不同
            t = pd.read_excel(dir, header=header_,skiprows=skiprows_,index_col=index_col_,sheet_name=sheet_name_)
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

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

tStart = time.time()
print('Reading file: '+NAME+'key, Time: ', int(time.time() - tStart),'s'+'\n')
df_key = readExcelFile(data_path+NAME+'key.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'key')
try:
    print('Reading file: '+NAME+'database, Time: ', int(time.time() - tStart),'s'+'\n')
    DATA_BASE_t = readExcelFile(data_path+NAME+'database.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
except:
    with open(data_path+'database_num.txt','r',encoding=ENCODING) as f:  #用with一次性完成open、close檔案
        database_num = int(f.read().replace('\n', ''))
    DATA_BASE_t = {}
    for i in range(1,database_num+1):
        print('Reading file: '+NAME+'database_'+str(i)+', Time: ', int(time.time() - tStart),'s'+'\n')
        DB_t = readExcelFile(data_path+NAME+'database_'+str(i)+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False, sheet_name_=None)
        for d in DB_t.keys():
            DATA_BASE_t[d] = DB_t[d]
    
AREMOS = []
AREMOS_DATA = []
print('Outputing AREMOS files, Time: ', int(time.time() - tStart),'s'+'\n')
for key in range(df_key.shape[0]):
    sys.stdout.write("\rLoading...("+str(round((key+1)*100/df_key.shape[0], 1))+"%)*")
    sys.stdout.flush()
    
    DATA = df_key.loc[key,'name']+'='
    nA = DATA_BASE_t[df_key.loc[key,'db_table']].shape[0]
    
    if part_file == True:
        if df_key.loc[key,'start'] <= to_year+'-01-01':
            SERIES_DATA = 'SERIES<FREQ '+df_key.loc[key,'freq']+' PER '+from_year+'D001'+' TO '+to_year+'D001'+'>!'
            #SERIES_DATA = 'SERIES<FREQ '+df_key.loc[key,'freq']+' PER '+from_year+'D001'+' TO '+str(date.fromisoformat(df_key.loc[key,'last']).year)+'D'+date.fromisoformat(df_key.loc[key,'last']).strftime('%j')+'>!'
            found = False
            for ar in reversed(range(nA)):
                if DATA_BASE_t[df_key.loc[key,'db_table']].index[ar] >= from_year+'-01-01' and DATA_BASE_t[df_key.loc[key,'db_table']].index[ar] <= to_year+'-01-01':
                #if DATA_BASE_t[df_key.loc[key,'db_table']].index[ar] >= from_year+'-01-01' and DATA_BASE_t[df_key.loc[key,'db_table']].index[ar] <= df_key.loc[key,'last']:
                    if found == True:
                        DATA = DATA + ',' 
                    if str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) == 'nan' or\
                        str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) == '':
                        DATA = DATA + 'M'
                    else:
                        DATA = DATA + str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']])
                    found = True
        else:
            continue
    else:
        SERIES_DATA = 'SERIES<FREQ '+df_key.loc[key,'freq']+' PER '+str(date.fromisoformat(df_key.loc[key,'start']).year)+'D'+date.fromisoformat(df_key.loc[key,'start']).strftime('%j')+\
            ' TO '+str(date.fromisoformat(df_key.loc[key,'last']).year)+'D'+date.fromisoformat(df_key.loc[key,'last']).strftime('%j')+'>!'
        found = False
        for ar in reversed(range(nA)):
            if DATA_BASE_t[df_key.loc[key,'db_table']].index[ar] >= df_key.loc[key,'start'] and DATA_BASE_t[df_key.loc[key,'db_table']].index[ar] <= df_key.loc[key,'last']:
                if found == True:
                    DATA = DATA + ',' 
                if str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) == 'nan' or\
                    str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) == '':
                    DATA = DATA + 'M'
                else:
                    DATA = DATA + str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']])
                found = True
    
    end = ';'
    DATA = DATA + end
    #DATA = DATA.replace('"','')
    if make_doc == True:
        SERIES = 'SERIES<FREQ '+FREQUENCY(df_key.loc[key,'freq'])+' >'+df_key.loc[key,'name']+'!'
        DESC = "'"+df_key.loc[key,'desc_e']+"'"+'!'
        AREMOS.append(SERIES)
        AREMOS.append(DESC)
        AREMOS.append(end)
    AREMOS_DATA.append(SERIES_DATA)
    AREMOS_DATA.append(DATA)
sys.stdout.write("\n\n")

if make_doc == True:
    aremos = pd.DataFrame(AREMOS)
    aremos.to_csv(out_path+NAME+"doc.txt", header=False, index=False, sep='|', quoting=csv.QUOTE_NONE, quotechar='')
aremos_data = pd.DataFrame(AREMOS_DATA)
aremos_data.to_csv(out_path+NAME+"data.txt", header=False, index=False, sep='|', quoting=csv.QUOTE_NONE, quotechar='')

print('Time: ', int(time.time() - tStart),'s'+'\n')