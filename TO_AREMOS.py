# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time, csv
import pandas as pd
import numpy as np
from datetime import datetime, date

ENCODING = 'utf-8-sig'
data_path = './output/'
out_path = './output/'
NAME = 'GERFIN_'

def ERROR(error_text):
    print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()
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
print('Reading file: '+NAME+'database, Time: ', int(time.time() - tStart),'s'+'\n')
DATA_BASE_t = readExcelFile(data_path+NAME+'database.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)

AREMOS = []
AREMOS_DATA = []
print('Outputing AREMOS files, Time: ', int(time.time() - tStart),'s'+'\n')
for key in range(df_key.shape[0]):
    sys.stdout.write("\rLoading...("+str(round((key+1)*100/df_key.shape[0], 1))+"%)*")
    sys.stdout.flush()
    SERIES = 'SERIES<FREQ '+FREQUENCY(df_key.loc[key,'freq'])+' >'+df_key.loc[key,'name']+'!'
    SERIES_DATA = 'SERIES<FREQ '+df_key.loc[key,'freq']+' PER '+'2018D002'+\
        ' TO '+str(date.fromisoformat(df_key.loc[key,'last']).year)+'D'+date.fromisoformat(df_key.loc[key,'last']).strftime('%j')+'>!'
    #SERIES_DATA = 'SERIES<FREQ '+df_key.loc[key,'freq']+' PER '+str(date.fromisoformat(df_key.loc[key,'start']).year)+'D'+date.fromisoformat(df_key.loc[key,'start']).strftime('%j')+\
    #    ' TO '+str(date.fromisoformat(df_key.loc[key,'last']).year)+'D'+date.fromisoformat(df_key.loc[key,'last']).strftime('%j')+'>!'
    DESC = "'"+df_key.loc[key,'desc_e']+"'"+'!'
    DATA = df_key.loc[key,'name']+'='
    nA = DATA_BASE_t[df_key.loc[key,'db_table']].shape[0]
    found = False
    for ar in reversed(range(1100)):
        if str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) != 'nan' and\
            DATA_BASE_t[df_key.loc[key,'db_table']].index[ar] >= '2018-01-02' and\
            str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) != '':
            if found == True:
                DATA = DATA + ',' 
            DATA = DATA + str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']])
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

print('Time: ', int(time.time() - tStart),'s'+'\n')