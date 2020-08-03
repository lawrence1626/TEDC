# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date

ENCODING = 'utf-8-sig'
data_path = './output/'

def ERROR(error_text):
    print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()
def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, \
             header_=None,skiprows_=None,index_col_=None,sheet_name_=None):
    try:
        t = pd.read_excel(dir, header=header_,skiprows=skiprows_,index_col=index_col_,sheet_name=sheet_name_)
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

def CONCATE(df_key, DB_A, DB_Q, DB_M, DB_name_A, DB_name_Q, DB_name_M):
    
    DB_TABLE = 'DB_'
    DB_CODE = 'data'
    this_year = datetime.now().year + 1
    Year_list = [tmp for tmp in range(1947,this_year)]
    Quarter_list = []
    for q in range(1947,this_year):
        for r in range(1,5):
            Quarter_list.append(str(q)+'-Q'+str(r))
    Month_list = []
    for y in range(1947,this_year):
        for m in range(1,13):
            Month_list.append(str(y)+'-'+str(m).rjust(2,'0'))
    
    tStart = time.time()
    print('Reading file: MEI_key, Time: ', int(time.time() - tStart),'s'+'\n')
    KEY_DATA_t = readExcelFile(data_path+'MEI_key.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='MEI_key')
    print('Reading file: MEI_database, Time: ', int(time.time() - tStart),'s'+'\n')
    DATA_BASE_t = readExcelFile(data_path+'MEI_database.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)

    print('Concating file: MEI_key, Time: ', int(time.time() - tStart),'s'+'\n')
    KEY_DATA_t = pd.concat([KEY_DATA_t, df_key], ignore_index=True)

    print('Concating file: MEI_database, Time: ', int(time.time() - tStart),'s'+'\n')
    for d in DB_name_A:
        sys.stdout.write("\rConcating sheet: "+str(d))
        sys.stdout.flush()
        if d in DATA_BASE_t.keys():
            DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_A[d])
        else:
            DATA_BASE_t[d] = DB_A[d]
    sys.stdout.write("\n")
    for d in DB_name_Q:
        sys.stdout.write("\rConcating sheet: "+str(d))
        sys.stdout.flush()
        if d in DATA_BASE_t.keys():
            DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_Q[d])
        else:
            DATA_BASE_t[d] = DB_Q[d]
    sys.stdout.write("\n")
    for d in DB_name_M:
        sys.stdout.write("\rConcating sheet: "+str(d))
        sys.stdout.flush()
        if d in DATA_BASE_t.keys():
            DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_M[d])
        else:
            DATA_BASE_t[d] = DB_M[d]
    sys.stdout.write("\n")

    print('Time: ', int(time.time() - tStart),'s'+'\n')
    KEY_DATA_t = KEY_DATA_t.sort_values(by=['name', 'db_table'], ignore_index=True)
    repeated = 0
    repeated_index = []
    for i in range(1, len(KEY_DATA_t)):
        if KEY_DATA_t['name'][i] == KEY_DATA_t['name'][i-1]:
            repeated += 1
            repeated_index.append(i)
            #print(KEY_DATA_t['name'][i],' ',KEY_DATA_t['name'][i-1])
            key = KEY_DATA_t.iloc[i]
            DATA_BASE_t[key['db_table']] = DATA_BASE_t[key['db_table']].drop(columns = key['db_code'])
        sys.stdout.write("\r"+str(repeated)+" repeated data key(s) found")
        sys.stdout.flush()
    sys.stdout.write("\n")
    for i in repeated_index:
        sys.stdout.write("\rDropping repeated data key(s): "+str(i))
        sys.stdout.flush()
        KEY_DATA_t = KEY_DATA_t.drop([i])
    sys.stdout.write("\n")
    KEY_DATA_t.reset_index(drop=True, inplace=True)
    if KEY_DATA_t.iloc[0]['snl'] != 1:
        KEY_DATA_t.loc[0, 'snl'] = 1
    for s in range(1,KEY_DATA_t.shape[0]):
        sys.stdout.write("\rSetting new snls: "+str(s))
        sys.stdout.flush()
        KEY_DATA_t.loc[s, 'snl'] = KEY_DATA_t.loc[0, 'snl'] + s
    sys.stdout.write("\n")
    #if repeated > 0:
    print('Setting new files:\n')
    
    start_code_A = 1
    start_code_Q = 1
    start_code_M = 1
    start_table_A = 1
    start_table_Q = 1
    start_table_M = 1
    DB_A_new = {}
    DB_Q_new = {}
    DB_M_new = {}
    db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
    db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
    db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
    DB_name_A_new = []
    DB_name_Q_new = []
    DB_name_M_new = []
    db_table_new = 0
    db_code_new = 0
    for f in range(KEY_DATA_t.shape[0]):
        sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
        sys.stdout.flush()
        if KEY_DATA_t.iloc[f]['freq'] == 'A':
            if start_code_A >= 200:
                DB_A_new[db_table_A] = db_table_A_t
                DB_name_A_new.append(db_table_A)
                start_table_A += 1
                start_code_A = 1
                db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
            db_table_A = DB_TABLE+'A_'+str(start_table_A).rjust(4,'0')
            db_code_A = DB_CODE+str(start_code_A).rjust(3,'0')
            db_table_A_t[db_code_A] = DATA_BASE_t[KEY_DATA_t.iloc[f]['db_table']][KEY_DATA_t.iloc[f]['db_code']]
            KEY_DATA_t.loc[f, 'db_table'] = db_table_A
            KEY_DATA_t.loc[f, 'db_code'] = db_code_A
            start_code_A += 1
            db_table_new = db_table_A
            db_code_new = db_code_A
        elif KEY_DATA_t.iloc[f]['freq'] == 'Q':
            if start_code_Q >= 200:
                DB_Q_new[db_table_Q] = db_table_Q_t
                DB_name_Q_new.append(db_table_Q)
                start_table_Q += 1
                start_code_Q = 1
                db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
            db_table_Q = DB_TABLE+'Q_'+str(start_table_Q).rjust(4,'0')
            db_code_Q = DB_CODE+str(start_code_Q).rjust(3,'0')
            db_table_Q_t[db_code_Q] = DATA_BASE_t[KEY_DATA_t.iloc[f]['db_table']][KEY_DATA_t.iloc[f]['db_code']]
            KEY_DATA_t.loc[f, 'db_table'] = db_table_Q
            KEY_DATA_t.loc[f, 'db_code'] = db_code_Q
            start_code_Q += 1
            db_table_new = db_table_Q
            db_code_new = db_code_Q
        elif KEY_DATA_t.iloc[f]['freq'] == 'M':
            if start_code_M >= 200:
                DB_M_new[db_table_M] = db_table_M_t
                DB_name_M_new.append(db_table_M)
                start_table_M += 1
                start_code_M = 1
                db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
            db_table_M = DB_TABLE+'M_'+str(start_table_M).rjust(4,'0')
            db_code_M = DB_CODE+str(start_code_M).rjust(3,'0')
            db_table_M_t[db_code_M] = DATA_BASE_t[KEY_DATA_t.iloc[f]['db_table']][KEY_DATA_t.iloc[f]['db_code']]
            KEY_DATA_t.loc[f, 'db_table'] = db_table_M
            KEY_DATA_t.loc[f, 'db_code'] = db_code_M
            start_code_M += 1
            db_table_new = db_table_M
            db_code_new = db_code_M
        
        if f == KEY_DATA_t.shape[0]-1:
            if db_table_A_t.empty == False:
                DB_A_new[db_table_A] = db_table_A_t
                DB_name_A_new.append(db_table_A)
            if db_table_Q_t.empty == False:
                DB_Q_new[db_table_Q] = db_table_Q_t
                DB_name_Q_new.append(db_table_Q)
            if db_table_M_t.empty == False:
                DB_M_new[db_table_M] = db_table_M_t
                DB_name_M_new.append(db_table_M)
    sys.stdout.write("\n")
    DB_A = DB_A_new
    DB_Q = DB_Q_new
    DB_M = DB_M_new
    DB_name_A = DB_name_A_new
    DB_name_Q = DB_name_Q_new
    DB_name_M = DB_name_M_new

    print('Concating new files: MEI_database, Time: ', int(time.time() - tStart),'s'+'\n')
    DATA_BASE_t = {}
    for d in DB_name_A:
        sys.stdout.write("\rConcating new sheet: "+str(d))
        sys.stdout.flush()
        DATA_BASE_t[d] = DB_A[d]
    sys.stdout.write("\n")
    for d in DB_name_Q:
        sys.stdout.write("\rConcating new sheet: "+str(d))
        sys.stdout.flush()
        DATA_BASE_t[d] = DB_Q[d]
    sys.stdout.write("\n")
    for d in DB_name_M:
        sys.stdout.write("\rConcating new sheet: "+str(d))
        sys.stdout.flush()
        DATA_BASE_t[d] = DB_M[d]
    sys.stdout.write("\n")
    print(KEY_DATA_t)
    print('Time: ', int(time.time() - tStart),'s'+'\n')

    return (KEY_DATA_t, DATA_BASE_t)
