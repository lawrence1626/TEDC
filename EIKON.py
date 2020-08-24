# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date
#from cif_new import createDataFrameFromOECD
from EIKON_concat import CONCATE, readExcelFile

ENCODING = 'utf-8-sig'

NAME = 'EIKON_'
data_path = './data/'
out_path = "./output/"
databank = 'EIKON'
#freq = 'A'
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'unit', 'name_ord', 'snl', 'book', 'form_e', 'form_c']
merge_file = readExcelFile(out_path+'EIKON_key.xlsx', header_ = 0, sheet_name_='EIKON_key')
#dataset_list = ['QNA', 'QNA_DRCHIVE']
#frequency_list = ['A','Q']
frequency = 'D'
start_file = 1
last_file = 2

# 回報錯誤、儲存錯誤檔案並結束程式
def ERROR(error_text):
    print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()

def readFile(dir, default=pd.DataFrame(), acceptNoFile=False, \
             header_=None,skiprows_=None,index_col_=None,encoding_=ENCODING):
    try:
        t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,\
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

def takeFirst(alist):
	return alist[0]
"""
country = readFile(data_path+'Country.csv', header_ = 0, index_col_=[0])
country.to_dict()

def COUNTRY_CODE(location):
    if location in country['Country_Code']:
        return country['Country_Code'][location]
    else:
        ERROR('國家代碼錯誤: '+location)

def COUNTRY_NAME(location):
    if location in country['Country_Name']:
        return country['Country_Name'][location]
    else:
        ERROR('找不到國家: '+location)

form_e_file = readExcelFile(data_path+'EIKON_form_e.xlsx', acceptNoFile=False, header_ = 0, sheet_name_='EIKON_form_e')
form_e_dict = {}
for form in form_e_file:
    form_e_dict[form] = form_e_file[form].dropna().to_list()
"""

Day_list = pd.date_range(start = '1/1/1947', end = datetime.today()).strftime('%Y-%m-%d').tolist()
Day_list.reverse()
nD = len(Day_list)
KEY_DATA = []
SORT_DATA_D = []
DATA_BASE_D = {}
db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
DB_name_D = []
DB_TABLE = 'DB_'
DB_CODE = 'data'

if merge_file.empty == False:
    snl = int(merge_file['snl'][merge_file.shape[0]-1]+1)
    for d in range(1,10000):
        if DB_TABLE+'D_'+str(d).rjust(4,'0') not in list(merge_file['db_table']):
            table_num_D = d-1
            code_t = []
            for c in range(merge_file.shape[0]):
                if merge_file['db_table'][c] == DB_TABLE+'D_'+str(d-1).rjust(4,'0'):
                    code_t.append(merge_file['db_code'][c])
            for code in range(1,200):
                if max(code_t) == DB_CODE+str(code).rjust(3,'0'):
                    code_num_D = code+1
                    break
            break
    
else:
    table_num_D = 1
    code_num_D = 1
    snl = 1
if code_num_D == 200:
    code_num_D = 1
start_snl = snl
start_table_D = table_num_D
start_code_D = code_num_D

#print(EIKON_t.head(10))
tStart = time.time()
#c_list = list(country.index)
#c_list.sort()

for g in range(start_file,last_file+1):
    print('Reading file: '+NAME+str(g)+' Time: ', int(time.time() - tStart),'s'+'\n')
    EIKON_t = readExcelFile(data_path+NAME+str(g)+'.xlsx', acceptNoFile=False, header_ = [0,1,2], sheet_name_= None)
    
    for sheet in EIKON_t:
        print('Reading sheet: '+str(sheet)+' Time: ', int(time.time() - tStart),'s'+'\n')
        EIKON_t[sheet].set_index(EIKON_t[sheet].columns[0], inplace = True)
        nG = EIKON_t[sheet].shape[1]
            
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()
            
            if EIKON_t[sheet].columns[i][0] == '#ERROR':
                continue
            
            if code_num_D >= 200:
                DATA_BASE_D[db_table_D] = db_table_D_t
                DB_name_D.append(db_table_D)
                table_num_D += 1
                code_num_D = 1
                db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
            
            name = frequency+'_'+str(EIKON_t[sheet].columns[i][1])+'.d'
        
            value = EIKON_t[sheet][EIKON_t[sheet].columns[i]]
            db_table_D = DB_TABLE+'A_'+str(table_num_D).rjust(4,'0')
            db_code_D = DB_CODE+str(code_num_D).rjust(3,'0')
            db_table_D_t[db_code_D] = ['' for tmp in range(nD)]
            end_found = False
            start_found = False
            head = 0
            for k in range(value.shape[0]):
                for j in range(head, nD):
                    if db_table_D_t.index[j] == str(value.index[k]).replace(' 00:00:00',''):
                        db_table_D_t[db_code_D][db_table_D_t.index[j]] = value[k]
                        head = j
                        if end_found == False:
                            if str(value[k]) != 'nan':
                                end_found = True
                        if end_found == True and start_found == False:
                            if k == value.shape[0]-1:
                                start_found = True
                                start = str(value.index[k]).replace(' 00:00:00','')
                            elif str(value[k]) == 'nan':
                                start_found = True
                                start = str(value.index[k-1]).replace(' 00:00:00','')
                        break
            
            #Subject = subjects_list[EIKON_t[sheet].columns[i][1]]
            #Measure = measures_list[EIKON_t[sheet].columns[i][2]]
            #PowerCode = EIKON_t[sheet].columns[i][4]
            Unit = EIKON_t[sheet].columns[i][2]
            desc_e = str(EIKON_t[sheet].columns[i][0]).replace('$TO',' $ TO ').replace('$','Dollars').replace('TO','per')
            form_e = ''
            
            desc_c = ''
            freq = frequency
            unit = str(Unit)
            name_ord = 'US dollars'
            book = ''
            form_c = ''
            
            key_tmp= [databank, name, db_table_D, db_code_D, desc_e, desc_c, freq, start, unit, name_ord, snl, book, form_e, form_c]
            KEY_DATA.append(key_tmp)
            sort_tmp_D = [name, snl, db_table_D, db_code_D]
            SORT_DATA_D.append(sort_tmp_D)
            snl += 1

            code_num_D += 1
                
        sys.stdout.write("\n\n") 

if db_table_D_t.empty == False:
    DATA_BASE_D[db_table_D] = db_table_D_t
    DB_name_D.append(db_table_D)       

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_D.sort(key=takeFirst)
repeated_D = 0
for i in range(1, len(SORT_DATA_D)):
    if SORT_DATA_D[i][0] == SORT_DATA_D[i-1][0]:
        repeated_D += 1
        #print(SORT_DATA_D[i][0],' ',SORT_DATA_D[i-1][1],' ',SORT_DATA_D[i][1],' ',SORT_DATA_D[i][2],' ',SORT_DATA_D[i][3])
        for key in KEY_DATA:
            if key[10] == SORT_DATA_D[i][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_D[SORT_DATA_D[i][2]] = DATA_BASE_D[SORT_DATA_D[i][2]].drop(columns = SORT_DATA_D[i][3])
        if DATA_BASE_D[SORT_DATA_D[i][2]].empty == True:
            DB_name_D.remove(SORT_DATA_D[i][2])
    sys.stdout.write("\r"+str(repeated_D)+" repeated daily data key(s) found")
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
#if repeated_D > 0 or repeated_Q > 0:
print('Setting new files, Time: ', int(time.time() - tStart),'s'+'\n')

DATA_BASE_D_new = {}
db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
DB_name_D_new = []
db_table_new = 0
db_code_new = 0
for f in range(df_key.shape[0]):
    sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
    sys.stdout.flush()
    if df_key.iloc[f]['freq'] == 'D':
        if start_code_D >= 200:
            DATA_BASE_D_new[db_table_D] = db_table_D_t
            DB_name_D_new.append(db_table_D)
            start_table_D += 1
            start_code_D = 1
            db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
        db_table_D = DB_TABLE+'D_'+str(start_table_D).rjust(4,'0')
        db_code_D = DB_CODE+str(start_code_D).rjust(3,'0')
        db_table_D_t[db_code_D] = DATA_BASE_D[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_D
        df_key.loc[f, 'db_code'] = db_code_D
        start_code_D += 1
        db_table_new = db_table_D
        db_code_new = db_code_D
    
    if f == df_key.shape[0]-1:
        if db_table_D_t.empty == False:
            DATA_BASE_D_new[db_table_D] = db_table_D_t
            DB_name_D_new.append(db_table_D)

sys.stdout.write("\n")
DATA_BASE_D = DATA_BASE_D_new
DB_name_D = DB_name_D_new

print(df_key)
#print(DATA_BASE_t)

print('Time: ', int(time.time() - tStart),'s'+'\n')
if merge_file.empty == False:
    df_key, DATA_BASE = CONCATE(df_key, DATA_BASE_D, DB_name_D)
    df_key.to_excel(out_path+NAME+"key.xlsx", sheet_name=NAME+'key')
    with pd.ExcelWriter(out_path+NAME+"database.xlsx") as writer: # pylint: disable=abstract-class-instantiated
        endl = True
        for key in sorted(DATA_BASE.keys()):
            if key.find('DB_D') >= 0:
                sys.stdout.write("\rOutputing sheet: "+str(key))
                sys.stdout.flush()
            DATA_BASE[key].to_excel(writer, sheet_name = key)
    sys.stdout.write("\n")
else:
    df_key.to_excel(out_path+NAME+"key.xlsx", sheet_name=NAME+'key')
    with pd.ExcelWriter(out_path+NAME+"database.xlsx") as writer: # pylint: disable=abstract-class-instantiated
        for d in DB_name_D:
            sys.stdout.write("\rOutputing sheet: "+str(d))
            sys.stdout.flush()
            if DATA_BASE_D[d].empty == False:
                DATA_BASE_D[d].to_excel(writer, sheet_name = d)
    sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')