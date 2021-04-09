# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=E1101
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
import requests as rq
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import webdriver_manager
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, date
import GERFIN_concat as CCT
from GERFIN_concat import ERROR, MERGE, NEW_KEYS, CONCATE, UPDATE, readFile, readExcelFile
import GERFIN_test as test
from GERFIN_test import GERFIN_identity

ENCODING = 'utf-8-sig'

NAME = 'GERFIN_'
data_path = './data2/'
out_path = "./output/"
databank = 'GERFIN'
find_unknown = False
main_suf = '?'
merge_suf = '?'
dealing_start_year = 1970
start_year = 1970
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
        start_year = dealing_start_year-2
START_YEAR = CCT.START_YEAR
DF_suffix = test.DF_suffix
main_file = readExcelFile(out_path+NAME+'key'+main_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
merge_file = readExcelFile(out_path+NAME+'key'+merge_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'last', 'base', 'quote', 'snl', 'source', 'form_e', 'form_c']
frequency = 'D'
start_file = 1
last_file = 4
update = datetime.today()
for i in range(len(key_list)):
    if key_list[i] == 'snl':
        snl_pos = i
        break
tStart = time.time()

def takeFirst(alist):
	return alist[0]

AREMOS_gerfin = readExcelFile(data_path+'AREMOS_gerfin.xlsx', header_ = [0], sheet_name_='AREMOS_gerfin')
Currency = readFile(data_path+'Currency.csv', header_ = 0)
Currency = Currency.set_index('Code').to_dict()
def CURRENCY(code):
    if code in Currency['Name']:
        return str(Currency['Name'][code])
    else:
        ERROR('貨幣代碼錯誤: '+code)

FREQNAME = {'D':'daily'}
FREQLIST = {}
FREQLIST['D'] = pd.date_range(start = str(start_year)+'-01-01', end = update).strftime('%Y-%m-%d').tolist()
FREQLIST['D'].reverse()

KEY_DATA = []
DATA_BASE_dict = {}
db_table_t_dict = {}
DB_name_dict = {}
for f in FREQNAME:
    DATA_BASE_dict[f] = {}
    db_table_t_dict[f] = pd.DataFrame(index = FREQLIST[f], columns = [])
    DB_name_dict[f] = []
DB_TABLE = 'DB_'
DB_CODE = 'data'

table_num_dict = {}
code_num_dict = {}
if merge_file.empty == False and merging == True and updating == False:
    print('Merging File: '+out_path+NAME+'key'+merge_suf+'.xlsx, Time:', int(time.time() - tStart),'s'+'\n')
    snl = int(merge_file['snl'][merge_file.shape[0]-1]+1)
    for f in FREQNAME:
        table_num_dict[f], code_num_dict[f] = MERGE(merge_file, DB_TABLE, DB_CODE, f)
    if main_file.empty == False:
        print('Main File Exists: '+out_path+NAME+'key'+main_suf+'.xlsx, Time:', int(time.time() - tStart),'s'+'\n')
        print('Reading file: '+NAME+'database'+main_suf+'.xlsx, Time: ', int(time.time() - tStart),'s'+'\n')
        main_database = readExcelFile(out_path+NAME+'database'+main_suf+'.xlsx', header_ = 0, index_col_=0)
        for s in range(main_file.shape[0]):
            sys.stdout.write("\rSetting snls: "+str(s+snl))
            sys.stdout.flush()
            main_file.loc[s, 'snl'] = s+snl
        sys.stdout.write("\n")
        print('Setting files, Time: ', int(time.time() - tStart),'s'+'\n')
        db_table_new = 0
        db_code_new = 0
        for f in range(main_file.shape[0]):
            sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
            sys.stdout.flush()
            freq = main_file.iloc[f]['freq']
            df_key, DATA_BASE_dict[freq], DB_name_dict[freq], db_table_t_dict[freq], table_num_dict[freq], code_num_dict[freq], db_table_new, db_code_new = \
                NEW_KEYS(f, freq, FREQLIST, DB_TABLE, DB_CODE, main_file, main_database, db_table_t_dict[freq], table_num_dict[freq], code_num_dict[freq], DATA_BASE_dict[freq], DB_name_dict[freq])
        sys.stdout.write("\n")
        for f in FREQNAME:
            if db_table_t_dict[f].empty == False:
                DATA_BASE_dict[f][DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
                DB_name_dict[f].append(DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0'))
else:    
    snl = 1
    for f in FREQNAME:
        table_num_dict[f] = 1
        code_num_dict[f] = 1

#print(GERFIN_t.head(10))
if updating == False and DF_suffix != merge_suf:
    print('Reading file: '+NAME+'key'+DF_suffix+', Time: ', int(time.time() - tStart),'s'+'\n')
    DF_KEY = readExcelFile(out_path+NAME+'key'+DF_suffix+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'key')
    DF_KEY = DF_KEY.set_index('name')
elif updating == False and DF_suffix == merge_suf:
    DF_KEY = merge_file
    DF_KEY = DF_KEY.set_index('name')

def GERFIN_DATA(i, name, GERFIN_t, code_num, table_num, KEY_DATA, DATA_BASE, db_table_t, DB_name, snl, freqlist, frequency, source, AREMOS_key=None, AREMOS_key2=None):
    freqlen = len(freqlist)
    NonValue = ['nan','-','.','0']
    if code_num >= 200:
        db_table2 = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        DATA_BASE[db_table2] = db_table_t
        DB_name.append(db_table2)
        table_num += 1
        code_num = 1
        db_table_t = pd.DataFrame(index = freqlist, columns = [])
    
    value = list(GERFIN_t[GERFIN_t.columns[i]])
    index = GERFIN_t[GERFIN_t.columns[i]].index
    new_table = False
    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
    db_code = DB_CODE+str(code_num).rjust(3,'0')
    db_table_t[db_code] = ['' for tmp in range(freqlen)]
    if AREMOS_key2 != None:
        code_num += 1
        if code_num >= 200:
            new_table = True
            DATA_BASE[db_table] = db_table_t
            DB_name.append(db_table)
            table_num += 1
            code_num = 1
            db_table_t2 = pd.DataFrame(index = freqlist, columns = [])
            db_table2 = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
            db_code2 = DB_CODE+str(code_num).rjust(3,'0')
            db_table_t2[db_code2] = ['' for tmp in range(freqlen)]
        else:
            db_table2 = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
            db_code2 = DB_CODE+str(code_num).rjust(3,'0')
            db_table_t[db_code2] = ['' for tmp in range(freqlen)]
    start_found = False
    last_found = False
    found = False
    for k in range(len(value)):
        try:
            freq_index = index[k].strftime('%Y-%m-%d')
        except AttributeError:
            freq_index = index[k]
        if freq_index in db_table_t.index and ((find_unknown == False and int(str(freq_index)[:4]) >= dealing_start_year) or find_unknown == True):
            if str(value[k]) in NonValue:
                db_table_t[db_code][freq_index] = ''
                if new_table == True:
                    db_table_t2[db_code2][freq_index] = ''
                elif AREMOS_key2 != None:
                    db_table_t[db_code2][freq_index] = ''
            else:
                found = True
                db_table_t[db_code][freq_index] = float(value[k])
                if new_table == True:
                    db_table_t2[db_code2][freq_index] = round(1/float(value[k]), 4)
                elif AREMOS_key2 != None:
                    db_table_t[db_code2][freq_index] = round(1/float(value[k]), 4)
                if start_found == False and found == True:
                    try:
                        start = index[k].strftime('%Y-%m-%d')
                    except AttributeError:
                        start = index[k]
                    start2 = start
                    start_found = True
                if start_found == True:
                    if k == len(value)-1:
                        last = freq_index
                        last2 = last
                        last_found = True
                    else:
                        for st in range(k+1, len(value)):
                            if str(value[st]) not in NonValue:
                                last_found = False
                            else:
                                last_found = True
                            if last_found == False:
                                break
                        if last_found == True:
                            last = freq_index
                            last2 = last
        else:
            continue
    
    if last_found == False:
        if found == True:
            ERROR('last not found: '+str(name))
    if start_found == False:
        if found == True:
            ERROR('start not found: '+str(name))                
    if found == False:
        ERROR(str(GERFIN_t.columns[i]))
    if new_table == True:
        db_table_t = db_table_t2

    desc_e = str(AREMOS_key['description'][0])
    base = str(AREMOS_key['base currency'][0])
    quote = str(AREMOS_key['quote currency'][0])
    desc_c = ''
    form_e = str(AREMOS_key['attribute'][0])
    form_c = ''
    if AREMOS_key2 != None:
        desc_e2 = str(AREMOS_key2['description'][0])
        base2 = str(AREMOS_key2['base currency'][0])
        quote2 = str(AREMOS_key2['quote currency'][0])
        desc_c2 = ''
        form_c2 = ''
    
    key_tmp= [databank, name, db_table, db_code, desc_e, desc_c, frequency, start, last, base, quote, snl, source, form_e, form_c]
    KEY_DATA.append(key_tmp)
    snl += 1
    if AREMOS_key2 != None:
        key_tmp2= [databank, name2, db_table2, db_code2, desc_e2, desc_c2, frequency, start2, last2, base2, quote2, snl, source, form_e, form_c2]
        KEY_DATA.append(key_tmp2)
        snl += 1

    code_num += 1

    return code_num, table_num, DATA_BASE, db_table_t, DB_name, snl

###########################################################################  Main Function  ###########################################################################
new_item_counts = 0

for g in range(start_file,last_file+1):
    if main_file.empty == False:
        break
    print('Reading file: '+NAME+str(g)+' Time: ', int(time.time() - tStart),'s'+'\n')
    if g == 1 or g == 4:
        #GERFIN_t = GERFIN_CRAW(g, head=[0,1,2], skip=[0,4])
        GERFIN_t = readFile(data_path+NAME+str(g)+'.csv', header_ = [0,1,2], index_col_=0, skiprows_=[0,4])
        if str(GERFIN_t.index[0]).find('/') >= 0:
            new_index = []
            for ind in GERFIN_t.index:
                new_index.append(pd.to_datetime(ind))
            GERFIN_t = GERFIN_t.reindex(new_index)
        if GERFIN_t.index[0] > GERFIN_t.index[1]:
            GERFIN_t = GERFIN_t[::-1]
        
        nG = GERFIN_t.shape[1]
        #print(GERFIN_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()

            source = 'Official ECB & EUROSTAT Reference'
            AREMOS_key = AREMOS_gerfin.loc[AREMOS_gerfin['source'] == source].loc[AREMOS_gerfin['quote currency'] == str(GERFIN_t.columns[i][1])].to_dict('list')
            AREMOS_key2 = AREMOS_gerfin.loc[AREMOS_gerfin['source'] == source].loc[AREMOS_gerfin['base currency'] == str(GERFIN_t.columns[i][1])].to_dict('list')
            if pd.DataFrame(AREMOS_key).empty == True:
                continue
            name = str(AREMOS_key['code'][0])
            name2 = str(AREMOS_key2['code'][0])
            if (name in DF_KEY.index and name2 in DF_KEY.index and find_unknown == True) or (name not in DF_KEY.index and name2 not in DF_KEY.index and find_unknown == False):
                continue
            elif name not in DF_KEY.index and name2 not in DF_KEY.index and find_unknown == True:
                new_item_counts+=2
            
            code_num_dict[frequency], table_num_dict[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl = \
                  GERFIN_DATA(i, name, GERFIN_t, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                       DB_name_dict[frequency], snl, FREQLIST[frequency], frequency, source, AREMOS_key=AREMOS_key, AREMOS_key2=AREMOS_key2)
    elif g == 2:
        GERFIN_t = readFile(data_path+NAME+str(g)+'.csv', header_ = [0,1,2], index_col_=0, skiprows_=[3,4], skipfooter_=1)
        if GERFIN_t.index[0] > GERFIN_t.index[1]:
            GERFIN_t = GERFIN_t[::-1]
        
        nG = GERFIN_t.shape[1]
        #print(GERFIN_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()

            if str(GERFIN_t.columns[i][0]).find('FLAGS') >= 0:
                continue
            source = 'Fin. Market Indicative Reference'
            AREMOS_key = AREMOS_gerfin.loc[AREMOS_gerfin['source'] == source].loc[AREMOS_gerfin['quote currency'] == CURRENCY(GERFIN_t.columns[i][2])].to_dict('list')
            AREMOS_key2 = AREMOS_gerfin.loc[AREMOS_gerfin['source'] == source].loc[AREMOS_gerfin['base currency'] == CURRENCY(GERFIN_t.columns[i][2])].to_dict('list')
            if pd.DataFrame(AREMOS_key).empty == True:
                continue
            name = str(AREMOS_key['code'][0])
            name2 = str(AREMOS_key2['code'][0])
            if (name in DF_KEY.index and name2 in DF_KEY.index and find_unknown == True) or (name not in DF_KEY.index and name2 not in DF_KEY.index and find_unknown == False):
                continue
            elif name not in DF_KEY.index and name2 not in DF_KEY.index and find_unknown == True:
                new_item_counts+=2
            
            code_num_dict[frequency], table_num_dict[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl = \
                  GERFIN_DATA(i, name, GERFIN_t, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                       DB_name_dict[frequency], snl, FREQLIST[frequency], frequency, source, AREMOS_key=AREMOS_key, AREMOS_key2=AREMOS_key2)    
    elif g == 3:
        GERFIN_t = readExcelFile(data_path+NAME+str(g)+'.xls', header_ =0, index_col_=0, sheet_name_='Daily')
        README_t = readExcelFile(data_path+NAME+str(g)+'.xls', sheet_name_='README')
        README = list(README_t[0])
        if GERFIN_t.index[0] > GERFIN_t.index[1]:
            GERFIN_t = GERFIN_t[::-1]

        nG = GERFIN_t.shape[1]
        nR = len(README)
        #print(GERFIN_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()
            if str(GERFIN_t.columns[i]).find('DEX') < 0:
                continue
            for r in range(nR):
                if README[r] == GERFIN_t.columns[i]:
                    for rr in range(r,nR):
                        if README[rr] == 'Units:':
                            if str(GERFIN_t.columns[i]).find('DEXUS') >= 0:
                                loc1 = README[rr+1].find('One ')
                                currency = README[rr+1][loc1+4:]
                            else:
                                loc1 = README[rr+1].find(' to')
                                currency = README[rr+1][:loc1]
                            break
                    break
            
            source = 'FRB NY'
            AREMOS_key = AREMOS_gerfin.loc[AREMOS_gerfin['source'] == source].loc[AREMOS_gerfin['quote currency'] == currency].to_dict('list')
            if pd.DataFrame(AREMOS_key).empty == True:
                continue
            name = str(AREMOS_key['code'][0])
            if (name in DF_KEY.index and find_unknown == True) or (name not in DF_KEY.index and find_unknown == False):
                continue
            elif name not in DF_KEY.index and find_unknown == True:
                new_item_counts+=1
            
            code_num_dict[frequency], table_num_dict[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl = \
                    GERFIN_DATA(i, name, GERFIN_t, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                        DB_name_dict[frequency], snl, FREQLIST[frequency], frequency, source, AREMOS_key=AREMOS_key)
                
    sys.stdout.write("\n\n")
    if find_unknown == True:
        print('Total New Items Found:', new_item_counts, 'Time: ', int(time.time() - tStart),'s'+'\n')  

for f in FREQNAME:
    if main_file.empty == False:
        break
    if db_table_t_dict[f].empty == False:
        DATA_BASE_dict[f][DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
        DB_name_dict[f].append(DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0'))

print('Time: ', int(time.time() - tStart),'s'+'\n')
if main_file.empty == True:
    df_key = pd.DataFrame(KEY_DATA, columns = key_list)
else:
    if merge_file.empty == True:
        ERROR('Missing Merge File')
if updating == True:
    df_key, DATA_BASE_dict = UPDATE(merge_file, main_file, key_list, NAME, out_path, merge_suf, main_suf, FREQLIST=FREQLIST)
else:
    if df_key.empty and find_unknown == False:
        ERROR('Empty dataframe')
    elif df_key.empty and find_unknown == True:
        ERROR('No new items were found.')
    df_key, DATA_BASE_dict = CONCATE(NAME, merge_suf, out_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, merge_file, DATA_BASE_dict, DB_name_dict, find_unknown=find_unknown)

print(df_key)
#print(DATA_BASE_t)

print('Time: ', int(time.time() - tStart),'s'+'\n')
df_key.to_excel(out_path+NAME+"key"+START_YEAR+".xlsx", sheet_name=NAME+'key')
with pd.ExcelWriter(out_path+NAME+"database"+START_YEAR+".xlsx") as writer: # pylint: disable=abstract-class-instantiated
    if updating == True:
        for d in DATA_BASE_dict:
            sys.stdout.write("\rOutputing sheet: "+str(d))
            sys.stdout.flush()
            if DATA_BASE_dict[d].empty == False:
                DATA_BASE_dict[d].to_excel(writer, sheet_name = d)
    else:
        for f in FREQNAME:
            for d in DATA_BASE_dict[f]:
                sys.stdout.write("\rOutputing sheet: "+str(d))
                sys.stdout.flush()
                if DATA_BASE_dict[f][d].empty == False:
                    DATA_BASE_dict[f][d].to_excel(writer, sheet_name = d)
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')
if updating == False:
    if find_unknown == True:
        checkNotFound = False
    else:
        checkNotFound = True
    unknown_list, toolong_list, update_list, unfound_list = GERFIN_identity(out_path, df_key, DF_KEY, checkNotFound=checkNotFound, checkDESC=True)