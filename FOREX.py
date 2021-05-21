# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=E1101
# pylint: disable=unbalanced-tuple-unpacking
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
import FOREX_extention as EXT
from FOREX_extention import ERROR, MERGE, NEW_KEYS, CONCATE, UPDATE, readFile, readExcelFile, FOREX_NAME, FOREX_DATA, FOREX_CROSSRATE, OLD_LEGACY
import FOREX_test as test
from FOREX_test import FOREX_identity

ENCODING = 'utf-8-sig'

NAME = EXT.NAME
data_path = './data/'
out_path = "./output/"
find_unknown = EXT.find_unknown
main_suf = EXT.main_suf
merge_suf = EXT.merge_suf
dealing_start_year = EXT.dealing_start_year
start_year = EXT.start_year
merging = EXT.merging
updating = EXT.updating
START_YEAR = EXT.START_YEAR
DF_suffix = test.DF_suffix
main_file = readExcelFile(out_path+NAME+'key'+main_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
merge_file = readExcelFile(out_path+NAME+'key'+merge_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'last', 'base', 'quote', 'snl', 'source', 'form_e', 'form_c']
start_file = 1
last_file = 9
update = EXT.update
for i in range(len(key_list)):
    if key_list[i] == 'snl':
        snl_pos = i
        break
tStart = EXT.tStart

def takeFirst(alist):
    return alist[0]

AREMOS_forex = readExcelFile(data_path+'forex2020.xlsx', header_ = [0], sheet_name_='forex')

<<<<<<< Updated upstream
FREQNAME = EXT.FREQNAME
FREQLIST = EXT.FREQLIST

=======
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
>>>>>>> Stashed changes
KEY_DATA = []
DATA_BASE_dict = {}
db_table_t_dict = {}
DB_name_dict = {}
SORT_DATA = {}
for f in FREQNAME:
    DATA_BASE_dict[f] = {}
    db_table_t_dict[f] = pd.DataFrame(index = FREQLIST[f], columns = [])
    DB_name_dict[f] = []
    SORT_DATA[f] = []
DB_TABLE = EXT.DB_TABLE
DB_CODE = EXT.DB_CODE

table_num_dict = {}
code_num_dict = {}
snl = 1
for f in FREQNAME:
    table_num_dict[f] = 1
    code_num_dict[f] = 1
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

if updating == False and DF_suffix != merge_suf:
    print('Reading file: '+NAME+'key'+DF_suffix+', Time: ', int(time.time() - tStart),'s'+'\n')
    DF_KEY = readExcelFile(out_path+NAME+'key'+DF_suffix+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'key')
    DF_KEY = DF_KEY.set_index('name')
elif updating == False and DF_suffix == merge_suf:
    DF_KEY = merge_file
    DF_KEY = DF_KEY.set_index('name')

###########################################################################  Main Function  ###########################################################################
SUFFIX = {'A':'', 'S':'.S', 'Q':'.Q', 'M':'.M', 'W':'.W'}
REPL = {'A':'', 'S':None, 'Q':'-Q', 'M':'-', 'W':None}
new_item_counts = 0

for g in range(start_file,last_file+1):
    if main_file.empty == False:
        break
    print('Reading file: '+NAME+str(g)+' Time: ', int(time.time() - tStart),'s'+'\n')
    if g == 1 or g == 2 or g == 8 or g == 9:############################################################ ECB ##################################################################
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
            freqnum = None
            freqsuffix = []
            keysuffix = []
            weekA = False
            if str(FOREX_t.columns[i][0]).find('EXR.A') >= 0:
                freqnum = 4
                freqsuffix = ['']
                frequency = 'A'
                keysuffix = ['-12-31']
                for opp in [False, True]:
                    code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                        FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                            DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, weekA=weekA)
            elif str(FOREX_t.columns[i][0]).find('EXR.H') >= 0:
                freqnum = 5
                freqsuffix = ['S1','S2']
                frequency = 'S'
                keysuffix = ['06-30','12-31']
                for opp in [False, True]:
                    code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                        FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                            DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, weekA=weekA)
            elif str(FOREX_t.columns[i][0]).find('EXR.M') >= 0:
                freqnum = 7
                freqsuffix = ['']
                frequency = 'M'
                keysuffix = ['-']
                for opp in [False, True]:
                    code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                        FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                            DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, weekA=weekA)
                if str(FOREX_t.columns[i][0]).find('SP00.E') >= 0:
                    freqnum = 5
                    freqsuffix = ['S1','S2']
                    frequency = 'S'
                    keysuffix = ['06-30','12-31']
                    for opp in [False, True]:
                        code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                            FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                                DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, weekA=weekA)
            elif str(FOREX_t.columns[i][0]).find('EXR.Q') >= 0:
                freqnum = 5
                freqsuffix = ['Q1','Q2','Q3','Q4']
                frequency = 'Q'
                keysuffix = ['03-31','06-30','09-30','12-31']
                for opp in [False, True]:
                    code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                        FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                            DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, weekA=weekA)
            elif str(FOREX_t.columns[i][0]).find('EXR.D') >= 0:
                frequency = 'W'
<<<<<<< Updated upstream
                weekA = True
                for opp in [False, True]:
                    code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                        FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                            DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, weekA=weekA)
                if str(FOREX_t.columns[i][0]).find('ISK') >= 0:
                    weekA = False
                    form_e = 'End of period (E)'
                    freqnum = 7
                    freqsuffix = ['','','','','','','']
                    frequency = 'M'
                    keysuffix = ['-25','-26','-27','-28','-29','-30','-31']
                    for opp in [False, True]:
                        code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                            FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                                DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, weekA=weekA)
                    freqnum = 5
                    freqsuffix = ['Q1','Q1','Q1','Q1','Q2','Q2','Q2','Q2','Q3','Q3','Q3','Q3','Q4','Q4','Q4','Q4']
                    frequency = 'Q'
                    keysuffix = ['03-28','03-29','03-30','03-31','06-27','06-28','06-29','06-30','09-27','09-28','09-29','09-30','12-28','12-29','12-30','12-31']
                    for opp in [False, True]:
                        code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                            FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                                DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, weekA=weekA)
                    freqnum = 5
                    freqsuffix = ['S1','S1','S1','S1','S2','S2','S2','S2']
                    frequency = 'S'
                    keysuffix = ['06-27','06-28','06-29','06-30','12-28','12-29','12-30','12-31']
                    for opp in [False, True]:
                        code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                            FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                                DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, weekA=weekA)
                    freqnum = 4
                    freqsuffix = ['','','','']
                    frequency = 'A'
                    keysuffix = ['12-28','12-29','12-30','12-31']
                    for opp in [False, True]:
                        code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                            FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                                DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, weekA=weekA)
    
    elif g >= 3 and g <= 6:############################################################ IMF ##################################################################
        FOREX_t = readExcelFile(data_path+NAME+str(g)+'.xlsx', header_ =0, index_col_=1, skiprows_=list(range(6)), sheet_name_=0)
        FOREX_t = FOREX_t.drop(columns=['Unnamed: 0', 'Scale', 'Base Year'])
=======
            
            if frequency == 'A':############################################################ Annual Data ##########################################################
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
            if frequency == 'M':############################################################ Month Data ##########################################################
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
                    Sdealing = True
                    loc1 = str(FOREX_t.columns[i][0]).find('.EUR')
                    code = str(FOREX_t.columns[i][0])[loc1-3:loc1]
                    name = frequency+COUNTRY(code)+'REXEUREECB.'+frequency
                    name2 = frequency+COUNTRY(code)+'REXEURIECB.'+frequency
                    name3 = 'S'+COUNTRY(code)+'REXEUREECB.'+'S'
                    name4 = 'S'+COUNTRY(code)+'REXEURIECB.'+'S'
                    AREMOS_key3 = AREMOS_forex.loc[AREMOS_forex['code'] == name3].to_dict('list')
                    AREMOS_key4 = AREMOS_forex.loc[AREMOS_forex['code'] == name4].to_dict('list')
                    if pd.DataFrame(AREMOS_key3).empty == True:
                        Sdealing = False
                    else:
                        if code_num_S >= 200:
                            DATA_BASE_S[db_table_S2] = db_table_S_t
                            DB_name_S.append(db_table_S2)
                            table_num_S += 1
                            code_num_S = 1
                            db_table_S_t = pd.DataFrame(index = HalfYear_list, columns = [])

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
                
                if Sdealing == True:
                    db_table_S = DB_TABLE+'S'+'_'+str(table_num_S).rjust(4,'0')
                    db_code_S = DB_CODE+str(code_num_S).rjust(3,'0')
                    db_table_S_t[db_code_S] = ['' for tmp in range(nH)]
                    code_num_S += 1
                    if code_num_S >= 200:
                        DATA_BASE_S[db_table_S] = db_table_S_t
                        DB_name_S.append(db_table_S)
                        table_num_S += 1
                        code_num_S = 1
                        db_table_S_t = pd.DataFrame(index = HalfYear_list, columns = [])
                    db_table_S2 = DB_TABLE+'S'+'_'+str(table_num_S).rjust(4,'0')
                    db_code_S2 = DB_CODE+str(code_num_S).rjust(3,'0')
                    db_table_S_t[db_code_S2] = ['' for tmp in range(nH)]
                    head = 0
                    start_found = False
                    last_found = False
                    for j in range(nH):
                        for k in range(head, len(value)):
                            if str(index[k]).find('06-30') >= 0:
                                halfyear_index = str(index[k])[:5]+'S1'
                            elif str(index[k]).find('12-31') >= 0:
                                halfyear_index = str(index[k])[:5]+'S2'
                            else:
                                halfyear_index = 'Nan'
                            if db_table_S_t.index[j] == halfyear_index:
                                if value[k] == 'nan':
                                    db_table_S_t[db_code_S][db_table_S_t.index[j]] = ''
                                    db_table_S_t[db_code_S2][db_table_S_t.index[j]] = ''
                                else:
                                    db_table_S_t[db_code_S][db_table_S_t.index[j]] = float(value[k])
                                    db_table_S_t[db_code_S2][db_table_S_t.index[j]] = round(1/float(value[k]), 4)
                                if last_found == False:
                                    last = str(db_table_S_t.index[j])
                                    last2 = last
                                    last_found = True
                                if last_found == True:
                                    if k == len(value)-1:
                                        start = str(db_table_S_t.index[j])
                                        start2 = start
                                        start_found = True
                                    elif str(value[k+1]) == 'nan':
                                        start = str(db_table_S_t.index[j])
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
                
                    desc_e = str(AREMOS_key3['description'][0])
                    if desc_e.find('FOREIGN EXCHANGE') >= 0:
                        desc_e = desc_e.replace('FOREIGN EXCHANGE',' FOREIGN EXCHANGE ').title()
                    base = str(AREMOS_key3['base currency'][0])
                    if base == 'nan':
                        base = 'Euro'
                    quote = str(AREMOS_key3['quote currency'][0])
                    if quote == 'nan':
                        quote = CURRENCY(code)
                    desc_c = ''
                    freq = 'S'
                    source = 'Official ECB & EUROSTAT Reference'
                    form_e = str(FOREX_t.columns[i][2])
                    form_c = ''
                    desc_e2 = str(AREMOS_key4['description'][0])
                    if desc_e2.find('FOREIGN EXCHANGE') >= 0:
                        desc_e2 = desc_e2.replace('FOREIGN EXCHANGE',' FOREIGN EXCHANGE ').title()
                    base2 = str(AREMOS_key4['base currency'][0])
                    if base2 == 'nan':
                        base2 = CURRENCY(code)
                    quote2 = str(AREMOS_key4['quote currency'][0])
                    if quote2 == 'nan':
                        quote2 = 'Euro'
                    desc_c2 = ''
                    form_c2 = ''

                    key_tmp= [databank, name3, db_table_S, db_code_S, desc_e, desc_c, freq, start, last, base, quote, snl, source, form_e, form_c]
                    KEY_DATA.append(key_tmp)
                    sort_tmp_S = [name, snl, db_table_S, db_code_S]
                    SORT_DATA_S.append(sort_tmp_S)
                    snl += 1
                    key_tmp2= [databank, name4, db_table_S2, db_code_S2, desc_e2, desc_c2, freq, start2, last2, base2, quote2, snl, source, form_e, form_c2]
                    KEY_DATA.append(key_tmp2)
                    sort_tmp_S2 = [name2, snl, db_table_S2, db_code_S2]
                    SORT_DATA_S.append(sort_tmp_S2)
                    snl += 1

                    code_num_S += 1
            if frequency == 'Q':############################################################ Quarter Data ##########################################################
                if code_num_Q >= 200:
                    DATA_BASE_Q[db_table_Q2] = db_table_Q_t
                    DB_name_Q.append(db_table_Q2)
                    table_num_Q += 1
                    code_num_Q = 1
                    db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
                
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
                db_table_Q = DB_TABLE+frequency+'_'+str(table_num_Q).rjust(4,'0')
                db_code_Q = DB_CODE+str(code_num_Q).rjust(3,'0')
                db_table_Q_t[db_code_Q] = ['' for tmp in range(nQ)]
                code_num_Q += 1
                if code_num_Q >= 200:
                    DATA_BASE_Q[db_table_Q] = db_table_Q_t
                    DB_name_Q.append(db_table_Q)
                    table_num_Q += 1
                    code_num_Q = 1
                    db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
                db_table_Q2 = DB_TABLE+frequency+'_'+str(table_num_Q).rjust(4,'0')
                db_code_Q2 = DB_CODE+str(code_num_Q).rjust(3,'0')
                db_table_Q_t[db_code_Q2] = ['' for tmp in range(nQ)]
                head = 0
                start_found = False
                last_found = False
                for j in range(nQ):
                    for k in range(head, len(value)):
                        if str(index[k]).find('03-31') >= 0:
                            quarter_index = str(index[k])[:5]+'Q1'
                        elif str(index[k]).find('06-30') >= 0:
                            quarter_index = str(index[k])[:5]+'Q2'
                        elif str(index[k]).find('09-30') >= 0:
                            quarter_index = str(index[k])[:5]+'Q3'
                        elif str(index[k]).find('12-31') >= 0:
                            quarter_index = str(index[k])[:5]+'Q4'
                        else:
                            quarter_index = 'Nan'
                        if db_table_Q_t.index[j] == quarter_index:
                            if value[k] == 'nan':
                                db_table_Q_t[db_code_Q][db_table_Q_t.index[j]] = ''
                                db_table_Q_t[db_code_Q2][db_table_Q_t.index[j]] = ''
                            else:
                                db_table_Q_t[db_code_Q][db_table_Q_t.index[j]] = float(value[k])
                                db_table_Q_t[db_code_Q2][db_table_Q_t.index[j]] = round(1/float(value[k]), 4)
                            if last_found == False:
                                last = str(db_table_Q_t.index[j])
                                last2 = last
                                last_found = True
                            if last_found == True:
                                if k == len(value)-1:
                                    start = str(db_table_Q_t.index[j])
                                    start2 = start
                                    start_found = True
                                elif str(value[k+1]) == 'nan':
                                    start = str(db_table_Q_t.index[j])
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
                
                key_tmp= [databank, name, db_table_Q, db_code_Q, desc_e, desc_c, freq, start, last, base, quote, snl, source, form_e, form_c]
                KEY_DATA.append(key_tmp)
                sort_tmp_Q = [name, snl, db_table_Q, db_code_Q]
                SORT_DATA_Q.append(sort_tmp_Q)
                snl += 1
                key_tmp2= [databank, name2, db_table_Q2, db_code_Q2, desc_e2, desc_c2, freq, start2, last2, base2, quote2, snl, source, form_e, form_c2]
                KEY_DATA.append(key_tmp2)
                sort_tmp_Q2 = [name2, snl, db_table_Q2, db_code_Q2]
                SORT_DATA_Q.append(sort_tmp_Q2)
                snl += 1
                
                code_num_Q += 1
            if frequency == 'S':############################################################ Semiannual Data ##########################################################
                if code_num_S >= 200:
                    DATA_BASE_S[db_table_S2] = db_table_S_t
                    DB_name_S.append(db_table_S2)
                    table_num_S += 1
                    code_num_S = 1
                    db_table_S_t = pd.DataFrame(index = HalfYear_list, columns = [])
                
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
                db_table_S = DB_TABLE+frequency+'_'+str(table_num_S).rjust(4,'0')
                db_code_S = DB_CODE+str(code_num_S).rjust(3,'0')
                db_table_S_t[db_code_S] = ['' for tmp in range(nH)]
                code_num_S += 1
                if code_num_S >= 200:
                    DATA_BASE_S[db_table_S] = db_table_S_t
                    DB_name_S.append(db_table_S)
                    table_num_S += 1
                    code_num_S = 1
                    db_table_S_t = pd.DataFrame(index = HalfYear_list, columns = [])
                db_table_S2 = DB_TABLE+frequency+'_'+str(table_num_S).rjust(4,'0')
                db_code_S2 = DB_CODE+str(code_num_S).rjust(3,'0')
                db_table_S_t[db_code_S2] = ['' for tmp in range(nH)]
                head = 0
                start_found = False
                last_found = False
                for j in range(nH):
                    for k in range(head, len(value)):
                        if str(index[k]).find('06-30') >= 0:
                            halfyear_index = str(index[k])[:5]+'S1'
                        elif str(index[k]).find('12-31') >= 0:
                            halfyear_index = str(index[k])[:5]+'S2'
                        else:
                            halfyear_index = 'Nan'
                        if db_table_S_t.index[j] == halfyear_index:
                            if value[k] == 'nan':
                                db_table_S_t[db_code_S][db_table_S_t.index[j]] = ''
                                db_table_S_t[db_code_S2][db_table_S_t.index[j]] = ''
                            else:
                                db_table_S_t[db_code_S][db_table_S_t.index[j]] = float(value[k])
                                db_table_S_t[db_code_S2][db_table_S_t.index[j]] = round(1/float(value[k]), 4)
                            if last_found == False:
                                last = str(db_table_S_t.index[j])
                                last2 = last
                                last_found = True
                            if last_found == True:
                                if k == len(value)-1:
                                    start = str(db_table_S_t.index[j])
                                    start2 = start
                                    start_found = True
                                elif str(value[k+1]) == 'nan':
                                    start = str(db_table_S_t.index[j])
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
                
                key_tmp= [databank, name, db_table_S, db_code_S, desc_e, desc_c, freq, start, last, base, quote, snl, source, form_e, form_c]
                KEY_DATA.append(key_tmp)
                sort_tmp_S = [name, snl, db_table_S, db_code_S]
                SORT_DATA_S.append(sort_tmp_S)
                snl += 1
                key_tmp2= [databank, name2, db_table_S2, db_code_S2, desc_e2, desc_c2, freq, start2, last2, base2, quote2, snl, source, form_e, form_c2]
                KEY_DATA.append(key_tmp2)
                sort_tmp_S2 = [name2, snl, db_table_S2, db_code_S2]
                SORT_DATA_S.append(sort_tmp_S2)
                snl += 1
                
                code_num_S += 1
    elif g >= 3 and g <= 6:
        FOREX_t = readFile(data_path+NAME+str(g)+'.csv', header_ = [0,1,2], index_col_=0, skiprows_=[3,4], skipfooter_=1)
        if FOREX_t.index[0] < FOREX_t.index[1]:
            FOREX_t = FOREX_t[::-1]
>>>>>>> Stashed changes
        
        nG = FOREX_t.shape[0]
        print('Total Rows:',nG,'Time: ', int(time.time() - tStart),'s'+'\n')
        #print(FOREX_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()
            
            source = 'International Financial Statistics (IFS)'
            freqnum = None
            freqsuffix = []
            keysuffix = []
            repl = None
            semiA = False
            semi = False
            if g == 3 or g == 5:
                form_e = 'End of period (E)'
                if g == 3:
                    FOREXcurrency = 'Special Drawing Rights (SDR)'
                elif g == 5:
                    FOREXcurrency = 'United States Dollar (USD)'
                for frequency in ['A','M','Q','S']:
                    if frequency == 'S':
                        freqnum = 4
                        freqsuffix = ['-S1','-S2']
                        keysuffix = ['M06','M12']
                        semi = True
                    for opp in [False, True]:
                        code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                            FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                                DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, repl=REPL[frequency], semi=semi, semiA=semiA)
            elif g == 4 or g == 6:
                form_e = 'Average of observations through period (A)'
                if g == 4:
                    FOREXcurrency = 'Special Drawing Rights (SDR)'
                elif g == 6:
                    FOREXcurrency = 'United States Dollar (USD)'
                for frequency in ['A','M','Q','S']:
                    if frequency == 'S':
                        freqnum = 4
                        freqsuffix = ['-S1','-S2']
                        keysuffix = ['Q2','Q4']
                        semiA = True
                        semi = True
                    for opp in [False, True]:
                        code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                            FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                                DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, repl=REPL[frequency], semi=semi, semiA=semiA)
                
        sys.stdout.write("\n\n") 
        
        df_key_temp = pd.DataFrame(KEY_DATA, columns = key_list)
        if g == 5:
            FOREXcurrency = 'Euro'
            form_e = 'End of period (E)'
            for frequency in ['A','M','Q','S']:
                for opp in [False, True]:
                    code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                        FOREX_CROSSRATE(g, new_item_counts, DF_KEY, df_key_temp, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                            DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency])
        elif g == 6:
            FOREXcurrency = 'Euro'
            form_e = 'Average of observations through period (A)'
            for frequency in ['A','M','Q','S']:
                for opp in [False, True]:
                    code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                        FOREX_CROSSRATE(g, new_item_counts, DF_KEY, df_key_temp, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                            DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency])
    
    elif g == 7 or g == 10 or g == 11:
        if g == 7:
            FOREX_t = readExcelFile(data_path+NAME+str(g)+'.xlsx', header_ =0, index_col_=1, skiprows_=list(range(4)), skipfooter_=3, sheet_name_=0)
            source = 'International Financial Statistics (IFS)'
            FOREXcurrency = 'United States Dollar (USD) (Millions of)'
            form_e = 'World Currency Composition of Official Foreign Exchange Reserves'
        else:
            FOREX_t = readExcelFile(data_path+NAME+str(g)+'.xlsx', header_ =0, index_col_=1, skiprows_=list(range(6)), skipfooter_=3, sheet_name_=0)
            source = 'International Financial Statistics (IFS)'
            FOREXcurrency = 'United States Dollar (USD) (Millions of)'
            if g == 9:
                form_e = 'Advanced Economies Currency Composition of Official Foreign Exchange Reserves'
            elif g == 10:
                form_e = 'Emerging and Developing Economies Currency Composition of Official Foreign Exchange Reserves'
        FOREX_t = FOREX_t.drop(columns=['Unnamed: 0'])
        
        nG = FOREX_t.shape[0]
        frequency = 'Q'
        print('Total Rows:',nG,'Time: ', int(time.time() - tStart),'s'+'\n')
        #print(FOREX_t)      
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()
            
<<<<<<< Updated upstream
            code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                    DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=False, suffix=SUFFIX[frequency], repl=REPL[frequency])
                    
    sys.stdout.write("\n\n")
    if find_unknown == True:
        print('Total New Items Found:', new_item_counts, 'Time: ', int(time.time() - tStart),'s'+'\n') 

for f in FREQNAME:
    if main_file.empty == False:
        break
    if db_table_t_dict[f].empty == False:
        if f == 'W':
            db_table_t_dict[f] = db_table_t_dict[f].reindex(FREQLIST['W_s'])
        DATA_BASE_dict[f][DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
        DB_name_dict[f].append(DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0'))      
=======
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
>>>>>>> Stashed changes

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
if main_file.empty == True:
    df_key = pd.DataFrame(KEY_DATA, columns = key_list)
else:
    if merge_file.empty == True:
        ERROR('Missing Merge File')
if updating == True:
    df_key, DATA_BASE_dict = UPDATE(merge_file, main_file, key_list, NAME, out_path, merge_suf, main_suf)
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
#print('Items not found: ', len(LEFT), '\n')
print('Time: ', int(time.time() - tStart),'s'+'\n')
if updating == False:
    if find_unknown == True:
        checkNotFound = False
    else:
        checkNotFound = True
    unknown_list, toolong_list, update_list, unfound_list = FOREX_identity(out_path, df_key, DF_KEY, checkNotFound=checkNotFound, checkDESC=True)
