# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=E1101
# pylint: disable=unbalanced-tuple-unpacking
import math, re, sys, calendar, os, copy, time, logging
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
from FOREX_extention import ERROR, MERGE, NEW_KEYS, CONCATE, UPDATE, readFile, readExcelFile, FOREX_NAME, FOREX_DATA, FOREX_CROSSRATE, OLD_LEGACY, PRESENT, FOREX_WEB, FOREX_IMF, COUNTRY
import FOREX_test as test
from FOREX_test import FOREX_identity
FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT, handlers=[logging.FileHandler("LOG.log", 'w', EXT.ENCODING)], datefmt='%Y-%m-%d %I:%M:%S %p')

ENCODING = EXT.ENCODING

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
excel_suffix = EXT.excel_suffix
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
LOG = ['excel_suffix', 'merging', 'updating', 'find_unknown','dealing_start_year']
for key in LOG:
    logging.info(key+': '+str(locals()[key])+'\n')
log = logging.getLogger()
stream = logging.StreamHandler(sys.stdout)
stream.setFormatter(logging.Formatter('%(message)s'))
log.addHandler(stream)
sys.stdout.write("\n\n")
if merging:
    logging.info('Process: File Merging\n')
elif updating:
    logging.info('Process: File Updating\n')
else:
    logging.info('Data Processing\n')

def takeFirst(alist):
    return alist[0]

AREMOS_forex = readExcelFile(data_path+'forex2020.xlsx', header_ = [0], sheet_name_='forex')

FREQNAME = EXT.FREQNAME
FREQLIST = EXT.FREQLIST

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
    logging.info('Merging File: '+out_path+NAME+'key'+merge_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
    snl = int(merge_file['snl'][merge_file.shape[0]-1]+1)
    for f in FREQNAME:
        table_num_dict[f], code_num_dict[f] = MERGE(merge_file, DB_TABLE, DB_CODE, f)
    if main_file.empty == False:
        logging.info('Main File Exists: '+out_path+NAME+'key'+main_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        logging.info('Reading file: '+NAME+'database'+main_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        main_database = readExcelFile(out_path+NAME+'database'+main_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
        for s in range(main_file.shape[0]):
            sys.stdout.write("\rSetting snls: "+str(s+snl))
            sys.stdout.flush()
            main_file.loc[s, 'snl'] = s+snl
        sys.stdout.write("\n")
        logging.info('Setting files, Time: '+str(int(time.time() - tStart))+' s'+'\n')
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
    logging.info('Reading file: '+NAME+'key'+DF_suffix+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
    DF_KEY = readExcelFile(out_path+NAME+'key'+DF_suffix+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'key')
    DF_KEY = DF_KEY.set_index('name')
elif updating == False and DF_suffix == merge_suf:
    DF_KEY = merge_file
    DF_KEY = DF_KEY.set_index('name')

###########################################################################  Main Function  ###########################################################################
SUFFIX = {'A':'', 'S':'.S', 'Q':'.Q', 'M':'.M', 'W':'.W'}
REPL = {'A':'', 'S':None, 'Q':'-Q', 'M':'-', 'W':None}
new_item_counts = 0
chrome = None

for g in range(start_file,last_file+1):
    if main_file.empty == False:
        break
    if chrome == None:
        options = Options()
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("ignore-certificate-errors")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        chrome = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        chrome.set_window_position(980,0)
    logging.info('Reading file: '+NAME+str(g)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')
    if g == 1 or g == 2 or g == 8 or g == 9:############################################################ ECB ##################################################################
        file_path = data_path+NAME+str(g)+'.csv'
        if PRESENT(file_path):
            if g == 1 or g == 2:
                skip = [0,4]
            else:
                skip = [3]
            FOREX_t = readFile(data_path+NAME+str(g)+'.csv', header_ = [0,1,2], index_col_=0, skiprows_=skip)
            #FOREX_t = readFile(data_path+NAME+str(g)+'.csv', header_=[0], index_col_=0)
        else:
            if g == 1 or g == 8:
                url = 'https://sdw.ecb.europa.eu/browse.do?node=9691296'
            else:
                url = 'https://sdw.ecb.europa.eu/browse.do?node=9691297'
            if g == 1 or g == 2:
                FREQ = {'A':1, 'H':2, 'M':2, 'Q':2}
                FOREX_t = FOREX_WEB(chrome, g, file_name=NAME+str(g), url=url, header=[0,1,2], index_col=0, skiprows=[0,4], csv=True, FREQ=FREQ)
            else:
                FREQ = {'D':2}
                index_file = readFile(file_path.replace('.csv','_columns.csv'), header_=[0])
                FOREX_t = FOREX_WEB(chrome, g, file_name=NAME+str(g), url=url, header=[0], index_col=0, skiprows=[1,2,3,4], output=True, csv=True, FREQ=FREQ, index_file=index_file)
        if str(FOREX_t.index[0]).find('/') >= 0:
            new_index = []
            for ind in FOREX_t.index:
                new_index.append(pd.to_datetime(ind))
            FOREX_t = FOREX_t.reindex(new_index)
        if FOREX_t.index[0] > FOREX_t.index[1]:
            FOREX_t = FOREX_t[::-1]
        
        nG = FOREX_t.shape[1]
        logging.info('Total Columns: '+str(nG)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')        
        for i in range(nG):
            #sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            #sys.stdout.flush()

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
            elif str(FOREX_t.columns[i][0]).find('EXR.H') >= 0:
                freqnum = 5
                freqsuffix = ['S1','S2']
                frequency = 'S'
                keysuffix = ['06-30','12-31']
            elif str(FOREX_t.columns[i][0]).find('EXR.M') >= 0:
                freqnum = 7
                freqsuffix = ['']
                frequency = 'M'
                keysuffix = ['-']
            elif str(FOREX_t.columns[i][0]).find('EXR.Q') >= 0:
                freqnum = 5
                freqsuffix = ['Q1','Q2','Q3','Q4']
                frequency = 'Q'
                keysuffix = ['03-31','06-30','09-30','12-31']
            elif str(FOREX_t.columns[i][0]).find('EXR.D') >= 0:
                frequency = 'W'
                weekA = True
            
            for opp in [False, True]:
                code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                    FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                        DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, weekA=weekA)
            
            if str(FOREX_t.columns[i][0]).find('EXR.M') >= 0 and str(FOREX_t.columns[i][0]).find('SP00.E') >= 0:#Using End of period Monthly data to produce End of period Semiannual data
                freqnum = 5
                freqsuffix = ['S1','S2']
                frequency = 'S'
                keysuffix = ['06-30','12-31']
                for opp in [False, True]:
                    code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                        FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                            DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency], freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, weekA=weekA)
            elif str(FOREX_t.columns[i][0]).find('EXR.D') >= 0 and str(FOREX_t.columns[i][0]).find('ISK') >= 0:#Using Iceland Daily data to produce End of period data of other frequency of Iceland
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
        for frequency in ['A','M','Q','S']:
            file_suffix = frequency
            if frequency == 'S' and (g == 3 or g == 5):
                file_suffix = 'M'
            elif frequency == 'S' and (g == 4 or g == 6):
                file_suffix = 'Q'
            file_path = data_path+NAME+str(g)+file_suffix+'_historical.xlsx'
            logging.info('Frequency = '+frequency+' Time: '+str(int(time.time() - tStart))+' s'+'\n')
            if PRESENT(file_path):
                FOREX_t = readExcelFile(file_path, header_ =[0], index_col_=0, sheet_name_=0)
            else:
                url = 'https://data.imf.org/regular.aspx?key=63087883'
                FREQ = {'A':'Annual', 'Q':'Quarterly', 'M':'Monthly'}
                ITEM = {3:'Domestic Currency per SDR, End of Period', 4:'Domestic Currency per SDR, Period Average', 5:'Domestic Currency per U.S. Dollar, End of Period', 6:'Domestic Currency per U.S. Dollar, Period Average'}
                FOREX_temp = FOREX_WEB(chrome, g, file_name=NAME+str(g)+file_suffix, url=url, header=[0], index_col=1, skiprows=list(range(6)), FREQ=FREQ, ITEM=ITEM, freq=file_suffix)
                FOREX_t = FOREX_IMF(FOREX_temp, file_path)
            #print(FOREX_t)
        
            nG = FOREX_t.shape[0]
            logging.info('Total Rows: '+str(nG)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')       
            for i in range(nG):
                #sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
                #sys.stdout.flush()
                
                try:
                    COUNTRY(FOREX_t.index[i], noprint=True)
                except:
                    ERROR('發現未知國家: '+FOREX_t.index[i]+', 請於 Country.xlsx 作調整')
                    #continue
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
                    if frequency == 'S':
                        freqnum = 4
                        freqsuffix = ['-S1','-S2']
                        keysuffix = ['M06','M12']
                        semi = True
                elif g == 4 or g == 6:
                    form_e = 'Average of observations through period (A)'
                    if g == 4:
                        FOREXcurrency = 'Special Drawing Rights (SDR)'
                    elif g == 6:
                        FOREXcurrency = 'United States Dollar (USD)'
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
            if g == 5 or g == 6:
                if g == 5:
                    FOREXcurrency = 'Euro'
                    form_e = 'End of period (E)'
                elif g == 6:
                    FOREXcurrency = 'Euro'
                    form_e = 'Average of observations through period (A)'
                for opp in [False, True]:
                    code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                        FOREX_CROSSRATE(g, new_item_counts, DF_KEY, df_key_temp, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                            DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=opp, suffix=SUFFIX[frequency])
    
    elif g == 7 or g == 10 or g == 11:
        if g == 7:
            file_path = data_path+NAME+str(g)+'_historical.xlsx'
            if PRESENT(file_path):
                FOREX_t = readExcelFile(file_path, header_ =[0], index_col_=0, sheet_name_=0)
            else:
                url = 'https://data.imf.org/regular.aspx?key=41175'
                FOREX_temp = FOREX_WEB(chrome, g, file_name=NAME+str(g), url=url, header=[0], index_col=1, skiprows=list(range(4)))
                FOREX_t = FOREX_IMF(FOREX_temp, file_path)
            #print(FOREX_t)
            source = 'International Financial Statistics (IFS)'
            FOREXcurrency = 'United States Dollar (USD) (Millions of)'
            form_e = 'World Currency Composition of Official Foreign Exchange Reserves'
        else:
            FOREX_t = readExcelFile(data_path+NAME+str(g)+'.xlsx', header_=0, index_col_=1, skiprows_=list(range(6)), skipfooter_=3, sheet_name_=0)
            source = 'International Financial Statistics (IFS)'
            FOREXcurrency = 'United States Dollar (USD) (Millions of)'
            if g == 10:
                form_e = 'Advanced Economies Currency Composition of Official Foreign Exchange Reserves'
            elif g == 11:
                form_e = 'Emerging and Developing Economies Currency Composition of Official Foreign Exchange Reserves'
            FOREX_t = FOREX_t.drop(columns=['Unnamed: 0'])
        
        nG = FOREX_t.shape[0]
        frequency = 'Q'
        logging.info('Total Rows: '+str(nG)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')
        #print(FOREX_t)      
        for i in range(nG):
            #sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            #sys.stdout.flush()
            try:
                COUNTRY(FOREX_t.index[i], noprint=True)
            except:
                continue
            
            code_num_dict[frequency], table_num_dict[frequency], SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, new_item_counts = \
                FOREX_DATA(i, new_item_counts, DF_KEY, FOREX_t, AREMOS_forex, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, SORT_DATA[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                    DB_name_dict[frequency], snl, source, FREQLIST[frequency], frequency, form_e, FOREXcurrency, opp=False, suffix=SUFFIX[frequency], repl=REPL[frequency])
                    
    sys.stdout.write("\n\n")
    if find_unknown == True:
        logging.info('Total New Items Found: '+str(new_item_counts)+' Time: '+str(int(time.time() - tStart))+' s'+'\n') 
if chrome != None:
    chrome.quit()
    chrome = None

for f in FREQNAME:
    if main_file.empty == False:
        break
    if db_table_t_dict[f].empty == False:
        if f == 'W':
            #db_table_t_dict[f] = db_table_t_dict[f].reindex(FREQLIST['W_s'])
            db_table_t_dict[f].index = FREQLIST['W_s']
        DATA_BASE_dict[f][DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
        DB_name_dict[f].append(DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0'))      

print('Time: '+str(int(time.time() - tStart))+' s'+'\n')
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
    elif new_item_counts == 0 and find_unknown == True:
        ERROR('No new items were found.')
    df_key, DATA_BASE_dict = CONCATE(NAME, merge_suf, out_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, merge_file, DATA_BASE_dict, DB_name_dict, find_unknown=find_unknown)    

logging.info(df_key)
#logging.info(DATA_BASE_t)

print('Time: '+str(int(time.time() - tStart))+' s'+'\n')
df_key.to_excel(out_path+NAME+"key"+excel_suffix+".xlsx", sheet_name=NAME+'key')
with pd.ExcelWriter(out_path+NAME+"database"+excel_suffix+".xlsx") as writer:
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

print('Time: '+str(int(time.time() - tStart))+' s'+'\n')

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
logging.debug('Total Old Legacy Currency items not found: '+str(len(OLCurrency))+'\n')
logging.debug('Total International Monetary Fund (IMF) SDRs items not found: '+str(len(SDR))+'\n')
#print('Items not found: ', len(LEFT), '\n')
print('Time: '+str(int(time.time() - tStart))+' s'+'\n')
if updating == False:
    if find_unknown == True:
        checkNotFound = False
    else:
        checkNotFound = True
    unknown_list, toolong_list, update_list, unfound_list = FOREX_identity(out_path, df_key, DF_KEY, checkNotFound=checkNotFound, checkDESC=True, tStart=tStart, start_year=dealing_start_year)
