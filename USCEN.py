# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=E1101
import math, sys, calendar, os, copy, time, logging, zipfile
import regex as re
import pandas as pd
import numpy as np
import quandl as qd
import requests as rq
import win32com.client as win32
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import webdriver_manager
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from iteration_utilities import duplicates
import US_extention as EXT
from US_extention import ERROR, readFile, readExcelFile, US_NOTE, US_HISTORYDATA, DATA_SETS, takeFirst, US_IHS, US_BLS, MERGE, NEW_KEYS, CONCATE, UPDATE, ATTRIBUTES,\
 US_POPP, US_FAMI, EXCHANGE, NEW_LABEL, US_STL, US_DOT, US_TICS, US_BTSDOL, US_ISM, US_RCM, US_CBS, US_DOA, US_AISI, US_EIAIRS, US_SEMI, US_WEB, GET_NAME, PRESENT, US_FTD_NEW, US_POPT
import US_test as test
from US_test import US_identity
FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT, handlers=[logging.FileHandler("LOG.log", 'w', EXT.ENCODING)], datefmt='%Y-%m-%d %I:%M:%S %p')

find_unknown = False
main_suf = '?'
merge_suf = '?'
dealing_start_year = 1900
start_year = 1900
start_yearQ = 1940
start_yearM = 1909
start_yearS = 1980
merging = bool(int(input('Merging data file (1/0): ')))
updating = bool(int(input('Updating TOT file (1/0): ')))
if merging and updating:
    ERROR('Cannot do merging and updating at the same time.')
elif merging or updating:
    merge_suf = input('Be Merged(Original) data suffix: ')
    main_suf = input('Main(Updated) data suffix: ')
else:
    find_unknown = bool(int(input('Check if new items exist (1/0): ')))
    """if find_unknown == False:
        dealing_start_year = int(input("Dealing with data from year: "))
        start_year = dealing_start_year-10
        start_yearQ = dealing_start_year-10
        start_yearM = dealing_start_year-10
        start_yearS = dealing_start_year-10"""
bls_start = dealing_start_year
TICS_start = str(dealing_start_year)+'-01'
DF_suffix = test.DF_suffix
Historical = False
make_discontinued = False
ENCODING = EXT.ENCODING
excel_suffix = EXT.excel_suffix
if main_suf == '?':
    keyword = input('keyword: ')
    keyword = re.split(r'/', keyword)
    if len(keyword) < 2:
        keyword.append('')
else:
    keyword = ['','']
if keyword[0] == 'BLS':
    ig = input('ignore: ')
    if ig != '':
        ignore = re.split(r',', ig)
    else:
        ignore = []
else:
    ignore = []
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
NAME = EXT.NAME
data_path = EXT.data_path
out_path = EXT.out_path
databank = NAME[:-1]
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'last', 'unit', 'type', 'snl', 'source', 'form_e', 'form_c', 'table_id']
main_file = readExcelFile(out_path+NAME+'key'+main_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
merge_file = readExcelFile(out_path+NAME+'key'+merge_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
this_year = datetime.now().year + 1
update = datetime.today()
for i in range(len(key_list)):
    if key_list[i] == 'snl':
        snl_pos = i
        break
tStart = time.time()

FREQNAME = {'A':'annual','M':'month','Q':'quarter','S':'semiannual','W':'week'}
FREQLIST = {}
FREQLIST['A'] = [tmp for tmp in range(start_year,this_year+50)]
FREQLIST['S'] = []
for y in range(start_yearS,this_year):
    for s in range(1,3):
        FREQLIST['S'].append(str(y)+'-S'+str(s))
#print(FREQLIST['S'])
FREQLIST['Q'] = []
for q in range(start_yearQ,this_year+20):
    for r in range(1,5):
        FREQLIST['Q'].append(str(q)+'-Q'+str(r))
#print(FREQLIST['Q'])
FREQLIST['M'] = []
for y in range(start_yearM,this_year):
    for m in range(1,13):
        FREQLIST['M'].append(str(y)+'-'+str(m).rjust(2,'0'))
#print(FREQLIST['M'])
calendar.setfirstweekday(calendar.SATURDAY)
FREQLIST['W'] = pd.date_range(start = str(start_year)+'-01-01',end=update,freq='W-SAT').strftime('%Y-%m-%d')
#FREQLIST['W_s'] = pd.date_range(start = str(start_year)+'-01-01',end=update,freq='W-SAT').strftime('%Y-%m-%d')

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
logging.info('Reading table: TABLES, Time: '+str(int(time.time() - tStart))+' s'+'\n')
TABLES = readExcelFile(data_path+'tables.xlsx', header_ = 0, sheet_name_=0)
if updating == False and DF_suffix != merge_suf:
    logging.info('Reading file: US_key'+DF_suffix+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
    DF_KEY = readExcelFile(out_path+'US_key'+DF_suffix+'.xlsx', header_=0, acceptNoFile=False, index_col_=0, sheet_name_='US_key')
    DF_KEY = DF_KEY.set_index('name')
elif updating == False and DF_suffix == merge_suf:
    DF_KEY = merge_file
    DF_KEY = DF_KEY.set_index('name')

CONTINUE = []
def SOURCE(TABLES):
    source = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Source'] not in source:
            source.append(TABLES.iloc[t]['Source'])
    return source
def FILE_ADDRESS(source):
    address = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Source'] == source and TABLES.iloc[t]['Subset']+TABLES.iloc[t]['Address'] not in address:
            address.append(TABLES.iloc[t]['Subset']+TABLES.iloc[t]['Address'])
    return address
def FILE_NAME(source, address):
    file_name = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Source'] == source and TABLES.iloc[t]['Address'] == address and TABLES.iloc[t]['File'] not in file_name:
            file_name.append(TABLES.iloc[t]['File'])
    return file_name  
def SHEET_NAME(address, fname):
    sheet_name = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Address'] == address and TABLES.iloc[t]['File'] == fname:
            if type(TABLES.iloc[t]['Sheet']) == int:
                sheet_name.append(TABLES.iloc[t]['Sheet'])
            else:
                sheet_name.extend(re.split(r', ', str(TABLES.iloc[t]['Sheet'])))
            #break
    return sheet_name
def FREQUENCY(address, fname, sname, distinguish_sheet=False):
    freq_list = []
    if address.find('NIPA') >= 0 or address.find('FAAT') >= 0:
        try:
            freq = re.split(r'\-', sname)[1]
            freq_list.append(freq)
        except IndexError:
            freq_list = []
        return freq_list
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Address'] == address and TABLES.iloc[t]['File'] == fname:
            if distinguish_sheet == True and str(sname) not in re.split(r', ', str(TABLES.iloc[t]['Sheet'])):
                continue
            freq_list.extend(re.split(r', ', str(TABLES.iloc[t]['Frequency'])))
    return freq_list
def SCALE(code, address, SERIES=None):
    if address.find('BEA') >= 0:
        if SERIES.loc[code, 'DefaultScale'] == -9:
            return('Billions of ')
        elif SERIES.loc[code, 'DefaultScale'] == -6:
            return('Millions of ')
        elif SERIES.loc[code, 'DefaultScale'] == -3:
            return('Thousands of ')
        elif SERIES.loc[code, 'DefaultScale'] == 0:
            return('')
        else:
            ERROR('Scale error: '+code)
    elif address.find('FRB') >= 0:
        if str(SERIES.loc[code, 'Multiplier:']).isnumeric():
            SERIES.loc[code, 'Multiplier:'] = int(SERIES.loc[code, 'Multiplier:'])
        if SERIES.loc[code, 'Multiplier:'] == 1000000:
            return(', Millions of')
        elif SERIES.loc[code, 'Multiplier:'] == 1000000000 or SERIES.loc[code, 'Multiplier:'] == '1e+09':
            return(', Billions of')
        elif SERIES.loc[code, 'Multiplier:'] == 1 and SERIES.loc[code, 'Unit:'].find('Currency') >= 0:
            return(',')
        elif SERIES.loc[code, 'Multiplier:'] == 1:
            return('')
        else:
            ERROR('Scale error: '+code)
Titles = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_='titles').to_dict()
def US_KEY(address, counting=False, key=None):
    if address.find('BOC') >= 0:
        logging.info('Reading file: BOC_datasets, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        if key.find('FTD') >= 0:
            BOC_datasets = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_='FTDdatasets').to_dict()
        else:
            BOC_datasets = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=[0,1,2], sheet_name_='BOCdatasets').to_dict()
        logging.info('Reading file: '+key+'_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        Series = readExcelFile(data_path+address+key+'_series.xlsx', header_ = 0, index_col_=0)
        return Series, BOC_datasets, Titles
    elif address.find('BTS') >= 0 or address.find('DOL') >= 0:
        logging.info('Reading file: '+address[:3]+'_datasets, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        Datasets = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_=address[:3]+'datasets').to_dict()
        logging.info('Reading file: '+key+'_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        Series = readExcelFile(data_path+address+key+'_series.xlsx', header_ = 0, index_col_=0)
        return Series, Datasets, Titles
    elif address.find('NIPA') >= 0 or address.find('FAAT') >= 0:
        address = re.sub(r'NIPA/.*', "NIPA/", address)
        BEA_datasets = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_='BEAdatasets')
        logging.info('Reading file: BEA TablesRegister, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        BEA_table = readFile(BEA_datasets.loc[address, 'Table'], header_ = 0, index_col_='TableId')#readExcelFile(data_path+address+'TablesRegister.xlsx', header_ = 0, index_col_='TableId', sheet_name_=0)
        logging.info('Reading file: BEA SeriesRegister, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        BEA_series = readFile(BEA_datasets.loc[address, 'Series'], header_ = 0, index_col_='%SeriesCode')#readExcelFile(data_path+address+'SeriesRegister.xlsx', header_ = 0, index_col_='%SeriesCode', sheet_name_=0)
        if counting == True:
            return BEA_series
        return BEA_series, BEA_table, Titles
    elif address.find('ITAS') >= 0 or address.find('NIIP') >= 0 or address.find('DIRI') >= 0 or address.find('FDSA') >= 0 or address.find('DOA') >= 0:
        Table = None
        logging.info('Reading file: '+key+'_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        Series = readExcelFile(data_path+address+key+'_series.xlsx', header_ = 0, index_col_=0)
        if address.find('ITAS') >= 0 or address.find('NIIP') >= 0 or address.find('DIRI') >= 0:
            Table = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=[0,1,2], sheet_name_='BEAdatasets')
        elif address.find('DOA') >= 0:
            Table = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_='DOAdatasets')
        return Series, Table, Titles
    elif address.find('EIA') >= 0 or address.find('SEMI') >= 0:
        Series = None
        if address.find('PETR') >= 0:
            logging.info('Reading file: '+key+'_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
            Series = readExcelFile(data_path+address+key+'_series.xlsx', header_ = 0, index_col_=0)
        Datasets = None
        if address.find('EIA') >= 0:
            logging.info('Reading file: '+address[:3]+'_datasets, Time: '+str(int(time.time() - tStart))+' s'+'\n')
            Datasets = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_=address[:3]+'datasets').to_dict()
        return Series, Datasets, Titles
    elif address.find('ISM') >= 0:
        logging.info('Reading file: '+key+'_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        Series = readExcelFile(data_path+address+key+'_series.xlsx', header_ = 0, index_col_=0)
        with open(data_path+address+'api_key.txt','r',encoding='ANSI') as f:
            API = f.read()
        return Series, API, Titles
    elif address.find('FRB') >= 0:
        logging.info('Reading file: '+key+'_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        if key == 'FRB_G17':
            keydata = readExcelFile(data_path+address+'keydata.xlsx', header_=0,index_col_=[0,1] , sheet_name_='keydata')
            FRB_series = readFile(data_path+address+key+'_series.csv', header_ = 0).drop_duplicates(subset=['Series Name:'], ignore_index=True)
            for i in range(FRB_series.shape[0]):
                new_name = re.split(r'\.', FRB_series.iloc[i]['Series Name:'])
                prefix = str(keydata.loc[(new_name[0], new_name[2]), 'Series Prefix'])
                if prefix == 'nan':
                    prefix = ''
                name = prefix+new_name[1]
                FRB_series.loc[i, 'Series Name:'] = name
            FRB_series = FRB_series.set_index('Series Name:')
        elif key == 'FRB_H6' or key == 'FRB_H6_discontinued' or key == 'FRB_G19' or key == 'FRB_H15':
            FRB_series = readFile(data_path+address+key+'.csv', index_col_ = 0, nrows_= 6).T
            FRB_series.columns = ['Descriptions:', 'Unit:', 'Multiplier:', 'Currency:', 'Unique Identifier:', 'Index']
            FRB_series = FRB_series.set_index('Index', drop=False)
            new_index = []
            for i in range(FRB_series.shape[0]):
                new_index.append(FRB_series.index[i][:-2])
            FRB_series.index = new_index
        if counting == True:
            return FRB_series
        return FRB_series, None, Titles
        """elif address.find('STL') >= 0:
        logging.info('Reading file: '+key+'_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        STL_series = readExcelFile(data_path+address+key+'.xls', sheet_name_=0)
        STL_series = list(STL_series[0])
        return STL_series, None, Titles"""
    elif address.find('IRS') >= 0:
        logging.info('Reading file: IRS_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        if address.find('UIIT') >= 0:
            return None, None, Titles
        else:
            Series = readExcelFile(data_path+address+'IRS_series.xlsx', header_ = 0, index_col_=0)
        logging.info('Reading file: '+address[:3]+'_datasets, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        Datasets = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_=address[:3]+'datasets').to_dict()
        return Series, Datasets, Titles
    elif address.find('NAR') >= 0:
        logging.info('Reading file: NAR_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        NAR_series = readExcelFile(data_path+address+'NAR_series.xlsx', header_ = 0, sheet_name_=0)
        return NAR_series, None, Titles
    elif address.find('CBS') >= 0:
        logging.info('Reading file: CBS_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        CBS_datasets = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_='CBSdatasets')
        return CBS_datasets, None, Titles
    elif address.find('DOT') >= 0:
        DOT_table = None
        DOTdatasets = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_='DOTdatasets')
        logging.info('Reading file: DOT_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        if address.find('MTST') >= 0:
            DOT_series = DOTdatasets
        elif address.find('TICS') >= 0:
            DOT_series = readExcelFile(data_path+address+key+'_series.xlsx', header_ = 0, index_col_=0)
            DOT_table = DOTdatasets
        return DOT_series, DOT_table, Titles
    elif address.find('bls') >= 0:
        logging.info('Reading file: BLS_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        BLS_datasets = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_='BLSdatasets')
        file_path = data_path+'BLS/'+address[-3:-1]+'/'+address[-3:-1]+"_table.csv"
        if PRESENT(file_path):
            BLS_table = readFile(file_path, header_=0, index_col_=0).to_dict()
        else:
            if address.find('ec/') >= 0:
                BLS_table = readFile(address+BLS_datasets.loc[address, 'SERIES'], names_=['series_id','comp_code','group_code','ownership_code','periodicity_code','seasonal',\
                'footnote_code','begin_year','begin_period','end_year','end_period'], index_col_=0, skiprows_=[0], acceptNoFile=False, sep_='\\t')#.to_dict()
            else:
                BLS_table = readFile(address+BLS_datasets.loc[address, 'SERIES'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')#.to_dict()
            BLS_table.to_csv(file_path)
            BLS_table = BLS_table.to_dict()
        BLS_series = {}
        BLS_series['datasets'] = BLS_datasets
        if str(BLS_datasets.loc[address, 'ISADJUSTED']) != 'nan':
            BLS_series['ISADJUSTED'] = readFile(address+BLS_datasets.loc[address, 'ISADJUSTED'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
        else:
            BLS_series['ISADJUSTED'] = pd.DataFrame()
        if str(BLS_datasets.loc[address, 'CATEGORIES']).find('.') >= 0:
            if address.find('wd/') >= 0 or address.find('wp/') >= 0 or address.find('pc/') >= 0:
                BLS_series['CATEGORIES'] = readFile(address+BLS_datasets.loc[address, 'CATEGORIES'], header_=0, index_col_=(0,1), acceptNoFile=False, sep_='\\t')
            elif address.find('ei/') >= 0:
                BLS_series['CATEGORIES'] = pd.DataFrame(readFile(address+BLS_datasets.loc[address, 'CATEGORIES'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')['series_name'])
            elif address.find('ec/') >= 0:
                BLS_series['CATEGORIES'] = readFile(address+BLS_datasets.loc[address, 'CATEGORIES'], header_=0, acceptNoFile=False, sep_='\\t')
                BLS_series['CATEGORIES'].columns = ['group_text']
                new_index = []
                new_label = []
                for b in range(BLS_series['CATEGORIES'].shape[0]):
                    if BLS_series['CATEGORIES'].loc[b,'group_text'] == '.':
                        break
                    new_index.append(int(re.split(r'\s{5}', BLS_series['CATEGORIES'].loc[b,'group_text'])[0]))
                    new_label.append(re.split(r'\s{5}', BLS_series['CATEGORIES'].loc[b,'group_text'])[1])
                BLS_series['CATEGORIES'] = pd.DataFrame(new_label, index=new_index, columns=['group_text'])
            else:
                BLS_series['CATEGORIES'] = readFile(address+BLS_datasets.loc[address, 'CATEGORIES'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
            if address.find('ce/') >= 0:
                Display = {'Total nonfarm':'Total private', 'Government':'Total private'}
                Sort = {'Goods-producing':['Mining and logging', -1], 'Service-providing':['Goods-producing', -1], 'Private service-providing':['Trade, transportation, and utilities', -0.5]}
                BLS_series['CATEGORIES'] = EXCHANGE(address, BLS_series['CATEGORIES'], 'industry_name', Display=Display, Sort=Sort)
            elif address.find('jt/') >= 0:
                Display = {'Total nonfarm':'Total private'}
                BLS_series['CATEGORIES'] = EXCHANGE(address, BLS_series['CATEGORIES'], 'industry_text', Display=Display)
            if str(BLS_datasets.loc[address, 'SORT_C']) == 'T':
                BLS_series['CATEGORIES'] = BLS_series['CATEGORIES'].sort_values(by='sort_sequence')   
        else:
            BLS_series['CATEGORIES'] = {}
            if address.find('ln/') >= 0:
                unkey = 'lfst|periodicity|tdat'
            elif address.find('bd/') >= 0:
                unkey = 'msa|state|county|unitanalysis|dataclass|ratelevel|periodicity|ownership'
            for code in list(BLS_table.keys()):
                if bool(re.search(r'code$', code)) and bool(re.search(unkey, code)) == False:
                    BLS_series['CATEGORIES'][code.replace('_code','')] = readFile(address+BLS_datasets.loc[address, 'CATEGORIES']+'.'+code.replace('_code',''), header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
            if address.find('bd/') >= 0:
                Display = {'Total private':'Goods-producing'}
                Sort = {'Service-providing':['Merchant wholesalers, durable goods', -0.5], 'Natural resources and mining':['Crop production', -0.5], 'Construction':['Construction of buildings', -0.5], \
                'Manufacturing':['Food manufacturing', -0.5], 'Wholesale trade':['Merchant wholesalers, durable goods', -0.1], 'Retail trade':['Motor vehicle and parts dealers', -0.5], \
                'Transportation and warehousing':['Air transportation', -0.5], 'Utilities':['Service-providing', 0.1], 'Information':['Publishing industries (except internet)', -0.5], \
                'Financial activities':['Credit intermediation and related activities', -0.5], 'Professional and business services':['Professional, scientific, and technical services', -0.5], \
                'Education and health services':['Educational services', -0.5], 'Leisure and hospitality':['Performing arts, spectator sports, and related industries', -0.5], \
                'Other services (except public administration)':['Repair and maintenance', -0.5]}
                BLS_series['CATEGORIES']['industry'] = EXCHANGE(address, BLS_series['CATEGORIES']['industry'], 'industry_name', Display=Display, Sort=Sort)
                BLS_series['CATEGORIES']['industry'] = BLS_series['CATEGORIES']['industry'].sort_values(by='sort_sequence')
        if str(BLS_datasets.loc[address, 'DATA TYPE']).find('.') >= 0:
            if address.find('ml/') >= 0:
                BLS_series['DATA TYPE'] = readFile(address+BLS_datasets.loc[address, 'DATA TYPE'], index_col_=0, acceptNoFile=False, sep_='\\t', names_=['dataelement_code','dataelement_text'])
            else:
                BLS_series['DATA TYPE'] = readFile(address+BLS_datasets.loc[address, 'DATA TYPE'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
            if address.find('bd/') >= 0:
                Sort = {'Establishment Births':['Gross Job Gains', 0.5]}
                BLS_series['DATA TYPE'] = EXCHANGE(address, BLS_series['DATA TYPE'], 'dataclass_name', Sort=Sort)
            if str(BLS_datasets.loc[address, 'SORT_D']) == 'T':
                BLS_series['DATA TYPE'] = BLS_series['DATA TYPE'].sort_values(by='sort_sequence')
        if str(BLS_datasets.loc[address, 'BASE']) != 'nan':
            if address.find('ml/') >= 0:
                BLS_series['BASE'] = readFile(address+BLS_datasets.loc[address, 'BASE'], index_col_=0, acceptNoFile=False, sep_='\\t', names_=['srd_code','srd_text'])
            else:
                BLS_series['BASE'] = readFile(address+BLS_datasets.loc[address, 'BASE'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
        else:
            BLS_series['BASE'] = pd.DataFrame()
        if str(BLS_datasets.loc[address, 'UNIT']) != 'nan':
            BLS_series['UNIT'] = readFile(address+BLS_datasets.loc[address, 'UNIT'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
        if str(BLS_datasets.loc[address, 'NOTE']) != 'nan':
            BLS_series['NOTE'] = readFile(address+BLS_datasets.loc[address, 'NOTE'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
                
        return BLS_series, BLS_table, Titles
    elif address.find('DSCO') >= 0:
        BLS_series = {}
        BLS_series['BASE'] = pd.DataFrame()
        return BLS_series, pd.DataFrame(), Titles
    elif address.find('STL') >= 0:
        return None, None, Titles
    else:
        ERROR('Series Error: '+address)
def US_LEVEL(LABEL, source, Series=None, loc1=None, loc2=None, name=None, indent=None):
    label_level = []
    for l in range(len(LABEL)):
        if source == 'Bureau of Economic Analysis':
            if str(LABEL.iloc[l]) != 'nan':
                label_level.append(re.search(r'\S',str(LABEL.iloc[l])).start())
            else:
                label_level.append(10000)
        elif source == 'Bureau Of Census' or source == 'National Association of Home Builders' or source == 'Department Of The Treasury':
            if str(LABEL.index[l]) != 'nan':
                if str(LABEL.index[l])[loc1:loc2].isnumeric():
                    label_level.append(Series[name].loc[int(re.sub(r'0+$', "", str(LABEL.index[l])[loc1:loc2])), indent])
                else:
                    label_level.append(Series[name].loc[re.sub(r'0+$', "", str(LABEL.index[l])[loc1:loc2]), indent])
            else:
                label_level.append(10000)
    return label_level

def US_ADDLABEL(begin, address, sheet_name, LABEL, label_level, UNIT, unit, Calculation_type, attribute, suffix=False, form=None):
    level = label_level[begin]
    if UNIT == 'nan':
        UNIT = unit+' '+Calculation_type
    for att in list(reversed(range(begin))):
        if str(LABEL.iloc[att]).find('[') >= 0 and label_level[att] == 0:
            UNIT = LABEL.iloc[att].replace('[','').replace(']','')
            break
        if UNIT == 'nan' and str(LABEL.iloc[att]).find(':') >= 0 and label_level[att] == 0:
            UNIT = LABEL.iloc[att].replace(':','')
            if UNIT == 'Addenda':
                UNIT = unit+' '+Calculation_type
            elif sheet_name == 'U70205S':
                UNIT = unit+' '+Calculation_type
            break
        elif UNIT == 'nan':
            UNIT = unit+' '+Calculation_type
        if label_level[att] < level:
            if str(LABEL.iloc[att]).find(':') >= 0 and str(LABEL.iloc[att])[-1:] == ':':
                attribute.insert(0, LABEL.iloc[att].replace(', ', ',').strip()+' ')
            elif str(LABEL.iloc[att]).find(':') >= 0:
                attribute.insert(0, LABEL.iloc[att][LABEL.iloc[att].find(':')+1:].replace(', ', ',').strip()+', ')
            else:
                if address.find('BOC') >= 0:
                    attribute.insert(0, LABEL.iloc[att].replace('/',' and ').replace('inc.','including').replace(', ', ',').strip()+', ')
                elif address.find('ml/') >= 0:
                    lab = LABEL.iloc[att]
                    if bool(re.search(r'[Aa]ll [Ii]ndustries', lab)):
                        level = label_level[att]
                        continue
                    if bool(re.search(r'\(\s*seasonally\s*adjusted\s*\)', lab)):
                        lab = re.sub(r'\(\s*seasonally\s*adjusted\s*\)', "", lab)
                    if bool(re.search(r'Total', lab)):
                        lab = re.sub(r'Total[\s,]*(.+)$', r"\1", lab)
                    attribute.insert(0, lab.replace(', ', ',').strip().capitalize()+', ')
                elif bool(re.search(r'\(*S[0-9]+\)', str(LABEL.iloc[att]))):
                    attribute.insert(0, re.sub(r'\(*S[0-9]+\)', "", LABEL.iloc[att]).replace(', ', ',').strip()+', ')
                else:
                    if suffix == True:
                        attribute[-1] = attribute[-1].replace(form, form.replace(', ', ',')+', '+str(LABEL.iloc[att]).replace(', ', ',').strip())
                    else:
                        attribute.insert(0, str(LABEL.iloc[att]).replace(', ', ',').strip()+', ')
            level = label_level[att]
    return UNIT, attribute

def US_ADDNOTE(attri, NOTE, note, note_num, note_part, specific=False, alphabet=False):
    note_suffix = ''
    if specific == True:
        dex_list = attri
    else:
        if alphabet == True:
            dex_list = re.findall(r'[a-z]+',attri)
        else:
            dex_list = re.findall(r'[0-9]+',attri)
    for dex in dex_list:
        already = False
        found = False
        for note_item in NOTE:
            if str(dex) == str(note_item[0]):
                found = True
                if note.find(note_item[1]) >= 0:
                    for part in note_part:
                        if part[1] == note_item[1]:
                            num = part[0]
                            break
                    already = True
                    break
                note = note+'('+str(note_num)+')'+note_item[1]+' '
                note_part.append([note_num, note_item[1]])
                break
        if already == False and found == True:
            note_suffix = note_suffix+'*('+str(note_num)+')'
            note_num += 1
        elif found == True:
            note_suffix = note_suffix+'*('+str(num)+')'

    return note, note_num, note_part, note_suffix

NonValue_t = ['nan','Nan', '.....', 'ND', 'None', '0', '(S)', '(NA)', 'N', 'NA', '-', '', '(-)', 'n.a.', '(*)', '(D)', 'U', '.', '*', '--', 'Not Available', 'Not Applicable','\xa0','ZZZZZZ']

def US_DATA(ind, name, US_t, address, file_name, sheet_name, value, index, code_num, table_num, KEY_DATA, DATA_BASE, db_table_t, DB_name, snl, source, freqlist, frequency, UNIT='nan', LABEL=pd.DataFrame(), label_level=[], NOTE=[], FOOTNOTE=[], series=None, table=None, titles=None, repl=None, repl2=None, formnote={}, YEAR=None, QUAR=None, RAUQ=None):
    freqlen = len(freqlist)
    unit = ''
    Calculation_type = ''
    form_e = ''
    form_c = ''
    NonValue = NonValue_t.copy()
    if source == 'Department Of The Treasury' or source == 'Federal Reserve Bank of Richmond' or source == 'Bureau of Economic Analysis' or source == 'Bureau Of Labor Statistics':
        NonValue.remove('0')
    if source == 'Bureau Of Labor Statistics' and address.find('DSCO') < 0:
        seasonal = re.sub(r'[a-z]+\.', "", str(series['datasets'].loc[address, 'ISADJUSTED']))
        group = re.sub(r'[a-z]+\.', "", series['datasets'].loc[address, 'DATA TYPE'])
        item = re.sub(r'[a-z]+\.', "", series['datasets'].loc[address, 'CATEGORIES'])
        base = re.sub(r'[a-z]+\.', "", str(series['datasets'].loc[address, 'BASE']))
        uni = re.sub(r'[a-z]+\.', "", str(series['datasets'].loc[address, 'UNIT']))
        text = series['datasets'].loc[address, 'CONTENT']
        p_text = text
        p_item = item
        if address.find('pr/') >= 0 or address.find('mp/') >= 0:
            p_text = 'name'
            p_group = group
            if address.find('pr/') >= 0:
                seasonal = seasonal.capitalize()
            if address.find('mp/') >= 0:
                uni = uni.upper()
                p_text = p_text.upper()
                p_group = p_group.upper()
                p_item = p_item.upper()
        elif address.find('ec/') >= 0:
            group = 'comp'
            p_text = 'name'
    if code_num >= 200:
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        #if frequency == 'W':
        #    db_table_t = db_table_t.reindex(FREQLIST['W_s'])
        DATA_BASE[db_table] = db_table_t
        DB_name.append(db_table)
        table_num += 1
        code_num = 1
        db_table_t = pd.DataFrame(index = freqlist, columns = [])
    
    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
    db_code = DB_CODE+str(code_num).rjust(3,'0')
    db_table_t[db_code] = ['' for tmp in range(freqlen)]
    content = ''
    note = ''
    note_num = 1
    note_part = []
    if source == 'Bureau of Economic Analysis' and (address.find('NIPA') >= 0 or address.find('FAAT') >= 0):
        unit = SCALE(US_t.iloc[ind]['Index'], address, series)+series.loc[US_t.iloc[ind]['Index'], 'MetricName']
        Calculation_type = series.loc[US_t.iloc[ind]['Index'], 'CalculationType']
        tabletitle = table.loc[sheet_name, 'TableTitle']
        form_e = re.split(r'Table\s[0-9A-Z\.]+\.\s', tabletitle)[1]
        form_c = re.findall(r'Table\s[0-9A-Z\.]+\.', tabletitle)[0]
    elif source == 'Bureau of Economic Analysis' and (address.find('ITAS') >= 0 or address.find('NIIP') >= 0 or address.find('DIRI') >= 0):
        unit = re.sub(r'\s+NOTE:.+', "", UNIT.replace('[','').replace(']',''))
        if address.find('ITAS') >= 0:
            Calculation_type = series['GEO LEVELS'].loc[file_name, 'geo_desc']
        elif address.find('NIIP') >= 0:
            Calculation_type = 'All countries'
        elif address.find('DIRI') >= 0:
            Calculation_type = series['DATA TYPES'].loc[file_name, 'dt_desc']
        form_e = repl2
        form_c = Series['ISADJUSTED'].loc[sheet_name, 'adj_desc']
    elif source == 'Institute for Supply Management':
        unit = US_t.iloc[ind]['unit']
        Calculation_type = series['DATA TYPES'].loc[str(US_t.iloc[ind]['Index'])[repl:repl2], 'dt_desc']
        form_e = series['INDUSTRY'].loc[str(US_t.iloc[ind]['Index'])[1:repl], 'ind_desc']
        form_c = series['ISADJUSTED'].loc[file_name, 'adj_desc']
        UNIT = unit
    elif source == 'Federal Reserve Economic Data' or file_name == 'UIWC' or file_name == 'UIIT' or file_name == 'BEOL':
        unit = US_t.iloc[ind]['unit']
        Calculation_type = ''
        form_e = US_t.iloc[ind]['form_e']
        form_c = US_t.iloc[ind]['is_adj']
        UNIT = unit
    elif source == 'Semiconductor Equipment and Materials International':
        unit = US_t.iloc[ind]['unit']
        Calculation_type = US_t.iloc[ind]['Label']
        if frequency == 'M':
            form_e = 'Semiconductor Equipment'
        elif frequency == 'Q':
            form_e = 'Shipments of Silicon Materials'
        form_c = 'Not Seasonally Adjusted'
        UNIT = unit
    elif source == 'American Iron and Steel Institute' or source == 'Energy Information Administration' or source == 'U.S. Geological Survey' or source == 'Internal Revenue Service':
        unit = US_t.iloc[ind]['unit']
        if address.find('PETR') >= 0:
            Calculation_type = series['DATA TYPES'].loc[repl, 'dt_desc']
        else:
            Calculation_type = US_t.iloc[ind]['Label']
        if file_name.find('http') >= 0:
            form_e = table['Form'][sheet_name]
        else:
            form_e = table['Form'][file_name]
        form_c = 'Not Seasonally Adjusted'
        UNIT = unit
        dt_key = repl
    elif source == 'U.S. Department of Agriculture':
        unit = US_t.iloc[ind]['unit']
        Calculation_type = series['CATEGORIES'].loc[str(US_t.iloc[ind]['Index'])[repl:repl2], 'cat_desc']
        form_e = US_t.iloc[ind]['form']
        form_c = 'Not Seasonally Adjusted'
        UNIT = unit
    elif source == 'Federal Reserve Bank of Richmond':
        unit = US_t.iloc[ind]['unit']
        Calculation_type = series['PERIOD'].loc[str(US_t.iloc[ind]['Index'])[-1:], 'pd_desc']
        form_e = series['INDUSTRY'].loc[str(US_t.iloc[ind]['Index'])[1:repl], 'ind_desc']
        form_c = series['ISADJUSTED'].loc[str(US_t.iloc[ind]['Index'])[:1], 'adj_desc']
        UNIT = unit
    elif source == 'National Federation of Independent Business' or source == 'Organization for Economic Cooperation and Development':
        unit = US_t.iloc[ind]['unit']
        if file_name.find('http') >= 0:
            Calculation_type = sheet_name
        else:
            Calculation_type = file_name
        if address.find('OECD') >= 0:
            form_e = 'Main Economic Indicators'
            form_c = 'Amplitude adjusted'
        else:
            form_e = 'Small Business Economic Trends'
            form_c = 'Seasonally Adjusted'
        UNIT = unit
    elif source == 'Bureau Of Transportation Statistics' or source == 'Department Of Labor':
        if file_name == 'TRPT' or source == 'Department Of Labor':
            unit = US_t.iloc[ind]['unit']
            UNIT = unit
            if str(US_t.iloc[ind]['Index'])[repl:].find('SAT') >= 0:
                repl2 = 8
        else:
            unit = UNIT
        cat_key = str(US_t.iloc[ind]['Index'])[1:repl]
        if str(US_t.iloc[ind]['Index'])[repl:].find('VMT') >= 0 or str(US_t.iloc[ind]['Index'])[repl:].find('SAT') >= 0:
            Calculation_type = series['GEO LEVELS'].loc[str(US_t.iloc[ind]['Index'])[repl2:], 'geo_desc']
            form_e = series['DATA TYPES'].loc[str(US_t.iloc[ind]['Index'])[repl:repl2], 'dt_desc']
        else:
            Calculation_type = series['CATEGORIES'].loc[cat_key, 'cat_desc']
            form_e = series['DATA TYPES'].loc[str(US_t.iloc[ind]['Index'])[repl:], 'dt_desc']
        if source == 'Department Of Labor':
            form_c = 'Not Seasonally Adjusted'
        else:
            form_c = series['ISADJUSTED'].loc[str(US_t.iloc[ind]['Index'])[:1], 'adj_desc']
    elif source == 'Department Of The Treasury, Bureau Of The Fiscal Service':
        unit = 'Millions of United States Dollars'
        Calculation_type = US_t.iloc[ind]['type']
        form_e = series.loc[file_name, 'Name']
        form_c = 'Not Seasonally Adjusted'
        UNIT = unit
    elif source == 'Department Of The Treasury':
        if sheet_name.find('s1_globl') >= 0:
            unit = 'Millions of United States Dollars'
            form_e = 'U.S. Transactions with Foreigners in Long-term Domestic and Foreign Securities'
        elif sheet_name.find('mfhhis01') >= 0:
            unit = 'Billions of United States Dollars'
            form_e = 'Portfolio Holdings of U.S. and Foreign Securities'
        Calculation_type = series['DATA TYPES'].loc[US_t.iloc[ind]['Index'][:1], 'dt_desc']
        form_c = 'Not Seasonally Adjusted'
        cat_key = str(US_t.iloc[ind]['Index'])[1:repl]
        UNIT = unit
    elif source == 'Federal Reserve Board':
        DOLLAR = {'USD':'United States Dollar', '':'United States Dollar'}
        ADJUST = {False:'Seasonally Adjusted', True:'Not Seasonally Adjusted'}
        unit = series.loc[US_t.iloc[ind]['Index'], 'Unit:'].replace('_100','=100').replace('_',' ')+SCALE(US_t.iloc[ind]['Index'], address, series)
        if unit.find('Currency') >= 0:
            unit = unit + ' ' + DOLLAR[Calculation_type]
        Calculation_type = series.loc[US_t.iloc[ind]['Index'], 'Currency:']
        if Calculation_type == 'NA' or str(Calculation_type) == 'nan':
            Calculation_type = ''
        Identifier = series.loc[US_t.iloc[ind]['Index'], 'Unique Identifier:']
        form_e = ''
        if address.find('G17') >= 0:
            ADJUST = {'S':'Seasonally Adjusted','N':'Not Seasonally Adjusted','s.a.':'Seasonally Adjusted','n.s.a.':'Not Seasonally Adjusted'}
            form_c = ADJUST[Identifier[-1:]]
            before = ['_','IP ','MVA','DIFF','CAPUTL','CAP','GVIP','RIW']
            after = [' ','','Motor Vehicle Assemblies','Diffusion Index of Industrial Production','Capacity Utilization','Industrial Capacity','Gross Value of Final Products and Nonidustrial Supplies','Relative Importance Weights for Industrial Production']
        elif address.find('H6') >= 0:
            form_c = ADJUST[bool(re.search(r'_N$', US_t.iloc[ind]['Index']))]
            M3IMF = 'M3 Institutional Money Funds'
            NON = ['Total Non-', 'Savings Deposits', 'Small-Denomination', 'Retail Money', 'Institutional Money']
            FNAME = {'FRB_H6': 'M1 M2', 'FRB_H6_discontinued': 'M2 M3'}
            before = ['H6','_','DISCONTINUED','M1','M2','MBASE','MEMO','M3']
            after = ['','','','Components of M1','Components of M2','Monetary Base','Memorandum Items','Components of M3']
            for word in NON:
                if LABEL[US_t.index[ind]].find(M3IMF) >= 0:
                    break
                elif LABEL[US_t.index[ind]].find(word) >= 0:
                    form_e = 'Components of Non-'+FNAME[sheet_name]
                    break
        elif address.find('G19') >= 0:
            form_c = ADJUST[bool(re.search(r'_N$', US_t.iloc[ind]['Index']))]
            before = ['CCOUT','TERMS']
            after = ['Consumer Credit Outstanding','Terms of Credit']
        elif address.find('H15') >= 0:
            unit = unit.replace(': Per Year', ' per year')
            form_c = ADJUST[bool(re.search(r'_N$', US_t.iloc[ind]['Index']))]
            treasury = 'Treasury'
            before = ['H15']
            after = ['Selected Interest Rates']
            if LABEL[US_t.index[ind]].find(treasury) >= 0:
                form_e = 'U.S. Government Securities'
        if form_e == '':
            form_e = re.split(r'/', Identifier)[1]
            for sub in range(len(before)):
                form_e = form_e.replace(before[sub], after[sub])
        form_e = form_e.title().replace('Of ', 'of ')
        UNIT = unit
    elif source == 'Bureau Of Census' or source == 'National Association of Home Builders':
        try:
            if address.find('HOUS') >= 0:
                categ = str(US_t.iloc[ind]['Index'])[1:5]
                unit = series['DATA TYPES'].loc[str(US_t.iloc[ind]['Index'])[repl:], 'dt_unit']
                Calculation_type = series['GEO LEVELS'].loc[str(US_t.iloc[ind]['Index'])[5:repl], 'geo_desc']
                form_e = series['CATEGORIES'].loc[categ, 'cat_desc']
                form_c = series['ISADJUSTED'].loc[str(US_t.iloc[ind]['Index'])[:1], 'adj_desc']  
            elif address.find('FTD') >= 0:
                categ = str(US_t.iloc[ind]['Index'])[1:repl]
                if categ.isnumeric():
                    categ = int(categ)
                unit = series['DATA TYPES'].loc[str(US_t.iloc[ind]['Index'])[repl:repl2], 'dt_unit']
                if str(US_t.iloc[ind]['Index'])[repl2:] == 'CSBR':
                    unit = 'Millions of Chained Dollars'
                Calculation_type = series['GEO LEVELS'].loc[str(US_t.iloc[ind]['Index'])[repl2:], 'geo_desc']
                if file_name.find('http') >= 0:
                    form_e = table['Form'][sheet_name]
                else:
                    form_e = table['Form'][file_name]
                if form_e == 'U.S. Imports of Energy-Related Petroleum Products':
                    unit = series['CATEGORIES'].loc[categ, 'unit']
                form_c = series['ISADJUSTED'].loc[str(US_t.iloc[ind]['Index'])[:1], 'adj_desc']
            else:
                if address.find('APEP') >= 0:
                    categ = str(US_t.iloc[ind]['Index'])[2:repl]
                    form_c = series['ISADJUSTED'].loc[str(US_t.iloc[ind]['Index'])[:2], 'adj_desc']
                    Calculation_type = series['DATA TYPES'].loc[str(US_t.iloc[ind]['Index'])[repl:repl2], 'dt_desc']
                    form_e = series['CATEGORIES'].loc[categ, 'cat_desc']
                    if not not formnote:
                        if series['CATEGORIES'].loc[categ, 'key_desc'] in formnote:
                            form_e = form_e+formnote[series['CATEGORIES'].loc[categ, 'key_desc']]
                    unit = series['DATA TYPES'].loc[str(US_t.iloc[ind]['Index'])[repl:repl2], 'dt_unit']
                else:
                    categ = re.sub(r'0+$', "", str(US_t.iloc[ind]['Index'])[1:repl])
                    if categ.isnumeric():
                        categ = int(categ)
                    form_c = series['ISADJUSTED'].loc[str(US_t.iloc[ind]['Index'])[:1], 'adj_desc']
                    form_e = series['DATA TYPES'].loc[str(US_t.iloc[ind]['Index'])[repl:], 'dt_desc']
                    Calculation_type = series['CATEGORIES'].loc[categ, 'cat_desc'].title().replace('/',' and ').replace('And','and').replace('Gafo','GAFO')
                    unit = series['DATA TYPES'].loc[str(US_t.iloc[ind]['Index'])[repl:], 'dt_unit']
        except KeyError:
            CONTINUE.append(name)
        if address.find('MSIO') >= 0:        
            total_note = 'Estimates in this item is based on the North American Industry Classification System (NAICS).'
            note = note+'('+str(note_num)+')'+total_note
            note_num += 1
        if UNIT == 'nan':
            UNIT = unit
    elif source == 'National Association of Realtors':
        unit = str(US_t.iloc[ind]['unit'])
        Calculation_type = str(US_t.iloc[ind]['type'])
        form_e = str(US_t.iloc[ind]['form_e'])
        form_c = str(US_t.iloc[ind]['form_c'])
        UNIT = unit
    elif source == 'Bureau Of Labor Statistics' and address.find('DSCO') < 0:
        #form_e
        if address.find('ce/') >= 0:
            form_e = re.sub(r',\s*[A-Z\s0-9\-=]+$|, SEASONALLY ADJUSTED', "", series['DATA TYPE'].loc[Table['data_type_code'][US_t.iloc[ind]['Index']], 'data_type_text']).title().replace('And','and').replace("Of","of")
            not_private = [1, 10, 25, 26]
            diffusion = [21, 22, 23, 24]
            if int(US_t.iloc[ind]['unit']) in not_private:
                form_e = form_e + ' on Nonfarm Payrolls by Industry Sector and Selected Industry Detail'
            elif int(US_t.iloc[ind]['unit']) in diffusion:
                form_e = form_e.replace(', ', ' of Employment Change, ')
            else:
                form_e = form_e + ' on Private Nonfarm Payrolls by Industry Sector and Selected Industry Detail'
        elif address.find('pr/') >= 0 or address.find('mp/') >= 0:
            form_e = series['DATA TYPE'].loc[Table[group+'_code'][US_t.iloc[ind]['Index']], p_group+'_'+p_text]
        elif address.find('ml/') >= 0:
            form_e = 'Mass Layoff, '+series['DATA TYPE'].loc[Table[group+'_code'][US_t.iloc[ind]['Index']], group+'_'+text]
        else:
            form_e = series['DATA TYPE'].loc[Table[group+'_code'][US_t.iloc[ind]['Index']], group+'_'+text]
        #unit
        if address.find('ln/') >= 0:
            if int(US_t.iloc[ind]['unit']) == 0:
                unit = 'Thousands of people'
            elif int(US_t.iloc[ind]['unit']) == 1:
                unit = 'Percent or rate'
            else:
                unit = series['UNIT'].loc[US_t.iloc[ind]['unit'], uni+'_text'].capitalize()
        elif address.find('ce/') >= 0:
            unit_t = series['UNIT'].loc[US_t.iloc[ind]['unit'], 'data_type_text'].capitalize()
            suffix = ''
            remark = ''
            if unit_t.find('Indexes') >= 0:
                prefix = 'Index base: '
            elif unit_t.find('earnings') >= 0 or unit_t.find('payrolls') >= 0:
                prefix = 'Dollars'
                if bool(re.search(r'.+,\s[0-9\-]+\sdollars$', unit_t)):
                    remark = re.sub(r'.+(,\s[0-9\-]+\sdollars$)', r"\1", unit_t)
            elif unit_t.find('hours') >= 0:
                prefix = 'Hours'
            elif unit_t.find('thousands') >= 0:
                prefix = 'Thousands of people'
            elif unit_t.find('ratio') >= 0:
                prefix = 'Rate'
            else:
                prefix = unit_t
            if unit_t.find('Indexes') >= 0:
                suffix = re.sub(r'.+,\s+([0-9=]+$)', r"\1", unit_t)
            elif unit_t.find('weekly') >= 0:
                suffix = ' per week'
            elif unit_t.find('hourly') >= 0:
                suffix = ' per hour'
            unit = prefix+suffix+remark
        elif address.find('in/') >= 0:
            unit_t = series['CATEGORIES'].loc[US_t.iloc[ind]['unit'], item+'_'+text].capitalize()
            if bool(re.search(r'[Rr]atio|[Rr]ates|[Ss]hare|[Pp]ercent', unit_t)):
                unit = 'Percentage'
            elif bool(re.search(r'[0-9]+=100', unit_t)):
                unit = 'Index base: '+re.sub(r'.+\s([0-9]+=100).+', r'\1', unit_t)
            elif unit_t.find('consumer prices') >= 0:
                unit = 'Index base: 1982-84=100'
            elif unit_t.find('Exchange rate index') >= 0:
                unit = 'Index base: 2002=100'
            elif unit_t.find('Exchange rate') >= 0:
                unit = 'National Currency per United States Dollar'
            elif unit_t.find('U.S.=100') >= 0:
                unit = 'Index base: U.S. Dollar=100'
            elif US_t.iloc[ind]['unit'] == 5004:
                unit = 'Millions of United States Dollar'
            elif unit_t.find('dollars') >= 0:
                unit = 'United States Dollar'
            elif (unit_t.find('Manufacturing') >= 0 and unit_t.find('index') >= 0) or unit_t.find('basis') >= 0 or unit_t.find('Output') >= 0:
                unit = 'Index base: 2002=100'
            elif unit_t.find('hours') >= 0:
                unit = 'Hours'
            elif unit_t.find('Purchasing power parities') >= 0 or unit_t.find('currency') >= 0:
                unit = 'National Currency'
            elif US_t.iloc[ind]['unit'] == 5005 or US_t.iloc[ind]['unit'] == 5006:
                unit = 'Millions of people'
            else:
                unit = 'Thousands of people'
        elif address.find('pr/') >= 0 or address.find('mp/') >= 0:
            if str(Table['base_year'][US_t.iloc[ind]['Index']]).isnumeric():
                unit = str(US_t.iloc[ind]['unit'])
            else:
                unit = series['UNIT'].loc[US_t.iloc[ind]['unit'], uni+'_'+text].capitalize().replace('%', 'Percent')
        elif address.find('ec/') >= 0:
            unit_t = series['UNIT'].loc[US_t.iloc[ind]['unit'], uni+'_'+text].capitalize()
            if unit_t.find('Index') >= 0:
                unit = 'Index base: 1989.06 = 100'
            else:
                unit = re.sub(r'cha[nge]*$', "change", unit_t)
        elif address.find('bd/') >= 0 or address.find('jt/') >= 0:
            unit_t = series['UNIT'].loc[US_t.iloc[ind]['unit'], uni+'_'+text].capitalize()
            if unit_t.find('Level') >= 0:
                unit = 'Thousands'
            elif unit_t.find('Rate') >= 0:
                unit = 'Percentange'
            else:
                unit = unit_t 
        elif address.find('ml/') >= 0:
            unit = 'Number'
        else:
            unit = str(US_t.iloc[ind]['unit'])
        #form_c
        if address.find('ml/') >= 0:
            ISADJUSTED = {'S':'Seasonally Adjusted', 'U':'Not Seasonally Adjusted'}
            form_c = ISADJUSTED[US_t.iloc[ind]['Index'].strip()[2]]
        elif series['ISADJUSTED'].empty == True:
            form_c = 'Not Seasonally Adjusted'
        elif address.find('mp/') >= 0:
            form_c = series['ISADJUSTED'].loc[US_t.iloc[ind]['Index'].strip()[2], seasonal+'_text']
        else:
            form_c = series['ISADJUSTED'].loc[Table['seasonal'][US_t.iloc[ind]['Index']], seasonal+'_text']
        #Calculation_type
        if address.find('cu') >= 0 or address.find('cw') >= 0 or address.find('li/') >= 0 or address.find('ce/') >= 0\
             or address.find('pr/') >= 0 or address.find('mp/') >= 0 or address.find('ec/') >= 0 or address.find('jt/') >= 0 or address.find('in/') >= 0 or address.find('ml/') >= 0:
            Calculation_type = series['CATEGORIES'].loc[Table[item+'_code'][US_t.iloc[ind]['Index']], p_item+'_'+text].title().replace('And','and').replace("'S","'s")
        else:
            if address.find('ei/') >= 0:
                Calculation_type = series['CATEGORIES'].loc[US_t.iloc[ind]['Index'], item+'_'+text].title().replace('And','and').replace("'S","'s")
            elif address.find('ln/') >= 0 or address.find('bd/') >= 0:
                Calculation_type = ''
                for catkey in series['CATEGORIES']:
                    if int(Table[catkey+'_code'][US_t.iloc[ind]['Index']]) != 0 and Calculation_type != '':
                        Calculation_type = Calculation_type+', '+series['CATEGORIES'][catkey].loc[Table[catkey+'_code'][US_t.iloc[ind]['Index']], catkey+'_'+text].title().replace('And','and').replace("'S","'s")
                    elif int(Table[catkey+'_code'][US_t.iloc[ind]['Index']]) != 0:
                        Calculation_type = Calculation_type+series['CATEGORIES'][catkey].loc[Table[catkey+'_code'][US_t.iloc[ind]['Index']], catkey+'_'+text].title().replace('And','and').replace("'S","'s")
            else:
                Calculation_type = series['CATEGORIES'].loc[(Table[group+'_code'][US_t.iloc[ind]['Index']], Table[item+'_code'][US_t.iloc[ind]['Index']]), item+'_'+text].title().replace('And','and').replace("'S","'s")
        
        UNIT = unit
    elif source == 'Bureau Of Labor Statistics' and address.find('DSCO') >= 0:
        form_e = 'Adjusted employment (CPS employment adjusted to CES concepts)'
        unit = 'Thousands of people'
        form_c = 'Seasonally Adjusted'
        Calculation_type = ''
        UNIT = unit
    if address.find('ec/') >= 0 and bool(re.search(r'[Uu]nion|[Rr]egion|[Aa]rea', Calculation_type)):
        title = titles['Other'][address]+', '
    else:
        title = titles['Titles'][address]+', '
    #desc_e = str(AREMOS_key['description'][0])
    if source != 'Bureau Of Labor Statistics' and address.find('MADI') < 0 and address.find('CHCG') < 0:
        content = content+form_e+', '
        if address.find('ISM') >= 0:
            note, note_num, note_part, note_suffix = US_ADDNOTE([str(US_t.iloc[ind]['Index'])[1:repl]], NOTE, note, note_num, note_part, specific=True)
            content = re.sub(r',\s$', note_suffix+', ', content)
        if source == 'Department Of The Treasury' or source == 'Institute for Supply Management' or source == 'Federal Reserve Bank of Richmond':
            content = content+Calculation_type+', '
            if address.find('ISM') >= 0:
                note, note_num, note_part, note_suffix = US_ADDNOTE([str(US_t.iloc[ind]['Index'])[repl:repl2]], NOTE, note, note_num, note_part, specific=True)
                content = re.sub(r',\s$', note_suffix+', ', content)
    if not not formnote:
        if series['CATEGORIES'].loc[categ, 'key_desc'] in formnote:
            cont = content[re.search(r'[0-9]+,\s',content).start():]
            note, note_num, note_part, note_suffix = US_ADDNOTE(cont, NOTE, note, note_num, note_part)
            content = re.sub(r'[0-9]+,\s', note_suffix+', ', content)
    attribute = []
    if LABEL.empty == True:
        label = Calculation_type+', '
        attribute.append(label)
    elif LABEL[US_t.index[ind]].find(':') >= 0 and source != 'Bureau Of Labor Statistics' and source != 'Federal Reserve Economic Data':
        attribute.append(LABEL[US_t.index[ind]][LABEL[US_t.index[ind]].find(':')+1:].replace(', ', ',').strip()+', ')
    else:
        if source == 'Bureau Of Labor Statistics' or address.find('APEP') >= 0:
            attribute.append(LABEL[US_t.index[ind]].strip()+', ')
            if address.find('bd/') >= 0:
                content = content+series['CATEGORIES']['dataelement'].loc[Table['dataelement_code'][US_t.iloc[ind]['Index']], 'dataelement_'+text].strip()+', '
            if address.find('ce/') >= 0 or address.find('pr/') >= 0 or address.find('ec/') >= 0 or address.find('jt/') >= 0 or address.find('ml/') >= 0:
                if address.find('ec/') >= 0 or address.find('ml/') >= 0:
                    form_e = form_e.title()
                content = content+form_e+', '
            if address.find('ei/') >= 0:
                for no in NOTE:
                    if attribute[0].find(', '+no[0]) >= 0:
                        subword = no[0]
                        note, note_num, note_part, note_suffix = US_ADDNOTE([subword], NOTE, note, note_num, note_part, specific=True)
                        attribute[0] = attribute[0].replace(subword, subword+note_suffix)
        elif source == 'Department Of The Treasury, Bureau Of The Fiscal Service':
            attribute.append(LABEL[US_t.index[ind]].replace(', ', ',').strip()+', '+Calculation_type+', ')
        else:
            attribute.append(LABEL[US_t.index[ind]].replace(', ', ',').strip()+', ')
        if address.find('FTD') >= 0:
            attri = ''
            for note_item in NOTE:
                if attribute[0].find(str(note_item[0])) >= 0:
                    attri = str(note_item[0])
            note, note_num, note_part, note_suffix = US_ADDNOTE([attri], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = attribute[0].replace(attri, attri+note_suffix)
            note, note_num, note_part, note_suffix = US_ADDNOTE([str(US_t.iloc[ind]['Index'])[1:repl]], NOTE, note, note_num, note_part, specific=True)
            cat_key = str(US_t.iloc[ind]['Index'])[1:repl]
            if cat_key.isnumeric():
                cat_key = int(cat_key)
            attribute[0] = attribute[0].replace(series['CATEGORIES'].loc[cat_key, 'cat_desc'], series['CATEGORIES'].loc[cat_key, 'cat_desc']+note_suffix)
            note, note_num, note_part, note_suffix = US_ADDNOTE([str(US_t.iloc[ind]['Index'])[repl2:]], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = re.sub(r',\s$', note_suffix+', ', attribute[0])
        if address.find('HOUS') >= 0:
            note, note_num, note_part, note_suffix = US_ADDNOTE([str(US_t.iloc[ind]['Index'])[repl:]], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = re.sub(r',\s$', note_suffix+', ', attribute[0]) + Calculation_type + ', '
        if address.find('MRTS') >= 0 or (address.find('UIWC') >= 0 and frequency == 'M') or sheet_name == 'Cargo Revenue Ton-Miles':
            note, note_num, note_part, note_suffix = US_ADDNOTE([str(US_t.iloc[ind]['Index'])[1:repl]], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = re.sub(r',\s$', note_suffix+', ', attribute[0])
        if address.find('CBS') >= 0:
            note, note_num, note_part, note_suffix = US_ADDNOTE([str(US_t.iloc[ind]['Index'])[2:repl]], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = re.sub(r',\s$', note_suffix+', ', attribute[0])
        if address.find('MWTS') >= 0 or address.find('MRTS') >= 0:
            note, note_num, note_part, note_suffix = US_ADDNOTE([str(US_t.iloc[ind]['Index'])[:1]], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = re.sub(r',\s$', note_suffix+', ', attribute[0])
        if str(sheet_name).find('mfhhis') >= 0:
            note, note_num, note_part, note_suffix = US_ADDNOTE([str(US_t.iloc[ind]['Index'])[1:]], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = re.sub(r',\s$', note_suffix+', ', attribute[0])
    if address.find('STL') >= 0 or file_name == 'TRPT' or file_name == 'UIWC' or file_name == 'UIIT':
        attri = []
        for note_item in NOTE:
            if note_item[0].find(str(US_t.iloc[ind]['Index'])+'.') >= 0:
                attri.append(note_item[0])
        note, note_num, note_part, note_suffix = US_ADDNOTE(attri, NOTE, note, note_num, note_part, specific=True)
    elif address.find('BTS') >= 0 and sheet_name != 'Cargo Revenue Ton-Miles':
        note, note_num, note_part, note_suffix = US_ADDNOTE(str(US_t.iloc[ind]['Label_note']), NOTE, note, note_num, note_part, alphabet=True)
        attribute[0] = re.sub(r', ', note_suffix+', ', attribute[0])
    if source == 'Bureau of Economic Analysis' or source == 'Bureau Of Census' or source == 'National Association of Home Builders'\
         or source.find('Department Of The Treasury') >= 0 or source == 'Bureau Of Transportation Statistics' or source == 'Energy Information Administration':
        if address.find('FTD') >= 0 or address.find('TICS') >= 0 or address.find('TRPT') >= 0:
            begin = list(series['CATEGORIES']['cat_desc'].index).index(cat_key)
            UNIT, attribute = US_ADDLABEL(begin, address, sheet_name, series['CATEGORIES']['cat_desc'], list(series['CATEGORIES']['cat_indent']), UNIT, unit, Calculation_type, attribute)
        elif address.find('PETR') >= 0:
            begin = list(series['DATA TYPES']['dt_desc'].index).index(dt_key)
            UNIT, attribute = US_ADDLABEL(begin, address, sheet_name, series['DATA TYPES']['dt_desc'], list(series['DATA TYPES']['dt_indent']), UNIT, unit, Calculation_type, attribute)
        else:
            begin = list(LABEL.index).index(US_t.index[ind])
            UNIT, attribute = US_ADDLABEL(begin, address, sheet_name, LABEL, label_level, UNIT, unit, Calculation_type, attribute)
        for a in range(len(attribute)):
            if address.find('ITAS') >= 0 or address.find('NIIP') >= 0 or address.find('DIRI') >= 0:
                attribute[a] = re.sub(r'\s*\([^\(]*?line[^\)]+?\)', "", attribute[a])
            if attribute[a].find('\\1\\0') > 0:
                attribute[a] = attribute[a].replace('\\1\\0', '\\10\\')
            if bool(re.search(r'\\[0-9,\\]+\\',attribute[a])):
                attri = attribute[a][attribute[a].find('\\'):]
                note, note_num, note_part, note_suffix = US_ADDNOTE(attri, NOTE, note, note_num, note_part)
                attribute[a] = re.sub(r'\\[0-9,\\]+\\', note_suffix, attribute[a])
            elif bool(re.search(r'/[0-9,/]+/',attribute[a])):
                attri = attribute[a][attribute[a].find('/'):]
                note, note_num, note_part, note_suffix = US_ADDNOTE(attri, NOTE, note, note_num, note_part)
                attribute[a] = re.sub(r'\s*/[0-9,/]+/', note_suffix, attribute[a])
            elif bool(re.search(r'[0-9]+,\s',attribute[a])) and source == 'Bureau Of Census':
                attri = attribute[a][re.search(r'[0-9]+,\s',attribute[a]).start():]
                note, note_num, note_part, note_suffix = US_ADDNOTE(attri, NOTE, note, note_num, note_part)
                attribute[a] = re.sub(r'[0-9]+,\s', note_suffix+', ', attribute[a])
            elif bool(re.search(r'[0-9]+/,\s',attribute[a])) and source == 'Department Of The Treasury':
                attri = attribute[a][re.search(r'[0-9]+/,\s',attribute[a]).start():attribute[a].find('/')]
                note, note_num, note_part, note_suffix = US_ADDNOTE(attri, NOTE, note, note_num, note_part)
                attribute[a] = re.sub(r'[0-9]+/,\s', note_suffix+', ', attribute[a])
        for note_item in NOTE:
            if note_item[0] == 'Note':
                note = note+'('+str(note_num)+')'+note_item[1]
                note_num += 1
    elif source == 'Bureau Of Labor Statistics':
        if address.find('cu/') >= 0 or address.find('cw/') >= 0:
            begin = list(series['CATEGORIES'][item+'_'+text].index).index(Table[item+'_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, address, sheet_name, series['CATEGORIES'][item+'_'+text], list(series['CATEGORIES']['display_level']), UNIT, unit, Calculation_type, attribute)
            begin = list(series['DATA TYPE'][group+'_'+text].index).index(Table[group+'_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, address, sheet_name, series['DATA TYPE'][group+'_'+text], list(series['DATA TYPE']['display_level']), UNIT, unit, Calculation_type, attribute, suffix=True, form=form_e)
        elif address.find('ce/') >= 0 or address.find('ml/') >= 0:
            begin = list(series['CATEGORIES'][item+'_'+text].index).index(Table[item+'_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, address, sheet_name, series['CATEGORIES'][item+'_'+text], list(series['CATEGORIES']['display_level']), UNIT, unit, Calculation_type, attribute)
        elif address.find('bd/') >= 0:
            begin = list(series['CATEGORIES']['industry']['industry_'+text].index).index(Table['industry_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, address, sheet_name, series['CATEGORIES']['industry']['industry_'+text], list(series['CATEGORIES']['industry']['display_level']), UNIT, unit, Calculation_type, attribute)
            attribute.insert(0, form_e+', ')
            begin = list(series['DATA TYPE'][group+'_'+text].index).index(Table[group+'_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, address, sheet_name, series['DATA TYPE'][group+'_'+text], list(series['DATA TYPE']['display_level']), UNIT, unit, Calculation_type, attribute)
            attribute.append('for firms with '+series['CATEGORIES']['sizeclass'].loc[Table['sizeclass_code'][US_t.iloc[ind]['Index']], 'sizeclass_'+text].strip()+', ')
            attribute.append(series['BASE'].loc[Table[base+'_code'][US_t.iloc[ind]['Index']], base+'_'+p_text].strip()+', ')
        elif address.find('jt/') >= 0:
            begin = list(series['CATEGORIES'][item+'_'+text].index).index(Table[item+'_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, address, sheet_name, series['CATEGORIES'][item+'_'+text], list(series['CATEGORIES']['display_level']), UNIT, unit, Calculation_type, attribute)
            attribute.append(series['BASE'].loc[Table[base+'_code'][US_t.iloc[ind]['Index']], base+'_'+p_text].strip()+', ')
        elif address.find('in/') >= 0:
            attribute.insert(0, form_e.replace(', ', ',')+', ')
            begin = list(series['DATA TYPE'][group+'_'+text].index).index(Table[group+'_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, address, sheet_name, series['DATA TYPE'][group+'_'+text], list(series['DATA TYPE']['display_level']), UNIT, unit, Calculation_type, attribute)
            attribute.append(series['BASE'].loc[Table[base+'_code'][US_t.iloc[ind]['Index']], base+'_'+text].title().strip().replace(' Of', ' of')+', ')
        for note_item in NOTE:
            if type(Table['footnote_codes'][US_t.iloc[ind]['Index']]) == float and Table['footnote_codes'][US_t.iloc[ind]['Index']].is_integer():
                Table['footnote_codes'][US_t.iloc[ind]['Index']] = int(Table['footnote_codes'][US_t.iloc[ind]['Index']])
            if note_item[0] in re.split(r',', str(Table['footnote_codes'][US_t.iloc[ind]['Index']])) and address.find('ei/') < 0:
                note = note+'('+str(note_num)+')'+note_item[1]
                note_num += 1
    for attri in attribute:
        content = content+attri
    if address.find('ITAS') >= 0 and file_name != 'Ita_T1.2':
        content = content+Calculation_type+', '
    if source != 'National Association of Realtors' and source != 'Bureau Of Labor Statistics' and address.find('APEP') < 0 and address.find('H6') < 0 and address.find('G19') < 0:
        content = content+form_c+', '
    elif address.find('MADI') >= 0:
        content = content+Calculation_type+', '
    elif source == 'Bureau Of Labor Statistics':
        if address.find('DSCO') >= 0 or address.find('ce/') >= 0 or address.find('pr/') >= 0 or address.find('mp/') >= 0 or address.find('ec/') >= 0 or address.find('bd/') >= 0 or address.find('jt/') >= 0 or address.find('in/') >= 0 or address.find('ml/') >= 0:
            content = content+form_c+', '
        SEAS = {'S':'Seas','U':'Unadj'}
        if address.find('ln/') >= 0 and bool(re.match(r'\(', content)) == False:
            content = '('+SEAS[US_t.iloc[ind]['Index'][2]]+') '+content
        SUB = ['Level','Rate','Not in Labor Force']
        for sub in SUB:
            if address.find('ln/') >= 0 and content.find(sub+' ') >= 0 and content.find(sub+' -') < 0 and content.find(sub+' to') < 0:
                content = content.replace(sub+' ', sub+' - ')
        if series['BASE'].empty == True and address.find('ln/') < 0 and address.find('mp/') < 0:
            content = re.sub(r'([^0-9])\-([^a-z])', r"\1, \2", content.replace('&', 'and'), 1)
        elif series['BASE'].empty == True and address.find('ln/') >= 0:
            content = re.sub(r'\-([\sA-Z])', r", \1", re.sub(r'Employment-[Pp]opulation|Employment to Population', "Employment Population", content.replace('&', 'and')+form_c+', '), 1)
        else:
            content = content.replace(', all urban consumers', '').replace(', urban wage earners and clerical workers', '')
        if address.find('ln/') >= 0:
            content = re.sub(r'\([Ss]eas\)\s+|\([Uu]nadj\)\s+', "", re.sub(r'\)\s+(Civilian\s)*[Ll]abor\s[Ff]orce\s([Ll]evel)*(\s)*([^F])', r") Civilian Labor Force \4", re.sub(r'[Pp]articipation [Rr]ate', "Participation Rate", \
                re.sub(r'yrs[\.]*', "years old", re.sub(r'\)\s+(Population)\s(Level)*(\s)*', r") Civilian Noninstitutional \1", re.sub(r'\)\s+Employment\s(Level)*(\s)*([^P])', r") Employed\3", \
                re.sub(r'[Ii]n [Ll]abor [Ff]orce\s(Level\s)*,', "in Labor Force,", re.sub(r'\)\s+Unemployment\s(Level)*(\s)*([^R])', r") Unemployed\3", \
                content.replace('Pvt W/S', 'Private Wage and salary').replace('EMPL. LEVEL', 'Employed,').replace(' rat', ' Rat').replace('Percent distribution', 'Percent Distribution')))))))))
        content = re.sub(r'\s+', " ", re.sub(r'\.*(\s\.)+([^0-9])', r"\2", re.sub(r"([0-9]+)'([^s])", r"\1 ft. \2", re.sub(r'([0-9]+)("|\'{2})', r"\1 in. ", re.sub(r'x(\s*[0-9])', r" times \1", \
            re.sub(r'([^0-9a-z])\.([0-9]+)', r"\1 0.\2", re.sub(r'([0-9]+)\s([0-9]+/[0-9]+)', r"\1 and \2", content))))))).replace('"', '').replace("'s", 's').replace("s'", 's').replace("'", '').replace(' ,', ',')
    note = note.strip()
    if note != '':
        desc_e = title + content + 'Unit: ' + UNIT.replace('[','').replace('] ',', ').replace(']','') + ', Source: ' + source + ', Note: ' + note
    else:
        desc_e = title + content + 'Unit: ' + UNIT.replace('[','').replace('] ',', ').replace(']','') + ', Source: ' + source
    for footnote_item in FOOTNOTE:
        if desc_e.find(footnote_item[0]) >= 0:
            desc_e = desc_e.replace(footnote_item[0],footnote_item[1])
    desc_e = desc_e.replace('"', '').replace("'", '').replace('#', ' ')
    if address.find('ITAS') >= 0 or address.find('NIIP') >= 0 or address.find('DIRI') >= 0:
        desc_c = re.sub(r'\*\(.+\)', "", attribute[0].replace(', ',''))
    elif source == 'Bureau of Economic Analysis' or UNIT != unit:
        desc_c = UNIT.replace('[','').replace('] ',', ').replace(']','')
    elif source == 'Bureau Of Labor Statistics' and series['BASE'].empty == False:
        if address.find('pr/') >= 0:
            desc_c = series['BASE'].loc[Table[base+'_code'][US_t.iloc[ind]['Index']], base+'_'+text].title()
        else:
            desc_c = series['BASE'].loc[Table[base+'_code'][US_t.iloc[ind]['Index']], base+'_'+p_text].title()
        if address.find('cu/') >= 0 or address.find('cw/') >= 0:
            desc_c = desc_c+' Reference Base'
    elif address.find('FTD') >= 0:
        desc_c = series['DATA TYPES'].loc[str(US_t.iloc[ind]['Index'])[repl:repl2], 'dt_desc']
    elif address.find('TICS') >= 0:
        desc_c = series['GEO LEVELS'].loc[int(US_t.iloc[ind]['Index'][repl:]), 'geo_desc']
    else:
        desc_c = ''
    table_id = address+','+file_name+','+str(sheet_name)
    
    start_found = False
    last_found = False
    found = False
    for k in range(len(value)):
        if (str(index[k]).find(frequency) >= 0 or str(index[k]).isnumeric()) and source == 'Bureau of Economic Analysis':
            if frequency == 'A':
                freq_index = int(index[k])
            else:
                freq_index = str(index[k]).replace(frequency,repl)
        else:
            try:
                freq_index = int(index[k])
            except:
                freq_index = str(index[k])
        if freq_index in db_table_t.index and ((find_unknown == False and int(str(freq_index)[:4]) >= dealing_start_year) or find_unknown == True):
            if str(value[k]).strip() in NonValue or bool(re.search(r'/[0-9]+/', str(value[k]))):
                db_table_t[db_code][freq_index] = ''
            else:
                found = True
                try:
                    db_table_t[db_code][freq_index] = float(value[k])
                except ValueError:
                    ERROR('Nontype Value detected: '+str(value[k]))
                if start_found == False:
                    if frequency == 'A':
                        start = int(freq_index)
                    else:
                        start = str(freq_index)
                    start_found = True
        else:
            continue
    
    if start_found == False:
        if found == True:
            ERROR('start not found: '+str(name))
    try:
        last = db_table_t[db_code].loc[~db_table_t[db_code].isin(NonValue)].index[-1]
    except IndexError:
        if found == True:
            ERROR('last not found: '+str(name))
    if found == False:
        start = 'Nan'
        last = 'Nan'
    
    if (bls_start == None or (bls_start != None and find_unknown == True)) and source == 'Bureau Of Labor Statistics':
        if frequency == 'M' and start.replace('-', '-M') != US_t.iloc[ind]['start'] and US_t.iloc[ind]['start'].find('-M13') < 0 and str(US_t.iloc[ind][US_t.iloc[ind]['start'].replace('-M','-')]).strip() not in NonValue:
            ERROR('start error: '+str(name)+', produced start = '+start.replace('-', '-M')+', dataframe start = '+US_t.iloc[ind]['start'])
        elif frequency == 'A' and str(start)+YEAR[repl] != US_t.iloc[ind]['start'] and US_t.iloc[ind]['start'].find(YEAR[repl]) >= 0 and str(US_t.iloc[ind][int(US_t.iloc[ind]['start'].replace(YEAR[repl],''))]).strip() not in NonValue:
            ERROR('start error: '+str(name)+', produced start = '+str(last)+YEAR[repl]+', dataframe start = '+US_t.iloc[ind]['start'])
        elif frequency == 'S' and start.replace('S', 'S0') != US_t.iloc[ind]['start'] and US_t.iloc[ind]['start'].find('S03') < 0 and str(US_t.iloc[ind][US_t.iloc[ind]['start'].replace('S0','S')]).strip() not in NonValue:
            ERROR('start error: '+str(name)+', produced start = '+start.replace('S', 'S0')+', dataframe start = '+US_t.iloc[ind]['start'])
        elif frequency == 'Q' and US_t.iloc[ind]['start'][-3:] in QUAR and str(US_t.iloc[ind][US_t.iloc[ind]['start'].replace(US_t.iloc[ind]['start'][-3:],QUAR[US_t.iloc[ind]['start'][-3:]])]).strip() not in NonValue:
            if start.replace(start[-2:], RAUQ[repl2][start[-2:]]) != US_t.iloc[ind]['start']:
                ERROR('start error: '+str(name)+', produced start = '+start.replace(start[-2:], RAUQ[repl2][start[-2:]])+', dataframe start = '+US_t.iloc[ind]['start'])
    if source == 'Bureau Of Labor Statistics' and str(US_t.iloc[ind]['last'])[:4] >= str(dealing_start_year):
        if frequency == 'M' and last.replace('-', '-M') != US_t.iloc[ind]['last'] and US_t.iloc[ind]['last'].find('-M13') < 0 and str(US_t.iloc[ind][US_t.iloc[ind]['last'].replace('-M','-')]).strip() not in NonValue:
            ERROR('last error: '+str(name)+', produced last = '+last.replace('-', '-M')+', dataframe last = '+US_t.iloc[ind]['last'])
        elif frequency == 'A' and str(last)+YEAR[repl] != US_t.iloc[ind]['last'] and US_t.iloc[ind]['last'].find(YEAR[repl]) >= 0 and str(US_t.iloc[ind][int(US_t.iloc[ind]['last'].replace(YEAR[repl],''))]).strip() not in NonValue:
            ERROR('last error: '+str(name)+', produced last = '+str(last)+YEAR[repl]+', dataframe last = '+US_t.iloc[ind]['last'])
        elif frequency == 'S' and last.replace('S', 'S0') != US_t.iloc[ind]['last'] and US_t.iloc[ind]['last'].find('S03') < 0 and str(US_t.iloc[ind][US_t.iloc[ind]['last'].replace('S0','S')]).strip() not in NonValue:
            ERROR('last error: '+str(name)+', produced last = '+last.replace('S', 'S0')+', dataframe last = '+US_t.iloc[ind]['last'])
        elif frequency == 'Q' and US_t.iloc[ind]['last'][-3:] in QUAR and str(US_t.iloc[ind][US_t.iloc[ind]['last'].replace(US_t.iloc[ind]['last'][-3:],QUAR[US_t.iloc[ind]['last'][-3:]])]).strip() not in NonValue:
            if last.replace(last[-2:], RAUQ[repl2][last[-2:]]) != US_t.iloc[ind]['last']:
                ERROR('last error: '+str(name)+', produced last = '+last.replace(last[-2:], RAUQ[repl2][last[-2:]])+', dataframe last = '+US_t.iloc[ind]['last'])

    key_tmp= [databank, name, db_table, db_code, desc_e, desc_c, frequency, start, last, unit, Calculation_type, snl, source, form_e, form_c, table_id]
    KEY_DATA.append(key_tmp)
    snl += 1
    
    code_num += 1
    
    return code_num, table_num, DATA_BASE, db_table_t, DB_name, snl

###########################################################################  Main Function  ###########################################################################
MONTH = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
TABLE_NAME = {'ISADJUSTED':'adj','CATEGORIES':'cat','DATA TYPES':'dt','GEO LEVELS':'geo'}
NEW_TABLES = TABLES.copy()
NEW_TABLES = NEW_TABLES.set_index(['Address','File','Sheet']).sort_index()
Zip_table = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=[0,1], sheet_name_='ZIPdatasets').sort_index()
chrome = None
new_item_counts = 0

for source in SOURCE(TABLES):
    if main_file.empty == False:
        break
    for address in FILE_ADDRESS(source):
        if make_discontinued == False and (address.find('DSCO') >= 0 or address.find('in/') >= 0 or address.find('ml/') >= 0):
            continue
        if str(address).find(keyword[0]) < 0:
            continue
        to_be_ignore =False
        for ig in ignore:
            if str(address).find(ig) >= 0:
                to_be_ignore = True
                break
        if to_be_ignore == True:
            continue            
        address = address[4:]
        zip_list = []
        #TABLES = TABLES.set_index(['Sheet'], drop=False)
        if address.find('FRB') < 0:
            Series, Table, Titles = US_KEY(address, key=re.sub(r'BEA|BOC|HOUS|APEP|FTD/|DOT|BTS|DOL|RCM|EIA|IRS', "", address).replace('/',''))
        if source == 'Institute for Supply Management':
            qd.ApiConfig.api_key = Table
        for fname in FILE_NAME(source, address):
            if make_discontinued == False and fname.find('discontinued') >= 0:
                continue
            if str(fname).find(keyword[1]) < 0:
                continue
            Zip = False
            if source == 'Bureau of Economic Analysis' or source == 'Federal Reserve Economic Data' or address.find('MTST') >= 0\
                 or fname == 'UIWC' or fname == 'UIIT' or fname == 'BEOL' or fname == 'TRPT':
                Zip = True
            if chrome == None and ((fname.find('http') >= 0 and address.find('AISI') < 0) or Zip == True or address.find('BOC') >= 0):
                options = Options()
                options.add_argument("--disable-notifications")
                options.add_argument("--disable-popup-blocking")
                options.add_argument("ignore-certificate-errors")
                options.add_experimental_option("excludeSwitches", ["enable-logging"])
                #options.add_experimental_option("prefs", {"profile.default_content_setting_values.cookies": 2})
                chrome = webdriver.Chrome(ChromeDriverManager().install(), options=options)
                chrome.set_window_position(980,0)
            if Zip == True:
                file_address = data_path+address
                file_path = file_address+Zip_table.at[(address,fname), 'Zipname']+'.zip'
                present_file_existed = PRESENT(file_path)
                if Zip_table.at[(address,fname), 'Zipname']+'.zip' not in zip_list:
                    if present_file_existed == True:
                        zipname = Zip_table.at[(address,fname), 'Zipname']+'.zip'
                    else:
                        zipname = US_WEB(chrome, address, Zip_table.at[(address,fname), 'website'], Zip_table.at[(address,fname), 'Zipname'], Table=Zip_table, Zip=True, file_name=fname)
                    zip_list.append(zipname)
                zf = zipfile.ZipFile(file_path,'r')
                if source == 'Federal Reserve Economic Data' or fname == 'UIWC' or fname == 'UIIT' or fname == 'BEOL':
                    Series_temp = readExcelFile(zf.open(fname+'.xls'), sheet_name_=0)
                    Series = list(Series_temp[0])
            if source == 'Bureau of Economic Analysis' and (address.find('NIPA') >= 0 or address.find('FAAT') >= 0):
                logging.info('Reading source file, Time: '+str(int(time.time() - tStart))+' s'+'\n')
                US_t_dict = readExcelFile(zf.open(fname+'.xlsx'), header_=0, index_col_=0, skiprows_=list(range(7)), acceptNoFile=False)
                unit_dict = readExcelFile(zf.open(fname+'.xlsx'), usecols_=[0], acceptNoFile=False)
                sheet_list = list(US_t_dict)
                sheet_list.remove('Contents')
            else:
                sheet_list = SHEET_NAME(address, fname)
            for sname in sheet_list:
                if sname == 'None':
                    sname = None
                if Historical == False:
                    try:
                        if str(NEW_TABLES.loc[(address,fname,sname), 'keyword']).find('historical') >= 0:
                            continue
                    except KeyError:
                        time.sleep(0)
                #bls_read = False
                if source == 'Bureau of Economic Analysis' and (address.find('NIPA') >= 0 or address.find('FAAT') >= 0):
                    US_t = US_t_dict[sname]
                distinguish_sheet = False
                if address.find('HOUS') >= 0 or address.find('NAR') >= 0 or address.find('POP') >= 0 or address.find('ITAS') >= 0 or address.find('NIIP') >= 0 or address.find('DIRI') >= 0 or address.find('PETR') >= 0:
                    distinguish_sheet = True
                for freq in FREQUENCY(address, fname, sname, distinguish_sheet):
                    logging.info('Reading file: '+fname+', sheet: '+str(sname)+', frequency: '+freq+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
                    unit = 'nan'
                    repl2 = None
                    formnote = {}
                    YEAR2 = None
                    QUAR2 = None
                    RAUQ = None
                    if source == 'Bureau of Economic Analysis':
                        if address.find('ITAS') >= 0 or address.find('NIIP') >= 0 or address.find('DIRI') >= 0:
                            unit = re.sub(r'\s+NOTE:.+', "", str(readExcelFile(zf.open(fname+'.xls'), usecols_=[0], sheet_name_=sname).iloc[1][0]).strip())
                            HEAD = {'A':1,'Q':2}
                            US_t = readExcelFile(zf.open(fname+'.xls'), index_col_=0, skiprows_=list(range(int(Table.loc[(address,fname,sname), 'skip'].item()))), sheet_name_=sname)
                            US_t.columns = [US_t.iloc[i].fillna(method='pad').str.strip() for i in range(HEAD[freq])]
                            BEA_cols = []
                            for j in range(len(US_t.columns)):
                                if str(US_t.columns[j][0]).isnumeric() == False:
                                    BEA_cols.append('Label')
                                else:
                                    if freq == 'A':
                                        BEA_cols.append(int(US_t.columns[j][0]))
                                    elif freq == 'Q':
                                        BEA_cols.append(str(US_t.columns[j][0])+str(US_t.columns[j][1]))
                            US_t.columns = BEA_cols
                            US_t = US_t[US_t.index.notnull()].drop(index=['Line'], errors='ignore')
                            BEA_inds = []
                            for i in range(len(US_t.index)):
                                if bool(re.match(r'[0-9]+[a-z]*$', str(US_t.index[i]))):
                                    if address.find('ITAS') >= 0:
                                        BEA_inds.append(Series['ISADJUSTED'].loc[sname, 'adj_code']+str(US_t.index[i]).rjust(4,'0')+'ITA'+Series['GEO LEVELS'].loc[fname, 'geo_code'])
                                    elif address.find('NIIP') >= 0:
                                        BEA_inds.append(Series['ISADJUSTED'].loc[sname, 'adj_code']+str(US_t.index[i]).rjust(4,'0')+'IIP')
                                    elif address.find('DIRI') >= 0:
                                        BEA_inds.append(Series['ISADJUSTED'].loc[sname, 'adj_code']+str(US_t.index[i]).rjust(4,'0')+Series['DATA TYPES'].loc[fname, 'dt_code'])
                                else:
                                    BEA_inds.append('nan')
                            US_t.insert(loc=0, column='Index', value=BEA_inds)
                            if address.find('DIRI') >= 0:
                                repl2 = re.sub(r'.+(U\.S\..+)', r"\1", str(readExcelFile(zf.open(fname+'.xls'), usecols_=[0], sheet_name_=sname).iloc[0][0]).strip())
                            else:
                                repl2 = re.sub(r'.+,\s+(.+)', r"\1", str(readExcelFile(zf.open(fname+'.xls'), usecols_=[0], sheet_name_=sname).iloc[0][0]).strip())
                        else:
                            unit = re.sub(r'\s+NOTE:.+', "", str(unit_dict[sname].iloc[1][0]).strip())
                        if US_t.empty == False:
                            #unit = re.sub(r'\s+NOTE:.+', "", str(readExcelFile(data_path+address+fname+'.xls'+excel, usecols_=[0], sheet_name_=sname).iloc[1][0]).strip())
                            if address.find('NIPA') >= 0 or address.find('FAAT') >= 0:
                                sname = re.split(r'\-', sname)[0]
                            US_t = US_t.rename(columns={'Unnamed: 1':'Label','Unnamed: 2':'Index'})
                            label = US_t['Label']
                            new_label = pd.Series(dtype='object')
                            new = False
                            for l in range(len(label)):
                                lab = str(label.iloc[l])
                                if bool(re.search(r'[\s]*S[0-9]+', lab)):
                                    new = True
                                    lab = lab+' ('+re.findall(r'S[0-9]+', lab)[0]+')'
                                    lab = re.sub(r'[\s]*S[0-9]+\s',"",lab, 1)
                                if new == True:
                                    new_label = new_label.append(pd.Series([lab], index=[label.index[l]]))
                            if new_label.empty == False:
                                label = new_label
                            label_level = US_LEVEL(label, source)
                            for item in range(len(label)):
                                if str(label.iloc[item]).strip() == 'nan':
                                    continue
                                if address.find('ITAS') >= 0 and (str(label.iloc[item]).find('account') >= 0 or str(label.iloc[item]).find('Statistical discrepancy') >= 0):
                                    label_level[item] = -1
                                elif address.find('NIIP') >= 0 and str(label.iloc[item]).find('U.S.') >= 0:
                                    label_level[item] = 0
                                elif address.find('DIRI') >= 0 and str(Series['CATEGORIES'].loc[Series['CATEGORIES']['cat_desc'] == re.sub(r'\s*\(.*line.+\)|\s*/[0-9]+/',"",str(label.iloc[item]).strip())].index[0]) == '\xa0':
                                    label_level[item] = -1
                            note, footnote = US_NOTE(US_t.index, sname, label, address)
                    elif source == 'Federal Reserve Board':
                        file_path = data_path+address+sname+'.csv'
                        if address.find('G17') >= 0:
                            if PRESENT(file_path):
                                US_temp = readFile(file_path, header_=[0], acceptNoFile=False)
                            else:
                                US_temp = readFile(fname, header_=None, names_=['code','year']+MONTH, acceptNoFile=False, sep_='\\s+')
                                US_temp.to_csv(file_path)
                            US_t = US_HISTORYDATA(US_temp, name='year', address=address, freq=freq, MONTH=MONTH, make_idx=True, find_unknown=find_unknown, DF_KEY=DF_KEY)
                        elif address.find('H6') >= 0 or address.find('G19') >= 0 or address.find('H15') >= 0:
                            if PRESENT(file_path):
                                US_t = readFile(file_path, header_=[0], index_col_=0, skiprows_=list(range(5))).T
                            else:
                                US_t = US_WEB(chrome, address, fname, sname, freq=freq, header=[0], index_col=0, skiprows=list(range(5)), csv=True).T
                            new_index = []
                            for i in range(US_t.shape[0]):
                                new_index.append(US_t.index[i][:-2])
                            US_t.insert(loc=0, column='Index', value=new_index)
                            US_t = US_t.set_index('Index', drop=False)
                        Series, Table, Titles = US_KEY(address, key=sname)
                        label = Series['Descriptions:']
                        label = NEW_LABEL(address[-3:], label.copy(), Series, Table)
                        label_level = None
                        note, footnote = US_NOTE(US_t.index, sname, label)
                        repl = None
                        for ind in list(US_t.index):
                            try:
                                Series['Descriptions:'][ind]
                            except KeyError:
                                CONTINUE.append(ind)
                    elif source == 'Federal Reserve Economic Data' or fname == 'UIWC' or fname == 'UIIT' or fname == 'BEOL':
                        Series_t = Series
                        if fname == 'UIWC':
                            Series = list(readExcelFile(zf.open(fname+'.xls'), sheet_name_=0)[0])
                        US_temp = readExcelFile(zf.open(fname+'.xls'), header_ =0, index_col_=0, sheet_name_=sname).T
                        US_t, label, note, footnote = US_STL(US_temp, address, Series)
                        label_level = None
                        repl = None
                        Series = Series_t
                    elif source == 'Federal Reserve Bank of Richmond':
                        file_path = data_path+address+sname+'.xlsx'
                        if PRESENT(file_path):
                            US_temp = readExcelFile(file_path, header_ =[0], index_col_=0, sheet_name_=0).T
                        else:
                            US_temp = US_WEB(chrome, address, fname, sname, freq=freq, header=[0], index_col=0).T
                        US_t, label, note, footnote = US_RCM(US_temp, fname, Series)
                        label_level = None
                        repl = 4
                    elif source == 'U.S. Department of Agriculture':
                        file_path = data_path+address+sname+'.csv'
                        if PRESENT(file_path):
                            US_temp = readFile(file_path, header_=0, usecols_=[1,2,16,19])
                        else:
                            US_temp = US_WEB(chrome, address, fname, sname, Table=Table, header=0, usecols=[1,2,16,19], csv=True)
                        US_t, label, note, footnote = US_DOA(US_temp, Series, Table, address, fname, sname, chrome)
                        label_level = None
                        repl = 3
                        repl2 = 6
                    elif source == 'Institute for Supply Management':
                        US_temp = qd.get(address+fname).T
                        US_t, label, note, footnote = US_ISM(US_temp, fname, Series)
                        other_notes = readExcelFile(data_path+address+'other_notes.xlsx', header_=0, index_col_=0, sheet_name_=0, acceptNoFile=False)
                        note = note + US_NOTE(other_notes, sname, address=address, other=True)
                        label_level = None
                        repl = 4
                        repl2 = 7
                    elif source == 'National Federation of Independent Business' or source == 'Organization for Economic Cooperation and Development':
                        skip, excel, head, index_col, use, nm, output, trans  = ATTRIBUTES(address, sname, Series.to_dict())
                        if sname == 'Consumer Confidence Index':
                            file_path = data_path+address+sname+'.csv'
                            csv = True
                        else:
                            file_path = data_path+address+sname+'.xls'+excel
                            csv = False
                        if PRESENT(file_path):
                            if sname == 'Consumer Confidence Index':
                                US_temp = readFile(file_path, header_=head, index_col_=index_col, usecols_=use)
                            else:
                                US_temp = readExcelFile(file_path, header_=head, index_col_=index_col, usecols_=use, sheet_name_=0)
                        else:
                            US_temp = US_WEB(chrome, address, fname, sname, header=head, index_col=index_col, excel=excel, usecols=use, csv=csv, output=output)
                        US_t, label, note, footnote = US_CBS(address, fname, sname, Series, US_temp, transpose=trans)
                        other_notes = readExcelFile(data_path+address+'other_notes.xlsx', header_=0, index_col_=0, sheet_name_=0, acceptNoFile=False)
                        note = note + US_NOTE(other_notes, sname, address=address, other=True)
                        label_level = None
                        repl = 5
                    elif source == 'Department Of The Treasury, Bureau Of The Fiscal Service':
                        US_temp = readFile(zf.open(fname+'.csv'), header_ =0)
                        US_temp = US_temp.sort_values(by=['Line Code Number','Calendar Year','Calendar Month Number'], ignore_index=True)
                        US_t, label, note, footnote, label_level = US_DOT(Series, US_temp, fname, key=Series.loc[fname, 'Key'], find_unknown=find_unknown, DF_KEY=DF_KEY)
                        repl = None
                    elif source == 'Department Of The Treasury':
                        idx = 0
                        if sname == 's1_globl':
                            idx = [0,1,2]
                        file_path = data_path+address+sname+'.csv'
                        if PRESENT(file_path):
                            US_temp = readFile(file_path, header_=[0], index_col_=idx, skiprows_=list(range(int(Table.loc[sname, 'skip'].item()))))
                            US_present = pd.DataFrame()
                            US_his = pd.DataFrame()
                        else:
                            sname_t = sname
                            if sname.find('mfhhis') >= 0:
                                sname_t = sname.replace('_historical','')
                            US_temp = US_WEB(chrome, address, fname, sname_t, freq=freq, header=[0], index_col=idx, skiprows=list(range(int(Table.loc[sname, 'skip'].item()))), csv=True)
                            if sname.find('mfhhis') >= 0:
                                US_present = pd.read_fwf('https://ticdata.treasury.gov/Publish/mfh.txt', header=[0], index_col=0, widths=[30]+[8]*13)
                                US_his = readFile(file_path, header_=[0], index_col_=0)
                        if sname == 's1_globl':
                            KEYS = {3:['Marketable'],4:["Gov't"],5:['corporate','bonds'],0:['corporate','stocks'],1:['Foreign securities','Bonds'],2:['Stocks']}
                            US_test = readFile(data_path+address+sname+'.csv')
                            for col in range(3, US_test.shape[1]):
                                for key in KEYS[col%6]:
                                    if key not in list(US_test[col]):
                                        ERROR('Items not found in column '+chr(ord('@')+col+1)+' of '+sname+'.csv: '+key)
                        US_t, label, note, footnote = US_TICS(US_temp, Series, data_path, address, sname, TICS_start, US_present=US_present, US_his=US_his, find_unknown=find_unknown, DF_KEY=DF_KEY)
                        repl = 4
                        label_level = US_LEVEL(label, source, Series, loc1=1, loc2=repl, name='CATEGORIES', indent='cat_indent')
                        if sname.find('mfhhis') >= 0:
                            other_notes = readExcelFile(data_path+address+'other_notes.xlsx', header_=0, index_col_=0, sheet_name_=0, acceptNoFile=False)
                            note = note + US_NOTE(other_notes, sname, address=address, other=True)
                    elif source == 'Bureau Of Transportation Statistics' or source == 'Department Of Labor':
                        file_name = fname
                        if fname.find('http') >= 0:
                            file_name = sname
                        TRPT_series = Series
                        if fname == 'TRPT':
                            Series_temp = readExcelFile(zf.open(fname+'.xls'), sheet_name_=0)
                            Series = list(Series_temp[0])
                        else:
                            zf = None
                        skip, excel, head, index, use, nm, output, trans  = ATTRIBUTES(address, file_name, Table)
                        suffix = ATTRIBUTES(address, file_name, Table, key='suffix')
                        US_t, label, note, footnote, unit = US_BTSDOL(data_path, address, fname, sname, Series, header=head, index_col=index, skiprows=skip, freq=freq, x=excel, usecols=use, transpose=trans, suffix=suffix, names=nm, TRPT=TRPT_series, chrome=chrome, zf=zf, output=output)
                        repl = 4
                        repl2 = 7
                        if suffix.find('SAT') >= 0:
                            repl2 = 8
                        if fname == 'TRPT':
                            Series = TRPT_series
                        label_level = US_LEVEL(label, source, Series, loc1=1, loc2=repl, name='CATEGORIES', indent='cat_indent')
                        if (address.find('UIWC') >= 0 and freq == 'M') or sname == 'Cargo Revenue Ton-Miles':
                            other_notes = readExcelFile(data_path+address+'other_notes.xlsx', header_=0, index_col_=0, sheet_name_=0, acceptNoFile=False)
                            note = note + US_NOTE(other_notes, sname, address=address, other=True)
                    elif source == 'Semiconductor Equipment and Materials International':
                        US_t, label, note, footnote = US_SEMI(data_path, address, fname, freq, chrome)
                        label_level = None
                        repl = None
                    elif source == 'American Iron and Steel Institute':
                        US_t, label, note, footnote = US_AISI(data_path, address, fname)
                        label_level = None
                        repl = None
                    elif source == 'Energy Information Administration' or address.find('PETR') >= 0 or source == 'Internal Revenue Service':
                        file_name = fname
                        if fname.find('http') >= 0:
                            file_name = sname
                        skip, excel, head, index, use, nm, output, trans  = ATTRIBUTES(address, file_name, Table)
                        prefix = ATTRIBUTES(address, file_name, Table, key='prefix')
                        tables = ATTRIBUTES(address, file_name, Table, key='tables')
                        US_t, label, note, footnote = US_EIAIRS(Series, data_path, address, fname, sname, freq, tables=tables, x=excel, header=head, index_col=index, skiprows=skip, transpose=trans, usecols=use, prefix=prefix, chrome=chrome)
                        repl = prefix
                        label_level = None
                    elif (source == 'Bureau Of Census' or source == 'National Association of Home Builders') and address.find('FTD') < 0:
                        file_name = tuple([address,fname,sname])
                        skip, excel, head, dex, use, nm, output, trans  = ATTRIBUTES(address, file_name, Table)
                        subword = ATTRIBUTES(address, file_name, Table, key='subword')
                        prefix = ATTRIBUTES(address, file_name, Table, key='prefix')
                        middle = ATTRIBUTES(address, file_name, Table, key='middle')
                        suffix = ATTRIBUTES(address, file_name, Table, key='suffix')
                        datasets = ATTRIBUTES(address, file_name, Table, key='datasets')
                        password = ATTRIBUTES(address, file_name, Table, key='password')
                        key_text = ATTRIBUTES(address, file_name, Table, key='key_text')
                        website = ATTRIBUTES(address, file_name, Table, key='website')
                        file_name = None
                        if str(Table['file_name'][(address,fname,sname)]) != 'nan':
                            file_name = fname
                        sheet_name = None
                        if str(Table['sheet_name'][(address,fname,sname)]) != 'nan':
                            sheet_name = sname
                        HIES = False
                        if address.find('HIES') >= 0:
                            HIES = True
                        if (address.find('MRTS') >= 0 and freq == 'Q') or (address.find('SHIP') >= 0 and freq == 'A'):
                            for table in Series:
                                Series[table] = Series[table].reset_index().set_index(TABLE_NAME[table]+'_code')
                        elif address.find('POPT') >= 0:
                            US_POPT(chrome, website, data_path, address, fname, sname)
                        if address.find('POPP') >= 0:
                            file_path = data_path+address+fname+'.csv'
                            if PRESENT(file_path):
                                US_temp = readFile(file_path, header_=[0])
                            else:
                                US_temp = US_WEB(chrome, address, website, fname, freq=freq, tables=[sname], header=[0], csv=True)
                            US_t, label, note, footnote = US_POPP(US_temp, data_path, address, datasets=datasets, DIY_series=Series, find_unknown=find_unknown, DF_KEY=DF_KEY)
                        elif address.find('FAMI') >= 0 or address.find('MADI') >= 0 or address.find('SCEN') >= 0:
                            US_t, label, note, footnote, formnote = US_FAMI(prefix, middle, data_path, address, fname, sname, Series, x=excel, chrome=chrome, website=website)
                        else:
                            US_t, label, note, footnote = DATA_SETS(data_path, address, fname=file_name, sname=sheet_name, datasets=datasets, DIY_series=Series, MONTH=MONTH, password=password, header=head, index_col=dex, skiprows=skip,\
                                 freq=freq, x=excel, usecols=use, transpose=trans, HIES=HIES, subword=subword, prefix=prefix, middle=middle, suffix=suffix, chrome=chrome, key_text=key_text, Zip_table=Zip_table, website=website, find_unknown=find_unknown, DF_KEY=DF_KEY)
                        if datasets == fname:
                            for table in Series:
                                if address.find('POPP') >= 0:
                                    if table in TABLE_NAME:
                                        Series[table] = Series[table].reset_index().set_index('aremos_key')
                                else:
                                    Series[table] = Series[table].reset_index().set_index(TABLE_NAME[table]+'_code')
                                if address.find('HIHV') >= 0:
                                    Series[table] = Series[table][~Series[table].index.duplicated()]
                        if bool(Table['other_notes'][(address,fname,sname)]):
                            other_notes = readExcelFile(data_path+address+'other_notes.xlsx', header_=0, index_col_=0, sheet_name_=0, acceptNoFile=False)
                            note = note + US_NOTE(other_notes, sname, address=address, other=True)
                        repl = int(Table['repl'][(address,fname,sname)])
                        if str(Table['repl2'][(address,fname,sname)]) != 'nan':
                            repl2 = int(Table['repl2'][(address,fname,sname)])
                        location = [0,0]
                        for l in [1,2]:
                            if Table['loc'+str(l)][(address,fname,sname)] == 'repl':
                                location[l-1] = repl
                            elif Table['loc'+str(l)][(address,fname,sname)] == 'repl2':
                                location[l-1] = repl2
                            else:
                                location[l-1] = int(Table['loc'+str(l)][(address,fname,sname)])
                        if Table['level'][(address,fname,sname)] == 'C':
                            level_name = 'CATEGORIES'
                            level_indent = 'cat_indent'
                        elif Table['level'][(address,fname,sname)] == 'D':
                            level_name = 'DATA TYPES'
                            level_indent = 'dt_indent'
                        label_level = US_LEVEL(label, source, Series, loc1=location[0], loc2=location[1], name=level_name, indent=level_indent)
                        if address.find('PRIC') >= 0:
                            unit = str(readExcelFile(data_path+address+fname+'.xls', usecols_=[0], sheet_name_=sname).iloc[1][0]).strip()
                    elif source == 'Bureau Of Census' and address.find('FTD') >= 0:
                        file_name = fname
                        if fname.find('http') >= 0:
                            file_name = sname
                        skip, excel, head, index, use, nm, output, trans  = ATTRIBUTES(address, file_name, Table)
                        prefix = ATTRIBUTES(address, file_name, Table, key='prefix')
                        middle = ATTRIBUTES(address, file_name, Table, key='middle')
                        suffix = ATTRIBUTES(address, file_name, Table, key='suffix')
                        multi = ATTRIBUTES(address, file_name, Table, key='multi')
                        final_name = ATTRIBUTES(address, file_name, Table, key='final_name')
                        ft900_name = ATTRIBUTES(address, file_name, Table, key='ft900_name')
                        if fname.find('http') >= 0:
                            US_t, label, note, footnote = DATA_SETS(data_path, address, fname=fname, sname=sname, DIY_series=Series, header=head, index_col=0, skiprows=skip, freq=freq,\
                                 x=excel, usecols=use, names=nm, transpose=trans, multi=multi, prefix=prefix, middle=middle, suffix=suffix, chrome=chrome)
                        else:
                            US_t, label, note, footnote = US_FTD_NEW(chrome, data_path, address, fname, Series, prefix, middle, suffix, freq, trans, Zip_table, excel=excel,\
                                 skip=skip, head=head, index_col=0, usecols=use, names=nm, multi=multi, final_name=final_name, ft900_name=ft900_name)
                        other_notes = readExcelFile(data_path+address+'other_notes.xlsx', header_=0, index_col_=0, sheet_name_=0, acceptNoFile=False)
                        note = note + US_NOTE(other_notes, sname, address=address, other=True)
                        repl = 4
                        repl2 = 6
                        label_level = None
                    elif source == 'National Association of Realtors':
                        US_t = readExcelFile(data_path+address+fname+'.xlsx', header_ =0, index_col_='Mnemonic', skiprows_=list(range(2)), sheet_name_=sname, skipfooter_=10)
                        if US_t.empty == True:
                            ERROR('Sheet Not Found: '+data_path+address+fname+', sheet name: '+sname)
                        US_t, label, note, footnote = US_IHS(US_t, Series, freq)
                        repl = None
                        label_level = None
                    elif source == 'Bureau Of Labor Statistics' and address.find('DSCO') >= 0:
                        US_t, label, note, footnote = DATA_SETS(data_path, address, fname=fname, sname=sname, DIY_series=Series, header=0, index_col=(0,1), skiprows=list(range(18)), freq=freq, x='x')
                        repl = ''
                        label_level = None
                    elif source == 'Bureau Of Labor Statistics':
                        YEAR = {'main':['M13'],'qua':['Q05'],'ann':['A01']}
                        YEAR2 = {'main':'-M13','qua':'-Q05','ann':'-A01'}
                        QUAR = {'main':{'M03':'Q1','M06':'Q2','M09':'Q3','M12':'Q4'}, 'other':{'Q01':'Q1','Q02':'Q2','Q03':'Q3','Q04':'Q4'}}
                        QUAR2 = {'M03':'Q1','M06':'Q2','M09':'Q3','M12':'Q4','Q01':'Q1','Q02':'Q2','Q03':'Q3','Q04':'Q4'}
                        RAUQ = {'main':{'Q1':'M03','Q2':'M06','Q3':'M09','Q4':'M12'}, 'other':{'Q1':'Q01','Q2':'Q02','Q3':'Q03','Q4':'Q04'}}
                        SEMI = ['S01','S02']
                        MON = ['M01','M02','M03','M04','M05','M06','M07','M08','M09','M10','M11','M12']
                        FREQ = {'A':'Annual','S':'Semiannual','Q':'Quarterly','M':'Monthly'}
                        new_label = bool(Series['datasets'].loc[address, 'NEW_LAB'])
                        bls_key = str(Series['datasets'].loc[address, 'Y_KEY'])
                        bls_key2 = str(Series['datasets'].loc[address, 'Q_KEY'])
                        PERIODS = {'A':YEAR[bls_key],'S':SEMI,'Q':QUAR[bls_key2],'M':MON}
                        freq_path = data_path+'BLS/'+address[-3:-1]+'/'+fname+' - '+FREQ[freq]+'.csv'
                        #if bls_read == False:
                        if PRESENT(freq_path):
                            US_temp = readFile(freq_path, header_=[0], index_col_=0)
                        else:
                            file_path = data_path+'BLS/'+address[-3:-1]+'/'+fname+'.csv'#'BLS/INTLINE_t_'+address[-3:-1]+'_'+freq+'.xlsx'
                            if PRESENT(file_path):
                                US_temp = readFile(file_path, header_=[0], index_col_=0)
                            else:
                                print('Waiting for Download...'+'\n')
                                US_temp = readFile(address+fname, header_=0, names_=['series_id','year','period','value','footnote_codes'], acceptNoFile=True, sep_='\\t')
                                US_temp.to_csv(file_path)
                                print('Download Complete, Time: '+str(int(time.time() - tStart))+' s'+'\n')
                            #bls_read = True
                            if address.find('in/') >= 0 and freq == 'A':
                                for code in Table['begin_year']:
                                    sys.stdout.write("\rCorrection...("+str(round((list(Table['begin_year']).index(code)+1)*100/len(Table['begin_year']), 1))+"%)*")
                                    sys.stdout.flush()
                                    if not not list(US_temp.loc[US_temp['series_id'] == code]['year']) and Table['begin_year'][code] not in list(US_temp.loc[US_temp['series_id'] == code]['year']):
                                        Table['begin_year'][code] = list(US_temp.loc[US_temp['series_id'] == code]['year'])[0]
                                sys.stdout.write("\n\n")
                            elif address.find('ml/') >= 0:
                                delete = []
                                for i in range(US_temp.shape[0]):
                                    sys.stdout.write("\rDropping redundant indexes...("+str(round((i+1)*100/US_temp.shape[0], 1))+"%)*")
                                    sys.stdout.flush()
                                    if Table['srd_code'][US_temp.iloc[i]['series_id']] != 'S00' or Table['dataseries_code'][US_temp.iloc[i]['series_id']] == 'Q' or Table['industryb_code'][US_temp.iloc[i]['series_id']] == 'S':
                                        delete.append(i)
                                sys.stdout.write("\n")
                                US_temp = US_temp.drop(delete)
                            US_temp = US_temp.sort_values(by=['series_id','year','period'], ignore_index=True)
                            US_temp = US_temp.loc[US_temp['period'].isin(PERIODS[freq])]
                            US_temp.to_csv(freq_path)
                        if US_temp.empty == False:
                            cat_idx = str(Series['datasets'].loc[address, 'CATEGORIES'])[3:]
                            item = str(Series['datasets'].loc[address, 'CONTENT']).lower()
                            idb = str(Series['datasets'].loc[address, 'UNIT'])[3:]+'_code'
                            labb = 'series_title'
                            if str(Series['datasets'].loc[address, 'LAB_BASE']) != 'nan':
                                labb = str(Series['datasets'].loc[address, 'LAB_BASE'])+'_code'
                            if address.find('bd/') >= 0:
                                cat_idx = 'industry'
                            if address.find('cu') >= 0 or address.find('cw') >= 0 or address.find('li/') >= 0 or address.find('ei/') >= 0:
                                idb = 'base_period'
                            elif address.find('ce/') >= 0:
                                idb = 'data_type_code'
                            elif address.find('pr/') >= 0 or address.find('mp/') >= 0:
                                idb = 'base_year'
                            elif address.find('in/') >= 0:
                                idb = 'economicseries_code'
                            elif address.find('ml/') >= 0:
                                idb = 'irc_code'
                            elif str(Series['datasets'].loc[address, 'UNIT']) == 'nan':
                                idb = 'base_date'
                            US_t, label, note, footnote = US_BLS(US_temp, Table, freq, YEAR, QUAR, index_base=idb, address=address, DF_KEY=DF_KEY, start=bls_start, key=bls_key, key2=bls_key2, lab_base=labb, find_unknown=find_unknown, Series=Series)
                        else:
                            continue
                        if US_t.empty == False:
                            if str(Series['datasets'].loc[address, 'NOTE']) != 'nan':
                                note = US_NOTE(Series['NOTE'], sname, LABEL=Table, address=address, other=True)
                            if new_label == True:
                                label = NEW_LABEL(address[-3:], label.copy(), Series, Table, cat_idx, item)
                        repl = bls_key
                        repl2 = bls_key2
                        label_level = None
                    #print(US_t)
                    #ERROR('')
                    index = []
                    rename = False
                    year = ''
                    for dex in US_t.columns:
                        if type(dex) == tuple:
                            if dex[0] == 'Index' or dex[0] == 'Label':
                                dex = dex[0]
                            else:
                                dex = dex[-1]
                            rename = True
                        if type(dex) == datetime or type(dex) == pd._libs.tslibs.timestamps.Timestamp:
                            if freq == 'A':
                                index.append(dex.strftime('%Y'))
                            elif freq == 'M':
                                index.append(dex.strftime('%Y-%m'))
                            elif freq == 'Q':
                                index.append(pd.Period(freq=freq,year=dex.year,month=dex.month,day=dex.day).strftime('%Y-Q%q'))
                                if address.find('STL') >= 0:
                                    rename = True
                            elif freq == 'W':
                                index.append(dex.strftime('%Y-%m-%d'))
                        elif (address.find('CONS') >= 0 or address.find('SHIP') >= 0) and freq == 'M':
                            month = [datetime.strptime(m,'%b').strftime('%B') for m in MONTH]
                            for m in month:
                                if str(dex).find(m) >= 0 and str(dex) != m:
                                    dex = m
                                    break
                            if bool(re.search(r'[0-9]+[a-z\s\*]+$', str(dex))):
                                dex = re.sub(r'[a-z\s\*]+$', "", str(dex))
                            if str(dex).isnumeric():
                                year = str(dex)
                                index.append('nan')
                                continue
                            try:
                                index.append(str(datetime.strptime(str(dex).strip(),'%b-%y').year)+'-'+str(datetime.strptime(str(dex).strip(),'%b-%y').month).rjust(2,'0'))
                            except ValueError:
                                try:
                                    index.append(year+'-'+str(datetime.strptime(str(dex).strip(),'%B').month).rjust(2,'0'))
                                    rename = True
                                except ValueError:
                                    index.append(dex)
                        elif address.find('MRTS') >= 0 and freq == 'Q':
                            dex = re.sub(r'\([a-z]+\)$',"", str(dex))
                            index.append(dex[-4:]+'-Q'+dex[:1])
                        elif address.find('APEP') >= 0:
                            try:
                                dex = int(re.sub(r'[a-z\*]+$',"", str(dex)))
                            except ValueError:
                                dex = dex
                            index.append(dex)
                        else:
                            index.append(dex)
                            if address.find('SHIP') >= 0:
                                rename = True
                    if rename == True:
                        US_t.columns = index
                        if address.find('SHIP') >= 0:
                            US_t = US_t.sort_index(axis=1)
                            index = list(US_t.columns)
                    if not not list(duplicates(label.index.dropna())):
                        if False in [d in NonValue_t for d in list(duplicates(label.index.dropna()))]:
                            ERROR('Duplicated Indices found in the file.')
                    
                    nG = US_t.shape[0]
                    if find_unknown == False:
                        logging.info('Total Items: '+str(nG)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')        
                    for i in range(nG):
                        sys.stdout.write("\rProducing Database...("+str(round((i+1)*100/nG, 1))+"%)*")
                        sys.stdout.flush()
                        
                        if str(US_t.iloc[i]['Index']) == 'ZZZZZZ' or str(US_t.iloc[i]['Index']) == 'nan' or str(US_t.iloc[i]['Index']) in CONTINUE:
                            continue
                        if address.find('bd/') >= 0:
                            if Table['state_code'][US_t.iloc[i]['Index']] != 0:
                                continue
                        
                        name = GET_NAME(address, freq, US_t.iloc[i]['Index'], source=source, Series=Series, Table=Table)
                        
                        if (name in DF_KEY.index and find_unknown == True) or (name not in DF_KEY.index and find_unknown == False):
                            continue
                        elif name not in DF_KEY.index and find_unknown == True:
                            new_item_counts+=1

                        value = list(US_t.iloc[i])
                        if sname == 'FAAt210':
                            sname = 'faaT210'
                        if freq == 'A' and source == 'Bureau of Economic Analysis':
                            repl = ''
                        elif freq == 'M' and source == 'Bureau of Economic Analysis':
                            repl = '-'
                        elif freq == 'Q' and source == 'Bureau of Economic Analysis':
                            repl = '-Q'
                        code_num_dict[freq], table_num_dict[freq], DATA_BASE_dict[freq], db_table_t_dict[freq], DB_name_dict[freq], snl = \
                            US_DATA(i, name, US_t, address, fname, sname, value, index, code_num_dict[freq], table_num_dict[freq], KEY_DATA, DATA_BASE_dict[freq],\
                                 db_table_t_dict[freq], DB_name_dict[freq], snl, source, FREQLIST[freq], freq, unit, label, label_level, note, footnote, series=Series, \
                                     table=Table, titles=Titles, repl=repl, repl2=repl2, formnote=formnote, YEAR=YEAR2, QUAR=QUAR2, RAUQ=RAUQ)
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
        #if f == 'W':
        #    db_table_t_dict[f] = db_table_t_dict[f].reindex(FREQLIST['W_s'])
        #    #FREQLIST['W'] = FREQLIST['W_s']
        DATA_BASE_dict[f][DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
        DB_name_dict[f].append(DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0'))       

print('Time: '+str(int(time.time() - tStart))+' s'+'\n')
if main_file.empty == True:
    df_key = pd.DataFrame(KEY_DATA, columns = key_list)
else:
    if merge_file.empty == True:
        ERROR('Missing Merge File')
if updating == True:
    df_key, DATA_BASE_dict = UPDATE(merge_file, main_file, key_list.remove('table_id'), NAME, out_path, merge_suf, main_suf)
else:
    if df_key.empty and find_unknown == False:
        ERROR('Empty dataframe')
    elif df_key.empty and find_unknown == True:
        ERROR('No new items were found.')
    df_key, DATA_BASE_dict = CONCATE(NAME, merge_suf, out_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, merge_file, DATA_BASE_dict, DB_name_dict)

if main_file.empty == True and find_unknown == True:
    NEW_TABLES['new_counts'] = [0 for i in range(NEW_TABLES.shape[0])]
    new_tables = pd.DataFrame()
    count = 0
    for ind in range(df_key.shape[0]):
        sys.stdout.write("\rCounting: "+str(ind+1)+" ")
        sys.stdout.flush()
        counted = False
        addr = re.split(r',', df_key.iloc[ind]['table_id'])[0]
        fnm = re.split(r',', df_key.iloc[ind]['table_id'])[1]
        snm = re.split(r',', df_key.iloc[ind]['table_id'])[2]
        snm_l = snm.lower()
        for i in range(NEW_TABLES.loc[(addr,fnm)].shape[0]):
            if NEW_TABLES.loc[(addr,fnm)].iloc[i]['Source'] == 'Bureau of Economic Analysis' and (snm_l.find(str(NEW_TABLES.loc[(addr,fnm)].index[i]).lower()) >= 0 or str(NEW_TABLES.loc[(addr,fnm)].index[i]).lower().find(snm_l) >= 0):
                NEW_TABLES.loc[(addr,fnm,NEW_TABLES.loc[(addr,fnm)].index[i]), 'new_counts'] = NEW_TABLES.loc[(addr,fnm)].iloc[i]['new_counts'] + 1
                counted = True
                count += 1
                break
            elif snm == str(NEW_TABLES.loc[(addr,fnm)].index[i]):
                NEW_TABLES.loc[(addr,fnm,NEW_TABLES.loc[(addr,fnm)].index[i]), 'new_counts'] = NEW_TABLES.loc[(addr,fnm)].iloc[i]['new_counts'] + 1
                counted = True
                count += 1
                break
        if counted == False:
            ERROR('Item not counted: name = '+df_key.iloc[ind]['name']+', table_id = '+df_key.iloc[ind]['table_id'])
    for ind in range(NEW_TABLES.shape[0]):
        if NEW_TABLES.iloc[ind]['new_counts'] != 0 and NEW_TABLES.iloc[ind]['counts'] != NEW_TABLES.iloc[ind]['new_counts']:
            new_tables = new_tables.append(NEW_TABLES.iloc[ind])
    sys.stdout.write("\n\n")
    df_key = df_key.drop(columns=['table_id'])
elif main_file.empty == True:
    df_key = df_key.drop(columns=['table_id'])

logging.info(df_key)
#logging.info(DATA_BASE_t)
logging.info('Total Items: '+str(df_key.shape[0]))

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

if main_file.empty == True and find_unknown == True: 
    if new_tables.empty == False:
        logging.info('New items were found')
        if bool(int(input('Update the table file (1/0): '))):
            new_tables['New Total Counts'] = new_tables['counts'].apply(lambda x: 0 if str(x) == 'nan' else x)+new_tables['new_counts']
            try:
                xl = win32.gencache.EnsureDispatch('Excel.Application')
            except:
                xl = win32.DispatchEx('Excel.Application')
            xl.DisplayAlerts=False
            xl.Visible = 0
            ExcelFile = xl.Workbooks.Open(Filename=os.path.realpath(data_path+'tables.xlsx'))
            Sheet = ExcelFile.Worksheets(1)
            SetNewCounts = dict.fromkeys(NEW_TABLES.index.names)
            SetNewCounts['counts'] = None
            for col in NEW_TABLES.index.names:
                for j in reversed(range(1, Sheet.UsedRange.Columns.Count+1)):
                    if SetNewCounts['counts'] == None and Sheet.Cells(1, j).Value == 'counts':
                        SetNewCounts['counts'] = j
                    elif Sheet.Cells(1, j).Value == col:
                        SetNewCounts[col] = j
                        break
            for new in range(new_tables.shape[0]):
                for i in range(2, Sheet.UsedRange.Rows.Count+1):
                    if False not in [Sheet.Cells(i,SetNewCounts[NEW_TABLES.index.names[k]]).Value == new_tables.index[new][k] for k in range(len(NEW_TABLES.index.names))]:
                        Sheet.Cells(i,SetNewCounts['counts']).Value = new_tables.iloc[new]['New Total Counts']
                        break
            ExcelFile.Save()
            ExcelFile.Close()
            xl.Quit()

print('Time: '+str(int(time.time() - tStart))+' s'+'\n')

if updating == False:
    if keyword[0].isupper():
        checkNotFound=True
        checkDESC=True
    else:
        checkNotFound=False
        checkDESC=True

    unknown_list, toolong_list, update_list, unfound_list = US_identity(out_path, df_key, DF_KEY, keyword=keyword[0], checkNotFound=checkNotFound, checkDESC=checkDESC, tStart=tStart, start_year=dealing_start_year)
