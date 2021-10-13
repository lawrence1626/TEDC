# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=E1101
import math, sys, calendar, os, copy, time, logging, zipfile
import regex as re
import pandas as pd
import numpy as np
import requests as rq
import win32com.client as win32
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import webdriver_manager
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from iteration_utilities import duplicates
import INTLINE_extention as EXT
from INTLINE_extention import ERROR, readFile, readExcelFile, GET_NAME, MERGE, NEW_KEYS, CONCATE, UPDATE, INTLINE_NOTE, takeFirst, INTLINE_BLS, INTLINE_BASE_YEAR, NEW_LABEL, \
 INTLINE_STL, INTLINE_FTD, INTLINE_DOE, INTLINE_LATEST_STEEL, INTLINE_STEEL, INTLINE_STOCK, INTLINE_ONS, INTLINE_WEB, INTLINE_BOE, INTLINE_EUC, INTLINE_EST, INTLINE_KERI, \
 INTLINE_BEIS, INTLINE_MULTIKEYS, INTLINE_SINGLEKEY, INTLINE_LTPLR, INTLINE_JREI, INTLINE_DATASETS, INTLINE_METI, INTLINE_MHLW, INTLINE_CBFI, INTLINE_GACC, INTLINE_PRESENT
import INTLINE_test as test
from INTLINE_test import INTLINE_identity
FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT, handlers=[logging.FileHandler("LOG"+EXT.BANK+".log", 'w', EXT.ENCODING)], datefmt='%Y-%m-%d %I:%M:%S %p')

find_unknown = False
main_suf = '?'
merge_suf = '?'
dealing_start_year = 1901
start_year = 1901
start_yearQ = 1901
start_yearM = 1901
start_yearS = 1901
merging = False
updating = False
data_processing = True#bool(int(input('Processing data (1/0): ')))#
keyword = ['','']
bls_start = dealing_start_year
STOCK_start = dealing_start_year
DF_suffix = test.DF_suffix
#Historical = True
make_discontinued = True#False#
ENCODING = EXT.ENCODING
excel_suffix = EXT.excel_suffix
LOG = ['excel_suffix', 'data_processing', 'find_unknown','dealing_start_year']
for key in LOG:
    logging.info(key+': '+str(locals()[key])+'\n')
log = logging.getLogger()
stream = logging.StreamHandler(sys.stdout)
stream.setFormatter(logging.Formatter('%(message)s'))
log.addHandler(stream)
if EXT.BANK == 'INTLINE':
    NAME = EXT.NAME
elif EXT.BANK == 'ASIA':
    NAME = EXT.ASIA_NAME
data_path = EXT.data_path
out_path = EXT.out_path
databank = NAME[:-1]
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'country', 'freq', 'start', 'last', 'unit', 'type', 'snl', 'source', 'form_e', 'form_c', 'table_id']
this_year = datetime.now().year + 1
update = datetime.today()
for i in range(len(key_list)):
    if key_list[i] == 'snl':
        snl_pos = i
        break
tStart = time.time()

FREQNAME = {'A':'annual','M':'month','Q':'quarter','S':'semiannual','W':'week','D':'daily'}
FREQLIST = {}
FREQLIST['A'] = [tmp for tmp in range(start_year,this_year+50)]
FREQLIST['S'] = []
for y in range(start_yearS,this_year):
    for s in range(1,3):
        FREQLIST['S'].append(str(y)+'-S'+str(s))
FREQLIST['Q'] = []
for q in range(start_yearQ,this_year+20):
    for r in range(1,5):
        FREQLIST['Q'].append(str(q)+'-Q'+str(r))
FREQLIST['M'] = []
for y in range(start_yearM,this_year):
    for m in range(1,13):
        FREQLIST['M'].append(str(y)+'-'+str(m).rjust(2,'0'))
calendar.setfirstweekday(calendar.SATURDAY)
FREQLIST['W'] = pd.date_range(start = str(start_year)+'-01-01',end=update,freq='W-SAT').strftime('%Y-%m-%d')
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

if data_processing:
    find_unknown = True
    #find_unknown = bool(int(input('Check if new items exist (1/0): ')))
    """if find_unknown == False:
        dealing_start_year = int(input("Dealing with data from year: "))
        start_year = dealing_start_year-10
        start_yearQ = dealing_start_year-10
        start_yearM = dealing_start_year-10
        start_yearS = dealing_start_year-10"""
    keyword = input('keyword: ')
    keyword = re.split(r'/', keyword)
    if len(keyword) < 2:
        keyword.append('')
    if keyword[0] == '134':
        ig = input('ignore: ')
        if ig != '':
            ignore = re.split(r',', ig)
        else:
            ignore = []
    else:
        ignore = []
    sys.stdout.write("\n\n")
    logging.info('Data Processing\n')
    main_file = pd.DataFrame()
    merge_file = pd.DataFrame()
    snl = 1
    for f in FREQNAME:
        table_num_dict[f] = 1
        code_num_dict[f] = 1
    logging.info('Reading table: Country, Time: '+str(int(time.time() - tStart))+'s'+'\n')
    Countries = readExcelFile(data_path+'Country.xlsx', header_ = 0, index_col_=0, sheet_name_=0)
    logging.info('Reading table: TABLES, Time: '+str(int(time.time() - tStart))+' s'+'\n')
    TABLES = readExcelFile(data_path+'tablesINT.xlsx', header_ = 0, sheet_name_=0)
    Titles = readExcelFile(data_path+'tablesINT.xlsx', header_ = 0, index_col_=1, sheet_name_='titles').to_dict()

merge_file_loaded = False
while data_processing == False:
    TABLES = pd.DataFrame()
    while True:
        try:
            merging = bool(int(input('Merging data file = 1/Updating TOT file = 0: ')))
            updating = not merging
            if merge_file_loaded == False:
                merge_suf = input('Be Merged(Original) data suffix: ')
                if os.path.isfile(out_path+NAME+'key'+merge_suf+'.xlsx') == False:
                    raise FileNotFoundError
            main_suf = input('Main(Updated) data suffix: ')
            if os.path.isfile(out_path+NAME+'key'+main_suf+'.xlsx') == False:
                raise FileNotFoundError
        except:
            print('= ! = Incorrect Input'+'\n')
        else:
            break
    sys.stdout.write("\n\n")
    if merging:
        logging.info('Process: File Merging\n')
    elif updating:
        logging.info('Process: File Updating\n')
    logging.info('Reading main key: '+NAME+'key'+main_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
    main_file = readExcelFile(out_path+NAME+'key'+main_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key', acceptNoFile=False)
    if main_file.empty:
        ERROR('Empty updated_file')
    logging.info('Reading main database: '+NAME+'database'+main_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
    main_database = readExcelFile(out_path+NAME+'database'+main_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
    if merge_file_loaded:
        merge_file = df_key
        merge_database = DATA_BASE_dict
    else:
        logging.info('Reading original key: '+NAME+'key'+merge_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        merge_file = readExcelFile(out_path+NAME+'key'+merge_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key', acceptNoFile=False)
        if merge_file.empty:
            ERROR('Empty original_file')
        logging.info('Reading original database: '+NAME+'database'+merge_suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
        merge_database = readExcelFile(out_path+NAME+'database'+merge_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
    #if merge_file.empty == False and merging == True and updating == False:
    if merging:
        logging.info('Merging File: '+out_path+NAME+'key'+merge_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        snl = int(merge_file['snl'][merge_file.shape[0]-1]+1)
        for f in FREQNAME:
            table_num_dict[f], code_num_dict[f] = MERGE(merge_file, DB_TABLE, DB_CODE, f)
        #if main_file.empty == False:
        #logging.info('Main File Exists: '+out_path+NAME+'key'+main_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
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
        df_key, DATA_BASE_dict = CONCATE(NAME, merge_suf, out_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, merge_file, DATA_BASE_dict, DB_name_dict, find_unknown=find_unknown, DATA_BASE_t=merge_database)
    elif updating:
        if 'table_id' in key_list:
            key_list.remove('table_id')
        df_key, DATA_BASE_dict = UPDATE(merge_file, main_file, key_list, NAME, out_path, merge_suf, main_suf, original_database=merge_database, updated_database=main_database)
    merge_file_loaded = True
    while True:
        try:
            continuing = bool(int(input('Merge or Update Another File With the Same Original File (1/0): ')))
        except:
            print('= ! = Incorrect Input'+'\n')
        else:
            break
    if continuing == False:
        break

DF_KEY = pd.DataFrame()
if updating == False and DF_suffix != merge_suf:
    logging.info('Reading file: INTLINE_key'+DF_suffix+', Time: '+str(int(time.time() - tStart))+'s'+'\n')
    DF_KEY = readExcelFile(out_path+'INTLINE_key'+DF_suffix+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='INTLINE_key')
    DF_KEY = DF_KEY.set_index('name')
elif updating == False and DF_suffix == merge_suf:
    DF_KEY = merge_file
    DF_KEY = DF_KEY.set_index('name')

CONTINUE = []
def COUNTRY(TABLES):
    country = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Country'] not in country:
            country.append(TABLES.iloc[t]['Country'])
    return country
def SOURCE(country):
    source = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Country'] == country and TABLES.iloc[t]['Source'] not in source:
            source.append(TABLES.iloc[t]['Source'])
    return source
def FILE_ADDRESS(country, source):
    address = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Country'] == country and TABLES.iloc[t]['Source'] == source and TABLES.iloc[t]['Address'] not in address:
            address.append(TABLES.iloc[t]['Address'])
    return address
def FILE_NAME(country, source, address):
    file_name = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Country'] == country and TABLES.iloc[t]['Source'] == source and TABLES.iloc[t]['Address'] == address and TABLES.iloc[t]['File'] not in file_name:
            file_name.append(TABLES.iloc[t]['File'])
    return file_name  
def SHEET_NAME(country, address, fname):
    sheet_name = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Country'] == country and TABLES.iloc[t]['Address'] == address and TABLES.iloc[t]['File'] == fname:
            if type(TABLES.iloc[t]['Sheet']) == int:
                sheet_name.append(TABLES.iloc[t]['Sheet'])
            else:
                sheet_name.extend(re.split(r', ', str(TABLES.iloc[t]['Sheet'])))
            #break
    return sheet_name
def FREQUENCY(country, address, fname, sname, distinguish_sheet=False):
    freq_list = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Country'] == country and TABLES.iloc[t]['Address'] == address and TABLES.iloc[t]['File'] == fname:
            if distinguish_sheet == True and str(sname) not in re.split(r', ', str(TABLES.iloc[t]['Sheet'])):
                continue
            freq_list.extend(re.split(r', ', str(TABLES.iloc[t]['Frequency'])))
    if address.find('BEA') >= 0:
        try:
            freq = re.split(r'\-', sname)[1]
            if freq not in freq_list:
                freq_list = []
            else:
                freq_list = [freq]
        except IndexError:
            freq_list = []
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
def INTLINE_KEY(country, address, key=None, US_address=None, country_datasets=False):
    if country_datasets == True:
        logging.info('Reading file: '+str(country)+'datasets, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        Datasets = readExcelFile(data_path+'tablesINT.xlsx', header_ = 0, index_col_=0, sheet_name_=str(country)+'datasets')
        if address.find('METI') >= 0 or address.find('MCPI') >= 0:
            return None, Datasets, Titles
    
    if address.find('FTD') >= 0:
        logging.info('Reading file: '+address[:3]+'_datasets, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        Datasets = readExcelFile(data_path+'tablesINT.xlsx', header_ = 0, index_col_=0, sheet_name_=address[:3]+'datasets').to_dict()
        logging.info('Reading file: '+key+'_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        Series = readExcelFile(US_address+key+'_series.xlsx', header_ = 0, index_col_=0)
        return Series, Datasets, Titles
    elif address.find('BEA') >= 0:
        BEA_datasets = readExcelFile(data_path+'tablesINT.xlsx', header_ = 0, index_col_=0, sheet_name_='BEAdatasets')
        logging.info('Reading file: BEA TablesRegister, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        BEA_table = readFile(BEA_datasets.loc[address, 'Table'], header_ = 0, index_col_='TableId')
        logging.info('Reading file: BEA SeriesRegister, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        BEA_series = readFile(BEA_datasets.loc[address, 'Series'], header_ = 0, index_col_='%SeriesCode')
        return BEA_series, BEA_table, Titles
        """elif address.find('STL') >= 0:
        logging.info('Reading file: '+key+'_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        STL_series = readExcelFile(data_path+str(country)+'/'+address+key+'.xls', sheet_name_=0)
        STL_series = list(STL_series[0])
        return STL_series, None, Titles"""
    elif address.find('bls') >= 0:
        logging.info('Reading file: BLS_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        BLS_datasets = readExcelFile(data_path+'tablesINT.xlsx', header_ = 0, index_col_=0, sheet_name_='BLSdatasets')
        file_path = data_path+str(country)+'/BLS/'+address[-3:-1]+'/'+address[-3:-1]+"_table.csv"
        if INTLINE_PRESENT(file_path):
            BLS_table = readFile(file_path, header_=0, index_col_=0).to_dict()
        else:
            BLS_table = readFile(address+BLS_datasets.loc[address, 'SERIES'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
            BLS_table.to_csv(file_path)
            BLS_table = BLS_table.to_dict()
        BLS_series = {}
        BLS_series['datasets'] = BLS_datasets
        if str(BLS_datasets.loc[address, 'ISADJUSTED']) != 'nan':
            BLS_series['ISADJUSTED'] = readFile(address+BLS_datasets.loc[address, 'ISADJUSTED'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
        else:
            BLS_series['ISADJUSTED'] = pd.DataFrame()
        if str(BLS_datasets.loc[address, 'CATEGORIES']).find('.') >= 0:
            BLS_series['CATEGORIES'] = readFile(address+BLS_datasets.loc[address, 'CATEGORIES'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
            if str(BLS_datasets.loc[address, 'SORT_C']) == 'T':
                BLS_series['CATEGORIES'] = BLS_series['CATEGORIES'].sort_values(by='sort_sequence')   
        else:
            BLS_series['CATEGORIES'] = {}
            if address.find('ln/') >= 0:
                unkey = 'lfst|periodicity|tdat'
            for code in list(BLS_table.keys()):
                if bool(re.search(r'code$', code)) and bool(re.search(unkey, code)) == False:
                    BLS_series['CATEGORIES'][code.replace('_code','')] = readFile(address+BLS_datasets.loc[address, 'CATEGORIES']+'.'+code.replace('_code',''), header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
        if str(BLS_datasets.loc[address, 'DATA TYPE']).find('.') >= 0:
            BLS_series['DATA TYPE'] = readFile(address+BLS_datasets.loc[address, 'DATA TYPE'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
            if str(BLS_datasets.loc[address, 'SORT_D']) == 'T':
                BLS_series['DATA TYPE'] = BLS_series['DATA TYPE'].sort_values(by='sort_sequence')
        if str(BLS_datasets.loc[address, 'BASE']) != 'nan':
            BLS_series['BASE'] = readFile(address+BLS_datasets.loc[address, 'BASE'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
        else:
            BLS_series['BASE'] = pd.DataFrame()
        if str(BLS_datasets.loc[address, 'UNIT']) != 'nan':
            BLS_series['UNIT'] = readFile(address+BLS_datasets.loc[address, 'UNIT'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
        if str(BLS_datasets.loc[address, 'NOTE']) != 'nan':
            BLS_series['NOTE'] = readFile(address+BLS_datasets.loc[address, 'NOTE'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
        
        """elif address.find('BOE') >= 0:
        logging.info('Reading file: '+key+'_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        Series = readExcelFile(data_path+str(country)+'/'+address+key+'_series.xlsx', header_ = 0, index_col_=0, sheet_name_=0)
        return Series, None, Titles"""
        return BLS_series, BLS_table, Titles
    elif address.find('WSA') >= 0 or address.find('SE/') >= 0 or address.find('DOE') >= 0 or address.find('BOE') >= 0 or address.find('CBFI') >= 0:
        logging.info('Reading file: Datasets, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        Datasets = readExcelFile(data_path+'tablesINT.xlsx', header_ = 0, index_col_=0, sheet_name_='OTHERdatasets').to_dict()
        return None, Datasets, Titles
    elif address.find('STL') >= 0 or address.find('JREI') >= 0:
        return None, None, Titles
    else:
        ERROR('Series Error: '+address)
def INTLINE_LEVEL(LABEL, source, Series=None, loc1=None, loc2=None, name=None, indent=None):
    label_level = []
    for l in range(len(LABEL)):
        if source == 'Bureau of Economic Analysis':
            if str(LABEL.iloc[l]) != 'nan':
                label_level.append(re.search(r'\S',str(LABEL.iloc[l])).start())
            else:
                label_level.append(10000)
        elif source == 'U.S. Census Bureau':
            if str(LABEL.index[l]) != 'nan':
                if str(LABEL.index[l])[loc1:loc2].isnumeric():
                    label_level.append(Series[name].loc[int(re.sub(r'0+$', "", str(LABEL.index[l])[loc1:loc2])), indent])
                else:
                    label_level.append(Series[name].loc[re.sub(r'0+$', "", str(LABEL.index[l])[loc1:loc2]), indent])
            else:
                label_level.append(10000)

    return label_level

def INTLINE_ADDLABEL(begin, address, sheet_name, LABEL, label_level, UNIT, unit, Calculation_type, attribute, suffix=False, form=None, other=False):
    level = label_level[begin]
    if other == True:
        begin_level = level
        level = re.sub(r'\(.+?\)', "", str(LABEL.iloc[begin]).replace(', ', ',')).strip()
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
            break
        elif UNIT == 'nan':
            UNIT = unit+' '+Calculation_type
        if other == False and label_level[att] < level:
            if str(LABEL.iloc[att]).find(':') >= 0 and str(LABEL.iloc[att])[-1:] == ':':
                if str(LABEL.iloc[att]).find('Addenda') >= 0 or str(LABEL.iloc[att]).find('Less:') >= 0:
                    level = label_level[att]
                    continue
                else:
                    attribute.insert(0, LABEL.iloc[att].replace(', ', ',').strip()+' ')
            elif str(LABEL.iloc[att]).find(':') >= 0:
                if str(LABEL.iloc[att]).find('Addenda') >= 0:
                    level = label_level[att]
                    continue
                else:
                    attribute.insert(0, LABEL.iloc[att][LABEL.iloc[att].find(':')+1:].replace(', ', ',').strip()+', ')
            else:
                if address.find('FTD') >= 0:
                    attribute.insert(0, LABEL.iloc[att].replace('/',' and ').replace('inc.','including').replace(', ', ',').strip()+', ')
                elif bool(re.search(r'\(*S[0-9]+\)', str(LABEL.iloc[att]))):
                    attribute.insert(0, re.sub(r'\(*S[0-9]+\)', "", LABEL.iloc[att]).replace(', ', ',').strip()+', ')
                else:
                    if suffix == True:
                        attribute[-1] = attribute[-1].replace(form, form.replace(', ', ',')+', '+str(LABEL.iloc[att]).replace(', ', ',').strip())
                    else:
                        attribute.insert(0, str(LABEL.iloc[att]).replace(', ', ',').strip()+', ')
            level = label_level[att]
        elif other == True and str(begin_level).find(re.sub(r'0+$', "", str(label_level[att]))) == 0:
            if re.sub(r'\(.+?\)', "", str(LABEL.iloc[att]).replace(', ', ',')).strip() != level and ((att != 0 and str(LABEL.index[att])[1] != 'G') or str(LABEL.index[att])[1] == 'G'):
                level = re.sub(r'\(.+?\)', "", str(LABEL.iloc[att]).replace(', ', ',')).strip()
                attribute.insert(0, level+', ')
            
    return UNIT, attribute

def INTLINE_ADDNOTE(attri, NOTE, note, note_num, note_part, specific=False, alphabet=False):
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

NonValue = ['nan','.....','ND','None','(S)','(NA)','N','NA','-','','(-)','n.a.','(*)','(D)','U','.','*','--','Not Available','Not Applicable',':','X','***','－','---','N.A.','na','\xa0','..','...','x','N.D.','(O)']

def INTLINE_DATA(ind, name, INTLINE_t, country, address, file_name, sheet_name, value, index, code_num, table_num, KEY_DATA, DATA_BASE, db_table_t, DB_name, snl, source, freqlist, frequency, UNIT='nan', LABEL=pd.DataFrame(), label_level=[], NOTE=[], FOOTNOTE=[], series=None, table=None, titles=None, repl=None, repl2=None, QUAR=None, RAUQ=None, country_series=False):
    freqlen = len(freqlist)
    country_name = Countries.loc[country, 'Country_Name']
    unit = ''
    Calculation_type = ''
    form_e = ''
    form_c = ''
    skip0 = False
    if address.find('JGBY') >= 0 or address.find('TRADE') >= 0 or address.find('NBS') >= 0:
        skip0 = True
    if source == 'Bureau Of Labor Statistics':
        seasonal = re.sub(r'[a-z]+\.', "", str(series['datasets'].loc[address, 'ISADJUSTED']))
        group = re.sub(r'[a-z]+\.', "", series['datasets'].loc[address, 'DATA TYPE'])
        item = re.sub(r'[a-z]+\.', "", series['datasets'].loc[address, 'CATEGORIES'])
        uni = re.sub(r'[a-z]+\.', "", str(series['datasets'].loc[address, 'UNIT']))
        text = series['datasets'].loc[address, 'CONTENT']
        p_text = text
        p_item = item
        if address.find('pr/') >= 0:
            p_text = 'name'
            p_group = group
            if address.find('pr/') >= 0:
                seasonal = seasonal.capitalize()
    if code_num >= 200:
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        DATA_BASE[db_table] = db_table_t
        DB_name.append(db_table)
        table_num += 1
        code_num = 1
        db_table_t = pd.DataFrame(index = freqlist, columns = [])
    
    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
    db_code = DB_CODE+str(code_num).rjust(3,'0')
    #db_table_t[db_code] = ['' for tmp in range(freqlen)]
    db_table_t = pd.concat([db_table_t, pd.DataFrame(['' for tmp in range(freqlen)], index=freqlist, columns=[db_code])], axis=1)
    content = ''
    note = ''
    note_num = 1
    note_part = []
    if country_series == True:
        unit = INTLINE_t.iloc[ind]['unit']
        Calculation_type = INTLINE_t.iloc[ind]['type']
        if address.find('METI') >= 0:
            form_e = repl
        else:
            form = re.split(r', ', INTLINE_t.iloc[ind]['Label'])
            if form[0] == 'Industry':
                form_e = form[0]+' '+form[1]
            else:
                form_e = form[0]
        form_c = INTLINE_t.iloc[ind]['form_c']
        UNIT = unit
    elif source == 'Bureau of Economic Analysis':
        unit = SCALE(INTLINE_t.iloc[ind]['Index'], address, series)+series.loc[INTLINE_t.iloc[ind]['Index'], 'MetricName']
        Calculation_type = series.loc[INTLINE_t.iloc[ind]['Index'], 'CalculationType']
        tabletitle = table.loc[sheet_name, 'TableTitle']
        form_e = re.split(r'Table\s[0-9A-Z\.]+\.\s', tabletitle)[1]
        form_c = re.findall(r'Table\s[0-9A-Z\.]+\.', tabletitle)[0]
    elif source == 'Bank of England':
        unit = INTLINE_t.iloc[ind]['unit']
        form_c = INTLINE_t.iloc[ind]['is_adj']
        form_e = table['Form'][file_name]
        Calculation_type = table['type'][file_name]
        UNIT = unit
    elif source == 'Federal Reserve Economic Data':
        unit = INTLINE_t.iloc[ind]['unit']
        Calculation_type = ''
        form_e = INTLINE_t.iloc[ind]['form_e']
        form_c = INTLINE_t.iloc[ind]['is_adj']
        UNIT = unit
    elif source == 'World Steel Association' or source == 'U.S. Department of Energy' or (source.find('Exchange') >= 0 and address.find('NIKK') < 0) or\
         (source == 'The People`s Bank of China' and address.find('CBFI') >= 0):
        unit = INTLINE_t.iloc[ind]['unit']
        Calculation_type = INTLINE_t.iloc[ind]['Label']
        if source == 'U.S. Department of Energy' and file_name.find('http') >= 0:
            form_e = table['Form'][sheet_name]
        else:
            form_e = table['Form'][file_name]
        form_c = 'Not Seasonally Adjusted'
        UNIT = unit
    elif address.find('IIPD') >= 0 or address.find('MCPI') >= 0:
        ISADJUSTED = {True: 'Seasonally Adjusted', False: 'Not Seasonally Adjusted'}
        unit = INTLINE_t.iloc[ind]['unit']
        Calculation_type = INTLINE_t.iloc[ind]['type']
        form_e = repl
        form_c = ISADJUSTED[table['Seasonally Adjusted'][file_name]]
        UNIT = unit
    elif source == 'U.S. Census Bureau':
        try:
            categ = str(INTLINE_t.iloc[ind]['Index'])[1:repl]
            if categ.isnumeric():
                categ = int(categ)
            unit = series['DATA TYPES'].loc[str(INTLINE_t.iloc[ind]['Index'])[repl:repl2], 'dt_unit']
            if str(INTLINE_t.iloc[ind]['Index'])[repl2:] == 'CSBR':
                unit = 'Millions of Chained Dollars'
            Calculation_type = series['GEO LEVELS'].loc[str(INTLINE_t.iloc[ind]['Index'])[repl2:], 'geo_desc']
            if file_name.find('http') >= 0:
                form_e = table['Form'][sheet_name]
            else:
                form_e = table['Form'][file_name]
            if form_e == 'U.S. Imports of Energy-Related Petroleum Products':
                unit = series['CATEGORIES'].loc[categ, 'unit']
            form_c = series['ISADJUSTED'].loc[str(INTLINE_t.iloc[ind]['Index'])[:1], 'adj_desc']
        except KeyError:
            CONTINUE.append(name)
        if UNIT == 'nan':
            UNIT = unit
    elif source == 'Bureau Of Labor Statistics':
        #form_e
        if address.find('ln/') >= 0:
            form_e = series['DATA TYPE'].loc[Table[group+'_code'][INTLINE_t.iloc[ind]['Index']], group+'_'+text]
        elif address.find('pr/') >= 0:
            form_e = series['DATA TYPE'].loc[Table[group+'_code'][INTLINE_t.iloc[ind]['Index']], p_group+'_'+p_text]
        #unit
        if address.find('ln/') >= 0:
            if int(INTLINE_t.iloc[ind]['unit']) == 0:
                unit = 'Thousands of people'
            elif int(INTLINE_t.iloc[ind]['unit']) == 1:
                unit = 'Percent or rate'
            else:
                unit = series['UNIT'].loc[INTLINE_t.iloc[ind]['unit'], uni+'_text'].capitalize()
        elif address.find('pr/') >= 0:
            if str(Table['base_year'][INTLINE_t.iloc[ind]['Index']]).isnumeric():
                unit = str(INTLINE_t.iloc[ind]['unit'])
            else:
                unit = series['UNIT'].loc[INTLINE_t.iloc[ind]['unit'], uni+'_'+text].capitalize().replace('%', 'Percent')
        #form_c
        if series['ISADJUSTED'].empty == True:
            form_c = 'Not Seasonally Adjusted'
        else:
            form_c = series['ISADJUSTED'].loc[Table['seasonal'][INTLINE_t.iloc[ind]['Index']], seasonal+'_text']
        #Calculation_type
        if address.find('ln/') >= 0:
            Calculation_type = ''
            for catkey in series['CATEGORIES']:
                if int(Table[catkey+'_code'][INTLINE_t.iloc[ind]['Index']]) != 0 and Calculation_type != '':
                    Calculation_type = Calculation_type+', '+series['CATEGORIES'][catkey].loc[Table[catkey+'_code'][INTLINE_t.iloc[ind]['Index']], catkey+'_'+text].title().replace('And','and').replace("'S","'s")
                elif int(Table[catkey+'_code'][INTLINE_t.iloc[ind]['Index']]) != 0:
                    Calculation_type = Calculation_type+series['CATEGORIES'][catkey].loc[Table[catkey+'_code'][INTLINE_t.iloc[ind]['Index']], catkey+'_'+text].title().replace('And','and').replace("'S","'s")
        elif address.find('pr/') >= 0:
            Calculation_type = series['CATEGORIES'].loc[Table[item+'_code'][INTLINE_t.iloc[ind]['Index']], p_item+'_'+text].title().replace('And','and').replace("'S","'s")
        UNIT = unit
    title = titles['Titles'][address]+', '
    if source == 'Bureau of Economic Analysis' and (form_c.find('Table 3') >= 0 or LABEL[INTLINE_t.index[ind]].find('Residual') >= 0 or LABEL[INTLINE_t.index[ind]].find('Statistical discrepancy') >= 0):
        content = content+form_e+', '
    elif source == 'Federal Reserve Economic Data' or source == 'U.S. Census Bureau' or source == 'Ministry of Economy, Trade and Industry of Japan' or address.find('CBFI') >= 0:
        content = content+form_e+', '
    elif address.find('MCPI') >= 0:
        content = content+form_e+' '
    attribute = []
    if LABEL.empty == True:
        label = Calculation_type+', '
        attribute.append(label)
    elif country != 111:
        attribute.append(LABEL[INTLINE_t.index[ind]].strip()+', ')
        if address.find('BOE') >= 0 and not not NOTE:
            note, note_num, note_part, note_suffix = INTLINE_ADDNOTE(INTLINE_t.iloc[ind]['note'], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = re.sub(r',\s$', note_suffix+', ', attribute[0])
    elif LABEL[INTLINE_t.index[ind]].find(':') >= 0 and source != 'Bureau Of Labor Statistics' and source != 'Federal Reserve Economic Data':
        attribute.append(LABEL[INTLINE_t.index[ind]][LABEL[INTLINE_t.index[ind]].find(':')+1:].replace(', ', ',').strip()+', ')
    else:
        if source == 'Bureau Of Labor Statistics':
            attribute.append(LABEL[INTLINE_t.index[ind]].strip()+', ')
            if address.find('pr/') >= 0:
                content = content+form_e+', '
        else:
            attribute.append(LABEL[INTLINE_t.index[ind]].replace(', ', ',').strip()+', ')
        if address.find('FTD') >= 0:
            attri = ''
            for note_item in NOTE:
                if attribute[0].find(str(note_item[0])) >= 0:
                    attri = str(note_item[0])
            note, note_num, note_part, note_suffix = INTLINE_ADDNOTE([attri], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = attribute[0].replace(attri, attri+note_suffix)
            note, note_num, note_part, note_suffix = INTLINE_ADDNOTE([str(INTLINE_t.iloc[ind]['Index'])[1:repl]], NOTE, note, note_num, note_part, specific=True)
            cat_key = str(INTLINE_t.iloc[ind]['Index'])[1:repl]
            if cat_key.isnumeric():
                cat_key = int(cat_key)
            attribute[0] = attribute[0].replace(series['CATEGORIES'].loc[cat_key, 'cat_desc'], series['CATEGORIES'].loc[cat_key, 'cat_desc']+note_suffix)
            note, note_num, note_part, note_suffix = INTLINE_ADDNOTE([str(INTLINE_t.iloc[ind]['Index'])[repl2:]], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = re.sub(r',\s$', note_suffix+', ', attribute[0])
    if address.find('STL') >= 0:
        attri = []
        for note_item in NOTE:
            if note_item[0].find(str(INTLINE_t.iloc[ind]['Index'])+'.') >= 0:
                attri.append(note_item[0])
        note, note_num, note_part, note_suffix = INTLINE_ADDNOTE(attri, NOTE, note, note_num, note_part, specific=True)
    if source == 'Bureau of Economic Analysis' or source == 'U.S. Census Bureau' or address.find('MCPI') >= 0:
        if source == 'Bureau of Economic Analysis' or address.find('MCPI') >= 0:
            begin = list(LABEL.index).index(INTLINE_t.index[ind])
            UNIT, attribute = INTLINE_ADDLABEL(begin, address, sheet_name, LABEL, label_level, UNIT, unit, Calculation_type, attribute)
        for a in range(len(attribute)):
            if address.find('ITAS') >= 0 or address.find('NIIP') >= 0 or address.find('DIRI') >= 0:
                attribute[a] = re.sub(r'\s*\([^\(]*?line[^\)]+?\)', "", attribute[a])
            if attribute[a].find('\\1\\0') > 0:
                attribute[a] = attribute[a].replace('\\1\\0', '\\10\\')
            if bool(re.search(r'\\[0-9,\\]+\\',attribute[a])):
                attri = attribute[a][attribute[a].find('\\'):]
                note, note_num, note_part, note_suffix = INTLINE_ADDNOTE(attri, NOTE, note, note_num, note_part)
                attribute[a] = re.sub(r'\\[0-9,\\]+\\', note_suffix, attribute[a])
            elif bool(re.search(r'/[0-9,/]+/',attribute[a])):
                attri = attribute[a][attribute[a].find('/'):]
                note, note_num, note_part, note_suffix = INTLINE_ADDNOTE(attri, NOTE, note, note_num, note_part)
                attribute[a] = re.sub(r'\s*/[0-9,/]+/', note_suffix, attribute[a])
            elif bool(re.search(r'[0-9]+,\s',attribute[a])) and source == 'U.S. Census Bureau':
                attri = attribute[a][re.search(r'[0-9]+,\s',attribute[a]).start():]
                note, note_num, note_part, note_suffix = INTLINE_ADDNOTE(attri, NOTE, note, note_num, note_part)
                attribute[a] = re.sub(r'[0-9]+,\s', note_suffix+', ', attribute[a])
    elif source == 'Bureau Of Labor Statistics':
        for note_item in NOTE:
            if type(Table['footnote_codes'][INTLINE_t.iloc[ind]['Index']]) == float and Table['footnote_codes'][INTLINE_t.iloc[ind]['Index']].is_integer():
                Table['footnote_codes'][INTLINE_t.iloc[ind]['Index']] = int(Table['footnote_codes'][INTLINE_t.iloc[ind]['Index']])
            if note_item[0] in re.split(r',', str(Table['footnote_codes'][INTLINE_t.iloc[ind]['Index']])) and address.find('ei/') < 0:
                note = note+'('+str(note_num)+')'+note_item[1]
                note_num += 1
    elif source == 'Ministry of Economy, Trade and Industry of Japan' and address.find('IIPD') >= 0:
        begin = list(LABEL.index).index(INTLINE_t.index[ind])
        UNIT, attribute = INTLINE_ADDLABEL(begin, address, sheet_name, LABEL, label_level, UNIT, unit, Calculation_type, attribute, other=True)
    try:
        if (type(INTLINE_t.iloc[ind]['note']) == str and str(INTLINE_t.iloc[ind]['note']) != 'nan') or (type(INTLINE_t.iloc[ind]['note']) == list and not not INTLINE_t.iloc[ind]['note']):
            note = note+'('+str(note_num)+')'+str(INTLINE_t.iloc[ind]['note'])
            note_num += 1
    except KeyError:
        time.sleep(0)
    for note_item in NOTE:
        if type(note_item) != int and note_item[0] == 'Note':
            note = note+'('+str(note_num)+')'+note_item[1]
            note_num += 1
    for attri in attribute:
        content = content+attri
    if source != 'Bureau Of Labor Statistics':
        if address.find('MCPI') >= 0:
            content = content+Calculation_type+', '
        content = content+form_c+', '
    else:
        if address.find('pr/') >= 0:
            content = content+form_c+', '
        SEAS = {'S':'Seas','U':'Unadj'}
        if address.find('ln/') >= 0 and bool(re.match(r'\(', content)) == False:
            content = '('+SEAS[INTLINE_t.iloc[ind]['Index'][2]]+') '+content
        SUB = ['Level','Rate','Not in Labor Force']
        for sub in SUB:
            if address.find('ln/') >= 0 and content.find(sub+' ') >= 0 and content.find(sub+' -') < 0 and content.find(sub+' to') < 0:
                content = content.replace(sub+' ', sub+' - ')
        if address.find('ln/') >= 0:
            content = re.sub(r'\-([\sA-Z])', r", \1", re.sub(r'Employment-[Pp]opulation|Employment to Population', "Employment Population", content.replace('&', 'and')+form_c+', '), 1)
            content = re.sub(r'\([Ss]eas\)\s+|\([Uu]nadj\)\s+', "", re.sub(r'\)\s+(Civilian\s)*[Ll]abor\s[Ff]orce\s([Ll]evel)*(\s)*([^F])', r") Civilian Labor Force \4", re.sub(r'[Pp]articipation [Rr]ate', "Participation Rate", \
                re.sub(r'yrs[\.]*', "years old", re.sub(r'\)\s+(Population)\s(Level)*(\s)*', r") Civilian Noninstitutional \1", re.sub(r'\)\s+Employment\s(Level)*(\s)*([^P])', r") Employed\3", \
                re.sub(r'[Ii]n [Ll]abor [Ff]orce\s(Level\s)*,', "in Labor Force,", re.sub(r'\)\s+Unemployment\s(Level)*(\s)*([^R])', r") Unemployed\3", \
                content.replace('Pvt W/S', 'Private Wage and salary').replace('EMPL. LEVEL', 'Employed,').replace(' rat', ' Rat').replace('Percent distribution', 'Percent Distribution')))))))))
        content = re.sub(r'\s+', " ", re.sub(r'\.*(\s\.)+([^0-9])', r"\2", re.sub(r"([0-9]+)'([^s])", r"\1 ft. \2", re.sub(r'([0-9]+)("|\'{2})', r"\1 in. ", re.sub(r'x(\s*[0-9])', r" times \1", \
            re.sub(r'([^0-9a-z])\.([0-9]+)', r"\1 0.\2", re.sub(r'([0-9]+)\s([0-9]+/[0-9]+)', r"\1 and \2", content))))))).replace('"', '').replace("'s", 's').replace("s'", 's').replace("'", '').replace(' ,', ',')
    note = note.strip()
    if note != '':
        desc_e = country_name + ' ' + title + content + 'Unit: ' + UNIT.replace('[','').replace('] ',', ').replace(']','') + ', Source: ' + source + ', Note: ' + note
    else:
        desc_e = country_name + ' ' + title + content + 'Unit: ' + UNIT.replace('[','').replace('] ',', ').replace(']','') + ', Source: ' + source
    for footnote_item in FOOTNOTE:
        if desc_e.find(footnote_item[0]) >= 0:
            desc_e = desc_e.replace(footnote_item[0],footnote_item[1])
    desc_e = desc_e.replace('"', '').replace("'", '').replace('#', ' ')
    table_id = str(country)+'國'+address+'國'+file_name+'國'+str(sheet_name)
    
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
            if str(value[k]).strip() in NonValue or bool(re.search(r'/[0-9]+/', str(value[k]))) or (skip0 == True and bool(re.search(r'^0(\.0+)*$', str(value[k])))):
                db_table_t[db_code][freq_index] = ''
            else:
                found = True
                try:
                    db_table_t[db_code][freq_index] = float(value[k])
                except ValueError:
                    ERROR('Nontype Value detected: '+str(value[k]))
                if start_found == False and found == True:
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
        if frequency == 'D':
            last = db_table_t[db_code].loc[~db_table_t[db_code].isin(NonValue)].index[0]
        else:
            last = db_table_t[db_code].loc[~db_table_t[db_code].isin(NonValue)].index[-1]
    except IndexError:
        if found == True:
            ERROR('last not found: '+str(name))
    if found == False:
        start = 'Nan'
        last = 'Nan'
    
    if bls_start == None or (bls_start != None and find_unknown == True) and source == 'Bureau Of Labor Statistics':
        if frequency == 'M' and start.replace('-', '-M') != INTLINE_t.iloc[ind]['start'] and INTLINE_t.iloc[ind]['start'].find('-M13') < 0 and str(INTLINE_t.iloc[ind][INTLINE_t.iloc[ind]['start'].replace('-M','-')]).strip() not in NonValue:
            ERROR('start error: '+str(name)+', produced start = '+start.replace('-', '-M')+', dataframe start = '+INTLINE_t.iloc[ind]['start'])
        elif frequency == 'Q' and INTLINE_t.iloc[ind]['start'][-3:] in QUAR and str(INTLINE_t.iloc[ind][INTLINE_t.iloc[ind]['start'].replace(INTLINE_t.iloc[ind]['start'][-3:],QUAR[INTLINE_t.iloc[ind]['start'][-3:]])]).strip() not in NonValue:
            if start.replace(start[-2:], RAUQ[repl2][start[-2:]]) != INTLINE_t.iloc[ind]['start']:
                ERROR('start error: '+str(name)+', produced start = '+start.replace(start[-2:], RAUQ[repl2][start[-2:]])+', dataframe start = '+INTLINE_t.iloc[ind]['start'])
    if source == 'Bureau Of Labor Statistics' and str(INTLINE_t.iloc[ind]['last'])[:4] >= str(dealing_start_year):
        if frequency == 'M' and last.replace('-', '-M') != INTLINE_t.iloc[ind]['last'] and INTLINE_t.iloc[ind]['last'].find('-M13') < 0 and str(INTLINE_t.iloc[ind][INTLINE_t.iloc[ind]['last'].replace('-M','-')]).strip() not in NonValue:
            ERROR('last error: '+str(name)+', produced last = '+last.replace('-', '-M')+', dataframe last = '+INTLINE_t.iloc[ind]['last'])
        elif frequency == 'Q' and INTLINE_t.iloc[ind]['last'][-3:] in QUAR and str(INTLINE_t.iloc[ind][INTLINE_t.iloc[ind]['last'].replace(INTLINE_t.iloc[ind]['last'][-3:],QUAR[INTLINE_t.iloc[ind]['last'][-3:]])]).strip() not in NonValue:
            if last.replace(last[-2:], RAUQ[repl2][last[-2:]]) != INTLINE_t.iloc[ind]['last']:
                ERROR('last error: '+str(name)+', produced last = '+last.replace(last[-2:], RAUQ[repl2][last[-2:]])+', dataframe last = '+INTLINE_t.iloc[ind]['last'])

    key_tmp= [databank, name, db_table, db_code, desc_e, country_name, frequency, start, last, unit, Calculation_type, snl, source, form_e, form_c, table_id]
    KEY_DATA.append(key_tmp)
    snl += 1
    
    code_num += 1
    
    return code_num, table_num, DATA_BASE, db_table_t, DB_name, snl

###########################################################################  Main Function  ###########################################################################
chrome = None
if data_processing:
    MONTH = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    TABLE_NAME = {'ISADJUSTED':'adj','CATEGORIES':'cat','DATA TYPES':'dt','GEO LEVELS':'geo'}
    ASIA = [158,924,532,576,542,534,536,566,578,528,582,548,456,186,522,518,546,466,524,453]
    NEW_TABLES = TABLES.copy()
    NEW_TABLES = NEW_TABLES.set_index(['Country','Address','File','Sheet']).sort_index()
    Zip_table = readExcelFile(data_path+'tablesINT.xlsx', header_ = 0, index_col_=0, sheet_name_='ZIPdatasets')
    steel_read = False
    new_item_counts = 0

for country in COUNTRY(TABLES):
    #if main_file.empty == False:
    #    break
    country_read = False
    country = int(country)
    if databank == 'ASIA' and country not in ASIA:
        continue
    for source in SOURCE(country):
        for address in FILE_ADDRESS(country, source):
            if (str(country)+str(address)).find(keyword[0]) < 0:
                continue
            to_be_ignore =False
            for ig in ignore:
                if str(address).find(ig) >= 0:
                    to_be_ignore = True
                    break
            if to_be_ignore == True:
                continue
            if country_read == False and country != 111:
                logging.info('Reading file: country_series, Time: '+str(int(time.time() - tStart))+' s'+'\n')
                C_Series = readExcelFile(data_path+str(country)+'/'+str(country)+'_series.xlsx', header_ = 0, index_col_=0)
                logging.info('Reading file: '+str(country)+'datasets, Time: '+str(int(time.time() - tStart))+' s'+'\n')
                C_Datasets = readExcelFile(data_path+'tablesINT.xlsx', header_ = 0, index_col_=0, sheet_name_=str(country)+'datasets')
                country_read = True
            #address = address[4:]
            zip_list = []
            Zip = False
            US_address = None
            if 'from US' in list(TABLES.loc[TABLES['Source'] == source]['keyword']):
                if source == 'Bureau of Economic Analysis':
                    Zip = True
                with open(data_path+str(country)+'/'+address+'US_address.txt','r',encoding='ANSI') as f:
                    US_address = Path(os.path.realpath(data_path).replace(databank, 'US')).as_posix()+f.read()
            country_series = Titles['country_series'][address]
            country_datasets = Titles['country_datasets'][address]
            single_key = Titles['single_key'][address]
            multi_keys = Titles['multiple_keys'][address]
            if country_series == True:
                Series = C_Series
                if country_datasets == True:
                    Table = C_Datasets
                else:
                    if address.find('GACC/CAT') >= 0 or address.find('KERI') >= 0:
                        logging.info('Reading file: Datasets, Time: '+str(int(time.time() - tStart))+' s'+'\n')
                        Table = readExcelFile(data_path+'tablesINT.xlsx', header_ = 0, index_col_=0, sheet_name_='OTHERdatasets').to_dict()
                    else:
                        if source == 'European Commission' or source == 'Eurostat':
                            Zip = True
                        Table = None
                Titles = readExcelFile(data_path+'tablesINT.xlsx', header_=[0], index_col_=1, sheet_name_='titles').to_dict()
            else:
                if source == 'Federal Reserve Economic Data':
                    Zip = True
                Series, Table, Titles = INTLINE_KEY(country, address, key=re.sub(r'FTD', "FTDE", address).replace('/',''), US_address=US_address, country_datasets=country_datasets)
            for fname in FILE_NAME(country, source, address):
                if make_discontinued == False and fname.find('discontinued') >= 0:
                    continue
                if str(fname).find(keyword[1]) < 0:
                    continue
                if chrome == None:# and ((str(fname).find('http') >= 0 and address.find('COUN') < 0) or (address.find('WSA') >= 0 and steel_read == False) or Zip == True or address.find('FTD') >= 0):
                    options = Options()
                    options.add_argument("--disable-notifications")
                    options.add_argument("--disable-popup-blocking")
                    options.add_argument("ignore-certificate-errors")
                    options.add_experimental_option("excludeSwitches", ["enable-logging"])
                    chrome = webdriver.Chrome(ChromeDriverManager().install(), options=options)
                    chrome.set_window_position(980,0)
                if Zip == True:
                    zip_country = country
                    if source == 'European Commission' or source == 'Eurostat':
                        zip_country = 163
                    file_address = data_path+str(zip_country)+'/'+address
                    if US_address != None:
                        file_address = US_address
                        if str(fname).find('Section7all') >= 0:
                            file_address = file_address.replace('Survey','Underlying')
                        else:
                            file_address = file_address.replace('Underlying','Survey')
                    file_path = file_address+Zip_table.loc[fname, 'Zipname']+'.zip'
                    present_file_existed = INTLINE_PRESENT(file_path)
                    if Zip_table.loc[fname, 'Zipname']+'.zip' not in zip_list:
                        if present_file_existed == True:
                            zipname = Zip_table.loc[fname, 'Zipname']+'.zip'
                        else:
                            zipname = INTLINE_WEB(chrome, zip_country, address, Zip_table.loc[fname, 'website'], Zip_table.loc[fname, 'Zipname'], Zip=True, US_address=US_address)
                        zip_list.append(zipname)
                    zf = zipfile.ZipFile(file_path,'r')
                    if source == 'European Commission':
                        Table = readExcelFile(zf.open(fname+'.xlsx'), index_col_=0, sheet_name_=0, usecols_=[0, 1], squeeze_=True, acceptNoFile=False).str.strip()
                        Table.index = [str(dex).strip() for dex in Table.index]
                    elif source == 'Federal Reserve Economic Data':
                        Series_temp = readExcelFile(zf.open(fname+'.xls'), sheet_name_=0)
                        Series = list(Series_temp[0])
                if source == 'Bureau of Economic Analysis':
                    logging.info('Reading source file, Time: '+str(int(time.time() - tStart))+' s'+'\n')
                    INTLINE_t_dict = readExcelFile(zf.open(fname+'.xlsx'), header_=0, index_col_=0, skiprows_=list(range(7)), acceptNoFile=False)
                    unit_dict = readExcelFile(zf.open(fname+'.xlsx'), usecols_=[0], acceptNoFile=False)
                    sheet_list = list(INTLINE_t_dict)
                else:
                    sheet_list = SHEET_NAME(country, address, fname)
                for sname in sheet_list:
                    if sname == 'None':
                        sname = None
                    if make_discontinued == False and source != 'Bureau of Economic Analysis' and str(NEW_TABLES.loc[(country,address,fname), 'keyword']).find('discontinued') >= 0:
                        continue
                    if address.find('BOJ') >= 0 and (str(sname).find('LTPLR') >= 0 or str(sname).find('PR') >= 0):
                        country_datasets = False
                    bls_read = False
                    MOS_read = False
                    datasets_read = False
                    distinguish_sheet = True
                    if source == 'Bureau of Economic Analysis':
                        INTLINE_t = INTLINE_t_dict[sname]
                        unit = re.sub(r'\s+NOTE:.+', "", str(unit_dict[sname].iloc[1][0]).strip())
                        dealing_sheets = ['T101','T107','T201','T206','T301','T302','T303','T309','T402','U702']
                        if (unit.find('dollars') < 0 or sname[:4] not in dealing_sheets) and fname.find('Section7') < 0:
                            continue
                        distinguish_sheet = False
                    else:
                        unit = 'nan'
                    for freq in FREQUENCY(country, address, fname, sname, distinguish_sheet=distinguish_sheet):
                        logging.info('Country: '+Countries.loc[country, 'Country_Name']+', Reading file or url: '+str(fname)+', sheet: '+str(sname)+', frequency: '+freq+', Time: '+str(int(time.time() - tStart))+'s'+'\n')
                        repl = None
                        repl2 = None
                        label_level = None
                        QUAR2 = None
                        RAUQ = None
                        if source == 'Bureau of Economic Analysis':
                            if INTLINE_t.empty == False:
                                sname = re.split(r'\-', sname)[0]
                                INTLINE_t = INTLINE_t.rename(columns={'Unnamed: 1':'Label','Unnamed: 2':'Index'})
                                label = INTLINE_t['Label']
                                label_level = INTLINE_LEVEL(label, source)
                                note, footnote = INTLINE_NOTE(INTLINE_t.index, sname, label, address)
                                for ind in list(INTLINE_t['Index']):
                                    if str(ind) != 'nan' and str(ind) != 'ZZZZZZ':
                                        try:
                                            Series.loc[ind]
                                        except KeyError:
                                            CONTINUE.append(ind)
                        elif source == 'Bank of England':
                            skip = list(range(int(Table['skip'][fname])))
                            head = list(range(int(Table['head'][fname])))
                            use = int(Table['usecols'][fname])
                            file_path = data_path+str(country)+'/'+address+str(sname)+'.xlsx'
                            if INTLINE_PRESENT(file_path):
                                INTLINE_temp = readExcelFile(file_path, header_=head, index_col_=0, sheet_name_=0)
                                #label_temp = readFile(file_path.replace('.xlsx','_Label.csv'), index_col_=0).squeeze(axis=1).to_dict()
                                note_temp = readFile(file_path.replace('.xlsx','_Notes.csv'), acceptNoFile=True).values.tolist()
                            else:
                                INTLINE_temp, note_temp = INTLINE_WEB(chrome, country, address, fname, sname, freq=freq, tables=[0], header=head, index_col=0, skiprows=skip, usecols=use, renote=True, output=True)
                            INTLINE_t, label, note, footnote = INTLINE_BOE(INTLINE_temp, note_temp, address, sname, freq)
                        elif country_datasets == True:
                            if datasets_read == False:
                                INTLINE_temp, csv, encode, webnote, Table, tables, skip, head, index_col, trans, excel, CDID, file_name, sheet_name, INTLINE_previous, Name = \
                                    INTLINE_DATASETS(chrome, data_path, country, address, fname, sname, freq, Series, Table, dealing_start_year, Zip_table=Zip_table)
                                datasets_read = True
                            if str(fname).find('http') >= 0:
                                website = fname
                            else:
                                try:
                                    website = str(Table.loc[fname, 'website'])
                                except KeyError:
                                    website = None
                            base_year, INTLINE_temp, Series, repl, is_period = INTLINE_BASE_YEAR(INTLINE_temp, chrome, data_path, country, address, file_name, freq, Series, csv, encode, sheet_name, excel, repl, Name, website=website)
                            if tables == sheet_list:
                                if single_key == True:
                                    INTLINE_t, label, note, footnote = INTLINE_SINGLEKEY(INTLINE_temp, data_path, country, address, fname, sname, Series, Countries, freq, head=head, index_col=index_col, transpose=trans, Table=Table, base_year=base_year, INTLINE_previous=INTLINE_previous, FREQLISTW=pd.date_range(start = '1901-01-01',end=update,freq='W-SAT').strftime('%Y-%m-%d'), find_unknown=find_unknown)
                                elif multi_keys == True:
                                    INTLINE_t, label, note, footnote = INTLINE_MULTIKEYS(INTLINE_temp, data_path, country, address, fname, sname, Series, Countries, freq, head=head, index_col=index_col, transpose=trans, base_year=base_year, INTLINE_previous=INTLINE_previous, is_period=is_period)
                                elif address.find('ONS') >= 0:
                                    INTLINE_t, label, note, footnote = INTLINE_ONS(INTLINE_temp, data_path, country, address, fname, sname, Series, Countries, freq, transpose=trans, CDID=CDID, table=sname)
                                elif address.find('IIPD') >= 0 or address.find('MCPI') >= 0:
                                    INTLINE_t, label, label_level, note, footnote = INTLINE_METI(INTLINE_temp, INTLINE_previous, data_path, country, address, fname, sname, Table, freq, transpose=trans, base_year=base_year)
                                elif address.find('MHLW') >= 0:
                                    INTLINE_t, label, note, footnote = INTLINE_MHLW(INTLINE_temp, data_path, country, address, fname, sname, Series, Countries, freq, transpose=trans, base_year=base_year)
                            else:
                                INTLINE_t = pd.DataFrame()
                                label = pd.Series(dtype=str)
                                label_level = pd.Series(dtype=str)
                                for t in tables:
                                    INTLINE_temp2 = INTLINE_temp.copy()
                                    INTLINE_previous2 = INTLINE_previous.copy()
                                    if csv == False and tables[0] != 0 and tables[0] != 'None':
                                        if tables[0] == '-1':
                                            INTLINE_temp2 = INTLINE_temp[list(INTLINE_temp.keys())[-1]].copy()
                                        else:
                                            INTLINE_temp2 = INTLINE_temp[t].copy()
                                        if type(INTLINE_previous) == dict:
                                            if address.find('ITIA') >= 0:
                                                INTLINE_previous2 = INTLINE_previous[Table.loc[fname, 'previous_sheet']].copy()
                                            else:
                                                INTLINE_previous2 = INTLINE_previous[t].copy()
                                    if single_key == True:
                                        INTLINE_tem, label_tem, note, footnote = INTLINE_SINGLEKEY(INTLINE_temp2, data_path, country, address, sname, t, Series, Countries, freq, head=head, index_col=index_col, transpose=trans, Table=Table, base_year=base_year, INTLINE_previous=INTLINE_previous2, FREQLISTW=pd.date_range(start = '1901-01-01',end=update,freq='W-SAT').strftime('%Y-%m-%d'), find_unknown=find_unknown, note=webnote)
                                    elif multi_keys == True:
                                        INTLINE_tem, label_tem, note, footnote = INTLINE_MULTIKEYS(INTLINE_temp2, data_path, country, address, sname, t, Series, Countries, freq, head=head, index_col=index_col, transpose=trans, base_year=base_year, INTLINE_previous=INTLINE_previous2, note=webnote, is_period=is_period)
                                    elif address.find('ONS') >= 0:
                                        INTLINE_tem, label_tem, note, footnote = INTLINE_ONS(INTLINE_temp2, data_path, country, address, sname, t, Series, Countries, freq, transpose=trans, CDID=CDID, table=t)
                                    elif address.find('MHLW') >= 0:
                                        INTLINE_tem, label_tem, note, footnote = INTLINE_MHLW(INTLINE_temp2, data_path, country, address, sname, t, Series, Countries, freq, transpose=trans, base_year=base_year)
                                    elif address.find('IIPD') >= 0 or address.find('MCPI') >= 0:
                                        INTLINE_tem, label_tem, label_level_tem, note, footnote = INTLINE_METI(INTLINE_temp2, INTLINE_previous2, data_path, country, address, sname, t, Table=Table.reset_index().set_index('File or Sheet'), freq=freq, transpose=trans, base_year=base_year)
                                        label_level = pd.concat([label_level, label_level_tem], ignore_index=False)
                                    INTLINE_t = pd.concat([INTLINE_t, INTLINE_tem], ignore_index=False)
                                    INTLINE_t = INTLINE_t.sort_index(axis=1)
                                    label = pd.concat([label, label_tem], ignore_index=False)
                            #print(INTLINE_t)
                            #continue
                        elif source == 'Bank of Japan' and (str(sname).find('LTPLR') >= 0 or str(sname).find('PR') >= 0):
                            if str(sname).find('LTPLR') >= 0:
                                st_year = 1966
                            elif str(sname).find('PR') >= 0:
                                st_year = 2009
                            if dealing_start_year > st_year:
                                st_year = dealing_start_year
                            INTLINE_t, label, note, footnote = INTLINE_LTPLR(chrome, data_path, country, address, fname, sname, freq, st_year, update, Countries)
                        elif source == 'Japan Real Estate Institute':
                            INTLINE_temp = readExcelFile(data_path+str(country)+'/'+address+fname+'.xlsx', header_=[0], index_col_=0, sheet_name_=sname, acceptNoFile=False)
                            INTLINE_t, label, note, footnote = INTLINE_JREI(INTLINE_temp, data_path, country, address, fname, sname)
                        elif source == 'JBA TIBOR Administration' or source == 'Swedish National Mediation Office':
                            INTLINE_temp = readExcelFile(data_path+str(country)+'/'+address+fname+'.xlsx', header_=[0], index_col_=0, sheet_name_=sname, acceptNoFile=False)
                            INTLINE_t, label, note, footnote = INTLINE_SINGLEKEY(INTLINE_temp, data_path, country, address, fname, sname, Series, Countries, freq, transpose=False)
                        elif source == 'European Commission':
                            Series_key = 'keyword'
                            if country == 112:
                                Series_key = 'SeriesID'
                            file_path = zf.open(fname+'.xlsx')
                            INTLINE_temp = readExcelFile(file_path, header_=[0], index_col_=0, sheet_name_=sname, acceptNoFile=False)
                            INTLINE_t, label, note, footnote = INTLINE_EUC(INTLINE_temp, data_path, country, address, fname, sname, Series, Table, Countries, freq, transpose=True, keyword=Series_key)
                        elif source == 'Eurostat':
                            Series_key = 'keyword'
                            if country == 112:
                                Series_key = 'SeriesID'
                            file_path = zf.open(fname+'.csv')
                            INTLINE_temp = readFile(file_path, header_=[0], acceptNoFile=False)
                            INTLINE_t, label, note, footnote = INTLINE_EST(INTLINE_temp, data_path, country, address, fname, Series, freq, Countries, keyword=Series_key)
                        elif source == 'Department for Business, Energy and Industrial Strategy of United Kingdom':
                            if str(fname).find('http') >= 0:
                                file_name = sname
                            else:
                                file_name = fname
                            file_path = data_path+str(country)+'/'+address+file_name+'.csv'
                            present_file_existed = INTLINE_PRESENT(file_path)
                            if str(fname).find('http') >= 0 and present_file_existed == False:
                                INTLINE_temp = INTLINE_WEB(chrome, country, address, fname, sname, header=[0], csv=True)
                            else:
                                INTLINE_temp = readFile(file_path, header_=[0], acceptNoFile=False)
                            INTLINE_t, label, note, footnote = INTLINE_BEIS(INTLINE_temp, data_path, country, address, file_name, Series, freq, Countries)
                        elif source == 'Federation of Korean Industries':
                            tables = [int(Table['Tables'][sname])]
                            INTLINE_t, label, note, footnote = INTLINE_KERI(chrome, data_path, country, address, fname, sname, tables, freq)
                        elif source == 'Federal Reserve Economic Data':
                            file_path = zf.open(fname+'.xls')
                            INTLINE_temp = readExcelFile(file_path, header_ =0, index_col_=0, sheet_name_=sname).T
                            INTLINE_t, label, note, footnote = INTLINE_STL(INTLINE_temp, address, Series, sname=sname, freq=freq)
                        elif source == 'World Steel Association':
                            file_path = data_path+'158/WSA/WorldCrudeSteelProduction.xls'
                            present_file_existed = INTLINE_PRESENT(file_path)
                            if steel_read == False:
                                if present_file_existed == True:
                                    INTLINE_steel = readExcelFile(file_path, header_=[0,1,2], index_col_=[0,1], skiprows_=[0,1], sheet_name_=0, acceptNoFile=False).T
                                else:
                                    url = 'https://www.jisf.or.jp/en/statistics/MainCountries/index.html'
                                    INTLINE_steel = INTLINE_WEB(chrome, country=158, address=address, fname=url, sname='WorldCrudeSteelProduction', tables=[0], header=[0,1,2], index_col=[0,1], skiprows=[0,1])
                                    INTLINE_steel = INTLINE_steel.T
                                INTLINE_steel = INTLINE_LATEST_STEEL(INTLINE_steel)
                                steel_read = True
                            INTLINE_t, label, note, footnote = INTLINE_STEEL(data_path, country, address, fname, INTLINE_steel, Countries)
                        elif source.find('Exchange') >= 0 and address.find('NIKK') < 0:
                            INTLINE_t, label, note, footnote = INTLINE_STOCK(chrome, data_path, country, address, fname, sname, freq, keyword=Table['keyword'][fname], STOCK_start=STOCK_start, find_unknown=find_unknown)
                        elif (source == 'The People`s Bank of China' and address.find('CBFI') >= 0) or source == 'Central Provident Fund of Singapore':
                            if address.find('CBFI') >= 0:
                                skip = list(range(int(Table['skip'][fname])))
                                head = list(range(int(Table['head'][fname])))
                            else:
                                skip = None
                                head = None
                            INTLINE_t, label, note, footnote = INTLINE_CBFI(chrome, data_path, country, address, fname, sname, freq, update, skip=skip, head=head, index_col=0)
                        elif source == 'General Administration of Customs of China' and address.find('CAT') >= 0:
                            skip = list(range(int(Table['skip'][fname])))
                            head = list(range(int(Table['head'][fname])))
                            index_col = list(range(int(Table['index_col'][fname])))
                            trans = Table['transpose'][fname]
                            INTLINE_temp = INTLINE_GACC(chrome, data_path, country, address, fname, sname, freq, skip=skip, head=head, index_col=index_col)
                            INTLINE_t, label, note, footnote = INTLINE_MULTIKEYS(INTLINE_temp, data_path, country, address, fname=sname, sname=0, Series=Series, Countries=Countries, freq=freq, transpose=trans)
                        elif source == 'U.S. Department of Energy':
                            if str(fname).find('http') >= 0:
                                key = sname
                            else:
                                key = fname
                            file_path = data_path+str(country)+'/'+address+key+'.xls'
                            file_name = key
                            skip = list(range(int(Table['skip'][key])))
                            head = list(range(int(Table['head'][key])))
                            trans = Table['transpose'][key]
                            tables = [t for t in re.split(r', ', str(Table['Tables'][key]))]
                            if str(fname).find('http') >= 0 and INTLINE_PRESENT(file_path) == False:
                                INTLINE_tem = INTLINE_WEB(chrome, country, address, fname, sname, freq, tables=tables, header=head, index_col=0, skiprows=skip)
                                INTLINE_temp = INTLINE_tem[tables[0]]
                            else:
                                INTLINE_temp = readExcelFile(file_path, header_=head, index_col_=0, skiprows_=skip, sheet_name_=tables[0], acceptNoFile=False)
                            INTLINE_t, label, note, footnote = INTLINE_DOE(INTLINE_temp, data_path, country, address, file_name, tables[0], freq, transpose=trans)
                        elif source == 'U.S. Census Bureau':
                            file_name = fname
                            if fname.find('http') >= 0:
                                file_name = sname
                            prefix = str(Table['prefix'][file_name])
                            middle = str(Table['middle'][file_name])
                            suffix = str(Table['suffix'][file_name])
                            skip = None
                            if str(Table['skip'][file_name]) != 'nan':
                                skip = list(range(int(Table['skip'][file_name])))
                            excel = ''
                            if str(Table['excel'][file_name]) != 'nan':
                                excel = Table['excel'][file_name]
                            head = None
                            if str(Table['head'][file_name]) != 'nan':
                                head = list(range(int(Table['head'][file_name])))
                            trans = Table['transpose'][file_name]
                            final_name = None
                            if str(Table['final_name'][file_name]) != 'nan':
                                final_name = re.split(r', ', str(Table['final_name'][file_name]))
                            ft900_name = None
                            if str(Table['ft900_name'][file_name]) != 'nan':
                                ft900_name = re.split(r', ', str(Table['ft900_name'][file_name]))
                            INTLINE_t, label, note, footnote = INTLINE_FTD(US_address, fname=fname, sname=sname, Series=Series, header=head, index_col=0, skiprows=skip, freq=freq,\
                                x=excel, trans=trans, prefix=prefix, middle=middle, suffix=suffix, chrome=chrome, Zip_table=Zip_table, final_name=final_name, ft900_name=ft900_name)
                            other_notes = readExcelFile(US_address+'other_notes.xlsx', header_=0, index_col_=0, sheet_name_=0, acceptNoFile=False)
                            note = note + INTLINE_NOTE(other_notes, sname, address=address, other=True)
                            repl = 4
                            repl2 = 6
                        elif source == 'Ministry of Finance, Japan' and address.find('COUN') >= 0:
                            if MOS_read == False:
                                with open(data_path+str(country)+'/'+'encode.txt','r',encoding='ANSI') as f:
                                    encode = f.read()
                                if str(sname) == 'World':
                                    head = [0]
                                else:
                                    head = [0, 1]
                                INTLINE_temp = readFile(fname, skiprows_=[0, 1], header_=head, index_col_=0, encoding_=encode, acceptNoFile=True).T
                                INTLINE_temp.to_excel(data_path+str(country)+'/'+address+sname+'.xlsx', sheet_name=sname)
                                MOS_read = True
                            if INTLINE_temp.empty == False:
                                INTLINE_t, label, note, footnote = INTLINE_MULTIKEYS(INTLINE_temp, data_path, country, address, sname, 0, Series, Countries, freq, transpose=False)
                        elif source == 'Bureau Of Labor Statistics':
                            QUAR = {'main':{'M03':'Q1','M06':'Q2','M09':'Q3','M12':'Q4'}, 'other':{'Q01':'Q1','Q02':'Q2','Q03':'Q3','Q04':'Q4'}}
                            QUAR2 = {'M03':'Q1','M06':'Q2','M09':'Q3','M12':'Q4','Q01':'Q1','Q02':'Q2','Q03':'Q3','Q04':'Q4'}
                            RAUQ = {'main':{'Q1':'M03','Q2':'M06','Q3':'M09','Q4':'M12'}, 'other':{'Q1':'Q01','Q2':'Q02','Q3':'Q03','Q4':'Q04'}}
                            MON = ['M01','M02','M03','M04','M05','M06','M07','M08','M09','M10','M11','M12']
                            FREQ = {'A':'Annual','S':'Semiannual','Q':'Quarterly','M':'Monthly'}
                            new_label = bool(Series['datasets'].loc[address, 'NEW_LAB'])
                            #bls_key = str(Series['datasets'].loc[address, 'Y_KEY'])
                            bls_key2 = str(Series['datasets'].loc[address, 'Q_KEY'])
                            PERIODS = {'Q':QUAR[bls_key2],'M':MON}
                            freq_path = data_path+str(country)+'/BLS/'+address[-3:-1]+'/'+fname+' - '+FREQ[freq]+'.csv'
                            #if bls_read == False:
                            if (address.find('pr/') >= 0 and freq != 'Q') or (address.find('ln/') >= 0 and freq != 'M'):
                                continue
                            if INTLINE_PRESENT(freq_path):
                                INTLINE_temp = readFile(freq_path, header_=[0], index_col_=0)
                            else:
                                file_path = data_path+str(country)+'/BLS/'+address[-3:-1]+'/'+fname+'.csv'#'BLS/INTLINE_t_'+address[-3:-1]+'_'+freq+'.xlsx'
                                if INTLINE_PRESENT(file_path):
                                    INTLINE_temp = readFile(file_path, header_=[0], index_col_=0)
                                else:
                                    print('Waiting for Download...'+'\n')
                                    INTLINE_temp = readFile(address+fname, header_=0, names_=['series_id','year','period','value','footnote_codes'], acceptNoFile=True, sep_='\\t')
                                    INTLINE_temp.to_csv(file_path)
                                    print('Download Complete, Time: '+str(int(time.time() - tStart))+' s'+'\n')
                                #bls_read = True
                                INTLINE_temp = INTLINE_temp.sort_values(by=['series_id','year','period'], ignore_index=True)
                                INTLINE_temp = INTLINE_temp.loc[INTLINE_temp['period'].isin(PERIODS[freq])]
                                INTLINE_temp.to_csv(freq_path)
                            if INTLINE_temp.empty == False:
                                print('Time: '+str(int(time.time() - tStart))+' s'+'\n') 
                                cat_idx = str(Series['datasets'].loc[address, 'CATEGORIES'])[3:]
                                item = str(Series['datasets'].loc[address, 'CONTENT']).lower()
                                idb = str(Series['datasets'].loc[address, 'UNIT'])[3:]+'_code'
                                labb = 'series_title'
                                if str(Series['datasets'].loc[address, 'LAB_BASE']) != 'nan':
                                    labb = str(Series['datasets'].loc[address, 'LAB_BASE'])+'_code'
                                if address.find('pr/') >= 0:
                                    idb = 'base_year'
                                elif str(Series['datasets'].loc[address, 'UNIT']) == 'nan':
                                    idb = 'base_date'
                                INTLINE_t, label, note, footnote = INTLINE_BLS(INTLINE_temp, Table, freq, QUAR, index_base=idb, address=address, DF_KEY=DF_KEY, start=bls_start, key2=bls_key2, lab_base=labb, find_unknown=find_unknown)
                            else:
                                continue
                            if INTLINE_t.empty == False:
                                if str(Series['datasets'].loc[address, 'NOTE']) != 'nan':
                                    note = INTLINE_NOTE(Series['NOTE'], sname, LABEL=Table, address=address, other=True)
                                if new_label == True:
                                    label = NEW_LABEL(address[-3:], label.copy(), Series, Table, cat_idx, item)
                            #repl = bls_key
                            repl2 = bls_key2
                        index = []
                        rename = False
                        year = ''
                        for dex in INTLINE_t.columns:
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
                                elif freq == 'W' or freq == 'D':
                                    index.append(dex.strftime('%Y-%m-%d'))
                            else:
                                index.append(dex)
                        if rename == True:
                            INTLINE_t.columns = index
                        if not not list(duplicates(label.index.dropna())):
                            if False in [d in NonValue for d in list(duplicates(label.index.dropna()))]:
                                ERROR('Duplicated Indices found in the file.')
                        #print(INTLINE_t)
                        #continue
                        nG = INTLINE_t.shape[0]
                        if find_unknown == False:
                            logging.info('Total Items: '+str(nG)+' Time: '+str(int(time.time() - tStart))+'s'+'\n')        
                        for i in range(nG):
                            sys.stdout.write("\rProducing Database...("+str(round((i+1)*100/nG, 1))+"%)*")
                            sys.stdout.flush()
                            
                            if str(INTLINE_t.iloc[i]['Index']) == 'ZZZZZZ' or str(INTLINE_t.iloc[i]['Index']) == 'nan' or str(INTLINE_t.iloc[i]['Index']) in CONTINUE:
                                continue
                            
                            name = GET_NAME(address, freq, country, INTLINE_t.iloc[i]['Index'])
                            
                            if (name in DF_KEY.index and find_unknown == True) or (name not in DF_KEY.index and find_unknown == False):
                                continue
                            elif name not in DF_KEY.index and find_unknown == True:
                                new_item_counts+=1

                            value = list(INTLINE_t.iloc[i])
                            if freq == 'A' and source == 'Bureau of Economic Analysis':
                                repl = ''
                            elif freq == 'M' and source == 'Bureau of Economic Analysis':
                                repl = '-'
                            elif freq == 'Q' and source == 'Bureau of Economic Analysis':
                                repl = '-Q'
                            code_num_dict[freq], table_num_dict[freq], DATA_BASE_dict[freq], db_table_t_dict[freq], DB_name_dict[freq], snl = \
                                INTLINE_DATA(i, name, INTLINE_t, country, address, fname, sname, value, index, code_num_dict[freq], table_num_dict[freq], KEY_DATA, DATA_BASE_dict[freq],\
                                    db_table_t_dict[freq], DB_name_dict[freq], snl, source, FREQLIST[freq], freq, unit, label, label_level, note, footnote, series=Series, \
                                        table=Table, titles=Titles, repl=repl, repl2=repl2, QUAR=QUAR2, RAUQ=RAUQ, country_series=country_series)
                        sys.stdout.write("\n\n")
                        if find_unknown == True:
                            logging.info('Total New Items Found: '+str(new_item_counts)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')
if chrome != None:
    chrome.quit()
    chrome = None

print('Time: '+str(int(time.time() - tStart))+' s'+'\n')
if data_processing:
    for f in FREQNAME:
        if db_table_t_dict[f].empty == False:
            DATA_BASE_dict[f][DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
            DB_name_dict[f].append(DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0'))
    df_key = pd.DataFrame(KEY_DATA, columns = key_list)
    if df_key.empty and find_unknown == False:
        ERROR('Empty dataframe')
    elif df_key.empty and find_unknown == True:
        ERROR('No new items were found.')
    df_key, DATA_BASE_dict = CONCATE(NAME, merge_suf, out_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, merge_file, DATA_BASE_dict, DB_name_dict, find_unknown=find_unknown)

    if find_unknown == True:
        NEW_TABLES['new_counts'] = [0 for i in range(NEW_TABLES.shape[0])]
        new_tables = pd.DataFrame()
        count = 0
        for ind in range(df_key.shape[0]):
            sys.stdout.write("\rCounting: "+str(ind+1)+" ")
            sys.stdout.flush()
            counted = False
            ctry = int(re.split(r'國', df_key.iloc[ind]['table_id'])[0])
            addr = re.split(r'國', df_key.iloc[ind]['table_id'])[1]
            fnm = re.split(r'國', df_key.iloc[ind]['table_id'])[2]
            snm = re.split(r'國', df_key.iloc[ind]['table_id'])[3]
            snm_l = snm.lower()
            for i in range(NEW_TABLES.loc[(ctry,addr,fnm)].shape[0]):
                if snm_l.find(str(NEW_TABLES.loc[(ctry,addr,fnm)].index[i]).lower()) >= 0 or str(NEW_TABLES.loc[(ctry,addr,fnm)].index[i]).lower().find(snm_l) >= 0:
                    NEW_TABLES.loc[(ctry,addr,fnm,NEW_TABLES.loc[(ctry,addr,fnm)].index[i]), 'new_counts'] = NEW_TABLES.loc[(ctry,addr,fnm)].iloc[i]['new_counts'] + 1
                    counted = True
                    count += 1
                    break
                elif snm == str(NEW_TABLES.loc[(ctry,addr,fnm)].index[i]):
                    NEW_TABLES.loc[(ctry,addr,fnm,NEW_TABLES.loc[(ctry,addr,fnm)].index[i]), 'new_counts'] = NEW_TABLES.loc[(ctry,addr,fnm)].iloc[i]['new_counts'] + 1
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
    else:
        df_key = df_key.drop(columns=['table_id'])

logging.info(df_key)
#logging.info(DATA_BASE_t)
logging.info('Total Items: '+str(df_key.shape[0]))

print('Time: '+str(int(time.time() - tStart))+' s'+'\n')
df_key.to_excel(out_path+NAME+"key"+excel_suffix+".xlsx", sheet_name=NAME+'key')
with pd.ExcelWriter(out_path+NAME+"database"+excel_suffix+".xlsx") as writer:
    #if updating == True:
    for d in DATA_BASE_dict:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_dict[d].empty == False:
            DATA_BASE_dict[d].to_excel(writer, sheet_name = d)
    """else:
        for f in FREQNAME:
            for d in DATA_BASE_dict[f]:
                sys.stdout.write("\rOutputing sheet: "+str(d))
                sys.stdout.flush()
                if DATA_BASE_dict[f][d].empty == False:
                    DATA_BASE_dict[f][d].to_excel(writer, sheet_name = d)"""
    sys.stdout.write("\n")

if data_processing and find_unknown == True: 
    if new_tables.empty == False:
        logging.info('New items were found')
        logging.info(new_tables)
        if 1<0:#bool(int(input('Update the table file (1/0): '))):
            new_tables['New Total Counts'] = new_tables['counts'].apply(lambda x: 0 if str(x) == 'nan' else x)+new_tables['new_counts']
            try:
                xl = win32.gencache.EnsureDispatch('Excel.Application')
            except:
                xl = win32.DispatchEx('Excel.Application')
            xl.DisplayAlerts=False
            xl.Visible = 0
            ExcelFile = xl.Workbooks.Open(Filename=os.path.realpath(data_path+'tablesINT.xlsx'))
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
            #ExcelFile.Close()
            #xl.Quit()

print('Time: '+str(int(time.time() - tStart))+' s'+'\n')

if updating == False:
    if keyword[0].isnumeric():
        checkNotFound=True
        checkDESC=True
    else:
        checkNotFound=False
        checkDESC=True

    unknown_list, toolong_list, update_list, unfound_list = INTLINE_identity(out_path, df_key, DF_KEY=DF_KEY, keyword=databank, checkNotFound=checkNotFound, checkDESC=checkDESC, tStart=tStart, start_year=dealing_start_year)
