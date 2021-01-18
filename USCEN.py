# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from US_extention import ERROR, readFile, readExcelFile, US_NOTE, US_HISTORYDATA, DATA_SETS, takeFirst, US_IHS, US_BLS, MERGE, NEW_KEYS, CONCATE, US_POPP, US_FAMI, EXCHANGE, NEW_LABEL, US_STL

ENCODING = 'utf-8-sig'

start_year = 1900
start_yearQ = 1940
start_yearM = 1910
start_yearS = 1980
HIES_old = True # ./HIES/
make_discontinued = True # ./wd/
bls_start = None
main_suf = '?'
merge_suf = '?'
keyword = 'STL'
ignore = '?'
NAME = 'US_'
data_path = './data/'
out_path = "./output/"
databank = 'US'
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'last', 'unit', 'type', 'snl', 'source', 'form_e', 'form_c']
SOURCE = ['Bureau of Economic Analysis','Federal Reserve Board','Federal Reserve Bank Of St. Louis','Bureau Of Census','National Association of Home Builders','National Association of Realtors','Bureau Of Labor Statistics']#
#FREQM = ['T206','T207','T208','M','S','U90100']
main_file = readExcelFile(out_path+NAME+'key'+main_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
merge_file = readExcelFile(out_path+NAME+'key'+merge_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
this_year = datetime.now().year + 1
update = datetime.today()
for i in range(len(key_list)):
    if key_list[i] == 'snl':
        snl_pos = i
        break
tStart = time.time()

def FILE_ADDRESS(source):
    address = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Source'] == source and TABLES.iloc[t]['Address'] not in address:
            address.append(TABLES.iloc[t]['Address'])
    return address
def FILE_NAME(address):
    file_name = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Address'] == address and TABLES.iloc[t]['File'] not in file_name:
            file_name.append(TABLES.iloc[t]['File'])
    return file_name  
def SHEET_NAME(address, fname, TABLE=None, loc=0):
    sheet_name = []
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Address'] == address and TABLES.iloc[t]['File'] == fname:
            if type(TABLES.iloc[t]['Sheet']) == int:
                sheet_name.append(TABLES.iloc[t]['Sheet'])
            else:
                sheet_name.extend(re.split(r', ', str(TABLES.iloc[t]['Sheet'])))
            #break
    return sheet_name
def FREQUENCY(address, fname, sname):
    freq_list = []
    if address.find('BEA') >= 0:
        try:
            freq = re.split(r'\-', sname)[1]
            freq_list.append(freq)
        except IndexError:
            freq_list = []
        return freq_list
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Address'] == address and TABLES.iloc[t]['File'] == fname:
            if (address.find('HOUS') >= 0 or address.find('NAR') >= 0 or address.find('POP') >= 0) and str(sname) not in re.split(r', ', str(TABLES.iloc[t]['Sheet'])):
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
FREQNAME = {'A':'annual','M':'month','Q':'quarter','S':'semiannual','W':'week'}
FREQLIST = {}
FREQLIST['A'] = [tmp for tmp in range(start_year,this_year+50)]
FREQLIST['S'] = []
for y in range(start_yearS,this_year):
    for s in range(1,3):
        FREQLIST['S'].append(str(y)+'-S'+str(s))
#print(FREQLIST['S'])
FREQLIST['Q'] = []
for q in range(start_yearQ,this_year):
    for r in range(1,5):
        FREQLIST['Q'].append(str(q)+'-Q'+str(r))
#print(FREQLIST['Q'])
FREQLIST['M'] = []
for y in range(start_yearM,this_year):
    for m in range(1,13):
        FREQLIST['M'].append(str(y)+'-'+str(m).rjust(2,'0'))
#print(FREQLIST['M'])
calendar.setfirstweekday(calendar.SATURDAY)
FREQLIST['W'] = pd.date_range(start = str(start_year)+'-01-01',end=update,freq='W-SAT')
FREQLIST['W_s'] = pd.date_range(start = str(start_year)+'-01-01',end=update,freq='W-SAT').strftime('%Y-%m-%d')

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
if merge_file.empty == False:
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

print('Reading table: TABLES, Time: ', int(time.time() - tStart),'s'+'\n')
TABLES = readExcelFile(data_path+'tables.xlsx', header_ = 0, sheet_name_=0)
TABLES = TABLES.set_index(['Sheet'], drop=False) 
CONTINUE = []

def US_KEY(address, counting=False, key=None):
    Titles = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_='titles').to_dict()
    if address.find('BOC') >= 0:
        print('Reading file: BOC_datasets, Time: ', int(time.time() - tStart),'s'+'\n')
        BOC_datasets = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_='BOCdatasets').to_dict()
        print('Reading file: '+key+'_series, Time: ', int(time.time() - tStart),'s'+'\n')
        Series = readExcelFile(data_path+address+key+'_series.xlsx', header_ = 0, index_col_=0)
        return Series, BOC_datasets, Titles
    elif address.find('NIPA') >= 0 or address.find('FAAT') >= 0:
        address = re.sub(r'NIPA/.*', "NIPA/", address)
        BEA_datasets = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_='BEAdatasets')
        print('Reading file: BEA TablesRegister, Time: ', int(time.time() - tStart),'s'+'\n')
        BEA_table = readFile(BEA_datasets.loc[address, 'Table'], header_ = 0, index_col_='TableId')#readExcelFile(data_path+address+'TablesRegister.xlsx', header_ = 0, index_col_='TableId', sheet_name_=0)
        print('Reading file: BEA SeriesRegister, Time: ', int(time.time() - tStart),'s'+'\n')
        BEA_series = readFile(BEA_datasets.loc[address, 'Series'], header_ = 0, index_col_='%SeriesCode')#readExcelFile(data_path+address+'SeriesRegister.xlsx', header_ = 0, index_col_='%SeriesCode', sheet_name_=0)
        if counting == True:
            return BEA_series
        return BEA_series, BEA_table, Titles
    elif address.find('FRB') >= 0:
        print('Reading file: '+key+'_series, Time: ', int(time.time() - tStart),'s'+'\n')
        if key == 'FRB_G17':
            keydata = readExcelFile(data_path+address+'keydata.xlsx', header_=0,index_col_=[0,1] , sheet_name_='keydata')
            FRB_series = readFile(data_path+address+key+'.csv', header_ = 0).drop_duplicates(subset=['Series Name:'], ignore_index=True)
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
    elif address.find('STL') >= 0:
        print('Reading file: '+key+'_series, Time: ', int(time.time() - tStart),'s'+'\n')
        STL_series = readExcelFile(data_path+address+key+'.xls', sheet_name_=0)
        STL_series = list(STL_series[0])
        return STL_series, None, Titles
    elif address.find('NAR') >= 0:
        print('Reading file: NAR_series, Time: ', int(time.time() - tStart),'s'+'\n')
        NAR_series = readExcelFile(data_path+address+'NAR_series.xlsx', header_ = 0, sheet_name_=0)
        return NAR_series, None, Titles
    elif address.find('bls') >= 0:
        print('Reading file: BLS_series, Time: ', int(time.time() - tStart),'s'+'\n')
        BLS_datasets = readExcelFile(data_path+'tables.xlsx', header_ = 0, index_col_=0, sheet_name_='BLSdatasets')
        BLS_series = {}
        if address.find('ec/') >= 0:
            BLS_table = readFile(address+BLS_datasets.loc[address, 'SERIES'], names_=['series_id','comp_code','group_code','ownership_code','periodicity_code','seasonal',\
            'footnote_code','begin_year','begin_period','end_year','end_period'], index_col_=0, skiprows_=[0], acceptNoFile=False, sep_='\\t').to_dict()
        else:
            BLS_table = readFile(address+BLS_datasets.loc[address, 'SERIES'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t').to_dict()
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
            BLS_series['DATA TYPE'] = readFile(address+BLS_datasets.loc[address, 'DATA TYPE'], header_=0, index_col_=0, acceptNoFile=False, sep_='\\t')
            if address.find('bd/') >= 0:
                Sort = {'Establishment Births':['Gross Job Gains', 0.5]}
                BLS_series['DATA TYPE'] = EXCHANGE(address, BLS_series['DATA TYPE'], 'dataclass_name', Sort=Sort)
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
                
        return BLS_series, BLS_table, Titles
    elif address.find('DSCO') >= 0:
        BLS_series = {}
        BLS_series['BASE'] = pd.DataFrame()
        return BLS_series, pd.DataFrame()
    else:
        ERROR('Series Error: '+address)
def US_LEVEL(LABEL, source, Series=None, loc1=None, loc2=None, name=None, indent=None):
    label_level = []
    for l in range(len(LABEL)):
        if source == 'Bureau of Economic Analysis':
            if str(LABEL.iloc[l]) != 'nan':
                label_level.append(re.search(r'\S',str(LABEL.iloc[l])).start())
        elif source == 'Bureau Of Census' or source == 'National Association of Home Builders':
            if str(LABEL.index[l]) != 'nan':
                if str(LABEL.index[l])[loc1:loc2].isnumeric():
                    label_level.append(Series[name].loc[int(re.sub(r'0+$', "", str(LABEL.index[l])[loc1:loc2])), indent])
                else:
                    label_level.append(Series[name].loc[re.sub(r'0+$', "", str(LABEL.index[l])[loc1:loc2]), indent])
            else:
                label_level.append(10000)
    return label_level

def US_ADDLABEL(begin, sheet_name, LABEL, label_level, UNIT, unit, Calculation_type, attribute, suffix=False, form=None):
    level = label_level[begin]
    if UNIT == 'nan':
        UNIT = unit+' '+Calculation_type
    for att in list(reversed(range(begin))):
        if LABEL.iloc[att].find('[') >= 0 and label_level[att] == 0:
            UNIT = LABEL.iloc[att].replace('[','').replace(']','')
            break
        if UNIT == 'nan' and LABEL.iloc[att].find(':') >= 0 and label_level[att] == 0:
            UNIT = LABEL.iloc[att].replace(':','')
            if UNIT == 'Addenda':
                UNIT = unit+' '+Calculation_type
            elif sheet_name == 'U70205S':
                UNIT = unit+' '+Calculation_type
            break
        elif UNIT == 'nan':
            UNIT = unit+' '+Calculation_type
        if label_level[att] < level:
            if LABEL.iloc[att].find(':') >= 0 and LABEL.iloc[att][-1:] == ':':
                attribute.insert(0, LABEL.iloc[att].replace(', ', ',').strip()+' ')
            elif LABEL.iloc[att].find(':') >= 0:
                attribute.insert(0, LABEL.iloc[att][LABEL.iloc[att].find(':')+1:].replace(', ', ',').strip()+', ')
            else:
                if source == 'Bureau Of Census':
                    attribute.insert(0, LABEL.iloc[att].replace('/',' and ').replace('inc.','including').replace(', ', ',').strip()+', ')
                elif bool(re.search(r'\(*S[0-9]+\)', LABEL.iloc[att])):
                    attribute.insert(0, re.sub(r'\(*S[0-9]+\)', "", LABEL.iloc[att]).replace(', ', ',').strip()+', ')
                else:
                    if suffix == True:
                        attribute[-1] = attribute[-1].replace(form, form.replace(', ', ',')+', '+LABEL.iloc[att].replace(', ', ',').strip())
                    else:
                        attribute.insert(0, LABEL.iloc[att].replace(', ', ',').strip()+', ')
            level = label_level[att]
    return UNIT, attribute

def US_ADDNOTE(attri, NOTE, note, note_num, note_part, specific=False):
    note_suffix = ''
    if specific == True:
        dex_list = [attri]
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

def US_DATA(ind, US_t, address, sheet_name, value, index, code_num, table_num, KEY_DATA, DATA_BASE, db_table_t, DB_name, snl, source, freqlist, frequency, UNIT='nan', LABEL=pd.DataFrame(), label_level=[], NOTE=[], FOOTNOTE=[], series=None, table=None, titles=None, repl=None, repl2=None, formnote={}, suffix=''):
    freqlen = len(freqlist)
    unit = ''
    Calculation_type = ''
    form_e = ''
    form_c = ''
    if source == 'Bureau Of Census':
        if address.find('CONS') >= 0:
            NonValue = '0'
        elif address.find('HOUS') >= 0:
            if sname == 'AuthNotSA':
                NonValue = '(S)'
            else:
                NonValue = '(NA)'
        elif address.find('MRTS') >= 0:
            NonValue = '(S)'
        elif address.find('FAMI') >= 0:
            NonValue = 'N'
        elif address.find('HSHD') >= 0:
            NonValue = '(NA)'
        elif address.find('SCEN') >= 0:
            NonValue = 'NA'
        else:
            NonValue = 'nan'
    elif source == 'Federal Reserve Board':
        if address.find('G19') >= 0 or address.find('H15') >= 0:
            NonValue = 'ND'
        else:
            NonValue = 'None'
    elif source == 'Bureau of Economic Analysis':
        NonValue = '.....'
    elif source == 'Bureau Of Labor Statistics':
        NonValue = '-'
    else:
        NonValue = 'nan'
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
        if frequency == 'W':
            db_table_t = db_table_t.reindex(FREQLIST['W_s'])
        DATA_BASE[db_table] = db_table_t
        DB_name.append(db_table)
        table_num += 1
        code_num = 1
        db_table_t = pd.DataFrame(index = freqlist, columns = [])
    
    if address.find('ln/') >= 0 and frequency == 'Q':
        name = frequency+'111'+US_t.iloc[ind]['Index'].replace('-','').strip()[:-1]+suffix
    elif address.find('bd/') >= 0:
        name = frequency+'111'+US_t.iloc[ind]['Index'].strip()[:3]+US_t.iloc[ind]['Index'].strip()[13]+US_t.iloc[ind]['Index'].strip()[16:19]+US_t.iloc[ind]['Index'].strip()[20:26]+suffix
    elif address.find('jt/') >= 0:
        name = frequency+'111'+US_t.iloc[ind]['Index'].strip()[:6]+US_t.iloc[ind]['Index'].strip()[7:11]+US_t.iloc[ind]['Index'].strip()[17:21]+suffix
    else:
        name = frequency+'111'+US_t.iloc[ind]['Index'].replace('-','').strip()+suffix
    
    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
    db_code = DB_CODE+str(code_num).rjust(3,'0')
    db_table_t[db_code] = ['' for tmp in range(freqlen)]
    content = ''
    note = ''
    note_num = 1
    note_part = []
    if source == 'Bureau of Economic Analysis':
        unit = SCALE(US_t.iloc[ind]['Index'], address, series)+series.loc[US_t.iloc[ind]['Index'], 'MetricName']
        Calculation_type = series.loc[US_t.iloc[ind]['Index'], 'CalculationType']
        tabletitle = table.loc[sheet_name, 'TableTitle']
        form_e = re.split(r'Table\s[0-9A-Z\.]+\.\s', tabletitle)[1]
        form_c = re.findall(r'Table\s[0-9A-Z\.]+\.', tabletitle)[0]
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
            NON = ['Total Non-', 'Savings Deposits', 'Small-Denomination', 'Retail Money Funds', 'Institutional Money Funds']
            FNAME = {'FRB_H6': 'M1 M2', 'FRB_H6_discontinued': 'M2 M3'}
            before = ['H6','_','DISCONTINUED','M1','M2','MBASE','MEMO','M3']
            after = ['','','','Components of M1','Components of M2','Monetary Base','Memorandum Items','Components of M3']
            for word in NON:
                if LABEL[US_t.index[ind]].find(M3IMF) >= 0:
                    break
                elif LABEL[US_t.index[ind]].find(word) >= 0:
                    form_e = 'Components of Non-'+FNAME[fname]
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
            if address.find('HOUS') < 0:  
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
            else:
                categ = str(US_t.iloc[ind]['Index'])[1:5]
                unit = series['DATA TYPES'].loc[str(US_t.iloc[ind]['Index'])[repl:], 'dt_unit']
                Calculation_type = series['GEO LEVELS'].loc[str(US_t.iloc[ind]['Index'])[5:repl], 'geo_desc']
                form_e = series['CATEGORIES'].loc[categ, 'cat_desc']
                form_c = series['ISADJUSTED'].loc[str(US_t.iloc[ind]['Index'])[:1], 'adj_desc']
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
        else:
            form_e = series['DATA TYPE'].loc[Table[group+'_code'][US_t.iloc[ind]['Index']], group+'_'+text]
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
        else:
            unit = str(US_t.iloc[ind]['unit'])
        if series['ISADJUSTED'].empty == True:
            form_c = 'Not Seasonally Adjusted'
        elif address.find('mp/') >= 0:
            form_c = series['ISADJUSTED'].loc[US_t.iloc[ind]['Index'].strip()[2], seasonal+'_text']
        else:
            form_c = series['ISADJUSTED'].loc[Table['seasonal'][US_t.iloc[ind]['Index']], seasonal+'_text']
        if address.find('cu') >= 0 or address.find('cw') >= 0 or address.find('li/') >= 0 or address.find('ce/') >= 0 or address.find('pr/') >= 0 or address.find('mp/') >= 0 or address.find('ec/') >= 0 or address.find('jt/') >= 0:
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
    if source != 'Bureau Of Labor Statistics' and address.find('MADI') < 0:
        content = content+form_e+', '
    if not not formnote:
        if series['CATEGORIES'].loc[categ, 'key_desc'] in formnote:
            cont = content[re.search(r'[0-9]+,\s',content).start():]
            note, note_num, note_part, note_suffix = US_ADDNOTE(cont, NOTE, note, note_num, note_part)
            content = re.sub(r'[0-9]+,\s', note_suffix+', ', content)
    attribute = []
    if LABEL.empty == True:
        label = Calculation_type+', '
        attribute.append(label)
    elif LABEL[US_t.index[ind]].find(':') >= 0 and source != 'Bureau Of Labor Statistics':
        attribute.append(LABEL[US_t.index[ind]][LABEL[US_t.index[ind]].find(':')+1:].replace(', ', ',').strip()+', ')
    else:
        if source == 'Bureau Of Labor Statistics' or address.find('APEP') >= 0:
            attribute.append(LABEL[US_t.index[ind]].strip()+', ')
            if address.find('bd/') >= 0:
                content = content+series['CATEGORIES']['dataelement'].loc[Table['dataelement_code'][US_t.iloc[ind]['Index']], 'dataelement_'+text].strip()+', '
            if address.find('ce/') >= 0 or address.find('pr/') >= 0 or address.find('ec/') >= 0  or address.find('jt/') >= 0:
                if address.find('ec/') >= 0:
                    form_e = form_e.title()
                content = content+form_e+', '
            if address.find('ei/') >= 0:
                for no in NOTE:
                    if attribute[0].find(', '+no[0]) >= 0:
                        subword = no[0]
                        note, note_num, note_part, note_suffix = US_ADDNOTE(subword, NOTE, note, note_num, note_part, specific=True)
                        attribute[0] = attribute[0].replace(subword, subword+note_suffix)
        else:
            attribute.append(LABEL[US_t.index[ind]].replace(', ', ',').strip()+', ')
        if address.find('HOUS') >= 0:
            note, note_num, note_part, note_suffix = US_ADDNOTE(str(US_t.iloc[ind]['Index'])[repl:], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = re.sub(r',\s$', note_suffix+', ', attribute[0]) + Calculation_type + ', '
        if address.find('MRTS') >= 0:
            note, note_num, note_part, note_suffix = US_ADDNOTE(str(US_t.iloc[ind]['Index'])[1:repl], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = re.sub(r',\s$', note_suffix+', ', attribute[0])
        if address.find('MWTS') >= 0 or address.find('MRTS') >= 0:
            note, note_num, note_part, note_suffix = US_ADDNOTE(str(US_t.iloc[ind]['Index'])[:1], NOTE, note, note_num, note_part, specific=True)
            attribute[0] = re.sub(r',\s$', note_suffix+', ', attribute[0])
    if source == 'Bureau of Economic Analysis' or source == 'Bureau Of Census' or source == 'National Association of Home Builders':
        begin = list(LABEL.index).index(US_t.index[ind])
        UNIT, attribute = US_ADDLABEL(begin, sheet_name, LABEL, label_level, UNIT, unit, Calculation_type, attribute)
        for a in range(len(attribute)):
            if attribute[a].find('\\1\\0') > 0:
                attribute[a] = attribute[a].replace('\\1\\0', '\\10\\')
            if bool(re.search(r'\\[0-9,\\]+\\',attribute[a])):
                attri = attribute[a][attribute[a].find('\\'):]
                note, note_num, note_part, note_suffix = US_ADDNOTE(attri, NOTE, note, note_num, note_part)
                attribute[a] = re.sub(r'\\[0-9,\\]+\\', note_suffix, attribute[a])
            elif bool(re.search(r'[0-9]+,\s',attribute[a])) and source == 'Bureau Of Census':
                attri = attribute[a][re.search(r'[0-9]+,\s',attribute[a]).start():]
                note, note_num, note_part, note_suffix = US_ADDNOTE(attri, NOTE, note, note_num, note_part)
                attribute[a] = re.sub(r'[0-9]+,\s', note_suffix+', ', attribute[a])
        for note_item in NOTE:
            if note_item[0] == 'Note':
                note = note+'('+str(note_num)+')'+note_item[1]
                note_num += 1
    elif source == 'Bureau Of Labor Statistics':
        if address.find('cu/') >= 0 or address.find('cw/') >= 0:
            begin = list(series['CATEGORIES'][item+'_'+text].index).index(Table[item+'_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, sheet_name, series['CATEGORIES'][item+'_'+text], list(series['CATEGORIES']['display_level']), UNIT, unit, Calculation_type, attribute)
            begin = list(series['DATA TYPE'][group+'_'+text].index).index(Table[group+'_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, sheet_name, series['DATA TYPE'][group+'_'+text], list(series['DATA TYPE']['display_level']), UNIT, unit, Calculation_type, attribute, suffix=True, form=form_e)
        elif address.find('ce/') >= 0:
            begin = list(series['CATEGORIES'][item+'_'+text].index).index(Table[item+'_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, sheet_name, series['CATEGORIES'][item+'_'+text], list(series['CATEGORIES']['display_level']), UNIT, unit, Calculation_type, attribute)
        elif address.find('bd/') >= 0:
            begin = list(series['CATEGORIES']['industry']['industry_'+text].index).index(Table['industry_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, sheet_name, series['CATEGORIES']['industry']['industry_'+text], list(series['CATEGORIES']['industry']['display_level']), UNIT, unit, Calculation_type, attribute)
            attribute.insert(0, form_e+', ')
            begin = list(series['DATA TYPE'][group+'_'+text].index).index(Table[group+'_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, sheet_name, series['DATA TYPE'][group+'_'+text], list(series['DATA TYPE']['display_level']), UNIT, unit, Calculation_type, attribute)
            attribute.append('for firms with '+series['CATEGORIES']['sizeclass'].loc[Table['sizeclass_code'][US_t.iloc[ind]['Index']], 'sizeclass_'+text].strip()+', ')
            attribute.append(series['BASE'].loc[Table[base+'_code'][US_t.iloc[ind]['Index']], base+'_'+p_text].strip()+', ')
        elif address.find('jt/') >= 0:
            begin = list(series['CATEGORIES'][item+'_'+text].index).index(Table[item+'_code'][US_t.iloc[ind]['Index']])
            UNIT, attribute = US_ADDLABEL(begin, sheet_name, series['CATEGORIES'][item+'_'+text], list(series['CATEGORIES']['display_level']), UNIT, unit, Calculation_type, attribute)
            attribute.append(series['BASE'].loc[Table[base+'_code'][US_t.iloc[ind]['Index']], base+'_'+p_text].strip()+', ')
        for note_item in NOTE:
            if type(Table['footnote_codes'][US_t.iloc[ind]['Index']]) == float and Table['footnote_codes'][US_t.iloc[ind]['Index']].is_integer():
                Table['footnote_codes'][US_t.iloc[ind]['Index']] = int(Table['footnote_codes'][US_t.iloc[ind]['Index']])
            if note_item[0] == str(Table['footnote_codes'][US_t.iloc[ind]['Index']]) and address.find('ei/') < 0:
                note = note+'('+str(note_num)+')'+note_item[1]
                note_num += 1
    for attri in attribute:
        content = content+attri
    if source != 'National Association of Realtors' and source != 'Bureau Of Labor Statistics' and address.find('APEP') < 0 and address.find('H6') < 0 and address.find('G19') < 0:
        content = content+form_c+', '
    elif address.find('MADI') >= 0:
        content = content+Calculation_type+', '
    elif source == 'Bureau Of Labor Statistics':
        if address.find('DSCO') >= 0 or address.find('ce/') >= 0 or address.find('pr/') >= 0 or address.find('mp/') >= 0 or address.find('ec/') >= 0 or address.find('bd/') >= 0 or address.find('jt/') >= 0:
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
    if note != '':
        desc_e = title + content + 'Unit: ' + UNIT.replace('[','').replace('] ',', ').replace(']','') + ', Source: ' + source + ', Note: ' + note
    else:
        desc_e = title + content + 'Unit: ' + UNIT.replace('[','').replace('] ',', ').replace(']','') + ', Source: ' + source
    for footnote_item in FOOTNOTE:
        if desc_e.find(footnote_item[0]) >= 0:
            desc_e = desc_e.replace(footnote_item[0],footnote_item[1])
    if source == 'Bureau of Economic Analysis' or UNIT != unit:
        desc_c = UNIT.replace('[','').replace('] ',', ').replace(']','')
    elif source == 'Bureau Of Labor Statistics' and series['BASE'].empty == False:
        desc_c = series['BASE'].loc[Table[base+'_code'][US_t.iloc[ind]['Index']], base+'_'+p_text]
        if address.find('cu/') >= 0 or address.find('cw/') >= 0:
            desc_c = desc_c+' Reference Base'
    else:
        desc_c = ''
    
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
        if freq_index in db_table_t.index:
            if str(value[k]).strip() == NonValue or str(value[k]) == 'nan':
                db_table_t[db_code][freq_index] = ''
            else:
                found = True
                db_table_t[db_code][freq_index] = float(value[k])
                if start_found == False and found == True:
                    if frequency == 'A':
                        start = int(freq_index)
                    else:
                        start = str(freq_index)
                    start_found = True
                if start_found == True:
                    if k == len(value)-1:
                        if frequency == 'A':
                            last = int(freq_index)
                        else:
                            last = str(freq_index)
                        last_found = True
                    else:
                        for st in range(k+1, len(value)):
                            if ((frequency != 'A' and str(index[st]).find(frequency) >= 0) or str(index[st]).isnumeric() or source == 'Federal Reserve Board') and str(value[st]).strip() != NonValue and str(value[st]) != 'nan':
                                last_found = False
                            else:
                                last_found = True
                            if last_found == False:
                                break
                        if last_found == True:
                            if frequency == 'A':
                                last = int(freq_index)
                            else:
                                last = str(freq_index)
        else:
            continue
    
    if start_found == False:
        if found == True:
            ERROR('start not found: '+str(name))
    elif last_found == False:
        if found == True:
            ERROR('last not found: '+str(name))
    if found == False:
        start = 'Nan'
        last = 'Nan'
    
    YEAR = {'main':'-M13','ln/':'-M13','pr/':'-Q05','mp/':'-A01','ec/':'-Q05'}
    QUAR = {'M03':'Q1','M06':'Q2','M09':'Q3','M12':'Q4','Q01':'Q1','Q02':'Q2','Q03':'Q3','Q04':'Q4'}
    RAUQ = {}
    RAUQ['main'] = {'Q1':'M03','Q2':'M06','Q3':'M09','Q4':'M12'}
    RAUQ['ln/'] = {'Q1':'Q01','Q2':'Q02','Q3':'Q03','Q4':'Q04'}
    RAUQ['pr/'] = RAUQ['ln/']
    RAUQ['mp/'] = RAUQ['main']
    RAUQ['ec/'] = RAUQ['ln/']
    RAUQ['bd/'] = RAUQ['ln/']
    if bls_start == None:
        if source == 'Bureau Of Labor Statistics' and frequency == 'M' and start.replace('-', '-M') != US_t.iloc[ind]['start'] and US_t.iloc[ind]['start'].find('-M13') < 0:
            ERROR('start error: '+str(name))
        elif source == 'Bureau Of Labor Statistics' and frequency == 'A' and str(start)+YEAR[repl] != US_t.iloc[ind]['start'] and US_t.iloc[ind]['start'].find(YEAR[repl]) >= 0:
            ERROR('start error: '+str(name))
        elif source == 'Bureau Of Labor Statistics' and frequency == 'S' and start.replace('S', 'S0') != US_t.iloc[ind]['start'] and US_t.iloc[ind]['start'].find('S03') < 0:
            ERROR('start error: '+str(name))
        elif source == 'Bureau Of Labor Statistics' and frequency == 'Q' and US_t.iloc[ind]['start'][-3:] in QUAR:
            if start.replace(start[-2:], RAUQ[repl][start[-2:]]) != US_t.iloc[ind]['start']:
                ERROR('start error: '+str(name))
    if source == 'Bureau Of Labor Statistics' and frequency == 'M' and last.replace('-', '-M') != US_t.iloc[ind]['last'] and US_t.iloc[ind]['last'].find('-M13') < 0 and str(US_t.iloc[ind][US_t.iloc[ind]['last'].replace('-M','-')]).strip() != NonValue:
        ERROR('last error: '+str(name))
    elif source == 'Bureau Of Labor Statistics' and frequency == 'A' and str(last)+YEAR[repl] != US_t.iloc[ind]['last'] and US_t.iloc[ind]['last'].find(YEAR[repl]) >= 0 and str(US_t.iloc[ind][int(US_t.iloc[ind]['last'].replace(YEAR[repl],''))]).strip() != NonValue:
        ERROR('last error: '+str(name))
    elif source == 'Bureau Of Labor Statistics' and frequency == 'S' and last.replace('S', 'S0') != US_t.iloc[ind]['last'] and US_t.iloc[ind]['last'].find('S03') < 0 and str(US_t.iloc[ind][US_t.iloc[ind]['last'].replace('S0','S')]).strip() != NonValue:
        ERROR('last error: '+str(name))
    elif source == 'Bureau Of Labor Statistics' and frequency == 'Q' and US_t.iloc[ind]['last'][-3:] in QUAR and str(US_t.iloc[ind][US_t.iloc[ind]['last'].replace(US_t.iloc[ind]['last'][-3:],QUAR[US_t.iloc[ind]['last'][-3:]])]).strip() != NonValue:
        if last.replace(last[-2:], RAUQ[repl][last[-2:]]) != US_t.iloc[ind]['last']:
            ERROR('last error: '+str(name))

    key_tmp= [databank, name, db_table, db_code, desc_e, desc_c, frequency, start, last, unit, Calculation_type, snl, source, form_e, form_c]
    KEY_DATA.append(key_tmp)
    snl += 1
    
    code_num += 1
    
    return code_num, table_num, DATA_BASE, db_table_t, DB_name, snl

###########################################################################  Main Function  ###########################################################################
MONTH = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
TABLE_NAME = {'ISADJUSTED':'adj','CATEGORIES':'cat','DATA TYPES':'dt','GEO LEVELS':'geo'}

for source in SOURCE:
    if main_file.empty == False:
        break
    for address in FILE_ADDRESS(source):
        if (HIES_old == False and address.find('HIES') >= 0) or (make_discontinued == False and (address.find('wd/') >= 0 or address.find('DSCO') >= 0)):
            continue
        if address.find(keyword) < 0 or address.find(ignore) >= 0:
            continue
        Series, Table, Titles = US_KEY(address, key=re.sub(r'FED|BOC|HOUS|APEP|STL', "", address).replace('/',''))
        for fname in FILE_NAME(address):
            if make_discontinued == False and fname.find('discontinued') >= 0:
                continue
            if source == 'Bureau of Economic Analysis': #and address.find('NIPA') >= 0:
                print('Reading source file, Time: ', int(time.time() - tStart),'s'+'\n')
                US_t_dict = readExcelFile(data_path+address+fname+'.xlsx', header_ =0, index_col_=0, skiprows_=list(range(7)))
                sheet_list = list(US_t_dict)
            else:
                sheet_list = SHEET_NAME(address, fname)
            for sname in sheet_list:
                bls_read = False
                if source == 'Bureau of Economic Analysis':
                    US_t = US_t_dict[sname]
                for freq in FREQUENCY(address, fname, sname):
                    print('Reading file: '+fname+', sheet: '+str(sname)+', frequency: '+freq+', Time: ', int(time.time() - tStart),'s'+'\n')
                    unit = 'nan'
                    repl2 = None
                    formnote = {}
                    if source == 'Bureau of Economic Analysis':
                        #US_t = readExcelFile(data_path+address+fname+'.xlsx', header_ =0, index_col_=0, skiprows_=list(range(7)), sheet_name_=sname+'-'+freq)
                        sname = re.split(r'\-', sname)[0]
                        if US_t.empty == False:
                            unit = str(readExcelFile(data_path+address+fname+'.xlsx', usecols_=[0], sheet_name_=sname).iloc[1][0]).strip()
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
                            note, footnote = US_NOTE(US_t.index, sname, label)
                    elif source == 'Federal Reserve Board':
                        if address.find('G17') >= 0:
                            US_temp = readFile(fname, header_=None, names_=['code','year']+MONTH, acceptNoFile=False, sep_='\\s+')
                            US_t = US_HISTORYDATA(US_temp, name='year', MONTH=MONTH, make_idx=True)
                        elif address.find('H6') >= 0 or address.find('G19') >= 0 or address.find('H15') >= 0:
                            if fname.find('discontinued') >= 0:
                                Series, Table, Titles = US_KEY(address, key=fname)
                            US_t = readFile(data_path+address+fname+'.csv', header_=0, index_col_=0, skiprows_=list(range(5))).T
                            new_index = []
                            for i in range(US_t.shape[0]):
                                new_index.append(US_t.index[i][:-2])
                            US_t.insert(loc=0, column='Index', value=new_index)
                            US_t = US_t.set_index('Index', drop=False)
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
                    elif source == 'Federal Reserve Bank Of St. Louis':
                        US_temp = readExcelFile(data_path+address+fname+'.xls', header_ =0, index_col_=0, sheet_name_=sname).T
                        US_t, label, note, footnote = US_STL(US_temp, address, Series)
                        label_level = None
                        repl = None
                    elif source == 'Bureau Of Census' and address.find('MSIO') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, datasets=fname, DIY_series=Series, MONTH=MONTH, password='MPC')
                        for table in Series:
                            Series[table] = Series[table].reset_index().set_index(TABLE_NAME[table]+'_code')
                        repl = 4
                        label_level = US_LEVEL(label, source, Series, loc1=1, loc2=repl, name='CATEGORIES', indent='cat_indent')
                    elif source == 'Bureau Of Census' and address.find('CONS') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, datasets=Table['DataSets'][address], fname=fname, sname=sname, DIY_series=Series, index_col=0, skiprows=[0,1,2])
                        repl = 5
                        label_level = US_LEVEL(label, source, Series, loc1=1, loc2=repl, name='CATEGORIES', indent='cat_indent')
                    elif source == 'Bureau Of Census' and address.find('RESC') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, datasets=Table['DataSets'][address], fname=fname, sname=sname, DIY_series=Series, header=[0,1], index_col=list(range(int(TABLES.loc[sname,'index_col'].item())+1)), skiprows=list(range(int(TABLES.loc[sname,'skiprows'].item()))))
                        repl = 7
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=10, name='DATA TYPES', indent='dt_indent')
                    elif source == 'Bureau Of Census' and address.find('SALE') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, datasets=fname, DIY_series=Series, MONTH=MONTH, password='ASLD')
                        other_notes = readExcelFile(data_path+address+'other_notes.xlsx', header_=0, index_col_=0, sheet_name_=0, acceptNoFile=False)
                        note = note + US_NOTE(other_notes, sname, address=address, other=True)
                        for table in Series:
                            Series[table] = Series[table].reset_index().set_index(TABLE_NAME[table]+'_code')
                        repl = 7
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=10, name='DATA TYPES', indent='dt_indent')
                    elif source == 'Bureau Of Census' and address.find('PRIC') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, fname=fname, sname=sname, DIY_series=Series, header=[0,1], index_col=0, skiprows=list(range(5)), freq=freq)
                        unit = str(readExcelFile(data_path+address+fname+'.xls', usecols_=[0], sheet_name_=sname).iloc[1][0]).strip()
                        repl = 7
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=10, name='DATA TYPES', indent='dt_indent')
                    elif source == 'Bureau Of Census' and address.find('HIHV') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, datasets=fname, DIY_series=Series)
                        for table in Series:
                            Series[table] = Series[table].reset_index().set_index(TABLE_NAME[table]+'_code')
                            Series[table] = Series[table][~Series[table].index.duplicated()]
                        repl = 7
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=10, name='DATA TYPES', indent='dt_indent')
                    elif source == 'Bureau Of Census' and address.find('HIES') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, datasets=Table['DataSets'][address], fname=fname, sname=sname, DIY_series=Series, freq=freq, HIES=True)
                        repl = 7
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=10, name='DATA TYPES', indent='dt_indent')
                    elif source == 'Bureau Of Census' and address.find('SHIP') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, datasets=Table['DataSets'][address], fname=fname, sname=sname, DIY_series=Series, MONTH=MONTH, header=[0], index_col=0, skiprows=list(range(3)), freq=freq, x='x')
                        repl = 7
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=10, name='DATA TYPES', indent='dt_indent')
                    elif source == 'National Association of Home Builders' and address.find('NAHB') >= 0:
                        if fname == 'table3-nahb-wells-fargo-national-hmi-components-history':
                            skip = 33
                        else:
                            skip = 2
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, fname=fname, sname=sname, DIY_series=Series, MONTH=MONTH, header=[0], index_col=0, skiprows=list(range(skip)), freq=freq)
                        repl = 7
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=10, name='DATA TYPES', indent='dt_indent')
                    elif source == 'National Association of Realtors':
                        US_t = readExcelFile(data_path+address+fname+'.xlsx', header_ =0, index_col_='Mnemonic', skiprows_=list(range(2)), sheet_name_=sname, skipfooter_=10)
                        if US_t.empty == True:
                            ERROR('Sheet Not Found: '+data_path+address+fname+', sheet name: '+sname)
                        US_t, label, note, footnote = US_IHS(US_t, Series, freq)
                        repl = None
                        label_level = None
                    elif source == 'Bureau Of Census' and address.find('MTIS') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, datasets=fname, DIY_series=Series, MONTH=MONTH, password='MPC')
                        for table in Series:
                            Series[table] = Series[table].reset_index().set_index(TABLE_NAME[table]+'_code')
                        repl = 5
                        label_level = US_LEVEL(label, source, Series, loc1=1, loc2=repl, name='CATEGORIES', indent='cat_indent')
                    elif source == 'Bureau Of Census' and address.find('MWTS') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, datasets=fname, DIY_series=Series, MONTH=MONTH, password='MPC')
                        other_notes = readExcelFile(data_path+address+'other_notes.xlsx', header_=0, index_col_=0, sheet_name_=0, acceptNoFile=False)
                        note = note + US_NOTE(other_notes, sname, address=address, other=True)
                        for table in Series:
                            Series[table] = Series[table].reset_index().set_index(TABLE_NAME[table]+'_code')
                        repl = 6
                        label_level = US_LEVEL(label, source, Series, loc1=1, loc2=repl, name='CATEGORIES', indent='cat_indent')
                    elif source == 'Bureau Of Census' and address.find('MRTS') >= 0:
                        if freq == 'M':
                            US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, datasets=fname, DIY_series=Series, MONTH=MONTH, password='MPC')
                            for table in Series:
                                Series[table] = Series[table].reset_index().set_index(TABLE_NAME[table]+'_code')
                        elif freq == 'Q':
                            for table in Series:
                                Series[table] = Series[table].reset_index().set_index(TABLE_NAME[table]+'_code')
                            US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, fname=fname, sname=sname, DIY_series=Series, header=[0], index_col=0, skiprows=list(range(7)), freq=freq, usecols=list(range(1,4)))
                        other_notes = readExcelFile(data_path+address+'other_notes.xlsx', header_=0, index_col_=0, sheet_name_=0, acceptNoFile=False)
                        note = note + US_NOTE(other_notes, sname, address=address, other=True)
                        repl = 8
                        label_level = US_LEVEL(label, source, Series, loc1=1, loc2=repl, name='CATEGORIES', indent='cat_indent')
                    elif source == 'Bureau Of Census' and address.find('POPT') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, fname=fname, sname=sname, DIY_series=Series, header=[0], index_col=0, skiprows=list(range(2)), freq=freq, x='x')
                        repl = 4
                        repl2 = 6
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=repl2, name='DATA TYPES', indent='dt_indent')
                    elif source == 'Bureau Of Census' and address.find('POPP') >= 0:
                        US_temp = readFile(data_path+address+fname+'.csv', header_=0)
                        US_t, label, note, footnote = US_POPP(US_temp, data_path, address, datasets=fname, DIY_series=Series, password='')
                        for table in Series:
                            if table in TABLE_NAME:
                                Series[table] = Series[table].reset_index().set_index('aremos_key')
                        repl = 4
                        repl2 = 6
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=repl2, name='DATA TYPES', indent='dt_indent')
                    elif source == 'Bureau Of Census' and address.find('CBRT') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, fname=fname, sname=sname, DIY_series=Series, header=[0], freq=freq, x='x')
                        repl = 4
                        repl2 = 6
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=repl2, name='DATA TYPES', indent='dt_indent')
                    elif source == 'Bureau Of Census' and address.find('FAMI') >= 0:
                        US_t, label, note, footnote, formnote = US_FAMI(TABLES, data_path, address, fname, sname, Series, x='x')
                        repl = 6
                        repl2 = 9
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=repl2, name='DATA TYPES', indent='dt_indent')
                    elif source == 'Bureau Of Census' and address.find('HSHD') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, fname=fname, sname=sname, DIY_series=Series, header=(0,1), index_col=0, skiprows=list(range(7)), freq=freq)
                        repl = 6
                        repl2 = 12
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=repl2, name='DATA TYPES', indent='dt_indent')
                    elif source == 'Bureau Of Census' and address.find('URIN') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, fname=fname, sname=sname, DIY_series=Series, index_col=0, freq=freq, x='x')
                        repl = 7
                        repl2 = 9
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=repl2, name='DATA TYPES', indent='dt_indent')
                    elif source == 'Bureau Of Census' and address.find('MADI') >= 0:
                        US_t, label, note, footnote, formnote = US_FAMI(TABLES, data_path, address, fname, sname, Series)
                        repl = 5
                        repl2 = 7
                        label_level = US_LEVEL(label, source, Series, loc1=2, loc2=repl, name='CATEGORIES', indent='cat_indent')
                    elif source == 'Bureau Of Census' and address.find('SCEN') >= 0:
                        US_t, label, note, footnote, formnote = US_FAMI(TABLES, data_path, address, fname, sname, Series, x='x')
                        repl = 6
                        repl2 = 12
                        label_level = US_LEVEL(label, source, Series, loc1=repl, loc2=repl2, name='DATA TYPES', indent='dt_indent')
                    elif source == 'Bureau Of Census' and address.find('QFRS') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, datasets=fname, DIY_series=Series)
                        for table in Series:
                            Series[table] = Series[table].reset_index().set_index(TABLE_NAME[table]+'_code')
                        repl = 4
                        label_level = US_LEVEL(label, source, Series, loc1=1, loc2=repl, name='CATEGORIES', indent='cat_indent')
                    elif source == 'Bureau Of Labor Statistics' and address.find('DSCO') >= 0:
                        US_t, label, note, footnote = DATA_SETS(TABLES, data_path, address, fname=fname, sname=sname, DIY_series=Series, header=0, index_col=(0,1), skiprows=list(range(18)), freq=freq, x='x')
                        repl = ''
                        label_level = None
                    elif source == 'Bureau Of Labor Statistics':
                        if address.find('li/') >= 0 or address.find('ce/') >= 0 or address.find('pr/') >= 0 or address.find('ec/') >= 0 or address.find('mp/') >= 0 or address.find('bd/') >= 0 or address.find('jt/') >= 0:
                            new_label = True
                        else:
                            new_label = False
                        if address.find('ln/') >= 0 or address.find('pr/') >= 0 or address.find('mp/') >= 0 or address.find('ec/') >= 0 or address.find('bd/') >= 0:
                            bls_key = address[-3:]
                        else:
                            bls_key = 'main'
                        if (address.find('ln/') >= 0 or address.find('bd/') >= 0) and bls_start == None:#
                            US_t = readExcelFile(data_path+'BLS/US_t_'+address[-3:-1]+'_'+freq+'.xlsx', header_ =0, index_col_=0, sheet_name_=0)
                            footnote = []
                            label = US_t['Label']
                            cat_idx = 'industry'
                            item = 'name'
                        else:
                            if bls_read == False:
                                US_temp = readFile(address+fname, header_=0, names_=['series_id','year','period','value','footnote_codes'], acceptNoFile=True, sep_='\\t')
                                bls_read = True
                            if US_temp.empty == False:
                                if address.find('cu') >= 0 or address.find('cw') >= 0 or address.find('li/') >= 0 or address.find('ei/') >= 0:
                                    idb = 'base_period'
                                    cat_idx = 'item'
                                    item = 'name'
                                elif address.find('ln/') >= 0:
                                    idb = 'tdat_code'
                                elif address.find('ce/') >= 0:
                                    idb = 'data_type_code'
                                    cat_idx = 'industry'
                                    item = 'name'
                                elif address.find('pr/') >= 0 or address.find('mp/') >= 0:
                                    idb = 'base_year'
                                    cat_idx = 'measure'
                                    item = 'text'
                                elif address.find('ec/') >= 0:
                                    idb = 'periodicity_code'
                                    cat_idx = 'group'
                                    item = 'text'
                                elif address.find('bd/') >= 0 or address.find('jt/') >= 0:
                                    idb = 'ratelevel_code'
                                    cat_idx = 'industry'
                                    item = 'name'
                                    if address.find('jt/') >= 0:
                                        item = 'text'
                                else:
                                    idb = 'base_date'
                                US_t, label, note, footnote = US_BLS(US_temp, Table, freq, index_base=idb, address=address, start=bls_start, key=bls_key)
                        if US_t.empty == False:
                            if address.find('ei/') >= 0 or address.find('ln/') >= 0 or address.find('ce/') >= 0 or address.find('mp/') >= 0 or address.find('bd/') >= 0:
                                note = US_NOTE(Series['NOTE'], sname, LABEL=Table, address=address, other=True)
                            if new_label == True:
                                label = NEW_LABEL(address[-3:], label.copy(), Series, Table, cat_idx, item)
                        repl = bls_key#fname
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
                    print(US_t)
                    ERROR('')
                    nG = US_t.shape[0]
                    print('Total Columns:',nG,'Time: ', int(time.time() - tStart),'s'+'\n')        
                    for i in range(nG):
                        sys.stdout.write("\rProducing Database...("+str(round((i+1)*100/nG, 1))+"%)*")
                        sys.stdout.flush()

                        if str(US_t.iloc[i]['Index']) == 'ZZZZZZ' or str(US_t.iloc[i]['Index']) == 'nan' or str(US_t.iloc[i]['Index']) in CONTINUE:
                            continue
                        if address.find('bd/') >= 0:
                            if Table['state_code'][US_t.iloc[i]['Index']] != 0:
                                continue
                        
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
                            US_DATA(i, US_t, address, sname, value, index, code_num_dict[freq], table_num_dict[freq], KEY_DATA, DATA_BASE_dict[freq], db_table_t_dict[freq], DB_name_dict[freq], snl, source, FREQLIST[freq], freq, unit, label, label_level, note, footnote, series=Series, table=Table, titles=Titles, repl=repl, repl2=repl2, formnote=formnote, suffix='.'+freq)
                    sys.stdout.write("\n\n")

for f in FREQNAME:
    if main_file.empty == False:
        break
    if db_table_t_dict[f].empty == False:
        if f == 'W':
            db_table_t_dict[f] = db_table_t_dict[f].reindex(FREQLIST['W_s'])
            FREQLIST['W'] = FREQLIST['W_s']
        DATA_BASE_dict[f][DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
        DB_name_dict[f].append(DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0'))       

print('Time: ', int(time.time() - tStart),'s'+'\n')
if main_file.empty == True:
    df_key = pd.DataFrame(KEY_DATA, columns = key_list)
else:
    if merge_file.empty == True:
        ERROR('Missing Merge File')
if df_key.empty:
    ERROR('Empty dataframe')
df_key, DATA_BASE_dict = CONCATE(NAME, merge_suf, out_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, DATA_BASE_dict, DB_name_dict)

print(df_key)
#print(DATA_BASE_t)

print('Time: ', int(time.time() - tStart),'s'+'\n')
df_key.to_excel(out_path+NAME+"key.xlsx", sheet_name=NAME+'key')
with pd.ExcelWriter(out_path+NAME+"database.xlsx") as writer: # pylint: disable=abstract-class-instantiated
    for f in FREQNAME:
        for d in DATA_BASE_dict[f]:
            sys.stdout.write("\rOutputing sheet: "+str(d))
            sys.stdout.flush()
            if DATA_BASE_dict[f][d].empty == False:
                DATA_BASE_dict[f][d].to_excel(writer, sheet_name = d)
        sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')

LEFT = {}
if keyword == 'NIPA':
    LEFT_NIPA = []
    LEFT['NIPA'] = {'address':'BEA/NIPA/', 'left':[]}
elif keyword == 'FAAT':
    LEFT_FAAT = []
    LEFT['FAAT'] = {'address':'BEA/FAAT/', 'left':[]}
elif keyword == 'FRB':
    LEFT_FRB = []
    LEFT['FRB'] = {'address':'FRB/FED_G17/', 'left':[]}
DF_NAME = []
for df_name in list(df_key['name']):
    DF_NAME.append(df_name[4:-2])
for l in LEFT:
    series = US_KEY(LEFT[l]['address'], counting=True, key=re.sub(r'FED|BOC|HOUS|APEP', "", LEFT[l]['address']).replace('/',''))
    for i in range(series.shape[0]):
        if str(series.index[i]) not in DF_NAME:
            LEFT[l]['left'].append(series.index[i])
if keyword == 'NIPA':
    print('Items of BEA/NIPA not found: ', len(LEFT['NIPA']['left']), '\n')
elif keyword == 'FAAT':
    print('Items of BEA/FAAT not found: ', len(LEFT['FAAT']['left']), '\n')
elif keyword == 'FRB':
    print('Items of FRB not found: ', len(LEFT['FRB']['left']), '\n')
    print('Labels of FRB not found: ', len(CONTINUE), '\n')
print('Time: ', int(time.time() - tStart),'s'+'\n')
