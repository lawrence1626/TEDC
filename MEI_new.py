# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time, logging
import pandas as pd
import numpy as np
from datetime import datetime, date
from cif_new import createDataFrameFromOECD
import MEI_concat as CCT
from MEI_concat import ERROR, MERGE, NEW_KEYS, CONCATE, UPDATE, readFile, readExcelFile, PRESENT
import MEI_test as test
from MEI_test import MEI_identity
FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT, handlers=[logging.FileHandler("LOG.log", 'w', CCT.ENCODING)], datefmt='%Y-%m-%d %I:%M:%S %p')

ENCODING = CCT.ENCODING

NAME = 'MEI_'
data_path = './data/'
out_path = "./output/"
databank = NAME[:-1]
find_unknown = False
main_suf = '?'
merge_suf = '?'
dealing_start_year = 1947
start_year = 1947
merging = False
updating = False
data_processing = bool(int(input('Processing data (1/0): ')))
excel_suffix = CCT.excel_suffix
DF_suffix = test.DF_suffix
LOG = ['excel_suffix', 'data_processing', 'find_unknown','dealing_start_year']
for key in LOG:
    logging.info(key+': '+str(locals()[key])+'\n')
log = logging.getLogger()
stream = logging.StreamHandler(sys.stdout)
stream.setFormatter(logging.Formatter('%(message)s'))
log.addHandler(stream)
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'last', 'unit', 'name_ord', 'snl', 'book', 'form_e', 'form_c']
dataset_list = ['MEI', 'MEI_CLI', 'MEI_BTS_COS']
frequency_list = ['A','Q','M']
for i in range(len(key_list)):
    if key_list[i] == 'snl':
        snl_pos = i
        break
tStart = time.time()

def takeFirst(alist):
	return alist[0]

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

def SUBJECT_CODE(code, slist):
    if code in subject_file['code2']:
        return subject_file['code2'][code]
    else:
        logging.info(slist.keys())
        ERROR('Subjects未知代碼: '+code)

def MEASURE_CODE(code, mlist):
    if str(code) in measure_file['code2']:
        return measure_file['code2'][code]
    elif code == '' or str(code) == 'nan':
        return ''
    else:
        logging.info(mlist.keys())
        ERROR('Measures未知代碼: '+code)

def START_DATE(freq):
    if find_unknown == False:
        if freq == 'A':
            return dealing_start_year
        elif freq == 'Q':
            return str(dealing_start_year)+'-Q1'
        elif freq == 'M':
            return str(dealing_start_year)+'-01'
        else:
            ERROR('頻率錯誤: '+freq)
    else:
        return None

this_year = datetime.now().year + 1
FREQNAME = {'A':'annual','Q':'quarter','M':'month'}
FREQLIST = {}
FREQLIST['A'] = [tmp for tmp in range(start_year,this_year)]
FREQLIST['Q'] = []
for q in range(start_year,this_year):
    for r in range(1,5):
        FREQLIST['Q'].append(str(q)+'-Q'+str(r))
FREQLIST['M'] = []
for y in range(start_year,this_year):
    for m in range(1,13):
        FREQLIST['M'].append(str(y)+'-'+str(m).rjust(2,'0'))

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
    find_unknown = bool(int(input('Check if new items exist (1/0): ')))
    """if find_unknown == False:
        dealing_start_year = int(input("Dealing with data from year: "))
        start_year = dealing_start_year-10"""
    sys.stdout.write("\n\n")
    logging.info('Data Processing\n')
    main_file = pd.DataFrame()
    merge_file = pd.DataFrame()
    snl = 1
    for f in FREQNAME:
        table_num_dict[f] = 1
        code_num_dict[f] = 1
    country = readFile(data_path+'Country.csv', header_ = 0, index_col_=[0], encoding_='Big5')
    country.to_dict()
    form_e_file1 = readExcelFile(data_path+'MEI_form_e.xlsx', header_ = 0, sheet_name_='MEI_CLI')
    form_e_file2 = readExcelFile(data_path+'MEI_form_e.xlsx', header_ = 0, sheet_name_='MEI_BTS_COS')
    form_e_dict1 = {}
    form_e_dict2 = {}
    for form in form_e_file1:
        form_e_dict1[form] = form_e_file1[form].dropna().to_list()
    for form in form_e_file2:
        form_e_dict2[form] = form_e_file2[form].dropna().to_list()
    subject_file = readExcelFile(data_path+'MEI_Subjects.xlsx', acceptNoFile=False, header_ = 0, index_col_=[0], sheet_name_='MEI_Subjects')
    measure_file = readExcelFile(data_path+'MEI_Measures.xlsx', acceptNoFile=False, header_ = 0, index_col_=[0], sheet_name_='MEI_Measures')

merge_file_loaded = False
while data_processing == False:
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

#print(MEI_t.head(10))
if updating == False and DF_suffix != merge_suf:
    logging.info('Reading file: '+NAME+'key'+DF_suffix+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
    DF_KEY = readExcelFile(out_path+NAME+'key'+DF_suffix+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'key')
    DF_KEY = DF_KEY.set_index('name')
elif updating == False and DF_suffix == merge_suf:
    DF_KEY = merge_file
    DF_KEY = DF_KEY.set_index('name')

def MEI_DATA(i, name, MEI_t, code_num, table_num, KEY_DATA, DATA_BASE, db_table_t, DB_name, snl, freqlist, frequency):
    freqlen = len(freqlist)
    NonValue = ['nan','']
    if code_num >= 200:
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        DATA_BASE[db_table] = db_table_t
        DB_name.append(db_table)
        table_num += 1
        code_num = 1
        db_table_t = pd.DataFrame(index = freqlist, columns = [])
    
    value = MEI_t[MEI_t.columns[i]]
    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
    db_code = DB_CODE+str(code_num).rjust(3,'0')
    #db_table_t[db_code] = ['' for tmp in range(freqlen)]
    db_table_t = pd.concat([db_table_t, pd.DataFrame(['' for tmp in range(freqlen)], index=freqlist, columns=[db_code])], axis=1)
    start_found = False
    last_found = False
    found = False
    for k in range(len(value)):
        try:
            freq_index = int(value.index[k])
        except:
            freq_index = str(value.index[k])
        if freq_index in db_table_t.index and ((find_unknown == False and int(str(freq_index)[:4]) >= dealing_start_year) or find_unknown == True):
            if str(value.iloc[k]).strip() in NonValue:
                db_table_t[db_code][freq_index] = ''
            else:
                try:
                    db_table_t[db_code][freq_index] = float(value.iloc[k])
                except ValueError:
                    ERROR('Nontype Value detected: '+str(value.iloc[k]))
                found = True
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
    
    Subject = subjects_list[MEI_t.columns[i][1]]
    Measure = measures_list[str(MEI_t.columns[i][2])]
    PowerCode = MEI_t.columns[i][4]
    if str(MEI_t.columns[i][3]).find('Unnamed') >= 0 or str(MEI_t.columns[i][3]).find('nan') == 0:
        if dataset == 'MEI_BTS_COS':
            Unit = 'Percentage'
        else:
            Unit = ''
    else:
        Unit = MEI_t.columns[i][3]
    if Measure == '':
        desc_e = str(Subject) + ', ' + str(PowerCode) + ' of ' + str(Unit)
    else:
        desc_e = str(Subject) + ', '+str(Measure) + ', ' + str(PowerCode) + ' of ' + str(Unit)
    if str(Subject).find('>') > 0:
        sub = str(Subject).find('>')-1
        form_e = str(Subject)[:sub]
    elif dataset == 'MEI_CLI':
        form_found = False
        for form in form_e_dict1:
            if MEI_t.columns[i][1] in form_e_dict1[form]:
                form_e = str(form)
                form_found = True
                break
        if form_found == False:
            form_e = 'Others'
    elif dataset == 'MEI_BTS_COS':
        form_found = False
        for form in form_e_dict2:
            if MEI_t.columns[i][1] in form_e_dict2[form]:
                form_e = str(form)
                form_found = True
                break
        if form_found == False:
            form_e = 'Others'
    else:
        form_e = 'Others'
    
    if str(MEI_t.columns[i][2]) == 'nan':
        desc_c = str(MEI_t.columns[i][1])
    else:
        desc_c = str(MEI_t.columns[i][1])+str(MEI_t.columns[i][2])
    unit = str(PowerCode) + ' of ' + str(Unit)
    name_ord = MEI_t.columns[i][0]
    book = COUNTRY_NAME(MEI_t.columns[i][0])
    desc_e = desc_e + ' - ' + book
    if str(MEI_t.columns[i][5]).isnumeric():
        form_c = int(MEI_t.columns[i][5])
        if desc_e.find('=') < 0:
            desc_e = desc_e.replace('of Index','of Index('+str(form_c)+')')
    elif str(MEI_t.columns[i][5]).find('Unnamed') >= 0 or str(MEI_t.columns[i][5]).find('nan') == 0  or str(MEI_t.columns[i][5]).strip() == '':
        form_c = ''
    else:
        form_c = MEI_t.columns[i][5]
        if desc_e.find('=') < 0:
            desc_e = desc_e.replace('of Index','of Index('+str(form_c)+')')
    #flags = MEI_t['Flags'][i]
    key_tmp= [databank, name, db_table, db_code, desc_e, desc_c, frequency, start, last, unit, name_ord, snl, book, form_e, form_c]
    KEY_DATA.append(key_tmp)
    snl += 1

    code_num += 1

    return code_num, table_num, DATA_BASE, db_table_t, DB_name, snl

###########################################################################  Main Function  ###########################################################################
if data_processing:
    c_list = list(country.index)
    c_list.sort()
    new_item_counts = 0

for dataset in dataset_list:
    if main_file.empty == False:
        break
    for coun in c_list:
        for frequency in frequency_list:
            logging.info('Getting data: dataset_name = '+dataset+', country = '+COUNTRY_NAME(coun)+', frequency = '+frequency+' Time: '+str(int(time.time() - tStart))+' s'+'\n')
            file_path = data_path+dataset+'/'+COUNTRY_NAME(coun).replace('/','.')+'_'+frequency+'.csv'
            if PRESENT(file_path):
                MEI_t = readFile(file_path, header_=list(range(6)), index_col_=0)
                if readFile(file_path).dropna().empty == False and MEI_t.empty == True:
                    MEI_t = readFile(file_path, index_col_=0)
                    MEI_t.columns = pd.MultiIndex.from_frame(MEI_t.iloc[list(range(6))].T)
                    if MEI_t.empty == True:
                        ERROR('DataFrame is not correctly read.')
                subjects = readFile(file_path.replace('.csv','_subjects.csv'), header_=[0], index_col_=0)
                measures = readFile(file_path.replace('.csv','_measures.csv'), header_=[0], index_col_=0)
            else:
                MEI_t, subjects, measures = createDataFrameFromOECD(countries = [coun], dsname = dataset, frequency = frequency, startDate = START_DATE(frequency))
                MEI_t.to_csv(file_path)
                subjects.to_csv(file_path.replace('.csv','_subjects.csv'))
                measures.to_csv(file_path.replace('.csv','_measures.csv'))
            #MEI_t = readFile(data_path+NAME+str(g)+'.csv', header_ = 0)
            subjects_list = {}
            unknown_subjects = []
            for s in range(subjects.shape[0]):
                if subjects['id'][s] not in list(subject_file.index):
                    unknown_subjects.append(subjects['id'][s])
                subjects_list[subjects['id'][s]] = subjects['name'][s]
            measures_list = {}
            unknown_measures = []
            for m in range(measures.shape[0]):
                if measures['id'][m] not in list(measure_file.index):
                    unknown_measures.append(measures['id'][m])
                measures_list[measures['id'][m]] = measures['name'][m]
            if not list(measures_list):
                measures_list[''] = ''
                measures_list['nan'] = ''
            nG = MEI_t.shape[1]
            if not not unknown_subjects or not not unknown_measures:
                #logging.info('unknown_subjects: '+str(unknown_subjects))
                #logging.info('unknown_measures: '+str(unknown_measures))
                if not not unknown_subjects:
                    unknown_subjects.sort()
                    logging.info('unknown_subjects found: '+str(len(unknown_subjects)))
                    for un in unknown_subjects:
                        for s in reversed(range(3, 6)):
                            if un[:3]+un[s]+un[-2:] not in list(subject_file['code2']):
                                subject_file.loc[un] = un[:3]+un[s]+un[-2:]
                                break
                        if un not in list(subject_file.index):
                            subjects.loc[subjects['id'].isin(unknown_subjects)].to_excel(out_path+"Unknown Subjects.xlsx", sheet_name='subjects')
                            ERROR('未知代碼無法自動調整，請改以手動調整')
                    subject_file = subject_file.sort_index()
                if not not unknown_measures:
                    unknown_measures.sort()
                    logging.info('unknown_measures found: '+str(len(unknown_measures)))
                    for un in unknown_measures:
                        for s in range(2, 4):
                            if un[:s] not in list(measure_file['code2']):
                                measure_file.loc[un] = un[:s]
                                break
                        if un not in list(measure_file.index):
                            measures.loc[measures['id'].isin(unknown_measures)].to_excel(out_path+"Unknown Measures.xlsx", sheet_name='measures')
                            ERROR('未知代碼無法自動調整，請改以手動調整')
                    measure_file = measure_file.sort_index()
                #ERROR('發現未知代碼，請於excel表上作調整')
            
            for i in range(nG):
                sys.stdout.write("\rLoading...("+str(int((i+1)*100/nG))+"%)*")
                sys.stdout.flush()
                
                name = frequency+str(COUNTRY_CODE(MEI_t.columns[i][0]))+str(SUBJECT_CODE(MEI_t.columns[i][1], subjects_list)).replace('_','')+str(MEASURE_CODE(MEI_t.columns[i][2], measures_list))+'.'+frequency.lower()
                if (name in DF_KEY.index and find_unknown == True) or (name not in DF_KEY.index and find_unknown == False):
                    continue
                elif name not in DF_KEY.index and find_unknown == True:
                    new_item_counts+=1

                code_num_dict[frequency], table_num_dict[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl = \
                  MEI_DATA(i, name, MEI_t, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl, FREQLIST[frequency], frequency)  
                 
            sys.stdout.write("\n\n")
            if find_unknown == True:
                logging.info('Total New Items Found: '+str(new_item_counts)+' Time: '+str(int(time.time() - tStart))+' s'+'\n') 

print('Time: '+str(int(time.time() - tStart))+' s'+'\n')
if data_processing:
    subject_file.to_excel(data_path+'MEI_Subjects.xlsx', sheet_name='MEI_Subjects')
    measure_file.to_excel(data_path+'MEI_Measures.xlsx', sheet_name='MEI_Measures')
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

logging.info(df_key)
#logging.info(DATA_BASE_t)

print('Time: '+str(int(time.time() - tStart))+' s'+'\n')
df_key.to_excel(out_path+NAME+"key"+excel_suffix+".xlsx", sheet_name=NAME+'key')
if os.path.isfile(out_path+"Unknown Subjects.xlsx"):
    os.remove(out_path+"Unknown Subjects.xlsx")
if os.path.isfile(out_path+"Unknown Measures.xlsx"):
    os.remove(out_path+"Unknown Measures.xlsx")
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

print('Time: '+str(int(time.time() - tStart))+' s'+'\n')
if updating == False:
    if find_unknown == True:
        checkNotFound = False
    else:
        checkNotFound = True
    unknown_list, toolong_list, update_list, unfound_list = MEI_identity(out_path, df_key, DF_KEY, checkNotFound=checkNotFound, checkDESC=True, tStart=tStart, start_year=dealing_start_year)