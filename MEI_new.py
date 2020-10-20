# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date
from cif_new import createDataFrameFromOECD
from MEI_concat import CONCATE, readExcelFile

ENCODING = 'utf-8-sig'

NAME = 'MEI_'
data_path = './data/'
out_path = "./output/"
databank = 'MEI'
specified_start_year = True
if specified_start_year == True:
    start_year = datetime.now().year - 10
    START_YEAR = '_'+str(start_year)
else:
    START_YEAR = ''
merge_file = pd.DataFrame()
#merge_file = readExcelFile(out_path+'MEI_key'+START_YEAR+'.xlsx', header_ = 0, sheet_name_='MEI_key')
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'last', 'unit', 'name_ord', 'snl', 'book', 'form_e', 'form_c']
dataset_list = ['MEI_CLI', 'MEI_BTS_COS', 'MEI']
frequency_list = ['A','Q','M']
for i in range(len(key_list)):
    if key_list[i] == 'snl':
        snl_pos = i
        break

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

def SUBJECT_CODE(code):
    if code in subject_file['code2']:
        return subject_file['code2'][code]
    else:
        ERROR('代碼錯誤: '+code)

def MEASURE_CODE(code):
    if code in measure_file['code2']:
        return measure_file['code2'][code]
    elif code == '':
        return ''
    else:
        ERROR('代碼錯誤: '+code)

def START_DATE(freq):
    if specified_start_year == True:
        if freq == 'A':
            return start_year
        elif freq == 'Q':
            return str(start_year)+'-Q1'
        elif freq == 'M':
            return str(start_year)+'-01'
        else:
            ERROR('頻率錯誤: '+freq)
    else:
        return None

this_year = datetime.now().year + 1
Year_list = [tmp for tmp in range(1947,this_year)]
Quarter_list = []
for q in range(1947,this_year):
    for r in range(1,5):
        Quarter_list.append(str(q)+'-Q'+str(r))
#print(Quarter_list)
Month_list = []
for y in range(1947,this_year):
    for m in range(1,13):
        Month_list.append(str(y)+'-'+str(m).rjust(2,'0'))
#print(Month_list)
nY = len(Year_list)
nQ = len(Quarter_list)
nM = len(Month_list)
KEY_DATA = []
SORT_DATA_A = []
SORT_DATA_Q = []
SORT_DATA_M = []
DATA_BASE_A = {}
DATA_BASE_Q = {}
DATA_BASE_M = {}
db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
DB_name_A = []
DB_name_Q = []
DB_name_M = []
DB_TABLE = 'DB_'
DB_CODE = 'data'

if merge_file.empty == False:
    print('Merge Old File\n')
    snl = int(merge_file['snl'][merge_file.shape[0]-1]+1)
    for a in range(1,10000):
        if DB_TABLE+'A_'+str(a).rjust(4,'0') not in list(merge_file['db_table']):
            table_num_A = a-1
            code_t = []
            for c in range(merge_file.shape[0]):
                if merge_file['db_table'][c] == DB_TABLE+'A_'+str(a-1).rjust(4,'0'):
                    code_t.append(merge_file['db_code'][c])
            for code in range(1,200):
                if max(code_t) == DB_CODE+str(code).rjust(3,'0'):
                    code_num_A = code+1
                    break
            break
    for q in range(1,10000):
        if DB_TABLE+'Q_'+str(q).rjust(4,'0') not in list(merge_file['db_table']):
            table_num_Q = q-1
            code_t = []
            for c in range(merge_file.shape[0]):
                if merge_file['db_table'][c] == DB_TABLE+'Q_'+str(q-1).rjust(4,'0'):
                    code_t.append(merge_file['db_code'][c])
            for code in range(1,200):
                if max(code_t) == DB_CODE+str(code).rjust(3,'0'):
                    code_num_Q = code+1
                    break
            break
    for m in range(1,10000):
        if DB_TABLE+'M_'+str(m).rjust(4,'0') not in list(merge_file['db_table']):
            table_num_M = m-1
            code_t = []
            for c in range(merge_file.shape[0]):
                if merge_file['db_table'][c] == DB_TABLE+'M_'+str(m-1).rjust(4,'0'):
                    code_t.append(merge_file['db_code'][c])
            for code in range(1,200):
                if max(code_t) == DB_CODE+str(code).rjust(3,'0'):
                    code_num_M = code+1
                    break
            break
else:
    table_num_A = 1
    table_num_Q = 1
    table_num_M = 1
    code_num_A = 1
    code_num_Q = 1
    code_num_M = 1
    snl = 1
if code_num_A == 200:
    code_num_A = 1
if code_num_Q == 200:
    code_num_Q = 1
if code_num_M == 200:
    code_num_M = 1
start_snl = snl
start_table_A = table_num_A
start_table_Q = table_num_Q
start_table_M = table_num_M
start_code_A = code_num_A
start_code_Q = code_num_Q
start_code_M = code_num_M

#print(MEI_t.head(10))

#for i in range(10):
#    print(MEI_t['TIME'][i], MEI_t['Value'][i])
tStart = time.time()
c_list = list(country.index)
c_list.sort()

for dataset in dataset_list:
    for coun in c_list:
        for frequency in frequency_list:
            print('Getting data: dataset_name = '+dataset+', country = '+COUNTRY_NAME(coun)+', frequency = '+frequency+' Time: ', int(time.time() - tStart),'s'+'\n')
            MEI_t, subjects, measures = createDataFrameFromOECD(countries = [coun], dsname = dataset, frequency = frequency, startDate = START_DATE(frequency))
            #MEI_t = readFile(data_path+NAME+str(g)+'.csv', header_ = 0)
            subjects_list = {}
            for s in range(subjects.shape[0]):
                subjects_list[subjects['id'][s]] = subjects['name'][s]
            measures_list = {}
            for m in range(measures.shape[0]):
                measures_list[measures['id'][m]] = measures['name'][m]
            if not list(measures_list):
                measures_list[''] = ''
            nG = MEI_t.shape[1]
            
            for i in range(nG):
                sys.stdout.write("\rLoading...("+str(int((i+1)*100/nG))+"%)*")
                sys.stdout.flush()
                
                if frequency == 'A':
                    if code_num_A >= 200:
                        DATA_BASE_A[db_table_A] = db_table_A_t
                        DB_name_A.append(db_table_A)
                        table_num_A += 1
                        code_num_A = 1
                        db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
                    
                    name = frequency+str(COUNTRY_CODE(MEI_t.columns[i][0]))+str(SUBJECT_CODE(MEI_t.columns[i][1])).replace('_','')+str(MEASURE_CODE(MEI_t.columns[i][2]))+'.a'
                
                    value = MEI_t[MEI_t.columns[i]]
                    db_table_A = DB_TABLE+'A_'+str(table_num_A).rjust(4,'0')
                    db_code_A = DB_CODE+str(code_num_A).rjust(3,'0')
                    db_table_A_t[db_code_A] = ['' for tmp in range(nY)]
                    for j in range(nY):
                        if db_table_A_t.index[j] == int(value.index[0]):
                            time_index = j
                            start_found = False
                            for k in range(value.shape[0]):
                                if start_found == False:
                                    if str(value[k]) != 'nan':
                                        start = int(value.index[k])
                                        start_found = True
                                db_table_A_t[db_code_A][db_table_A_t.index[time_index]] = value[k]
                                time_index += 1
                            for k in reversed(range(value.shape[0])):
                                if str(value[k]) != 'nan':
                                    last = int(value.index[k])
                                    break
                            break
                    
                    Subject = subjects_list[MEI_t.columns[i][1]]
                    Measure = measures_list[MEI_t.columns[i][2]]
                    PowerCode = MEI_t.columns[i][4]
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
                    
                    desc_c = ''
                    freq = frequency
                    unit = str(PowerCode) + ' of ' + str(Unit)
                    name_ord = MEI_t.columns[i][0]
                    book = COUNTRY_NAME(MEI_t.columns[i][0])
                    if MEI_t.columns[i][5] != '' and MEI_t.columns[i][5].find('=') < 0:
                        form_c = int(MEI_t.columns[i][5])
                    else:
                        form_c = MEI_t.columns[i][5]
                    #flags = MEI_t['Flags'][i]
                    key_tmp= [databank, name, db_table_A, db_code_A, desc_e, desc_c, freq, start, last, unit, name_ord, snl, book, form_e, form_c]
                    KEY_DATA.append(key_tmp)
                    sort_tmp_A = [name, snl, db_table_A, db_code_A]
                    SORT_DATA_A.append(sort_tmp_A)
                    snl += 1

                    code_num_A += 1
                elif frequency == 'Q':
                    if code_num_Q >= 200:
                        DATA_BASE_Q[db_table_Q] = db_table_Q_t
                        DB_name_Q.append(db_table_Q)
                        table_num_Q += 1
                        code_num_Q = 1
                        db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
                    
                    name = str(frequency)+str(COUNTRY_CODE(MEI_t.columns[i][0]))+str(SUBJECT_CODE(MEI_t.columns[i][1])).replace('_','')+str(MEASURE_CODE(MEI_t.columns[i][2]))+'.q'
                    
                    value = MEI_t[MEI_t.columns[i]]
                    db_table_Q = DB_TABLE+'Q_'+str(table_num_Q).rjust(4,'0')
                    db_code_Q = DB_CODE+str(code_num_Q).rjust(3,'0')
                    db_table_Q_t[db_code_Q] = ['' for tmp in range(nQ)]
                    for j in range(nQ):
                        if db_table_Q_t.index[j] == value.index[0]:
                            time_index = j
                            start_found = False
                            for k in range(value.shape[0]):
                                if start_found == False:
                                    if str(value[k]) != 'nan':
                                        start = value.index[k]
                                        start_found = True
                                db_table_Q_t[db_code_Q][db_table_Q_t.index[time_index]] = value[k]
                                time_index += 1
                            for k in reversed(range(value.shape[0])):
                                if str(value[k]) != 'nan':
                                    last = str(value.index[k])
                                    break
                            break
                    
                    Subject = subjects_list[MEI_t.columns[i][1]]
                    Measure = measures_list[MEI_t.columns[i][2]]
                    PowerCode = MEI_t.columns[i][4]
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
                    
                    desc_c = ''
                    freq = frequency
                    unit = str(PowerCode) + ' of ' + str(Unit)
                    name_ord = MEI_t.columns[i][0]
                    book = COUNTRY_NAME(MEI_t.columns[i][0])
                    if MEI_t.columns[i][5] != '' and MEI_t.columns[i][5].find('=') < 0:
                        form_c = int(MEI_t.columns[i][5])
                    else:
                        form_c = MEI_t.columns[i][5]
                    #flags = MEI_t['Flags'][i]
                    key_tmp= [databank, name, db_table_Q, db_code_Q, desc_e, desc_c, freq, start, last, unit, name_ord, snl, book, form_e, form_c]
                    KEY_DATA.append(key_tmp)
                    sort_tmp_Q = [name, snl, db_table_Q, db_code_Q]
                    SORT_DATA_Q.append(sort_tmp_Q)
                    snl += 1

                    code_num_Q += 1
                elif frequency == 'M':
                    if code_num_M >= 200:
                        DATA_BASE_M[db_table_M] = db_table_M_t
                        DB_name_M.append(db_table_M)
                        table_num_M += 1
                        code_num_M = 1
                        db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
                    
                    name = str(frequency)+str(COUNTRY_CODE(MEI_t.columns[i][0]))+str(SUBJECT_CODE(MEI_t.columns[i][1])).replace('_','')+str(MEASURE_CODE(MEI_t.columns[i][2]))+'.m'
                    
                    value = MEI_t[MEI_t.columns[i]]
                    db_table_M = DB_TABLE+'M_'+str(table_num_M).rjust(4,'0')
                    db_code_M = DB_CODE+str(code_num_M).rjust(3,'0')
                    db_table_M_t[db_code_M] = ['' for tmp in range(nM)]
                    for j in range(nM):
                        if db_table_M_t.index[j] == value.index[0]:
                            time_index = j
                            start_found = False
                            for k in range(value.shape[0]):
                                if start_found == False:
                                    if str(value[k]) != 'nan':
                                        start = value.index[k]
                                        start_found = True
                                db_table_M_t[db_code_M][db_table_M_t.index[time_index]] = value[k]
                                time_index += 1
                            for k in reversed(range(value.shape[0])):
                                if str(value[k]) != 'nan':
                                    last = str(value.index[k])
                                    break
                            break
                    
                    Subject = subjects_list[MEI_t.columns[i][1]]
                    Measure = measures_list[MEI_t.columns[i][2]]
                    PowerCode = MEI_t.columns[i][4]
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
                    
                    desc_c = ''
                    freq = frequency
                    unit = str(PowerCode) + ' of ' + str(Unit)
                    name_ord = MEI_t.columns[i][0]
                    book = COUNTRY_NAME(MEI_t.columns[i][0])
                    if MEI_t.columns[i][5] != '' and MEI_t.columns[i][5].find('=') < 0:
                        form_c = int(MEI_t.columns[i][5])
                    else:
                        form_c = MEI_t.columns[i][5]
                    #flags = MEI_t['Flags'][i]
                    key_tmp= [databank, name, db_table_M, db_code_M, desc_e, desc_c, freq, start, last, unit, name_ord, snl, book, form_e, form_c]
                    KEY_DATA.append(key_tmp)
                    sort_tmp_M = [name, snl, db_table_M, db_code_M]
                    SORT_DATA_M.append(sort_tmp_M)
                    snl += 1

                    code_num_M += 1
        
            sys.stdout.write("\n\n")

if db_table_A_t.empty == False:
    DATA_BASE_A[db_table_A] = db_table_A_t
    DB_name_A.append(db_table_A)
if db_table_Q_t.empty == False:
    DATA_BASE_Q[db_table_Q] = db_table_Q_t
    DB_name_Q.append(db_table_Q)
if db_table_M_t.empty == False:
    DATA_BASE_M[db_table_M] = db_table_M_t
    DB_name_M.append(db_table_M)

        

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
#if repeated_A > 0 or repeated_Q > 0 or repeated_M > 0:
print('Setting new files, Time: ', int(time.time() - tStart),'s'+'\n')

DATA_BASE_A_new = {}
DATA_BASE_Q_new = {}
DATA_BASE_M_new = {}
db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
DB_name_A_new = []
DB_name_Q_new = []
DB_name_M_new = []
db_table_new = 0
db_code_new = 0
for f in range(df_key.shape[0]):
    sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
    sys.stdout.flush()
    if df_key.iloc[f]['freq'] == 'A':
        if start_code_A >= 200:
            DATA_BASE_A_new[db_table_A] = db_table_A_t
            DB_name_A_new.append(db_table_A)
            start_table_A += 1
            start_code_A = 1
            db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
        db_table_A = DB_TABLE+'A_'+str(start_table_A).rjust(4,'0')
        db_code_A = DB_CODE+str(start_code_A).rjust(3,'0')
        db_table_A_t[db_code_A] = DATA_BASE_A[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_A
        df_key.loc[f, 'db_code'] = db_code_A
        start_code_A += 1
        db_table_new = db_table_A
        db_code_new = db_code_A
    elif df_key.iloc[f]['freq'] == 'Q':
        if start_code_Q >= 200:
            DATA_BASE_Q_new[db_table_Q] = db_table_Q_t
            DB_name_Q_new.append(db_table_Q)
            start_table_Q += 1
            start_code_Q = 1
            db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
        db_table_Q = DB_TABLE+'Q_'+str(start_table_Q).rjust(4,'0')
        db_code_Q = DB_CODE+str(start_code_Q).rjust(3,'0')
        db_table_Q_t[db_code_Q] = DATA_BASE_Q[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_Q
        df_key.loc[f, 'db_code'] = db_code_Q
        start_code_Q += 1
        db_table_new = db_table_Q
        db_code_new = db_code_Q
    elif df_key.iloc[f]['freq'] == 'M':
        if start_code_M >= 200:
            DATA_BASE_M_new[db_table_M] = db_table_M_t
            DB_name_M_new.append(db_table_M)
            start_table_M += 1
            start_code_M = 1
            db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
        db_table_M = DB_TABLE+'M_'+str(start_table_M).rjust(4,'0')
        db_code_M = DB_CODE+str(start_code_M).rjust(3,'0')
        db_table_M_t[db_code_M] = DATA_BASE_M[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_M
        df_key.loc[f, 'db_code'] = db_code_M
        start_code_M += 1
        db_table_new = db_table_M
        db_code_new = db_code_M
    
    if f == df_key.shape[0]-1:
        if db_table_A_t.empty == False:
            DATA_BASE_A_new[db_table_A] = db_table_A_t
            DB_name_A_new.append(db_table_A)
        if db_table_Q_t.empty == False:
            DATA_BASE_Q_new[db_table_Q] = db_table_Q_t
            DB_name_Q_new.append(db_table_Q)
        if db_table_M_t.empty == False:
            DATA_BASE_M_new[db_table_M] = db_table_M_t
            DB_name_M_new.append(db_table_M)
sys.stdout.write("\n")
DATA_BASE_A = DATA_BASE_A_new
DATA_BASE_Q = DATA_BASE_Q_new
DATA_BASE_M = DATA_BASE_M_new
DB_name_A = DB_name_A_new
DB_name_Q = DB_name_Q_new
DB_name_M = DB_name_M_new
  
print(df_key)
#print(DATA_BASE_t)

print('Time: ', int(time.time() - tStart),'s'+'\n')
if merge_file.empty == False:
    df_key, DATA_BASE = CONCATE(df_key, DATA_BASE_A, DATA_BASE_Q, DATA_BASE_M, DB_name_A, DB_name_Q, DB_name_M)
    df_key.to_excel(out_path+NAME+"key"+START_YEAR+".xlsx", sheet_name=NAME+'key')
    with pd.ExcelWriter(out_path+NAME+"database"+START_YEAR+".xlsx") as writer: # pylint: disable=abstract-class-instantiated
        endl = True
        for key in sorted(DATA_BASE.keys()):
            if key.find('DB_A') >= 0:
                sys.stdout.write("\rOutputing sheet: "+str(key))
                sys.stdout.flush()
            elif key.find('DB_M') >= 0:
                if endl == True:
                    sys.stdout.write("\n")
                    endl = False
                sys.stdout.write("\rOutputing sheet: "+str(key))
                sys.stdout.flush()
            elif key.find('DB_Q') >= 0:
                if endl == False:
                    sys.stdout.write("\n")
                    endl = True
                sys.stdout.write("\rOutputing sheet: "+str(key))
                sys.stdout.flush()
            DATA_BASE[key].to_excel(writer, sheet_name = key)
    sys.stdout.write("\n")
else:
    df_key.to_excel(out_path+NAME+"key"+START_YEAR+".xlsx", sheet_name=NAME+'key')
    with pd.ExcelWriter(out_path+NAME+"database"+START_YEAR+".xlsx") as writer: # pylint: disable=abstract-class-instantiated
        for d in DB_name_A:
            sys.stdout.write("\rOutputing sheet: "+str(d))
            sys.stdout.flush()
            if DATA_BASE_A[d].empty == False:
                DATA_BASE_A[d].to_excel(writer, sheet_name = d)
        sys.stdout.write("\n")
        for d in DB_name_Q:
            sys.stdout.write("\rOutputing sheet: "+str(d))
            sys.stdout.flush()
            if DATA_BASE_Q[d].empty == False:
                DATA_BASE_Q[d].to_excel(writer, sheet_name = d)
        sys.stdout.write("\n")
        for d in DB_name_M:
            sys.stdout.write("\rOutputing sheet: "+str(d))
            sys.stdout.flush()
            if DATA_BASE_M[d].empty == False:
                DATA_BASE_M[d].to_excel(writer, sheet_name = d)
    sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')