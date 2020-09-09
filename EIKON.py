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
#freq = 'D'
key_list = ['databank', 'name', 'old_name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'last', 'base', 'quote', 'snl', 'source', 'form_e', 'form_c']
merge_file = readExcelFile(out_path+'EIKON_key.xlsx', header_ = 0, sheet_name_='EIKON_key')
#dataset_list = ['QNA', 'QNA_DRCHIVE']
#frequency_list = ['A','Q']
frequency = 'D'
start_file = 1
last_file = 14
maximum = 10
#TO_EXCEL = True
update = '26/8/2020'#datetime.today()
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

Datatype = readFile(data_path+'Datatype.csv', header_ = 0)
Datatype = Datatype.set_index('Symbol').to_dict()
source_FromUSD = readFile(data_path+'sourceFROM.csv', header_ = 0)
source_ToUSD = readFile(data_path+'sourceTO.csv', header_ = 0)
source_USD = pd.concat([source_FromUSD, source_ToUSD], ignore_index=True)
source_USD = source_USD.set_index('Symbol').to_dict()
Currency = readFile(data_path+'Currency2.csv', header_ = 0)
Currency = Currency.set_index('Code').to_dict()

def CURRENCY_CODE(code):
    if code in Currency['Country_Code']:
        return str(Currency['Country_Code'][code]).rjust(3,'0')
    else:
        return 'not_exists'
def CURRENCY(code):
    if code in Currency['Name']:
        return str(Currency['Name'][code])
    else:
        ERROR('貨幣代碼錯誤: '+code)
"""
def SOURCE(code):
    if code in source_USD['Source']:
        return str(source_USD['Source'][code])
    else:
        ERROR('來源代碼錯誤: '+code)
"""

Day_list = pd.date_range(start = '1/1/1947', end = update).strftime('%Y-%m-%d').tolist()
Day_list.reverse()
nD = len(Day_list)
KEY_DATA = []
SORT_DATA_D = []
DATA_BASE_D = {}
db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
DB_name_D = []
DB_TABLE = 'DB_'
DB_CODE = 'data'

try:
    with open(out_path+'database_num.txt','r',encoding=ENCODING) as f:  #用with一次性完成open、close檔案
        database_num = int(f.read().replace('\n', ''))
except FileNotFoundError:
    if merge_file.empty == False:
        ERROR('找不到database_num.txt')
if merge_file.empty == False:
    print(merge_file)
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
    EIKON_t = readExcelFile(data_path+NAME+str(g)+'.xlsx', header_ = [0,1,2], sheet_name_= None)
    
    for sheet in EIKON_t:
        if CURRENCY_CODE(sheet) == 'not_exists':
            continue
        print('Reading sheet: '+CURRENCY(sheet)+' Time: ', int(time.time() - tStart),'s'+'\n')
        EIKON_t[sheet].set_index(EIKON_t[sheet].columns[0], inplace = True)
        nG = EIKON_t[sheet].shape[1]
            
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()
            
            if EIKON_t[sheet].columns[i][0] == '#ERROR':
                continue
            
            loc1 = str(EIKON_t[sheet].columns[i][1]).find('(')
            loc2 = str(EIKON_t[sheet].columns[i][1]).find(')')
            code = str(EIKON_t[sheet].columns[i][1])[:loc1]
            source = str(source_USD['Source'][code])
            if source != 'WM/Reuters':
                continue

            if code_num_D >= 200:
                DATA_BASE_D[db_table_D] = db_table_D_t
                DB_name_D.append(db_table_D)
                table_num_D += 1
                code_num_D = 1
                db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
            
            name = frequency+CURRENCY_CODE(sheet)+str(EIKON_t[sheet].columns[i][1])+'.d'
            old_name = str(EIKON_t[sheet].columns[i][1])
        
            value = list(EIKON_t[sheet][EIKON_t[sheet].columns[i]])
            index = EIKON_t[sheet][EIKON_t[sheet].columns[i]].index
            db_table_D = DB_TABLE+'D_'+str(table_num_D).rjust(4,'0')
            db_code_D = DB_CODE+str(code_num_D).rjust(3,'0')
            db_table_D_t[db_code_D] = ['' for tmp in range(nD)]
            head = 0
            last = str(index[0]).replace(' 00:00:00','')
            for k in range(len(value)):
                find = False
                for j in range(head, nD):
                    if db_table_D_t.index[j] == str(index[k]).replace(' 00:00:00',''):
                        find = True
                        db_table_D_t[db_code_D][db_table_D_t.index[j]] = value[k]
                        head = j+1
                        break
                if find == False:
                    ERROR(str(sheet)+' '+str(EIKON_t[sheet].columns[i]))        
            
            dtype = str(EIKON_t[sheet].columns[i][1])[loc1+1:loc2]
            form_e = str(Datatype['Name'][dtype])+', '+str(Datatype['Type'][dtype])
            desc_e = str(source_USD['Category'][code])+': '+str(source_USD['Full Name'][code]).replace('to', 'per', 1).replace('Tous', 'per US ').replace('To_us_$', 'per US dollar').replace('?', '$', 1)+', '+form_e+', '+'source from '+str(source_USD['Source'][code])
            start = str(source_USD['Start Date'][code])
            loc11 = start.find('/')
            loc12 = start.find('/',loc11+1)
            start_year = start[:loc11].rjust(4,'0')
            start_month = start[loc11+1:loc12].rjust(2,'0')
            start_day = start[loc12+1:].rjust(2,'0')
            start = start_year+'-'+start_month+'-'+start_day
            if str(source_USD['Full Name'][code]).find('USD /') >= 0 or str(source_USD['Full Name'][code]).find('USD/') >= 0 or str(source_USD['Full Name'][code]).find('US Dollar /') >= 0:
                if source_USD['From Currency'][code] == 'United States Dollar':
                    base = source_USD['From Currency'][code]
                    quote = source_USD['To Currency'][code]
                else:
                    base = source_USD['To Currency'][code]
                    quote = source_USD['From Currency'][code]
            elif str(source_USD['Full Name'][code]).find('/ USD') >= 0 or str(source_USD['Full Name'][code]).find('/USD') >= 0:
                if source_USD['From Currency'][code] == 'United States Dollar':
                    base = source_USD['To Currency'][code]
                    quote = source_USD['From Currency'][code]
                else:
                    base = source_USD['From Currency'][code]
                    quote = source_USD['To Currency'][code]
            else:
                base = source_USD['To Currency'][code]
                quote = source_USD['From Currency'][code]
            desc_c = ''
            freq = frequency
            
            if str(source_USD['Full Name'][code]).find('Butterfly') >= 0 or str(source_USD['Full Name'][code]).find('Reversal') >= 0:
                form_c = 'Options'
            elif str(source_USD['Full Name'][code]).find('Forecast') >= 0:
                form_c = 'Forecast'
            elif str(source_USD['Full Name'][code]).find('FX Volatility') >= 0:
                form_c = 'FX Volatility'
            elif str(source_USD['Full Name'][code]).find('Hourly') >= 0:
                form_c = 'Hourly Rate'
            elif str(source_USD['Full Name'][code]).find('Ptax') >= 0:
                form_c = 'Ptax Rate'    
            elif str(source_USD['Full Name'][code]).find('Forw') >= 0 or str(source_USD['Full Name'][code]).find('FW') >= 0 or str(source_USD['Full Name'][code]).find('MF') >= 0 or str(source_USD['Full Name'][code]).find('YF') >= 0 \
                or str(source_USD['Full Name'][code]).find('Week') >= 0 or str(source_USD['Full Name'][code]).find('Month') >= 0 or str(source_USD['Full Name'][code]).find('Year') >= 0 or str(source_USD['Full Name'][code]).find('Overnight') >= 0 \
                or str(source_USD['Full Name'][code]).find('Tomorrow Next') >= 0 or str(source_USD['Full Name'][code]).find('MONTH') >= 0:
                form_c = 'Forward'
            else:
                form_c = ''
            
            key_tmp= [databank, name, old_name, db_table_D, db_code_D, desc_e, desc_c, freq, start, last, base, quote, snl, source, form_e, form_c]
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
            if key[snl_pos] == SORT_DATA_D[i][1]:
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
    df_key, DATA_BASE, DB_name_D = CONCATE(df_key, DATA_BASE_D, DB_name_D, Day_list)
    df_key.to_excel(out_path+NAME+"key.xlsx", sheet_name=NAME+'key')
    DB_keys = sorted(DATA_BASE.keys())
    database_num = int(((len(DB_name_D)-1)/maximum))+1
    for d in range(1, database_num+1):
        with pd.ExcelWriter(out_path+NAME+"database_"+str(d)+".xlsx") as writer: # pylint: disable=abstract-class-instantiated
            print('Outputing file: '+NAME+"database_"+str(d))
            if maximum*d > len(DB_name_D):
                for db in range(maximum*(d-1), len(DB_name_D)):
                    sys.stdout.write("\rOutputing sheet: "+str(DB_name_D[db])+'  Time: '+str(int(time.time() - tStart))+'s')
                    sys.stdout.flush()
                    if DATA_BASE[DB_name_D[db]].empty == False:
                        DATA_BASE[DB_name_D[db]].to_excel(writer, sheet_name = DB_name_D[db])
                writer.save()
                sys.stdout.write("\n")
            else:
                for db in range(maximum*(d-1), maximum*d):
                    sys.stdout.write("\rOutputing sheet: "+str(DB_name_D[db])+'  Time: '+str(int(time.time() - tStart))+'s')
                    sys.stdout.flush()
                    if DATA_BASE[DB_name_D[db]].empty == False:
                        DATA_BASE[DB_name_D[db]].to_excel(writer, sheet_name = DB_name_D[db])
                writer.save()
                sys.stdout.write("\n")
    
    print('\ndatabase_num =', database_num)
    with open(out_path+'database_num.txt','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(str(database_num))
else:
    df_key.to_excel(out_path+NAME+"key.xlsx", sheet_name=NAME+'key')
    database_num = int(((len(DB_name_D)-1)/maximum))+1
    for d in range(1, database_num+1):
        with pd.ExcelWriter(out_path+NAME+"database_"+str(d)+".xlsx") as writer: # pylint: disable=abstract-class-instantiated
            print('Outputing file: '+NAME+"database_"+str(d))
            if maximum*d > len(DB_name_D):
                for db in range(maximum*(d-1), len(DB_name_D)):
                    sys.stdout.write("\rOutputing sheet: "+str(DB_name_D[db])+'  Time: '+str(int(time.time() - tStart))+'s')
                    sys.stdout.flush()
                    if DATA_BASE_D[DB_name_D[db]].empty == False:
                        DATA_BASE_D[DB_name_D[db]].to_excel(writer, sheet_name = DB_name_D[db])
                writer.save()
                sys.stdout.write("\n")
            else:
                for db in range(maximum*(d-1), maximum*d):
                    sys.stdout.write("\rOutputing sheet: "+str(DB_name_D[db])+'  Time: '+str(int(time.time() - tStart))+'s')
                    sys.stdout.flush()
                    if DATA_BASE_D[DB_name_D[db]].empty == False:
                        DATA_BASE_D[DB_name_D[db]].to_excel(writer, sheet_name = DB_name_D[db])
                writer.save()
                sys.stdout.write("\n")
    
    print('\ndatabase_num =', database_num)
    with open(out_path+'database_num.txt','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(str(database_num))

print('Time: ', int(time.time() - tStart),'s'+'\n')

def FREQUENCY(freq):
    if freq == 'D':
        return 'Daily'
    elif freq == 'W':
        return 'Weekly'
    elif freq == 'M':
        return 'Monthly'
    elif freq == 'Q':
        return 'Quarterly'
    elif freq == 'S':
        return 'Semiannual'
    elif freq == 'A':
        return 'Annual'

AREMOS = []
AREMOS_DATA = []
print('Outputing AREMOS files:'+'\n')
for key in range(df_key.shape[0]):
    sys.stdout.write("\rLoading...("+str(round((key+1)*100/df_key.shape[0], 1))+"%)*")
    sys.stdout.flush()
    SERIES = 'SERIES<FREQ '+FREQUENCY(frequency)+' >'+df_key.loc[key,'name']+'!'
    SERIES_DATA = 'SERIES<FREQ '+frequency+' PER '+str(date.fromisoformat(df_key.loc[key,'start']).year)+'D'+date.fromisoformat(df_key.loc[key,'start']).strftime('%j')+\
        ' TO '+str(date.fromisoformat(df_key.loc[key,'last']).year)+'D'+date.fromisoformat(df_key.loc[key,'last']).strftime('%j')+'>!'
    DESC = "'"+df_key.loc[key,'desc_e']+"'"+'!'
    DATA = df_key.loc[key,'name']+'='
    nA = DATA_BASE_D[df_key.loc[key,'db_table']].shape[0]
    found = False
    for ar in reversed(range(nA)):
        if str(DATA_BASE_D[df_key.loc[key,'db_table']].loc[DATA_BASE_D[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) != 'nan' and\
            str(DATA_BASE_D[df_key.loc[key,'db_table']].loc[DATA_BASE_D[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) != '':
            if found == True:
                DATA = DATA + ',' 
            DATA = DATA + str(DATA_BASE_D[df_key.loc[key,'db_table']].loc[DATA_BASE_D[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']])
            found = True
    end = ';'
    DATA = DATA + end
    #DATA = DATA.replace('"','')
    AREMOS.append(SERIES)
    AREMOS.append(DESC)
    AREMOS.append(end)
    AREMOS_DATA.append(SERIES_DATA)
    AREMOS_DATA.append(DATA)
sys.stdout.write("\n\n")

aremos = pd.DataFrame(AREMOS)
aremos_data = pd.DataFrame(AREMOS_DATA)
aremos.to_csv(out_path+NAME+"key.txt", header=False, index=False)
aremos_data.to_csv(out_path+NAME+"database.txt", header=False, index=False)

print('Time: ', int(time.time() - tStart),'s'+'\n')