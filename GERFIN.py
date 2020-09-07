# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date
#from cif_new import createDataFrameFromOECD
from EIKON_concat import CONCATE, readExcelFile

ENCODING = 'utf-8-sig'

NAME = 'GERFIN_'
data_path = './data2/'
out_path = "./output/"
databank = 'GERFIN'
#freq = 'D'
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'base', 'quote', 'snl', 'source', 'form_e', 'form_c']
merge_file = readExcelFile(out_path+'GERFIN_key.xlsx', header_ = 0, sheet_name_='GERFIN_key')
#dataset_list = ['QNA', 'QNA_DRCHIVE']
#frequency_list = ['A','Q']
frequency = 'D'
start_file = 1
last_file = 1
maximum = 10
update = '31/8/2020'#datetime.today()

# 回報錯誤、儲存錯誤檔案並結束程式
def ERROR(error_text):
    print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()

def readFile(dir, default=pd.DataFrame(), acceptNoFile=False, \
             header_=None,skiprows_=None,index_col_=None,skipfooter_=0,encoding_=ENCODING):
    try:
        t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
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

AREMOS_gerfin = readExcelFile(data_path+'AREMOS_gerfin.xlsx', header_ = [0], sheet_name_='AREMOS_gerfin')
"""
Datatype = readFile(data_path+'Datatype.csv', header_ = 0)
Datatype = Datatype.set_index('Symbol').to_dict()
source_FromUSD = readFile(data_path+'sourceFROM.csv', header_ = 0)
source_ToUSD = readFile(data_path+'sourceTO.csv', header_ = 0)
source_USD = pd.concat([source_FromUSD, source_ToUSD], ignore_index=True)
source_USD = source_USD.set_index('Symbol').to_dict()
Currency = readFile(data_path+'Currency.csv', header_ = 0)
Currency = Currency.set_index('Code').to_dict()

def CURRENCY(code):
    if code in Currency['Name']:
        return str(Currency['Name'][code])
    else:
        ERROR('貨幣代碼錯誤: '+code)

def SOURCE(code):
    if code in source_USD['Source']:
        return str(source_USD['Source'][code])
    else:
        ERROR('來源代碼錯誤: '+code)
"""

Day_list = pd.date_range(start = '1/1/1970', end = update).strftime('%Y-%m-%d').tolist()
Day_list.reverse()
nD = len(Day_list)
KEY_DATA = []
SORT_DATA_D = []
DATA_BASE_D = {}
db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
DB_name_D = []
DB_TABLE = 'DB_'
DB_CODE = 'data'
"""
try:
    with open(out_path+'database_num.txt','r',encoding=ENCODING) as f:  #用with一次性完成open、close檔案
        database_num = int(f.read().replace('\n', ''))
except FileNotFoundError:
    if merge_file.empty == False:
        ERROR('找不到database_num.txt')"""
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

#print(GERFIN_t.head(10))
tStart = time.time()

for g in range(start_file,last_file+1):
    print('Reading file: '+NAME+str(g)+' Time: ', int(time.time() - tStart),'s'+'\n')
    if g == 1:
        GERFIN_t = readFile(data_path+NAME+str(g)+'.csv', header_ = [0,1,2], index_col_=0, skiprows_=[0,4])
        if str(GERFIN_t.index[0]).find('/') >= 0:
            new_index = []
            for ind in GERFIN_t.index:
                new_index.append(pd.to_datetime(ind))
            GERFIN_t = GERFIN_t.reindex(new_index)
        
        nG = GERFIN_t.shape[1]
        print(GERFIN_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()
            
            if code_num_D >= 200:
                DATA_BASE_D[db_table_D] = db_table_D_t
                DB_name_D.append(db_table_D)
                table_num_D += 1
                code_num_D = 1
                db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
            
            AREMOS_key = AREMOS_gerfin.loc[AREMOS_gerfin['source'] == 'Official ECB & EUROSTAT Reference'].loc[AREMOS_gerfin['quote currency'] == str(GERFIN_t.columns[i][1])].to_dict('list')
            if pd.DataFrame(AREMOS_key).empty == True:
                continue
            name = str(AREMOS_key['code'][0])
            
            value = list(GERFIN_t[GERFIN_t.columns[i]])
            index = GERFIN_t[GERFIN_t.columns[i]].index
            db_table_D = DB_TABLE+'D_'+str(table_num_D).rjust(4,'0')
            db_code_D = DB_CODE+str(code_num_D).rjust(3,'0')
            db_table_D_t[db_code_D] = ['' for tmp in range(nD)]
            head = 0
            for k in range(len(value)):
                find = False
                for j in range(head, nD):
                    if db_table_D_t.index[j] == str(index[k]).replace(' 00:00:00',''):
                        find = True
                        db_table_D_t[db_code_D][db_table_D_t.index[j]] = value[k]
                        head = j+1
                        break
                if k == len(value)-1:
                    start = str(index[k]).replace(' 00:00:00','')
                if find == False:
                    ERROR(str(GERFIN_t.columns[i]))        
        
            desc_e = str(AREMOS_key['description'][0])
            base = str(AREMOS_key['base currency'][0])
            quote = str(AREMOS_key['quote currency'][0])
            desc_c = ''
            freq = frequency
            source = str(AREMOS_key['source'][0])
            form_e = str(AREMOS_key['attribute'][0])
            form_c = ''
            
            key_tmp= [databank, name, db_table_D, db_code_D, desc_e, desc_c, freq, start, base, quote, snl, source, form_e, form_c]
            KEY_DATA.append(key_tmp)
            sort_tmp_D = [name, snl, db_table_D, db_code_D]
            SORT_DATA_D.append(sort_tmp_D)
            snl += 1

            code_num_D += 1
    elif g == 2:
        GERFIN_t = readFile(data_path+NAME+str(g)+'.csv', header_ = [0,1,2], index_col_=0, skiprows_=[3,4], skipfooter_=1)
        if GERFIN_t.index[0] < GERFIN_t.index[1]:
            GERFIN_t = GERFIN_t[::-1]
        
        nG = GERFIN_t.shape[1]
        print(GERFIN_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()

            if str(GERFIN_t.columns[i][0]).find('FLAGS') >= 0:
                continue
            
            if code_num_D >= 200:
                DATA_BASE_D[db_table_D] = db_table_D_t
                DB_name_D.append(db_table_D)
                table_num_D += 1
                code_num_D = 1
                db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
            
            if g == 1:
                name = frequency+'_'+str(GERFIN_t.columns[i][0]).replace('D', '', 1).replace('.', '')+'.d'
            
                value = list(GERFIN_t[GERFIN_t.columns[i]])
                index = GERFIN_t[GERFIN_t.columns[i]].index
                db_table_D = DB_TABLE+'D_'+str(table_num_D).rjust(4,'0')
                db_code_D = DB_CODE+str(code_num_D).rjust(3,'0')
                db_table_D_t[db_code_D] = ['' for tmp in range(nD)]
                head = 0
                for k in range(len(value)):
                    find = False
                    for j in range(head, nD):
                        if db_table_D_t.index[j] == str(index[k]).replace(' 00:00:00',''):
                            find = True
                            db_table_D_t[db_code_D][db_table_D_t.index[j]] = value[k]
                            head = j+1
                            break
                    if k == len(value)-1:
                        start = str(index[k]).replace(' 00:00:00','')
                    if find == False:
                        ERROR(str(GERFIN_t.columns[i]))        
                
                #loc1 = str(GERFIN_t.columns[i][1]).find('(')
                #loc2 = str(GERFIN_t.columns[i][1]).find(')')
                #code = str(GERFIN_t.columns[i][1])[:loc1]
                #dtype = str(GERFIN_t.columns[i][1])[loc1+1:loc2]
                #form_e = str(Datatype['Name'][dtype])+', '+str(Datatype['Type'][dtype])
                if g == 1:
                    desc_e = 
                desc_e = str(source_USD['Category'][code])+': '+str(source_USD['Full Name'][code]).replace('to', 'per', 1).replace('Tous', 'per US ').replace('To_us_$', 'per US dollar').replace('?', '$', 1)+', '+form_e+', '+'source from '+str(source_USD['Source'][code])
                #start = source_USD['Start Date'][code]
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
                source = str(source_USD['Source'][code])
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
                
                key_tmp= [databank, name, db_table_D, db_code_D, desc_e, desc_c, freq, start, base, quote, snl, source, form_e, form_c]
                KEY_DATA.append(key_tmp)
                sort_tmp_D = [name, snl, db_table_D, db_code_D]
                SORT_DATA_D.append(sort_tmp_D)
                snl += 1

                code_num_D += 1
    elif g == 3:
        GERFIN_t = readExcelFile(data_path+NAME+str(g)+'.xls', header_ = [0,1], index_col_=0, sheet_name_='Daily')
        if GERFIN_t.index[0] < GERFIN_t.index[1]:
            GERFIN_t = GERFIN_t[::-1]
    
        nG = GERFIN_t.shape[1]
        print(GERFIN_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()

            if str(GERFIN_t.columns[i][0]).find('FLAGS') >= 0:
                continue
            
            if code_num_D >= 200:
                DATA_BASE_D[db_table_D] = db_table_D_t
                DB_name_D.append(db_table_D)
                table_num_D += 1
                code_num_D = 1
                db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
            
            if g == 1:
                name = frequency+'_'+str(GERFIN_t.columns[i][0]).replace('D', '', 1).replace('.', '')+'.d'
            
                value = list(GERFIN_t[GERFIN_t.columns[i]])
                index = GERFIN_t[GERFIN_t.columns[i]].index
                db_table_D = DB_TABLE+'D_'+str(table_num_D).rjust(4,'0')
                db_code_D = DB_CODE+str(code_num_D).rjust(3,'0')
                db_table_D_t[db_code_D] = ['' for tmp in range(nD)]
                head = 0
                for k in range(len(value)):
                    find = False
                    for j in range(head, nD):
                        if db_table_D_t.index[j] == str(index[k]).replace(' 00:00:00',''):
                            find = True
                            db_table_D_t[db_code_D][db_table_D_t.index[j]] = value[k]
                            head = j+1
                            break
                    if k == len(value)-1:
                        start = str(index[k]).replace(' 00:00:00','')
                    if find == False:
                        ERROR(str(GERFIN_t.columns[i]))        
                
                #loc1 = str(GERFIN_t.columns[i][1]).find('(')
                #loc2 = str(GERFIN_t.columns[i][1]).find(')')
                #code = str(GERFIN_t.columns[i][1])[:loc1]
                #dtype = str(GERFIN_t.columns[i][1])[loc1+1:loc2]
                #form_e = str(Datatype['Name'][dtype])+', '+str(Datatype['Type'][dtype])
                if g == 1:
                    desc_e = 
                desc_e = str(source_USD['Category'][code])+': '+str(source_USD['Full Name'][code]).replace('to', 'per', 1).replace('Tous', 'per US ').replace('To_us_$', 'per US dollar').replace('?', '$', 1)+', '+form_e+', '+'source from '+str(source_USD['Source'][code])
                #start = source_USD['Start Date'][code]
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
                source = str(source_USD['Source'][code])
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
                
                key_tmp= [databank, name, db_table_D, db_code_D, desc_e, desc_c, freq, start, base, quote, snl, source, form_e, form_c]
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
"""
print('Time: ', int(time.time() - tStart),'s'+'\n')