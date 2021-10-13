# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time, csv
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, date

ENCODING = 'utf-8-sig'
data_path = './output/'
out_path = './output/'
BANKS = ['QNIA','MEI','GERFIN','EIKON','FOREX','US','INTLINE','ASIA']

def SPECIAL(special_text):
    print('\n= ! = '+special_text+'\n\n')
    #with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
    #    f.write(special_text)
    sys.exit()
def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=False, \
             header_=None,skiprows_=None,index_col_=None,sheet_name_=None):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,index_col=index_col_,skiprows=skiprows_)
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            SPECIAL('Several files input')
    except:
        try: #檔案編碼格式不同
            t = pd.read_excel(dir, header=header_,skiprows=skiprows_,index_col=index_col_,sheet_name=sheet_name_)
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了

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

NAME = input("Bank: ")
if NAME not in BANKS:
    SPECIAL('Unknown Bank: '+NAME)
if NAME == 'EIKON':
    data_path = Path(os.path.realpath(data_path)).as_posix().replace('TO_AREMOS','GERFIN')+'/'
elif NAME == 'ASIA':
    data_path = Path(os.path.realpath(data_path)).as_posix().replace('TO_AREMOS','INTLINE')+'/'
else:
    data_path = Path(os.path.realpath(data_path)).as_posix().replace('TO_AREMOS',NAME)+'/'
data_suffix = input("Database suffix: ")
BOOL = {'T':True, 'F':False}
if NAME == 'EIKON' or NAME == 'GERFIN':
    START_YEAR = int(input("The .bnk file start from year: "))

make_doc = BOOL[input("\nMaking Document(T/F): ")]
doc_done = False
if make_doc == False:
    doc_done = True

tStart = time.time()
end = ';'
print('Reading file: '+NAME+'_key'+data_suffix+', Time: ', int(time.time() - tStart),'s'+'\n')
df_key = readExcelFile(data_path+NAME+'_key'+data_suffix+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'_key')   
try:
    with open(data_path+NAME+'_database_num'+data_suffix+'.txt','r',encoding=ENCODING) as f:  #用with一次性完成open、close檔案
        database_num = int(f.read().replace('\n', ''))
    DATA_BASE_t = {}
    for i in range(1,database_num+1):
        print('Reading file: '+NAME+'_database_'+str(i)+data_suffix+', Time: ', int(time.time() - tStart),'s'+'\n')
        DB_t = readExcelFile(data_path+NAME+'_database_'+str(i)+data_suffix+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False, sheet_name_=None)
        for d in DB_t.keys():
            DATA_BASE_t[d] = DB_t[d]
except:
    print('Reading file: '+NAME+'_database'+data_suffix+', Time: ', int(time.time() - tStart),'s'+'\n')
    DATA_BASE_t = readExcelFile(data_path+NAME+'_database'+data_suffix+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)

continue_making_data = False
while True:
    if doc_done == True:
        make_doc = False
        if continue_making_data == False:
            part_file = BOOL[input("\nDealing among specified year range only or processing long time series of daily data(T/F): ")]
        else:
            part_file = True
        if NAME == 'GERFIN' or NAME == 'EIKON':
            IncludeDaily = True
            make_daily = True
        elif NAME == 'INTLINE' or NAME == 'ASIA':
            make_daily = BOOL[input("\nDealing with Daily Data(T/F): ")]
            if make_daily:
                IncludeDaily = True
            else:
                IncludeDaily = False
        else:
            IncludeDaily = False
            make_daily = False
        #IncludeDaily = BOOL[input("\nDoes this bank include daily data?(T/F): ")]
        #SUFFIX = ''
        if NAME == 'FOREX' or NAME == 'US' or NAME == 'INTLINE' or NAME == 'ASIA':
            make_week = BOOL[input("\nDealing with Weekly Data(T/F): ")]
        else:
            make_week = False
        if part_file == True:
            from_year = input("\nFrom Year: ")
            latest = BOOL[input("\nTo the latest year(T/F): ")]
            if latest == False:
                to_year = input("\nTo Year: ")
            else:
                to_year = str(date.today().year)
        else:
            from_year = ''
            to_year = str(date.today().year)
        print('\n')
    
        print('Outputing AREMOS files, Time: ', int(time.time() - tStart),'s'+'\n')
        if part_file == True and IncludeDaily == True:
            Daily_suffix = 'D'
            if latest == True:
                part_to_year = str(date.today().year) 
            else:
                part_to_year = to_year
            part_from_year = str(int(part_to_year)-2)
            if part_from_year < from_year:
                part_from_year = from_year
        else:
            Daily_suffix = ''
            part_to_year = to_year
            part_from_year = from_year
    
        while part_from_year >= from_year:
            AREMOS_DATA = []
            if part_file == False:
                print('From the earliest year to the latest data year', '\n')
            else:
                if latest == True:
                    print('From year',part_from_year,'to the latest data year', '\n')
                else:
                    print('From year',part_from_year,'to year',part_to_year, '\n')
            for key in range(df_key.shape[0]):
                sys.stdout.write("\rLoading...("+str(round((key+1)*100/df_key.shape[0], 1))+"%)*")
                sys.stdout.flush()

                if df_key.loc[key,'start'] == 'Nan':
                    continue
                freq = df_key.loc[key,'freq']
                freq2 = freq
                if freq == 'A':
                    freq2 = ''
                elif freq == 'W':
                    if make_week == False:
                        continue
                    freq2 = 'D'
                    if type(DATA_BASE_t[df_key.loc[key,'db_table']].index[0]) != str:
                        DATA_BASE_t[df_key.loc[key,'db_table']].index = DATA_BASE_t[df_key.loc[key,'db_table']].index.strftime('%Y-%m-%d')
                elif freq == 'D':
                    if make_daily == False:
                        continue
                
                DATA = df_key.loc[key,'name']+'='
                nA = DATA_BASE_t[df_key.loc[key,'db_table']].shape[0]
                if DATA_BASE_t[df_key.loc[key,'db_table']].index[0] > DATA_BASE_t[df_key.loc[key,'db_table']].index[1]:
                    array = reversed(range(nA))
                else:
                    array = range(nA)
                
                if part_file == True:
                    if freq == 'A':
                        part_from_date = int(part_from_year)
                        part_to_date = int(part_to_year)
                        suffix = ''
                        start_suffix = ''
                        latest_suffix = ''
                    elif freq == 'S':
                        part_from_date = part_from_year+'-S1'
                        part_to_date = part_to_year+'-S1'
                        suffix = '1'
                        start_suffix = str(df_key.loc[key,'start'])[-1:]
                        latest_suffix = str(df_key.loc[key,'last'])[-1:]
                    elif freq == 'Q':
                        part_from_date = part_from_year+'-Q1'
                        part_to_date = part_to_year+'-Q1'
                        suffix = '1'
                        start_suffix = str(df_key.loc[key,'start'])[-1:]
                        latest_suffix = str(df_key.loc[key,'last'])[-1:]
                    elif freq == 'M':
                        part_from_date = part_from_year+'-01'
                        part_to_date = part_to_year+'-01'
                        suffix = '01'
                        start_suffix = str(df_key.loc[key,'start'])[-2:]
                        latest_suffix = str(df_key.loc[key,'last'])[-2:]
                    elif freq == 'D':
                        part_from_date = part_from_year+'-01-01'
                        part_to_date = part_to_year+'-01-01'
                        suffix = '001'
                        start_suffix = date.fromisoformat(df_key.loc[key,'start']).strftime('%j')
                        latest_suffix = date.fromisoformat(df_key.loc[key,'last']).strftime('%j')
                    elif freq == 'W':
                        for i in range(1,8):
                            if date(int(part_from_year), 1, i).weekday() == 5:
                                part_from_date = date(int(part_from_year), 1, i).strftime('%Y-%m-%d')
                            if date(int(part_to_year), 1, i).weekday() == 5:
                                part_to_date = date(int(part_to_year), 1, i).strftime('%Y-%m-%d')
                    if part_from_date <= df_key.loc[key,'last']:
                        if latest == True and part_to_year == to_year:
                            if df_key.loc[key,'start'] <= part_from_date:
                                date_from = part_from_date
                                date_to = df_key.loc[key,'last']
                                if freq == 'W':
                                    SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+date.fromisoformat(part_from_date).strftime('%Y:%m:%d').replace(':0',':')+' TO '+date.fromisoformat(df_key.loc[key,'last']).strftime('%Y:%m:%d').replace(':0',':')+'>!'
                                else:
                                    SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+str(part_from_year)+freq2+suffix+' TO '+str(df_key.loc[key,'last'])[:4]+freq2+latest_suffix+'>!'
                            elif df_key.loc[key,'start'] > part_from_date:
                                date_from = df_key.loc[key,'start']
                                date_to = df_key.loc[key,'last']
                                if freq == 'W':
                                    SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+date.fromisoformat(df_key.loc[key,'start']).strftime('%Y:%m:%d').replace(':0',':')+' TO '+date.fromisoformat(df_key.loc[key,'last']).strftime('%Y:%m:%d').replace(':0',':')+'>!'
                                else:
                                    SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+str(df_key.loc[key,'start'])[:4]+freq2+start_suffix+' TO '+str(df_key.loc[key,'last'])[:4]+freq2+latest_suffix+'>!'
                        else:
                            if df_key.loc[key,'start'] <= part_to_date and df_key.loc[key,'start'] <= part_from_date:
                                date_from = part_from_date
                                date_to = part_to_date
                                if freq == 'W':
                                    SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+date.fromisoformat(part_from_date).strftime('%Y:%m:%d').replace(':0',':')+' TO '+date.fromisoformat(part_to_date).strftime('%Y:%m:%d').replace(':0',':')+'>!'
                                else:
                                    SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+str(part_from_year)+freq2+suffix+' TO '+str(part_to_year)+freq2+suffix+'>!'
                            elif df_key.loc[key,'start'] <= part_to_date and df_key.loc[key,'start'] > part_from_date:
                                if freq == 'D':
                                    date_from = part_from_date
                                else:
                                    date_from = df_key.loc[key,'start']
                                date_to = part_to_date
                                if freq == 'W':
                                    SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+date.fromisoformat(df_key.loc[key,'start']).strftime('%Y:%m:%d').replace(':0',':')+' TO '+date.fromisoformat(part_to_date).strftime('%Y:%m:%d').replace(':0',':')+'>!'
                                elif freq == 'D':
                                    SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+str(part_from_year)+freq2+suffix+' TO '+str(part_to_year)+freq2+suffix+'>!'
                                else:
                                    SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+str(df_key.loc[key,'start'])[:4]+freq2+start_suffix+' TO '+str(part_to_year)+freq2+suffix+'>!'
                            else:
                                continue
                        found = False
                        for ar in array:
                            if DATA_BASE_t[df_key.loc[key,'db_table']].index[ar] >= date_from and DATA_BASE_t[df_key.loc[key,'db_table']].index[ar] <= date_to:
                                if found == True:
                                    DATA = DATA + ',' 
                                if str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) == 'nan' or\
                                    str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) == '':
                                    DATA = DATA + 'M'
                                else:
                                    if str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]).find('e-') >= 0:
                                        loc1 = str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]).find('e-')
                                        significand = str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']])[:loc1]
                                        power = int(str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']])[-2:])
                                        number = significand.replace('.','').rjust(len(significand)+power-2,'0')
                                        number = '0.'+number
                                        DATA = DATA + number
                                    else:
                                        DATA = DATA + str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']])
                                found = True
                    else:
                        continue
                else:
                    if freq == 'D':
                        SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+str(date.fromisoformat(df_key.loc[key,'start']).year)+freq2+date.fromisoformat(df_key.loc[key,'start']).strftime('%j')+\
                            ' TO '+str(date.fromisoformat(df_key.loc[key,'last']).year)+freq2+date.fromisoformat(df_key.loc[key,'last']).strftime('%j')+'>!'
                    elif freq == 'W':
                        SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+date.fromisoformat(df_key.loc[key,'start']).strftime('%Y:%m:%d').replace(':0',':')+\
                            ' TO '+date.fromisoformat(df_key.loc[key,'last']).strftime('%Y:%m:%d').replace(':0',':')+'>!'
                    elif freq == 'M':
                        SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+str(df_key.loc[key,'start'])[:4]+freq2+str(df_key.loc[key,'start'])[-2:]+\
                            ' TO '+str(df_key.loc[key,'last'])[:4]+freq2+str(df_key.loc[key,'last'])[-2:]+'>!'
                    elif freq == 'Q':
                        SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+str(df_key.loc[key,'start'])[:4]+freq2+str(df_key.loc[key,'start'])[-1:]+\
                            ' TO '+str(df_key.loc[key,'last'])[:4]+freq2+str(df_key.loc[key,'last'])[-1:]+'>!'
                    elif freq == 'S':
                        SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+str(df_key.loc[key,'start'])[:4]+freq2+str(df_key.loc[key,'start'])[-1:]+\
                            ' TO '+str(df_key.loc[key,'last'])[:4]+freq2+str(df_key.loc[key,'last'])[-1:]+'>!'
                    elif freq == 'A':
                        SERIES_DATA = 'SERIES<FREQ '+freq+' PER '+str(df_key.loc[key,'start'])[:4]+freq2+\
                            ' TO '+str(df_key.loc[key,'last'])[:4]+freq2+'>!'
                    found = False
                    for ar in array:
                        if DATA_BASE_t[df_key.loc[key,'db_table']].index[ar] >= df_key.loc[key,'start'] and DATA_BASE_t[df_key.loc[key,'db_table']].index[ar] <= df_key.loc[key,'last']:
                            if found == True:
                                DATA = DATA + ',' 
                            if str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) == 'nan' or\
                                str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]) == '':
                                DATA = DATA + 'M'
                            else:
                                if str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]).find('e-') >= 0:
                                    loc1 = str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']]).find('e-')
                                    significand = str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']])[:loc1]
                                    power = int(str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']])[-2:])
                                    number = significand.replace('.','').rjust(len(significand)+power-2,'0')
                                    number = '0.'+number
                                    DATA = DATA + number
                                else:
                                    DATA = DATA + str(DATA_BASE_t[df_key.loc[key,'db_table']].loc[DATA_BASE_t[df_key.loc[key,'db_table']].index[ar], df_key.loc[key,'db_code']])
                            found = True
                
                DATA = DATA + end
                #DATA = DATA.replace('"','')
                AREMOS_DATA.append(SERIES_DATA)
                AREMOS_DATA.append(DATA)
            sys.stdout.write("\n\n")

            aremos_data = pd.DataFrame(AREMOS_DATA)
            #aremos_data.to_csv(out_path+NAME+"data.txt", header=False, index=False, sep='|', quoting=csv.QUOTE_NONE, quotechar='')
            year_suffix = part_from_year[-2:]
            if str(part_from_year).isnumeric() and datetime.today().year - int(part_from_year) >= 100:
                year_suffix = part_from_year
            aremos_data.to_csv(out_path+NAME+"_data"+data_suffix+Daily_suffix+year_suffix+".txt", header=False, index=False, sep='|', quoting=csv.QUOTE_NONE, quotechar='') #

            print('Time: ', int(time.time() - tStart),'s'+'\n')
            if part_from_year == from_year:
                break
            part_to_year = part_from_year
            part_from_year = str(int(part_from_year)-2)
            if part_from_year < from_year:
                part_from_year = from_year
    else:
        print('Outputing AREMOS document, Time: ', int(time.time() - tStart),'s'+'\n')
        AREMOS = []
        num = 0
        for key in range(df_key.shape[0]):
            sys.stdout.write("\rLoading...("+str(round((key+1)*100/df_key.shape[0], 1))+"%)*")
            sys.stdout.flush()

            if df_key.loc[key,'start'] == 'Nan':
                continue
            if NAME == 'EIKON' or NAME == 'GERFIN':
                if datetime.today().year - START_YEAR > 20 and (str(df_key.loc[key,'start'])[:4] >= str(START_YEAR+10) or str(df_key.loc[key,'last'])[:4] < str(START_YEAR)):
                    continue
                elif str(df_key.loc[key,'last'])[:4] < str(START_YEAR):
                    continue
            if (NAME == 'INTLINE' or NAME == 'ASIA') and df_key.loc[key,'freq'] == 'D':
                continue
            SERIES = 'SERIES<FREQ '+FREQUENCY(df_key.loc[key,'freq'])+' >'+df_key.loc[key,'name']+'!'
            DESC = "'"+str(df_key.loc[key,'desc_e']).replace("'",'"').replace('#','')+"'"+'!'
            AREMOS.append(SERIES)
            AREMOS.append(DESC)
            AREMOS.append(end)
            """if NAME == 'EIKON' and int(key/600) > num:
                aremos = pd.DataFrame(AREMOS)
                aremos.to_csv(out_path+NAME+"_doc"+data_suffix+str(num)+".txt", header=False, index=False, sep='|', quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\') #
                AREMOS = []
                num += 1"""
        
        aremos = pd.DataFrame(AREMOS)
        aremos.to_csv(out_path+NAME+"_doc"+data_suffix+".txt", header=False, index=False, sep='|', quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\') #
        doc_done = True
  
    if make_doc == False:
        continue_making_data = BOOL[input("\nDealing among other year range?(T/F): ")]
        if continue_making_data == True:
            continue
        else:
            break