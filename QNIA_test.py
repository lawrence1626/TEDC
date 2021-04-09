# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time, numpy
import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import QNIA_concat as CCT
from QNIA_concat import ERROR, readExcelFile

ENCODING = 'utf-8-sig'
data_path = "./output/"
with open(data_path+'TOT_name.txt','r',encoding='ANSI') as f:
    DF_suffix = f.read()

if CCT.START_YEAR != "0":
    local = False
    checkDESC = True
else:
    local = True #bool(int(input('Check from local data (1/0): ')))
    checkDESC = bool(int(input('Check data description (1/0): ')))

def QNIA_identity(data_path, df_key, DF_KEY, checkNotFound=False, checkDESC=True, checkOnly='', checkIgnore=[]):
    
    tStart = time.time()

    print('Checking Identities: ', int(time.time() - tStart),'s'+'\n')
    df_key = df_key.set_index('name')
    
    unknown = 0
    unknown_list = []
    unknown_earliest = str(datetime.today().year)
    toolong = 0
    toolong_list = []
    toomanymonths = False
    includingweekly = False
    notsame = 0
    updated = 0
    update_list = []
    CHECK = ['desc_e', 'desc_c', 'freq', 'unit', 'name_ord', 'book', 'form_e', 'form_c']
    UPDATE = ['last']
    for ind in df_key.index:
        sys.stdout.write("\rChecking Index: "+ind+" ")
        sys.stdout.flush()
        if str(df_key.loc[ind, 'desc_e']).find(checkOnly) < 0:
            continue
        to_be_ignore = False
        for ignore in checkIgnore:
            if str(df_key.loc[ind, 'desc_e']).find(ignore) >= 0:
                to_be_ignore = True
                break
        if to_be_ignore == True:
            continue
        if len(ind) > 17:
            toolong_list.append(ind)
            toolong += 1
        if df_key.loc[ind, 'freq'] == 'M' and str(df_key.loc[ind, 'last']) !='Nan':
            if datetime.strptime(df_key.loc[ind, 'last'], '%Y-%m')-relativedelta(months=1000) > datetime.strptime(df_key.loc[ind, 'start'], '%Y-%m'):
                toomanymonths = True
        if df_key.loc[ind, 'freq'] == 'W':
            includingweekly = True
        if ind not in DF_KEY.index:
            #print('Index Unknown: '+ind)
            unknown_list.append([ind, df_key.loc[ind, 'start']])
            if str(df_key.loc[ind, 'start'])[:4] < unknown_earliest:
                unknown_earliest = str(df_key.loc[ind, 'start'])[:4]
            unknown += 1
        else:
            for check in CHECK:
                if str(df_key.loc[ind, check]).strip().lower() != str(DF_KEY.loc[ind, check]).strip().lower():
                    if check == 'start' and (str(DF_KEY.loc[ind, check]).strip() == 'Nan' or str(df_key.loc[ind, check]).strip() < str(DF_KEY.loc[ind, check]).strip()):
                        continue
                    elif str(DF_KEY.loc[ind, check]).strip() == 'nan' and str(df_key.loc[ind, check]).strip() == '':
                        continue
                    elif checkDESC == False and (check == 'desc_e' or check == 'desc_c' or check == 'form_e' or check == 'form_c'):
                        continue
                    print(check+' error')
                    if check == 'desc_e' and str(df_key.loc[ind, check]).replace(str(DF_KEY.loc[ind, check]), '') != str(df_key.loc[ind, check]):
                        print('df_key(not equal part) = '+str(df_key.loc[ind, check]).replace(str(DF_KEY.loc[ind, check]), ''))
                    else:
                        print('DF_KEY = '+str(DF_KEY.loc[ind, check]))
                        print('df_key = '+str(df_key.loc[ind, check]))
                    notsame += 1
            for update in UPDATE:
                if type(df_key.loc[ind, update]) != type(DF_KEY.loc[ind, update]) and df_key.loc[ind, update] != DF_KEY.loc[ind, update]:
                    if str(DF_KEY.loc[ind, update]).strip() == 'Nan' or ((type(DF_KEY.loc[ind, update]) == int or type(DF_KEY.loc[ind, update]) == numpy.int64) and (type(df_key.loc[ind, update]) == int or type(df_key.loc[ind, update]) == numpy.int64)):
                        continue
                    print('DF_KEY = '+str(DF_KEY.loc[ind, update])+', type =', type(DF_KEY.loc[ind, update]))
                    print('df_key = '+str(df_key.loc[ind, update])+', type =', type(df_key.loc[ind, update]))
                    print('Incorrect Time Type')
                    continue
                if df_key.loc[ind, update] != DF_KEY.loc[ind, update] and str(DF_KEY.loc[ind, update]).strip() == 'Nan':
                    continue
                if df_key.loc[ind, update] > DF_KEY.loc[ind, update]:
                    update_list.append(ind)
                    updated += 1
                elif df_key.loc[ind, update] < DF_KEY.loc[ind, update]:
                    print('The program did not fetch the latest data for: '+ind)  
    sys.stdout.write("\n")
    print('unknown: ', unknown)
    if unknown != 0:
        print('unknown earliest: ', unknown_earliest)
    print('nametoolong: ', toolong)
    print('notsame: ', notsame)
    print('updated: ', updated)
    print('includingweekly: ', str(includingweekly))
    print('toomanymonths: ', str(toomanymonths))

    unfound = 0
    unfound_list = []
    if checkNotFound == True:
        for ind in DF_KEY.index:
            sys.stdout.write("\rChecking Index: "+ind+" ")
            sys.stdout.flush()
            if ind not in df_key.index:
                #print('Index Not Found: '+ind)
                unfound_list.append(ind)
                unfound += 1
        sys.stdout.write("\n")
        print('unfound: ', unfound)
    
    return unknown_list, toolong_list, update_list, unfound_list

if local == True:
    main_suf = input('Main data suffix: ')
    print('Reading file: QNIA_key'+main_suf+'\n')
    df_key = readExcelFile(data_path+'QNIA_key'+main_suf+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='QNIA_key')
    print('Reading TOT file: QNIA_key'+DF_suffix+'\n')
    DF_KEY = readExcelFile(data_path+'QNIA_key'+DF_suffix+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='QNIA_key')
    DF_KEY = DF_KEY.set_index('name') 
    unknown_list, toolong_list, update_list, unfound_list = QNIA_identity(data_path, df_key, DF_KEY, checkDESC=checkDESC)
