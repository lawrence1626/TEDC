# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=E1101
import math, sys, calendar, os, copy, time, shutil
import regex as re
import pandas as pd
import numpy as np
import quandl as qd
import requests as rq
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
import webdriver_manager
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from urllib.error import HTTPError
#from US_concat import CONCATE, readExcelFile

ENCODING = 'utf-8-sig'
data_path = "./data/"
out_path = "./output/"
excel_suffix = input('Output file suffix (If test identity press 0): ')

def takeFirst(alist):
    return alist[0]

# 回報錯誤、儲存錯誤檔案並結束程式
def ERROR(error_text, waiting=False):
    if waiting == True:
        sys.stdout.write("\r"+error_text)
        sys.stdout.flush()
    else:
        print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()

def readFile(dir, default=pd.DataFrame(), acceptNoFile=False,header_=None,names_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,nrows_=None,encoding_=ENCODING,engine_='python',sep_=None, wait=False):
    try:
        t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        names=names_,usecols=usecols_,nrows=nrows_,encoding=encoding_,engine=engine_,sep=sep_)
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            if wait == True:
                ERROR('Waiting for Download...', waiting=True)
            else:
                ERROR('找不到檔案：'+dir)
    except HTTPError as err:
        if acceptNoFile:
            return default
        else:
            ERROR(str(err))
    except:
        try: #檔案編碼格式不同
            t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        names=names_,usecols=usecols_,nrows=nrows_,encoding=encoding_,engine=engine_,sep=sep_)
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, na_filter_=True, \
             header_=None,names_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,nrows_=None,sheet_name_=None, wait=False):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,names=names_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_,nrows=nrows_,na_filter=na_filter_)
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            if wait == True:
                ERROR('Waiting for Download...', waiting=True)
            else:
                ERROR('找不到檔案：'+dir)
    except:
        try: #檔案編碼格式不同
            t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,names=names_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_,nrows=nrows_,na_filter=na_filter_)
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

def NEW_LABEL(key, label, Series, Table, cat_idx=None, item=None):
    normal = ['li/', 'ce/', 'pr/', 'jt/']

    if key in normal:
        for l in range(label.shape[0]):
            label.loc[label.index[l]] = Series['CATEGORIES'].loc[Table[cat_idx+'_code'][label.index[l]], cat_idx+'_'+item].title().replace('And','and').replace("'S","'s").replace(', ',',')
    else:
        if key == 'ec/': 
            for l in range(label.shape[0]):
                label.loc[label.index[l]] = (Series['BASE'].loc[Table['ownership_code'][label.index[l]], 'ownership_name']+', '+\
                Series['CATEGORIES'].loc[Table[cat_idx+'_code'][label.index[l]], cat_idx+'_'+item].replace(', ',',')).title().replace('And','and').replace("'S","'s").replace('Sic','SIC').replace('Nec','NEC')
        elif key == 'mp/':
            for l in range(label.shape[0]):
                label.loc[label.index[l]] = re.sub(r'R&D', "R and D", (Series['CATEGORIES'].loc[Table[cat_idx+'_code'][label.index[l]], cat_idx.upper()+'_'+item.upper()]+\
                ' for '+Series['DATA TYPE'].loc[Table['sector_code'][label.index[l]], 'SECTOR_NAME']).replace(', ',','))
        elif key == 'bd/':
            for l in range(label.shape[0]):
                label.loc[label.index[l]] = Series['CATEGORIES']['industry'].loc[Table[cat_idx+'_code'][label.index[l]], cat_idx+'_'+item].title().replace('And','and').replace("'S","'s").replace(', ',',')
        elif key == 'in/':
            for l in range(label.shape[0]):
                label.loc[label.index[l]] = re.sub(r'\s+', " ", re.sub(r'\(.+\)', "", re.sub(r'US ', "U.S. ", re.sub(r',.+SEASONALLY ADJUSTED', "", re.sub(r'NATL', "NATIONAL", re.sub(r'HR', "HOURLY", re.sub(r'MANUFACTURING ', "", \
                    Series['CATEGORIES'].loc[Table[cat_idx+'_code'][label.index[l]], cat_idx+'_'+item]))))))).title().replace('And','and').replace('As ','as ').replace('Cpi ', 'CPI ').replace('For ','for ').replace('Gdp ', 'GDP ').replace('Of ', 'of ').replace('Per ','per ').replace("'S","'s").replace(', ',',  ')
        elif key == 'ml/':
            for l in range(label.shape[0]):
                label.loc[label.index[l]] = re.sub(r'(Total)\s*,\s*', r"\1 ", re.sub(r'\s*\(.+?\)\s*$', "", Series['CATEGORIES'].loc[Table[cat_idx+'_code'][label.index[l]], cat_idx+'_'+item].title().replace('And','and').replace("'S","'s").replace(', ',',')))
        elif key == 'H6/' or key == '19/':
            for l in range(label.shape[0]):
                label.loc[label.index[l]] = label.loc[label.index[l]].title().replace('Monetary Base; ', '').replace('Us','US').replace('Ira','IRA').replace('And','and').replace('To ', 'to ').replace('Of ', 'of ').replace("'S","'s").replace(',',', ').replace(';',', ')
        elif key == '17/':
            for l in range(label.shape[0]):
                label.loc[label.index[l]] = re.sub(r"[;\sNn\.]*[Ss]\.[Aa]\.[\sA-Z,]*|'", "", label.loc[label.index[l]]).title().replace('Naics','NAICS').replace('Sic','SIC').replace('Pt','pt').replace('And','and').replace('Of ', 'of ').replace("'S","'s").replace(', ',',')
        elif key == '15/':
            for l in range(label.shape[0]):
                label.loc[label.index[l]] = re.sub(r'\s+', " ", label.loc[label.index[l]]).title().replace('Aa','AA').replace('And','and').replace('To ', 'to ').replace('Of ', 'of ').replace("'S","'s").replace("^","").replace(',',', ')
    
    return label

def EXCHANGE(address, Series, label, Display={}, Sort={}):
    for name in Display:
        try:
            Series.loc[Series.loc[Series[label] == name].index, 'display_level'] = Series.loc[Series.loc[Series[label] == Display[name]].index, 'display_level'].item()
        except ValueError:
            ERROR(address+': '+label+' level could not be revised.')
    for name in Sort:
        try:
            Series.loc[Series.loc[Series[label] == name].index, 'sort_sequence'] = Series.loc[Series.loc[Series[label] == Sort[name][0]].index, 'sort_sequence'].item()+Sort[name][1]
        except ValueError:
            ERROR(address+': '+label+' sequence could not be revised.')
    
    return Series

def US_WEBDRIVER(chrome, address, fname, sname, header=None, index_col=None, skiprows=None, usecols=None, names=None, csv=True):

    chrome.execute_script("window.open()")
    chrome.switch_to.window(chrome.window_handles[-1])
    chrome.get('chrome://downloads')
    time.sleep(5)
    excel_file = chrome.execute_script("return document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('div#content  #file-link').text")
    new_file_name = sname+re.sub(r'.+?(\..+)$', r"\1", excel_file)
    chrome.close()
    chrome.switch_to.window(chrome.window_handles[0])
    while True:
        try:            
            if csv == True:
                US_t = readFile((Path.home() / "Downloads" / excel_file).as_posix(), header_=header, index_col_=index_col, skiprows_=skiprows, acceptNoFile=False, usecols_=usecols, names_=names, wait=True)
            else:
                US_t = readExcelFile((Path.home() / "Downloads" / excel_file).as_posix(), header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=0, acceptNoFile=False, usecols_=usecols, names_=names, wait=True)
        except:
            time.sleep(1)
        else:
            if os.path.isfile((Path.home() / "Downloads" / new_file_name)) and excel_file != new_file_name:
                os.remove((Path.home() / "Downloads" / new_file_name))
            os.rename((Path.home() / "Downloads" / excel_file), (Path.home() / "Downloads" / new_file_name))
            if os.path.isfile(data_path+address+new_file_name):
                if os.path.isfile(data_path+address+'old/'+new_file_name):
                    os.remove(data_path+address+'old/'+new_file_name)
                shutil.move(data_path+address+new_file_name, data_path+address+'old/'+new_file_name)
            shutil.move((Path.home() / "Downloads" / new_file_name), data_path+address+new_file_name)
            break

    return US_t

def MERGE(merge_file, DB_TABLE, DB_CODE, freq):
    i = 0
    found = False
    while found == False:
        i += 1
        if DB_TABLE+freq+'_'+str(i).rjust(4,'0') not in list(merge_file['db_table']) and i > 1:
            found = True
            code_t = []
            for c in range(merge_file.shape[0]):
                if merge_file['db_table'][c] == DB_TABLE+freq+'_'+str(i-1).rjust(4,'0'):
                    code_t.append(merge_file['db_code'][c])
            if max(code_t) == DB_CODE+str(199).rjust(3,'0'):
                table_num = i
                code_num = 1
            else:
                table_num = i-1
                code_num = int(max(code_t).replace(DB_CODE, ''))+1
        elif DB_TABLE+freq+'_'+str(i).rjust(4,'0') not in list(merge_file['db_table']) and i == 1:
            found = True
            table_num = 1
            code_num = 1
    
    return table_num, code_num

def NEW_KEYS(f, freq, FREQLIST, DB_TABLE, DB_CODE, df_key, DATA_BASE, db_table_t, start_table, start_code, DATA_BASE_new, DB_name_new):
    
    if start_code >= 200:
        if freq == 'W':
            db_table_t = db_table_t.reindex(FREQLIST['W_s'])
        DATA_BASE_new[DB_TABLE+freq+'_'+str(start_table).rjust(4,'0')] = db_table_t
        DB_name_new.append(DB_TABLE+freq+'_'+str(start_table).rjust(4,'0'))
        start_table += 1
        start_code = 1
        db_table_t = pd.DataFrame(index = FREQLIST[freq], columns = [])
    db_table = DB_TABLE+freq+'_'+str(start_table).rjust(4,'0')
    db_code = DB_CODE+str(start_code).rjust(3,'0')
    db_table_t[db_code] = DATA_BASE[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
    df_key.loc[f, 'db_table'] = db_table
    df_key.loc[f, 'db_code'] = db_code
    start_code += 1
    db_table_new = db_table
    db_code_new = db_code
    
    return df_key, DATA_BASE_new, DB_name_new, db_table_t, start_table, start_code, db_table_new, db_code_new

def CONCATE(NAME, suf, data_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, KEY_DATA_t, DB_dict, DB_name_dict, find_unknown=True):
    if find_unknown == True:
        repeated_standard = 'start'
    else:
        repeated_standard = 'last'
    #print('Reading file: '+NAME+'key'+suf+', Time: ', int(time.time() - tStart),'s'+'\n')
    #KEY_DATA_t = readExcelFile(data_path+NAME+'key'+suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
    print('Reading file: '+NAME+'database'+suf+', Time: ', int(time.time() - tStart),'s'+'\n')
    DATA_BASE_t = readExcelFile(data_path+NAME+'database'+suf+'.xlsx', header_ = 0, index_col_=0)
    if type(DATA_BASE_t) != dict:
        DATA_BASE_t = {}
    
    print('Concating file: '+NAME+'key'+suf+', Time: ', int(time.time() - tStart),'s'+'\n')
    KEY_DATA_t = pd.concat([KEY_DATA_t, df_key], ignore_index=True)
    
    print('Concating file: '+NAME+'database'+suf+', Time: ', int(time.time() - tStart),'s'+'\n')
    for f in FREQNAME:
        for d in DB_name_dict[f]:
            sys.stdout.write("\rConcating sheet: "+str(d))
            sys.stdout.flush()
            if d in DATA_BASE_t.keys():
                DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_dict[f][d])
            else:
                DATA_BASE_t[d] = DB_dict[f][d]
        sys.stdout.write("\n")

    print('Time: ', int(time.time() - tStart),'s'+'\n')
    KEY_DATA_t = KEY_DATA_t.sort_values(by=['freq', 'name', 'db_table', 'snl'], ignore_index=True)
    
    repeated = 0
    repeated_index = []
    Repeat = {}
    Repeat['key'] = []
    Repeat[repeated_standard] = []
    for i in range(1, len(KEY_DATA_t)):
        if i in Repeat['key']:
            continue
        Repeat['key'] = []
        Repeat[repeated_standard] = []
        if KEY_DATA_t.iloc[i]['name'] == KEY_DATA_t.iloc[i-1]['name']:
            j = i
            Repeat['key'].append(j-1)
            Repeat[repeated_standard].append(str(KEY_DATA_t.iloc[j-1][repeated_standard]))
            while KEY_DATA_t.iloc[j]['name'] == KEY_DATA_t.iloc[j-1]['name']:
                repeated += 1
                Repeat['key'].append(j)
                Repeat[repeated_standard].append(str(KEY_DATA_t.iloc[j][repeated_standard]))
                j += 1
                if j >= len(KEY_DATA_t):
                    break
            if repeated_standard == 'start':
                keep = Repeat['key'][Repeat[repeated_standard].index(min(Repeat[repeated_standard]))]
            elif repeated_standard == 'last':
                keep = Repeat['key'][Repeat[repeated_standard].index(max(Repeat[repeated_standard]))]
            for k in Repeat['key']:
                if k != keep and ((repeated_standard == 'start' and Repeat[repeated_standard][Repeat['key'].index(k)] == min(Repeat[repeated_standard])) or (repeated_standard == 'last' and Repeat[repeated_standard][Repeat['key'].index(k)] == max(Repeat[repeated_standard]))):
                    if k < keep or Repeat[repeated_standard][Repeat['key'].index(keep)] == 'Nan':
                        repeated_index.append(keep)
                        keep = k
                    else:
                        repeated_index.append(k)
                elif k != keep and ((repeated_standard == 'start' and Repeat[repeated_standard][Repeat['key'].index(k)] > min(Repeat[repeated_standard])) or (repeated_standard == 'last' and Repeat[repeated_standard][Repeat['key'].index(k)] < max(Repeat[repeated_standard]))):
                    if Repeat[repeated_standard][Repeat['key'].index(keep)] == 'Nan':
                        repeated_index.append(keep)
                        keep = k
                    else:
                        repeated_index.append(k)
            #print(KEY_DATA_t.iloc[keep]) 
        sys.stdout.write("\r"+str(repeated)+" repeated data key(s) found")
        sys.stdout.flush()
    sys.stdout.write("\n")
    #rp_idx = []
    for target in repeated_index:
        sys.stdout.write("\rDropping repeated database column(s)...("+str(round((repeated_index.index(target)+1)*100/len(repeated_index), 1))+"%)*")
        sys.stdout.flush()
        DATA_BASE_t[KEY_DATA_t.iloc[target]['db_table']] = DATA_BASE_t[KEY_DATA_t.iloc[target]['db_table']].drop(columns = KEY_DATA_t.iloc[target]['db_code'])
        #rp_idx.append([KEY_DATA_t.iloc[target]['name'], KEY_DATA_t.iloc[target]['form_c']])
    sys.stdout.write("\n")
    #print('Dropping repeated database column(s)')
    #pd.DataFrame(rp_idx, columns = ['name', 'fname']).to_excel(data_path+"repeated.xlsx", sheet_name='repeated')
    KEY_DATA_t = KEY_DATA_t.drop(repeated_index)
    KEY_DATA_t.reset_index(drop=True, inplace=True)
    #print(KEY_DATA_t)
    print('Time: ', int(time.time() - tStart),'s'+'\n')
    for s in range(KEY_DATA_t.shape[0]):
        sys.stdout.write("\rSetting new snls: "+str(s+1))
        sys.stdout.flush()
        KEY_DATA_t.loc[s, 'snl'] = s+1
    sys.stdout.write("\n")
    #if repeated > 0:
    print('Setting new files, Time: ', int(time.time() - tStart),'s'+'\n')
    
    start_table_dict = {}
    start_code_dict = {}
    DB_new_dict = {}
    db_table_t_dict = {}
    DB_name_new_dict = {}
    for f in FREQNAME:
        start_table_dict[f] = 1
        start_code_dict[f] = 1
        DB_new_dict[f] = {}
        db_table_t_dict[f] = pd.DataFrame(index = FREQLIST[f], columns = [])
        DB_name_new_dict[f] = []
    db_table_new = 0
    db_code_new = 0
    for f in range(KEY_DATA_t.shape[0]):
        sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
        sys.stdout.flush()
        freq = KEY_DATA_t.iloc[f]['freq']
        if not DB_name_dict[freq]:
            db_table_new = KEY_DATA_t.iloc[f]['db_table']
            db_code_new = KEY_DATA_t.iloc[f]['db_code']
            if db_table_new not in DB_dict[freq].keys():
                DB_dict[freq][db_table_new] = DATA_BASE_t[db_table_new]
            continue
        df_key, DB_new_dict[freq], DB_name_new_dict[freq], db_table_t_dict[freq], start_table_dict[freq], start_code_dict[freq], db_table_new, db_code_new = \
            NEW_KEYS(f, freq, FREQLIST, DB_TABLE, DB_CODE, KEY_DATA_t, DATA_BASE_t, db_table_t_dict[freq], start_table_dict[freq], start_code_dict[freq], DB_new_dict[freq], DB_name_new_dict[freq])
    sys.stdout.write("\n")
    for f in FREQNAME:
        if db_table_t_dict[f].empty == False:
            #if freq == 'W':
            #    db_table_t_dict[freq] = db_table_t_dict[freq].reindex(FREQLIST['W_s'])
            DB_new_dict[f][DB_TABLE+f+'_'+str(start_table_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
            DB_name_new_dict[f].append(DB_TABLE+f+'_'+str(start_table_dict[f]).rjust(4,'0'))
        if not not DB_name_new_dict[f]:
            DB_dict[f] = DB_new_dict[f]
            DB_name_dict[f] = DB_name_new_dict[f]

    print('Concating new files: '+NAME+'database, Time: ', int(time.time() - tStart),'s'+'\n')
    DATA_BASE_dict = {}
    for f in FREQNAME:
        if not DB_name_dict[f]:
            DATA_BASE_dict[f] = DB_dict[f]
            continue
        DATA_BASE_dict[f] = {}
        for d in DB_name_dict[f]:
            sys.stdout.write("\rConcating sheet: "+str(d))
            sys.stdout.flush()
            DATA_BASE_dict[f][d] = DB_dict[f][d]
        sys.stdout.write("\n")
    
    #print(KEY_DATA_t)
    print('Time: ', int(time.time() - tStart),'s'+'\n')

    return KEY_DATA_t, DATA_BASE_dict

def UPDATE(original_file, updated_file, key_list, NAME, data_path, orig_suf, up_suf):
    updated = 0
    tStart = time.time()
    print('Updating file: ', int(time.time() - tStart),'s'+'\n')
    print('Reading original database: '+NAME+'database'+orig_suf+', Time: ', int(time.time() - tStart),'s'+'\n')
    original_database = readExcelFile(data_path+NAME+'database'+orig_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
    print('Reading updated database: '+NAME+'database'+up_suf+'.xlsx, Time: ', int(time.time() - tStart),'s'+'\n')
    updated_database = readExcelFile(data_path+NAME+'database'+up_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)

    original_file = original_file.set_index('name')
    updated_file = updated_file.set_index('name')
    for ind in updated_file.index:
        sys.stdout.write("\rUpdating latest data time: "+ind+" ")
        sys.stdout.flush()

        if ind in original_file.index:
            original_file.loc[ind, 'desc_e'] = updated_file.loc[ind, 'desc_e']
            if updated_file.loc[ind, 'last'] == 'Nan':
                continue
            elif (original_file.loc[ind, 'last'] == 'Nan' and updated_file.loc[ind, 'last'] != 'Nan') or updated_file.loc[ind, 'last'] > original_file.loc[ind, 'last']:
                updated+=1
            if updated_file.loc[ind, 'last'] != 'Nan':
                original_file.loc[ind, 'last'] = updated_file.loc[ind, 'last']
            for period in updated_database[updated_file.loc[ind, 'db_table']].index:
                if updated_file.loc[ind, 'db_table'][3] == 'W' and type(period) != str:
                    period = period.strftime('%Y-%m-%d')
                if str(updated_database[updated_file.loc[ind, 'db_table']].loc[period, updated_file.loc[ind, 'db_code']]) != 'nan':
                    original_database[original_file.loc[ind, 'db_table']].loc[period, original_file.loc[ind, 'db_code']] = updated_database[updated_file.loc[ind, 'db_table']].loc[period, updated_file.loc[ind, 'db_code']]
        else:
            ERROR('Updated file index does not belongs to the original file index list: '+ind)
    sys.stdout.write("\n\n")
    for key in original_database.keys():
        original_database[key] = original_database[key].sort_index(axis=0)
    original_file = original_file.reset_index()
    original_file = original_file.reindex(key_list, axis='columns')
    print('updated:', updated, '\n')

    return original_file, original_database

def US_NOTE(LINE, sname=None, LABEL=[], address='', other=False, fname=None):
    note = []
    footnote = []
    FOOT = ['nan', 'Legend / Footnotes:']
    footchar = []
    if other == True:
        for n in range(LINE.shape[0]):
            line = LINE.index[n]
            if str(line).isnumeric():
                line = int(line)
            if address.find('ei/') >= 0:
                for code in LABEL['footnote_codes']:
                    footnote = LABEL['footnote_codes'][code]
                    if type(footnote) == float and footnote.is_integer():
                        footnote = int(footnote)
                    if footnote == line:
                        if bool(re.search(r'[A-Za-z\s]+\-', LINE.iloc[n]['footnote_text'])):
                            subword = re.sub(r'\s+\-[A-Za-z\s,\.]+', "", LINE.iloc[n]['footnote_text']).strip()
                            Note = 'Including '+re.sub(r'[A-Za-z\s]+\-\s+', "", LINE.iloc[n]['footnote_text']).strip()
                        else:
                            subword = re.sub(r'\-.+', '', LABEL['series_name'][code])
                            Note = 'Including '+LINE.iloc[n]['footnote_text']
                        note.append([subword, Note])
                        break
            elif address.find('ln/') >= 0 or address.find('mp/') >= 0 or address.find('bd/') >= 0:
                for code in LABEL['footnote_codes']:
                    footnote = LABEL['footnote_codes'][code]
                    if type(footnote) == float and footnote.is_integer():
                        footnote = int(footnote)
                    if footnote == line:
                        Note = LINE.iloc[n]['footnote_text'].strip()
                        note.append([str(LINE.index[n]), Note])
                        break
            elif address.find('ce/') >= 0:
                for code in LABEL['footnote_codes']:
                    if str(LABEL['footnote_codes'][code]) == line:
                        Note = LINE.iloc[n]['footnote_text'].strip()
                        note.append([str(LINE.index[n]), Note])
                        break
            elif address.find('ml/') >= 0:
                for code in LABEL['footnote_codes']:
                    footnote = re.split(',', str(LABEL['footnote_codes'][code]))
                    for foot in footnote:
                        if foot.isnumeric():
                            foot = int(foot)
                        if foot == line and str(LINE.index[n]) not in footchar:
                            Note = LINE.iloc[n]['footnote_text'].strip()
                            note.append([str(LINE.index[n]), Note])
                            footchar.append(str(LINE.index[n]))
                            break
            else:
                note.append([LINE.index[n], LINE.iloc[n]['note']])
        return note
    for n in range(len(LINE)):
        if sname != 0 and bool(re.match(r'[0-9]+\.', str(LINE[n]))):
            whole = str(LINE[n])[str(LINE[n]).find('.')+1:]
            whole = re.sub(r'\s\([0-9]\)|\s\(see\sfootnote\s[0-9]+\)|<.*?>',"",whole)
            if bool(re.search(r'[Ll]ine', whole)):
                if address.find('ITAS') < 0:
                    whole = re.sub(r'\s\([Ll]ine\s[0-9]+\)|[Ll]ine\s[0-9]+,\s',"",whole)
                if whole.find('residual') >= 0:
                    whole = whole.replace('the first line',LABEL['1'].strip()).replace('detailed lines','detailed items')
                if bool(re.search(r'[Ll]ine\s[0-9]+', whole)) or bool(re.search(r'[Ll]ines\s[0-9]+', whole)):
                    whole = re.sub(r'\s[Ll]ine'," Item of line", whole)
                else:
                    whole = re.sub(r'\s[Ll]ine'," item", whole)
            note.append([int(str(LINE[n])[:str(LINE[n]).find('.')]),whole.strip()])
        elif (address.find('BOC') >= 0 or address.find('PETR') >= 0) and bool(re.match(r'[0-9]+\s*[A-Z]+', str(LINE[n]).strip())):
            whole = str(LINE[n])[re.search(r'[A-Z]',str(LINE[n])).start():]
            m = n
            while str(LINE[m+1]) != 'nan' and bool(re.match(r'[0-9]+\s*[A-Z]+', str(LINE[m+1]).strip())) == False and address.find('SCEN') < 0 and address.find('PETR') < 0 and address.find('RESC') < 0:
                whole = whole+str(LINE[m+1])
                m+=1
                if m+1 >= len(LINE):
                    break
            note.append([int(str(LINE[n])[:re.search(r'[A-Z]',str(LINE[n])).start()]),whole.replace('\xa0',' ').strip()])
        elif address.find('BTS') >= 0 and bool(re.match(r'[a-z]+\s*[A-Z0-9]+', str(LINE[n]).strip())):
            whole = str(LINE[n])[re.search(r'[A-Z0-9]',str(LINE[n])).start():]
            m = n
            while str(LINE[m+1]) != 'nan' and bool(re.match(r'[a-z]+\s*[A-Z0-9]+', str(LINE[m+1]).strip())) == False and bool(re.match(r'Note:', str(LINE[m+1]))) == False:
                whole = whole+str(LINE[m+1])
                m+=1
                if m+1 >= len(LINE):
                    break
            note.append([str(LINE[n])[:re.search(r'[A-Z0-9]',str(LINE[n])).start()].strip(),whole.strip()])
        elif str(fname).find('mfhhis01') >= 0 and bool(re.match(r'[0-9]+/\s+', str(LINE[n]).strip())):
            whole = str(LINE[n])[re.search(r'[A-Z]',str(LINE[n])).start():]
            m = n
            while str(LINE[m+1]) != 'nan' and bool(re.match(r'[0-9]+/\s+', str(LINE[m+1]).strip())) == False and str(LINE[m+1]).strip() != '':
                whole = whole+str(LINE[m+1])
                m+=1
                if m+1 >= len(LINE):
                    break
            note.append([int(str(LINE[n])[:re.search(r'/',str(LINE[n])).start()]),re.sub(r'\s+', " ", whole.strip())])
        elif address.find('US_temp') >= 0:
            if str(LINE[n]).find('year-round unit') >= 0:
                whole = re.sub(r'[0-9]\s',"", str(LINE[n]))
                note.append(['YRV', whole.strip()])
        elif sname != 0 and str(LINE[n]).find('Note.') >= 0:
            whole = str(LINE[n])[str(LINE[n]).find('Note.')+5:].replace('table are','item is').replace('Except as noted in footnotes 1, 2 and 3, c','C').replace('This table is','This item is').replace(' (see table footnotes)','')
            whole = re.sub(r'\s\([0-9]\)|\s\(see\sfootnote\s[0-9]+\)',"",whole)
            if bool(re.search(r'[Ll]ine', whole)):
                whole = re.sub(r'\s\([Ll]ine\s[0-9]+\)|[Ll]ine\s[0-9]+,\s',"",whole)
                if whole.find('residual') >= 0:
                    whole = whole.replace('the first line',LABEL['1'].strip()).replace('detailed lines','detailed items')
                if bool(re.search(r'[Ll]ine\s[0-9]+', whole)) or bool(re.search(r'[Ll]ines\s[0-9]+', whole)):
                    whole = re.sub(r'\s[Ll]ine'," Item of line", whole)
                else:
                    whole = re.sub(r'\s[Ll]ine'," item", whole)
            note.append(['Note',whole.strip()])
        elif bool(re.search(r'Note[s]*:', str(LINE[n]))) and address.find('SCEN') < 0:
            whole = str(LINE[n])[re.search(r'Note[s]*:', str(LINE[n])).start()+5:]
            if address.find('CONS') < 0:
                whole = whole.strip("',(): ")
            m = n
            if m+1 < len(LINE):
                while str(LINE[m+1]) != 'nan' and bool(re.match(r'Source:', str(LINE[m+1]))) == False and bool(re.search(r'Note:', str(LINE[m+1]))) == False and address.find('APEP') < 0 and address.find('PETR') < 0:
                    whole = whole+' '+str(LINE[m+1])
                    m+=1
                    if m+1 >= len(LINE):
                        break
            if whole.find('Single-family') >= 0:
                key = 'ONE'
            else:
                key = 'Note'
            if whole.find('Universe') >= 0:
                whole = whole+'.'
            whole = re.sub(r'\s+', " ", whole)
            if whole.find('how_surveys_are_collected') >= 0:
                continue
            note.append([key, whole.replace("'",'').replace('\xa0',' ').strip()])
        elif str(sname).find('U70206') >= 0 and str(LINE[n]) != 'nan' and str(LINE[n]).isnumeric() == False:
            whole = str(LINE[n]).replace('table are','item is').replace('This table is','This item is')
            note.append(['Note', whole.strip()])
        elif str(fname).find('mfhhis01') < 0 and str(sname).find('Page') < 0 and sname != 0 and str(LINE[n]) not in FOOT and str(LINE[n]).isnumeric() == False and str(LINE[n]).strip() != '':
            not_footnote = False
            for no in note:
                if no[1].find(re.sub(r'\s+', " ", str(LINE[n])).strip()) >= 0:
                    not_footnote = True
                    break
            if not_footnote == True:
                continue
            if address.find('NIPA') >= 0:
                foot = re.split(r'[\s=:]+', str(LINE[n]), 1)
            else:
                foot = re.split(r'[\s=:]+', re.sub(r'\.$', "", str(LINE[n])), 1)
            if len(foot) == 2 and foot[0].isnumeric() == False and foot[1] != '00:00:00':
                footnote.append(foot)
    return note, footnote

def US_HISTORYDATA(US_temp, name, MONTH=None, QUARTER=None, make_idx=False, summ=False):
    nU = US_temp.shape[0]
    
    US_t = pd.DataFrame()
    new_item_t = []
    new_item = 0
    if make_idx == True:
        new_index_t = ['Index']
    else:
        new_index_t = []
    new_dataframe = []
    for new in range(nU):
        sys.stdout.write("\rLoading...("+str(round((new+1)*100/nU, 1))+"%)*")
        sys.stdout.flush()
        if str(US_temp.iloc[new][name]).replace('.0','').isnumeric() == False and summ == False:
            continue
        if new == 0 and make_idx == True:
            new_item_t.append(US_temp.iloc[new]['code'].replace('"',''))
        elif (str(US_temp.iloc[new][name]).replace('.0','').isnumeric() == False or str(US_temp.iloc[new][name]) < str(US_temp.iloc[new-1][name])) and summ == False:
            new_dataframe = []
            new_item_t = []
            if make_idx == True:
                new_index_t = ['Index']
                new_code = re.split(r'\.', US_temp.iloc[new]['code'].replace('"',''))
                code = ''
                for n in new_code:
                    code = code+n
                new_item_t.append(code)
            else:
                new_index_t = []
        if MONTH != None:
            if summ == True:
                if bool(re.search(r'[0-9]+[a-z\s\*]+$', str(US_temp.iloc[new][name]))):
                    dex = re.sub(r'[a-z\s\*]+$', "", str(US_temp.iloc[new][name])).strip()
                else:
                    dex = str(US_temp.iloc[new][name]).strip()
                if dex.isnumeric():
                    if not not new_index_t:
                        new_item_t.append(new_item)
                    year = dex
                    new_index_t.append(year)
                    new_item = 0
                    continue
                if dex in MONTH:
                    new_item = new_item + US_temp.iloc[new].loc[US_temp.iloc[new].index[1]]
            else:
                for month in MONTH:
                    new_index_t.append(str(int(US_temp.iloc[new][name]))+'-'+str(datetime.strptime(month,'%b').month).rjust(2,'0'))
                    new_item_t.append(US_temp.iloc[new][month])
        elif QUARTER != None:
            for ind in range(len(US_temp.iloc[new].index)):
                if US_temp.iloc[new].index[ind][1] in QUARTER:
                    new_index_t.append(str(int(US_temp.iloc[new][name]))+'-'+QUARTER[US_temp.iloc[new].index[ind][1]])
                    new_item_t.append(US_temp.iloc[new].loc[US_temp.iloc[new].index[ind]])
        if new == nU - 1:
            if summ == True:
                new_item_t.append(new_item)
            new_dataframe.append(new_item_t)
            US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
            US_t = pd.concat([US_t, US_new], ignore_index=True)
        elif (str(US_temp.iloc[new+1][name]).replace('.0','').isnumeric() == False or str(US_temp.iloc[new][name]) > str(US_temp.iloc[new+1][name])) and summ == False:
            new_dataframe.append(new_item_t)
            US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
            US_t = pd.concat([US_t, US_new], ignore_index=True)
    sys.stdout.write("\n\n")
    if make_idx == True:
        US_t = US_t.set_index('Index', drop=False)

    return US_t

def US_country(US_temp, Series, prefix, middle, freq, name, bal=False):
    FREQ = {}
    FREQ['M'] = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    FREQ['Q'] = ['Q1', 'Q2', 'Q3', 'Q4']
    FREQ['A'] = ['YR']
    suf_item = ['IM', 'EX', 'BL']
    nU = US_temp.shape[0]
    
    US_t = pd.DataFrame()
    new_item_t = {'I':[], 'E':[], 'B':[]}
    new_item_l = {'I':[], 'E':[], 'B':[]}
    new_index_t = []
    new_code_t = []
    new_label_t = []
    new_order_t = []
    new_dataframe = []
    firstfound = False
    country = ''
    for new in range(nU):
        sys.stdout.write("\rLoading...("+str(round((new+1)*100/nU, 1))+"%)*")
        sys.stdout.flush()
        if str(US_temp.iloc[new][name]) != country:
            if firstfound == True:
                for key in new_item_t.keys():
                    new_dataframe.append(new_item_t[key])
                US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
                if US_new.empty == False:
                    new_code_t.extend(code)
                    new_label_t.extend(lab)
                    new_order_t.extend(order)
                    US_t = pd.concat([US_t, US_new], ignore_index=True)
                new_dataframe = []
                for key in new_item_t.keys():
                    new_item_t[key] = []
                new_index_t = []
            fix = ''
            code = []
            lab = []
            order = []
            country = str(US_temp.iloc[new][name])
            for item in list(Series['GEO LEVELS']['name']):
                if country in re.split(r'//', item):
                    fix = Series['GEO LEVELS'].loc[Series['GEO LEVELS']['name'] == item].index[0]
                    break
            if fix != '':
                code = [prefix+middle+suf+fix for suf in suf_item]
                lab = [Series['DATA TYPES'].loc[suf, 'dt_desc']+',  '+Series['GEO LEVELS'].loc[fix, 'geo_desc'] for suf in suf_item]
                order = [Series['CATEGORIES'].loc[middle, 'order']]*3
            firstfound = True
        if fix == '':
            continue
        if freq == 'A' and str(US_temp.iloc[new]['year']).replace('.0', '').isnumeric():
            new_index_t.append(str(int(US_temp.iloc[new]['year'])))
        elif freq == 'M' and str(US_temp.iloc[new]['year']).replace('.0', '').isnumeric():
            for month in FREQ['M']:
                new_index_t.append(str(int(US_temp.iloc[new]['year']))+'-'+str(datetime.strptime(month,'%b').month).rjust(2,'0'))
        elif freq == 'Q' and str(US_temp.iloc[new]['year']).replace('.0', '').isnumeric():
            for quar in FREQ['Q']:
                new_index_t.append(str(int(US_temp.iloc[new]['year']))+'-'+quar)
        for key in new_item_l.keys():
            new_item_l[key] = []
        for ind in range(US_temp.shape[1]):
            if str(US_temp.columns[ind])[1:] in FREQ[freq]:
                key = str(US_temp.columns[ind])[:1]
                new_item_l[key].append(US_temp.iloc[new][US_temp.columns[ind]])
                new_item_t[key].append(US_temp.iloc[new][US_temp.columns[ind]])
        if bal == True and not new_item_l['B']:
            if len(new_item_l['E']) != len(new_item_l['I']):
                ERROR('Balance Calculation Not Available: '+country)
            for ind in range(len(new_item_l['E'])):
                new_item_t['B'].append(new_item_l['E'][ind]-new_item_l['I'][ind])
    sys.stdout.write("\n\n")
    for key in new_item_t.keys():
        new_dataframe.append(new_item_t[key])
    US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
    if US_new.empty == False:
        new_code_t.extend(code)
        new_label_t.extend(lab)
        new_order_t.extend(order)
        US_t = pd.concat([US_t, US_new], ignore_index=True)

    return US_t, new_code_t, new_label_t, new_order_t

def DATA_SETS(data_path, address, datasets=None, fname=None, sname=None, DIY_series=None, MONTH=None, password='', header=None, index_col=None, skiprows=None, freq=None, x='', transpose=True, HIES=False, usecols=None, names=None, multi=None, subword=None, prefix=None, middle=None, suffix=None, chrome=None, key_text=''):
    note = []
    footnote = []
    if datasets != None:
        with open(data_path+address+datasets+'.csv','r',encoding='ANSI') as f:
            lines = f.readlines()
        Series = {}
        Series['ERROR TYPES'] = pd.DataFrame()
        l = 0
        while l < len(lines):
            if lines[l].replace('\n','').replace(',','').isupper():
                if bool(re.match(r'ERROR TYPES$', lines[l].replace('\n','').replace(',','').strip('"'))) and fname == None:
                    et_head = l+1
                    for m in range(l+2, len(lines)):
                        if lines[m+1].replace('\n','').replace(',','') == '' or m == len(lines)-1:
                            et_tail = m
                            break
                    Series['ERROR TYPES'] = readFile(data_path+address+datasets+'.csv', header_ = 0, index_col_ = 0, skiprows_ = list(range(et_head)), nrows_ = et_tail - et_head)
                elif bool(re.match(r'GEO LEVELS$', lines[l].replace('\n','').replace(',','').strip('"'))) and fname == None:
                    geo_head = l+1
                    for m in range(l+2, len(lines)):
                        if lines[m+1].replace('\n','').replace(',','') == '' or m == len(lines)-1:
                            geo_tail = m
                            break
                    Series['GEO LEVELS'] = readFile(data_path+address+datasets+'.csv', header_ = 0, index_col_ = 0, skiprows_ = list(range(geo_head)), nrows_ = geo_tail - geo_head)
                elif bool(re.match(r'TIME PERIODS$', lines[l].replace('\n','').replace(',','').strip('"'))) and fname == None:
                    per_head = l+1
                    for m in range(l+2, len(lines)):
                        if lines[m+1].replace('\n','').replace(',','') == '' or m == len(lines)-1:
                            per_tail = m
                            break
                    Series['TIME PERIODS'] = readFile(data_path+address+datasets+'.csv', header_ = 0, index_col_ = 0, skiprows_ = list(range(per_head)), nrows_ = per_tail - per_head)
                elif bool(re.match(r'NOTES$', lines[l].replace('\n','').replace(',','').strip('"'))):
                    note_head = False
                    note_tail = False
                    for m in range(l+1, len(lines)):
                        if bool(re.match(r'DATA UPDATED ON$', lines[m].replace('\n','').replace(',','').strip('"'))) or m == len(lines)-1:
                            break
                        elif re.sub(r'<.*?>|\s*\[.*?\]|\&\#[0-9]*|Note:|\n|\t|"', "", lines[m]) == '':
                            continue
                        elif lines[m].replace('\n','').find('<p>') >= 0 and bool(re.match(r'<p>\(*[A-Z]+\)*\s[-=]\s',lines[m].replace('\n','').replace('"',''))) == False:
                            if note_head == False:
                                note_head = m
                            if note_head != False:
                                note_tail = m
                            whole = lines[m].replace('\n','')
                            n = m
                            while lines[n].replace('\n','').find('</p>') < 0 and lines[n].replace('\n','').find('<p/>') < 0:
                                whole = whole + lines[n+1].replace('\n','')
                                n+=1
                                if note_head != False:
                                    note_tail = n
                            whole = re.sub(r"<.*?>|\s*\[.*?\]|\&\#[0-9]*|Note:|'", "", whole)
                            whole = re.sub(r'[\s]+', " ", whole).replace('for the following industries (defined in Box 3 above)','for industries with the following codes')
                            note.append(['Note',whole.replace('"','').replace("\\",'').replace(",,",'').replace('Inventories/Sales','Inventories-to-Sales').strip()])
                        elif lines[m].find('</p>') >= 0:
                            if note_head != False:
                                note_tail = m
                    if note_head == False or note_tail == False:
                        ERROR('Note head or tail not found.')
                    #else:
                    #   del lines[note_head+1:note_tail+1]
                elif bool(re.match(r'DATA$', lines[l].replace('\n','').replace(',','').strip('"'))) and fname == None:
                    data_head = l+1
                    for m in range(l+2, len(lines)):
                        if m == len(lines)-1:
                            data_tail = m
                            break
                    US_temp = readFile(data_path+address+datasets+'.csv', header_ = 0, skiprows_ = list(range(data_head)), nrows_ = data_tail - data_head, acceptNoFile=False)
            l+=1
        
    if HIES == True:
        US_t, label, note2, footnote2 = HIES_OLD(prefix, middle, data_path, address, fname, sname, DIY_series)
    if fname == None and HIES == False:
        if Series['ERROR TYPES'].empty:
            US_temp = US_temp.sort_values(by=['is_adj','geo_idx','dt_idx','cat_idx','per_idx'], ignore_index=True)
        else:
            US_temp = US_temp.sort_values(by=['et_idx','is_adj','geo_idx','dt_idx','cat_idx','per_idx'], ignore_index=True)
        
        US_t = pd.DataFrame()
        new_item_t = []
        new_index_t = ['Index', 'Label', 'order']
        new_dataframe = []
        firstfound = False
        for i in range(US_temp.shape[0]):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/US_temp.shape[0], 1))+"%)*")
            sys.stdout.flush()
            if Series['ERROR TYPES'].empty == False: 
                if US_temp.iloc[i]['et_idx'] > 0:
                    continue
            if password != '' and (str(DIY_series['DATA TYPES'].loc[US_temp.iloc[i]['dt_idx'], 'dt_code']).find(password) >= 0 or \
                str(DIY_series['CATEGORIES'].loc[US_temp.iloc[i]['cat_idx'], 'cat_code']).find(password) >= 0 or \
                (Series['GEO LEVELS'].shape[0] > 1 and str(DIY_series['GEO LEVELS'].loc[US_temp.iloc[i]['geo_idx'], 'geo_code']).find(password) >= 0)):
                continue 
            if i == 0:
                new_code = True
            elif US_temp.iloc[i]['per_idx'] < US_temp.iloc[i-1]['per_idx']:
                new_code = True
            else:
                new_code = False
            if new_code == True:
                if firstfound == True:
                    new_dataframe.append(new_item_t)
                    US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
                    if US_new.empty == False:
                        US_t = pd.concat([US_t, US_new], ignore_index=True)
                    new_dataframe = []
                    new_item_t = []
                    new_index_t = ['Index', 'Label', 'order']
                prefix = str(DIY_series['ISADJUSTED'].loc[US_temp.iloc[i]['is_adj'], 'adj_code'])
                middle = str(DIY_series['CATEGORIES'].loc[US_temp.iloc[i]['cat_idx'], 'cat_code'])
                if address.find('MWTS') >= 0:
                    middle = middle.ljust(5,'0')
                elif address.find('MRTS') >= 0:
                    middle = middle.ljust(7,'0')
                if Series['GEO LEVELS'].shape[0] > 1:
                    suffix = str(DIY_series['GEO LEVELS'].loc[US_temp.iloc[i]['geo_idx'], 'geo_code']) + str(DIY_series['DATA TYPES'].loc[US_temp.iloc[i]['dt_idx'], 'dt_code'])
                    lab = DIY_series['DATA TYPES'].loc[US_temp.iloc[i]['dt_idx'], 'dt_desc']
                    order = DIY_series['DATA TYPES'].loc[US_temp.iloc[i]['dt_idx'], 'order']
                else:
                    suffix = str(DIY_series['DATA TYPES'].loc[US_temp.iloc[i]['dt_idx'], 'dt_code'])
                    lab = DIY_series['CATEGORIES'].loc[US_temp.iloc[i]['cat_idx'], 'cat_desc']
                    order = DIY_series['CATEGORIES'].loc[US_temp.iloc[i]['cat_idx'], 'order']
                code = prefix+middle+suffix
                new_item_t.extend([code, lab, order])
                firstfound = True
            new_item_t.append(US_temp.iloc[i]['val'])
            period_index = Series['TIME PERIODS'].loc[US_temp.iloc[i]['per_idx'], 'per_name']
            if freq == 'M':
                for month in MONTH:
                    if period_index[:3] == month:
                        date = datetime.strptime(str(period_index[-2:]),'%y')
                        if date > datetime.now():
                            date = date - relativedelta(years=100)
                        period = str(date.year)+'-'+str(datetime.strptime(month,'%b').month).rjust(2,'0')
                        break
            else:
                period = period_index[-4:]+'-'+period_index[:2]
            new_index_t.append(period)   
        sys.stdout.write("\n\n")
        new_dataframe.append(new_item_t)
        US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
        if US_new.empty == False:
            US_t = pd.concat([US_t, US_new], ignore_index=True)
        US_t = US_t.set_index('Index', drop=False)
        US_t = US_t.sort_values(by='order')
        label = US_t['Label']
    elif HIES == False:
        if fname.find('http') >= 0:
            chrome.get(fname)
            y = 0
            while True:
                try:
                    chrome.execute_script("window.scrollTo(0,"+str(y)+")")
                    WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/a[text()="'+key_text+'"]'))).click()
                except:
                    y+=500
                else:
                    break
            time.sleep(5)
            US_t = US_WEBDRIVER(chrome, address, fname, sname, header, index_col, skiprows, usecols, names, csv=False)
        else:
            US_t = readExcelFile(data_path+address+fname+'.xls'+x, header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=sname, acceptNoFile=False, usecols_=usecols, names_=names)
        if type(US_t) != dict and US_t.empty == True:
            ERROR('Sheet Not Found: '+data_path+address+fname+'.xls'+x+', sheet name: '+str(sname))
        if sname != 'shipment' and address.find('FTD') < 0:
            US_t = US_t[~US_t.index.duplicated()]
        if multi != None:
            US_t.columns = [US_t.iloc[i].fillna(method='pad') for i in range(multi)]
            US_t = US_t.drop(index=[US_t.index[i] for i in range(multi)])
        note_line = []
        if type(US_t) != dict:
            for dex in range(len(US_t.index)):
                if bool(re.match(r'Note:$', str(US_t.index[dex]))) and address.find('HOUS') >= 0:
                    if str(US_t.loc[US_t.index[dex], US_t.columns[0]]).find('Universe') >= 0:
                        string = US_t[US_t.columns[0]]
                        note_line.append('Note: '+str(string[dex]))
                    elif str(US_t.index[dex+1]).find('Universe') >= 0:
                        string = US_t.index
                    d = dex+1
                    if d < len(string):
                        while str(string[d]).find('Universe') >= 0:
                            note_line.append('Note: '+str(string[d]))
                            d += 1
                            if d >= len(US_t.index):
                                break
                    break
                else:
                    note_line.append(US_t.index[dex])      
        note2, footnote2 = US_NOTE(note_line, sname, address=address)
        note = note + note2
        if address.find('PRIC') >= 0 or address.find('SHIP') >= 0 or address.find('APEP') >= 0 or address.find('FTD') >= 0:
            footnote = footnote
        else:
            footnote = footnote + footnote2
        if transpose == True:
            US_t = US_t.T
        new_index = []
        new_order = []
        new_label = []
        
        if address.find('CBRT') >= 0:
            US_t = US_t.rename(columns={'Label':'old_label'})
            for ind in range(US_t.shape[0]):
                new_label.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix]['dt_desc'].item()+', '+US_t.iloc[ind]['old_label'])
            US_t = US_t.iloc[:, ::-1]
        elif address.find('CONS') >= 0:
            for ind in range(1,US_t.shape[0]+1):
                US_t.loc[ind, 'Date'] = str(US_t['Date'][ind]).replace('\n\r',' ').replace(subword, '', 1).strip()
                temp = re.sub(r'[0-9]+|\(.*\)'," ", str(US_t['Date'][ind])).strip()
                temp = re.sub(r'[\s]*/[\s]*',"/", temp)
                middle = ''
                for dex in range(DIY_series['CATEGORIES'].shape[0]):
                    if temp == str(DIY_series['CATEGORIES'].iloc[dex]['cat_desc']) or str(DIY_series['CATEGORIES'].iloc[dex]['other names']).find(temp) >= 0:
                        middle = str(DIY_series['CATEGORIES'].index[dex]).rjust(4,'0')
                        order = DIY_series['CATEGORIES'].loc[DIY_series['CATEGORIES'].index[dex], 'order']
                        break
                if middle == '':
                    new_index.append('nan')
                    new_order.append(10000)
                else:
                    new_index.append(prefix+middle+suffix)
                    new_order.append(order)
                US_t.loc[ind, 'Date'] = re.sub(r'[\s]*/[\s]*'," and ", str(US_t['Date'][ind]))
                US_t.loc[ind, 'Date'] = str(US_t['Date'][ind]).replace('Total Construction','Total').replace('Construction','Total').title().replace('And','and').replace('Inc.','including').strip()
        elif address.find('DSCO') >= 0:
            new_start = []
            new_last = []
            if fname == 'smoothed_lf':
                new_index.append('LNSLFCTTSPC')
                new_label.append('Labor Force Research Series Smoothed for Population Control Adjustments')
                new_order.append(0)
            elif fname == 'smoothed_emp':
                new_index.append('LNSEMCTTSPC')
                new_label.append('Employment Research Series Smoothed for Population Control Adjustments')
                new_order.append(1)
            new_start.append('1990-M01')
            new_last.append('2017-M12')
            new_columns = []
            for col in range(len(US_t.columns)):
                new_columns.append(str(US_t.columns[col][0])+'-'+str(US_t.columns[col][1]).replace('M',''))
            US_t.columns = new_columns
        elif address.find('FTD') >= 0:
            US_tem = US_t.copy()
            US_t = pd.DataFrame()
            datatype = [None]
            AMV = [None]
            if fname == 'exh14_Y' or fname == 'exh15' or fname == 'UGDSSITC' or fname == 'UGDSSITC_Y' or fname == 'exh17_Y' or fname == 'exh18' or fname == 'UAMVCSB' or fname == 'UAMVCSB_Y':
                datatype = ['EX', 'IM']
            if fname == 'exh17_Y' or fname == 'exh18' or fname == 'UAMVCSB' or fname == 'UAMVCSB_Y':
                AMV = ['AMV', 'PSC', 'TBV', 'PAR']
            for data in datatype:
                for auto in AMV:
                    if data != None and auto != None:
                        print(data, auto)
                    US_temp, new_index_t, new_label_t, new_order_t = US_FTD(copy.deepcopy(US_tem), fname, DIY_series, prefix, middle, suffix, freq, transpose, data, auto)
                    US_t = pd.concat([US_t, US_temp])
                    new_index.extend(new_index_t)
                    new_label.extend(new_label_t)
                    new_order.extend(new_order_t)
        elif address.find('HOUS') >= 0 and address.find('SHIP') < 0 and address.find('NAHB') < 0:
            geography = ''
            for ind in range(US_t.shape[0]):
                suffix = ''
                if address.find('PRIC') >= 0:
                    if (freq == 'A' and str(US_t.index[ind][1]).find('Annual') >= 0) or (freq == 'Q' and str(US_t.index[ind][1]).find('First Quarter') >= 0):
                        lab = str(DIY_series['DATA TYPES'].iloc[0]['dt_desc'])
                        order = DIY_series['DATA TYPES'].iloc[0]['order']
                        new_label.append(lab)
                        new_order.append(order)
                        suffix = 'US'+str(DIY_series['DATA TYPES'].index[0])
                        if freq == 'Q' and str(US_t.index[ind][1]).find('First Quarter') >= 0:
                            QUARTER = {'First Quarter':'Q1','Second Quarter':'Q2','Third Quarter':'Q3','Fourth Quarter':'Q4'}
                            US_t = US_HISTORYDATA(US_t.T.reset_index(col_fill='index'), name=('index','index'), QUARTER=QUARTER)
                            new_index.append(prefix+middle+suffix)
                            break
                    elif freq == 'A' and str(US_t.index[ind][1]) in list(DIY_series['GEO LEVELS']['name']):
                        geography = str(US_t.index[ind][1])
                        new_label.append(str(DIY_series['DATA TYPES'].iloc[0]['dt_desc']))
                        new_order.append(DIY_series['DATA TYPES'].iloc[0]['order'])
                        suffix = str(DIY_series['GEO LEVELS'].loc[DIY_series['GEO LEVELS']['name'] == geography].index.item()) + str(DIY_series['DATA TYPES'].index[0])
                elif str(US_t.index[ind][0]) in list(DIY_series['DATA TYPES']['name']):
                    lab = str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == US_t.index[ind][0]]['dt_desc'].item())
                    order = DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == US_t.index[ind][0]]['order'].item()
                    if str(US_t.index[ind][1]).find('Unnamed') < 0 and bool(re.search(r'[0-9]+$', str(US_t.index[ind][1]))):
                        lab = lab+str(US_t.index[ind][1])[re.search(r'[0-9]+$', str(US_t.index[ind][1])).start():]
                    new_label.append(lab)
                    new_order.append(order)
                    suffix = 'US'+str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == US_t.index[ind][0]].index.item())
                elif str(US_t.index[ind][0]) in list(DIY_series['GEO LEVELS']['name']):
                    geography = str(US_t.index[ind][0])
                    new_label.append(str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == US_t.index[ind][1]]['dt_desc'].item()))
                    new_order.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == US_t.index[ind][1]]['order'].item())
                    suffix = str(DIY_series['GEO LEVELS'].loc[DIY_series['GEO LEVELS']['name'] == geography].index.item()) + str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == US_t.index[ind][1]].index.item())
                else:
                    new_label.append(str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == US_t.index[ind][1]]['dt_desc'].item()))
                    new_order.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == US_t.index[ind][1]]['order'].item())
                    suffix = str(DIY_series['GEO LEVELS'].loc[DIY_series['GEO LEVELS']['name'] == geography].index.item()) + str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == US_t.index[ind][1]].index.item())
                if suffix == '' and freq != 'Q':
                    new_index.append('nan')
                    new_label.append('nan')
                    new_order.append(10000)
                elif freq != 'Q':
                    new_index.append(prefix+middle+suffix)
        elif address.find('HSHD') >= 0:
            for ind in range(US_t.shape[0]):
                suffix = ''
                if str(US_t.index[ind][0]).find('Unnamed') < 0:
                    name = str(US_t.index[ind][0])
                if str(US_t.index[ind][1]).find('Unnamed') < 0:
                    key = str(US_t.index[ind][1])
                else:
                    key ='None'
                if DIY_series['DATA TYPES'].loc[(DIY_series['DATA TYPES']['name'] == name) & (DIY_series['DATA TYPES']['key_desc'] == key)].empty == False:
                    suffix = DIY_series['DATA TYPES'].loc[(DIY_series['DATA TYPES']['name'] == name) & (DIY_series['DATA TYPES']['key_desc'] == key)].index[0]
                    new_label.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix]['dt_desc'].item())
                    new_order.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix]['order'].item())
                if suffix == '':
                    new_index.append('nan')
                    new_label.append('nan')
                    new_order.append(10000)
                else:
                    new_index.append(prefix+middle+suffix)
            US_t = US_t.iloc[:, ::-1]
        elif address.find('MRTS') >= 0:
            for ind in range(US_t.shape[0]):
                if str(US_t.index[ind]) in list(DIY_series['CATEGORIES']['cat_desc']):
                    new_index.append(prefix+middle+suffix)
                    new_label.append(DIY_series['CATEGORIES'].loc[DIY_series['CATEGORIES'].index == int(middle[:3])]['cat_desc'].item())
                    new_order.append(DIY_series['CATEGORIES'].loc[DIY_series['CATEGORIES'].index == int(middle[:3])]['order'].item())
                else:
                    new_index.append('nan')
                    new_label.append('nan')
                    new_order.append(10000)
            US_t = US_t.iloc[:, ::-1]
        elif address.find('NAHB') >= 0:
            new_index.append(prefix+middle+suffix)
            new_label.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix[-3:]]['dt_desc'].item())
            new_order.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix[-3:]]['order'].item())
            US_t.columns = MONTH
            US_t = US_HISTORYDATA(US_t.reset_index(), name='index', MONTH=MONTH)
        elif address.find('POPT') >= 0:
            for ind in range(US_t.shape[0]):
                if str(US_t.index[ind]) != 'nan':
                    if str(US_t.index[ind]).find('Under') >= 0:
                        year = 'LT'+re.sub(r'[a-zA-Z\s]+', "", str(US_t.index[ind]))
                    elif str(US_t.index[ind]).find('over') >= 0:
                        year = 'GE'+re.sub(r'[a-zA-Z\s]+', "", str(US_t.index[ind]))
                    elif str(US_t.index[ind]).find('5 to 9 years') >= 0:
                        year = '0509'
                    elif str(US_t.index[ind]).find('5 to 13 years') >= 0:
                        year = '0513'
                    else:
                        year = re.sub(r'[a-zA-Z\s]+', "", str(US_t.index[ind]))
                    new_index.append(prefix+middle+suffix+year)
                    new_label.append(DIY_series['DATA TYPES'].loc[suffix, 'dt_desc']+', '+str(US_t.index[ind]))
                    new_order.append(DIY_series['DATA TYPES'].loc[suffix, 'order'])
                else:
                    new_index.append('nan')
                    new_label.append('nan')
                    new_order.append(10000)
        elif address.find('SHIP') >= 0:
            new_columns = []
            """month = [datetime.strptime(m,'%b').strftime('%B') for m in MONTH]
            if freq == 'M':
                for ind in range(US_t.shape[0]):
                    prefix = ''
                    if str(US_t.index[ind]).strip() in list(DIY_series['ISADJUSTED']['name']):
                        new_label.append(str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix[-3:]]['dt_desc'].item()))
                        new_order.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix[-3:]]['order'].item())
                        prefix = str(DIY_series['ISADJUSTED'].loc[DIY_series['ISADJUSTED']['name'] == US_t.index[ind].strip()].index.item())
                        new_columns.append(str(DIY_series['ISADJUSTED'].loc[DIY_series['ISADJUSTED']['name'] == US_t.index[ind].strip()]['adj_desc'].item()))
                    if prefix == '':
                        new_index.append('nan')
                        new_label.append('nan')
                        new_columns.append('Unnamed: '+str(ind))
                        new_order.append(10000)
                    else:
                        new_index.append(prefix+middle+suffix)
                US_t.index = new_columns
                new_columns = []
                US_temp = readExcelFile(data_path+address+'shipment.xls'+x, header_=header, index_col_=index_col, usecols_=list(range(4)), skiprows_=list(range(2)), nrows_=13, sheet_name_=sname, acceptNoFile=False).T
                for ind in range(US_temp.shape[0]):
                    if str(US_temp.index[ind]).strip() in list(DIY_series['ISADJUSTED']['adj_desc']):
                        new_columns.append(str(US_temp.index[ind]).strip())
                    else:
                        new_columns.append('Unnamed: 3')
                US_temp.index = new_columns
                US_t = pd.concat([US_t, US_temp], axis=1)
            elif freq == 'A':"""
            if fname == 'shipment':
                US_temp1 = US_t[US_t.columns[0:2]].set_index([US_t.columns[0]])
                US_temp1.columns = ['Not Seasonally']
                US_temp2 = US_t[US_t.columns[2:4]].set_index([US_t.columns[2]])
                US_temp2.columns = ['Not Seasonally']
                US_t = pd.concat([US_temp1, US_temp2]).T
                US_temp = readExcelFile(data_path+address+'shiphist.xls'+x, header_=header, index_col_=0, usecols_=[0,2], skiprows_=list(range(3)), sheet_name_=sname).T
                if US_temp.empty == False:
                    US_temp.index = ['Not Seasonally']
                    US_t = pd.concat([US_temp, US_t], axis=1)
            year = ''
            for col in range(US_t.shape[1]):
                if re.sub(r'([0-9]+)[a-z\s\*]+$', r"\1", str(US_t.columns[col]).strip()).strip().isnumeric():
                    year = re.sub(r'([0-9]+)[a-z\s\*]+$', r"\1", str(US_t.columns[col]).strip()).strip()
                    new_columns.append(None)
                elif str(US_t.columns[col]).find('Total') >= 0:
                    if year != '':
                        new_columns.append(year)
                        year = ''
                    else:
                        print(new_columns)
                        ERROR('Year not found')
                else:
                    new_columns.append(None)
            US_t.columns = new_columns
            US_t = US_t.loc[:, US_t.columns.dropna()]
            US_t = US_t.loc[:, ~US_t.columns.duplicated()]
            US_t = US_t.sort_index(axis=1)
            for ind in range(US_t.shape[0]):
                if str(US_t.index[ind]).strip() == 'Not Seasonally':
                    new_label.append(str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix[-3:]]['dt_desc'].item()))
                    new_order.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix[-3:]]['order'].item())
                    #US_t = US_HISTORYDATA(US_t.loc[US_t.index[ind]].T.reset_index(), name='Period', MONTH=month, summ=True)
                    new_index.append(prefix+middle+suffix)
                    break
        elif address.find('URIN') >= 0:
            US_t = US_t.rename(columns={'Label':'old_label'})
            new_label = list(US_t['old_label'])
            US_t = US_t.iloc[:, ::-1]
        
        if address.find('CBRT') < 0 and address.find('URIN') < 0:
            for item in new_order:
                if type(item) == pd.core.series.Series:
                    print(item)
                    ERROR('Order type incorrect: '+str(item.index[0]))
            US_t.insert(loc=0, column='Index', value=new_index)
            US_t.insert(loc=1, column='order', value=new_order)
        US_t = US_t.set_index('Index', drop=False)
        if address.find('CONS') >= 0:
            US_t = US_t.rename(columns={'Date':'Label'})
            US_t = US_t.iloc[:, ::-1]
        elif address.find('HOUS') >= 0 or address.find('MRTS') >= 0 or address.find('APEP') >= 0 or address.find('DSCO') >= 0 or address.find('FTD') >= 0:
            for item in new_label:
                if type(item) == pd.core.series.Series:
                    print(item)
                    ERROR('Label type incorrect: '+str(item.index[0]))
            US_t.insert(loc=1, column='Label', value=new_label)
        if address.find('DSCO') >= 0:
            US_t.insert(loc=3, column='start', value=new_start)
            US_t.insert(loc=4, column='last', value=new_last)
        US_t = US_t.sort_values(by=['order','Label'])
        label = US_t['Label']
        if address.find('MRTS') >= 0 and freq == 'Q':
            label = pd.Series(['Retail Trade and Food Services','Retail Trade'], index=['U44X7200SMR','U4400000SMR']).append(label)
        
    return US_t, label, note, footnote

def HIES_OLD(prefix, middle, data_path, address, fname=None, sname=None, DIY_series=None):
    note = []
    footnote = []
    QUARTER = {'1st Qtr':'Q1','2nd Qtr':'Q2','3rd Qtr':'Q3','4th Qtr':'Q4'}
    QUARTER2 = {'First':'Q1','Second':'Q2','Third':'Q3','Fourth':'Q4'}

    HIES = readExcelFile(data_path+address+fname+'.xlsx', sheet_name_=sname, acceptNoFile=False)  
    note2, footnote2 = US_NOTE(HIES[0], sname, address=address)
    note = note + note2
    footnote = footnote + footnote2
    tables = {}
    for h in range(HIES.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((h+1)*100/HIES.shape[0], 1))+"%)*")
        sys.stdout.flush()
        if fname == 'histtab8' and str(HIES.iloc[h][1])[:4].isnumeric() and str(HIES.iloc[h][0]) == 'nan':
            table_head = h+1
            for i in range(h+2, HIES.shape[0]):
                if str(HIES.iloc[i][0]).find('Renter') >= 0:
                    table_tail = i
                    break
            tables[str(HIES.iloc[h][1])[:4]] = readExcelFile(data_path+address+fname+'.xlsx', header_ = 0, index_col_ = 0, skiprows_ = list(range(table_head)), nrows_ = table_tail - table_head, sheet_name_=sname, usecols_=list(range(5)))
            if tables[str(HIES.iloc[h][1])[:4]].empty == True:
                ERROR('Sheet Not Found: '+data_path+address+fname+'.xlsx'+', sheet name: '+sname)
            index = []
            for dex in tables[str(HIES.iloc[h][1])[:4]].columns:
                index.append(str(HIES.iloc[h][1])[:4]+'-'+QUARTER[dex])
            tables[str(HIES.iloc[h][1])[:4]].columns = index
        elif fname == 'histtab10' and bool(re.search(r'[Qq]uarter', str(HIES.iloc[h][1]))) == True and str(HIES.iloc[h][0]) == 'nan':
            quarter = str(HIES.iloc[h][1])[:re.search(r'[Qq]uarter', str(HIES.iloc[h][1])).start()-1]
            if bool(re.search(r'r[0-9]$', str(HIES.iloc[h][1]))):
                date = str(HIES.iloc[h][1])[-6:-2]+'-'+QUARTER2[quarter]
            else:
                date = str(HIES.iloc[h][1])[-4:]+'-'+QUARTER2[quarter]
            table_head = h+2
            for i in range(h+3, HIES.shape[0]):
                if str(HIES.iloc[i][0]).find('Renter') >= 0:
                    table_tail = i
                    break
            tables[date] = readExcelFile(data_path+address+fname+'.xlsx', header_ = 0, index_col_ = 0, skiprows_ = list(range(table_head)), nrows_ = table_tail - table_head, sheet_name_=sname, usecols_=list(range(5)))
            if tables[date].empty == True:
                ERROR('Sheet Not Found: '+data_path+address+fname+'.xlsx'+', sheet name: '+sname)
            tables[date].index.names = ['index']
            index = []
            for dex in tables[date].index:
                new_dex = re.sub(r"[^A-Za-z\s\-',]+", "", str(dex))
                if new_dex == 'Rented or Sold':
                    new_dex = 'Rented or sold'
                index.append(new_dex)
            tables[date].index = index
            if 'Rented, not yet occupied' in tables[date].index:
                new_row = [0, 0, 0, 0]
                drop_dex = []
                for d in range(tables[date].shape[0]):
                    if tables[date].index[d].find('not yet occupied') >= 0:
                        drop_dex.append(tables[date].index[d])
                        for e in range(len(tables[date].iloc[d])):
                            new_row[e] = new_row[e] + tables[date].iloc[d][e]
                for drop in drop_dex:
                    tables[date] = tables[date].drop(index=drop)
                new_df = pd.DataFrame([new_row], index=['Rented or sold'], columns=tables[date].columns)
                tables[date] = pd.concat([tables[date], new_df])
            tables[date] = tables[date][~tables[date].index.duplicated()]
            region = []
            for dex in tables[date].columns:
                if bool(re.search(r'\.[0-9]+$', dex)):
                    dex = re.sub(r'\.[0-9]+$', "", dex)
                region.append(dex)
            new_table = pd.concat([pd.Series(list(tables[date][dex]), index=tables[date].index) for dex in tables[date].columns], keys=region)
            tables[date] = new_table
    sys.stdout.write("\n\n")
    
    US_t = pd.DataFrame()
    for key in tables:
        tables[key] = tables[key][~tables[key].index.duplicated()]
        US_t = pd.concat([US_t, tables[key]], axis=1)
    if fname == 'histtab10':
        US_t.columns = list(tables)
    
    new_index = []
    new_label = []
    new_order = []
    geography = ''
    for ind in range(US_t.shape[0]):
        suffix = ''
        if fname == 'histtab8':
            dex = re.sub(r"[^A-Za-z\s\-']+", "", str(US_t.index[ind]))
            if dex in list(DIY_series['DATA TYPES']['name']):
                new_label.append(str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == dex]['dt_desc'].item()))
                new_order.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == dex]['order'].item())
                suffix = 'US'+str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == dex].index.item())
        elif fname == 'histtab10':
            if US_t.index[ind][0] in list(DIY_series['GEO LEVELS']['name']) and US_t.index[ind][1] != 'nan':
                geography = US_t.index[ind][0]
                dex = US_t.index[ind][1]
                new_label.append(str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == dex]['dt_desc'].item()))
                new_order.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == dex]['order'].item())
                suffix = str(DIY_series['GEO LEVELS'].loc[DIY_series['GEO LEVELS']['name'] == geography].index.item()) + str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['name'] == dex].index.item())
        if suffix == '':
            new_index.append('nan')
            new_label.append('nan')
            new_order.append(10000)
        else:
            new_index.append(prefix+middle+suffix)
    US_t.insert(loc=0, column='Index', value=new_index)
    US_t = US_t.set_index('Index', drop=False)
    US_t.insert(loc=1, column='Label', value=new_label)
    US_t.insert(loc=2, column='order', value=new_order)
    US_t = US_t.sort_values(by='order')
    label = US_t['Label']

    return US_t, label, note, footnote

def US_IHS(US_temp, Series, freq):
    #AREMOS = pd.DataFrame()
    note = []
    footnote = [['NSA','Not Seasonally Adjusted'],['SAAR','Seasonally Adjusted Annual Rate'],[' - United States',''],['Total,','United States Total,'],['North East','Northeast'],['Mid West','Midwest']]
    TYPE = ['Northeast','Midwest','South','West']
    FORMC = {'NSA':'Not Seasonally Adjusted','SAAR':'Seasonally Adjusted Annual Rate'}
    UNIT = {'Fixed':'Composite Index','Pending Home Sales Index':'Index',"Month's Supply":'Percentage','Single Family Home':'Thousands of Housing Units','Prices':'U.S. Dollars','Mortgage Rate':'Percentage','Monthly Payment as a Percent of Income':'Percentage','Housing Affordability':'U.S. Dollars','First Time Buyer Index':'Composite Index'}

    new_index = []
    description = []
    unit = []
    new_type = []
    form_e = []
    form_c = []
    for i in range(US_temp.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/US_temp.shape[0], 1))+"%)*")
        sys.stdout.flush()
        found = False
        key = str(US_temp.index[i])
        if freq == 'Q':
            key = key.replace('.Q','')
        for j in range(Series.shape[0]):
            if key.replace('.HIST','') == Series.iloc[j]['code']:
                found = True
                unit_found = False
                type_found = False
                form_cf = False
                if freq == 'Q':
                    new_index.append(Series.iloc[j]['code'])
                else:
                    new_index.append(Series.iloc[j]['code'][:-2])
                uni = Series.iloc[j]['unit']
                for uni in UNIT:
                    if US_temp.iloc[i]['Short Label'].find(uni) >= 0:
                        unit_found = True
                        unit.append(UNIT[uni])
                        break
                if unit_found == False:
                    unit.append('Housing Units')
                loc1 = US_temp.iloc[i]['Short Label'].find(',')+2
                loc2 = US_temp.iloc[i]['Short Label'].find(',',loc1)
                if loc2 < 0:
                    loc2 = US_temp.iloc[i]['Short Label'].find('-',loc1)-1
                form_e.append(US_temp.iloc[i]['Short Label'][loc1:loc2])
                description.append(US_temp.iloc[i]['Short Label'][loc2+2:].replace("'s",""))
                for adj in FORMC:
                    if US_temp.iloc[i]['Short Label'].find(adj) >= 0:
                        form_cf = True
                        form_c.append(FORMC[adj])
                        break
                if form_cf == False:
                    if freq == 'A':
                        form_c.append('Annual')
                    elif Series.iloc[j]['code'].find('NS') > 0:
                        form_c.append('Not Seasonally Adjusted')
                    else:
                        form_c.append('Seasonally Adjusted Annual Rate')
                for typ in TYPE:
                    if US_temp.iloc[i]['Short Label'].find(typ) >= 0:
                        type_found = True
                        if US_temp.iloc[i]['Short Label'].find('Mid West') >= 0:
                            new_type.append('Midwest')
                        else:
                            new_type.append(typ)
                        break
                if type_found == False:
                    if US_temp.iloc[i]['Short Label'].find('North East') >= 0:
                        new_type.append('Northeast')
                    else:
                        new_type.append('United States Total')
                #AREMOS = AREMOS.append(Series.iloc[j])
                break
        if found == False:
            new_index.append('nan')
            description.append('nan')
            unit.append('nan')
            new_type.append('nan')
            form_e.append('nan')
            form_c.append('nan')
    sys.stdout.write("\n\n")

    US_temp.insert(loc=0, column='Index', value=new_index)
    US_temp.insert(loc=1, column='Label', value=description)
    US_temp.insert(loc=2, column='unit', value=unit)
    US_temp.insert(loc=3, column='type', value=new_type)
    US_temp.insert(loc=4, column='form_e', value=form_e)
    US_temp.insert(loc=5, column='form_c', value=form_c)
    US_temp = US_temp.set_index('Index', drop=False)
    label = US_temp['Label']

    #AREMOS.to_excel(out_path+"NAR_series"+str(AREMOS.shape[0])+".xlsx", sheet_name='NAR_series')
    return US_temp, label, note, footnote

def US_BLS(US_temp, Table, freq, YEAR, QUAR, index_base, address, start=None, key='main', key2='main', lab_base='series_title', find_unknown=False):
    MONTH = ['JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE','JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER']
    SEMI = ['S01','S02']
    MON = ['M01','M02','M03','M04','M05','M06','M07','M08','M09','M10','M11','M12']
    note = []
    footnote = []
    US_temp = US_temp.sort_values(by=['series_id','year','period'], ignore_index=True)
    
    US_t = pd.DataFrame()
    new_item_t = []
    new_index_t = []
    new_code_t = []
    new_label_t = []
    new_unit_t = []
    new_dataframe = []
    new_start_t = []
    new_last_t = []
    firstfound = False
    code = ''
    for i in range(US_temp.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/US_temp.shape[0], 1))+"%)*")
        sys.stdout.flush()
        if (freq == 'M' and US_temp.iloc[i]['period'] not in MON) or (freq == 'A' and US_temp.iloc[i]['period'] not in YEAR[key]) or (freq == 'S' and US_temp.iloc[i]['period'] not in SEMI)\
            or (freq == 'Q' and US_temp.iloc[i]['period'] not in QUAR[key2]):
            continue
        if US_temp.iloc[i]['series_id'] != code:
            if firstfound == True:
                new_dataframe.append(new_item_t)
                US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
                if US_new.empty == False:
                    new_code_t.append(code)
                    new_label_t.append(lab)
                    new_unit_t.append(unit)
                    new_start_t.append(str(Table['begin_year'][code])+'-'+str(Table['begin_period'][code]))
                    new_last_t.append(str(Table['end_year'][code])+'-'+str(Table['end_period'][code]))
                    US_t = pd.concat([US_t, US_new], ignore_index=True)
                new_dataframe = []
                new_item_t = []
                new_index_t = []
            code = US_temp.iloc[i]['series_id']
            lab = Table[lab_base][code]
            month = ''
            if address.find('ln/') < 0 and address.find('ce/') < 0 and address.find('ec/') < 0 and address.find('bd/') < 0 and address.find('jt/') < 0 and address.find('in/') < 0 and address.find('ml/') < 0:
                if bool(re.search(r'=\s*100', str(Table[index_base][code]))) and bool(re.match(r'[A-Za-z]+', str(Table[index_base][code]))):
                    for m in MONTH:
                        if str(Table[index_base][code]).find(m) >= 0 or str(Table[index_base][code]).find(m.capitalize()) >= 0:
                            month = str(datetime.strptime(m,'%B').month).rjust(2,'0')
                            unit = 'Index base: '+re.sub(r'[A-Za-z]+\s*', "", re.sub(r'(\s*=\s*100)', "."+month+r"\1", str(Table[index_base][code])))
                elif bool(re.search(r'=\s*100', str(Table[index_base][code]))):
                    unit = 'Index base: '+str(Table[index_base][code])
                elif (address.find('pr/') >= 0 or address.find('mp/') >= 0) and str(Table[index_base][code]).isnumeric():
                    unit = 'Index base: '+str(Table[index_base][code])+' = 100'
                elif address.find('pr/') >= 0 or address.find('mp/') >= 0:
                    unit = Table['duration_code'][code]
                else:
                    if str(Table[index_base][code])[-2:] != '00':
                        month = '.'+str(Table[index_base][code])[-2:]
                    unit = 'Index base: '+str(Table[index_base][code])[:4]+month+' = 100'
                if Table[index_base][code] == 0:
                    unit = 'Index base'
            else:
                unit = Table[index_base][code]
            firstfound = True
        if address.find('bd/') >= 0 and code != '':
            if Table['state_code'][code] != 0:
                continue
        if start != None and find_unknown == False:
            if US_temp.iloc[i]['year'] < start:
                continue
        new_item_t.append(US_temp.iloc[i]['value'])
        if freq == 'M' or freq == 'S':
            period_index = str(US_temp.iloc[i]['year'])+'-'+str(US_temp.iloc[i]['period']).replace('M','').replace('S0','S')
        elif freq == 'A':
            period_index = US_temp.iloc[i]['year']
        elif freq == 'Q':
            period_index = str(US_temp.iloc[i]['year'])+'-'+QUAR[key2][str(US_temp.iloc[i]['period'])]
        new_index_t.append(period_index)  
    sys.stdout.write("\n\n")
    new_dataframe.append(new_item_t)
    US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
    if US_new.empty == False:
        new_code_t.append(code)
        new_label_t.append(lab)
        new_unit_t.append(unit)
        new_start_t.append(str(Table['begin_year'][code])+'-'+str(Table['begin_period'][code]))
        new_last_t.append(str(Table['end_year'][code])+'-'+str(Table['end_period'][code]))
        US_t = pd.concat([US_t, US_new], ignore_index=True)
    US_t = US_t.sort_index(axis=1)
    US_t.insert(loc=0, column='Index', value=new_code_t)
    US_t.insert(loc=1, column='Label', value=new_label_t)
    US_t.insert(loc=2, column='unit', value=new_unit_t)
    US_t.insert(loc=3, column='start', value=new_start_t)
    US_t.insert(loc=4, column='last', value=new_last_t)
    US_t = US_t.set_index('Index', drop=False)
    label = US_t['Label']

    return US_t, label, note, footnote

def US_POPP(US_temp, data_path, address, datasets, DIY_series, password=''):
    note = []
    footnote = []
    SUM = {'LT5':[0,4], 'LT18':[0,17], 'GE65':[65,120], 'GE16':[16,120], 'GE18':[18,120],\
         '0509':[5,9], '1014':[10,14], '1519':[15,19], '2024':[20,24], '2529':[25,29], '3034':[30,34], '3539':[35,39], '4044':[40,44], '4549':[45,49],\
              '5054':[50,54], '5559':[55,59], '6064':[60,64], '6569':[65,69], '7074':[70,74], '7579':[75,79], '8084':[80,84], '8589':[85,89], '9094':[90,94], '9599':[95,99],\
                  '0513':[5,13], '1417':[14,17], '1864':[18,64], '1824':[18,24], '2544':[25,44], '4564':[45,64], '1544':[15,44]}
    ORDER = list(DIY_series['INDEX']['idx'])
    US_temp = US_temp.sort_values(by=ORDER, ignore_index=True)
    US_temp = US_temp.set_index(ORDER)
    US_temp = US_temp.T
    age = []
    for i in range(US_temp.shape[0]):
        year = re.sub(r'[a-zA-Z_]+', "", str(US_temp.index[i]))
        if year.isnumeric():
            year = int(year)
        elif str(US_temp.index[i]).upper().find('TOTAL') >= 0:
            year = -1
        age.append(year)
    US_temp.insert(loc=0, column=tuple(ORDER), value=age)
    US_temp = US_temp.set_index(tuple(ORDER), drop=False)
    US_temp.index.name = None
    for ages in SUM:
        US_temp = US_temp.append(US_temp[(US_temp[tuple(ORDER)] >= SUM[ages][0]) & (US_temp[tuple(ORDER)] <= SUM[ages][1])].sum().rename(ages))
    try:
        SEX = US_temp.columns.names.index('SEX')
        YEAR = US_temp.columns.names.index('YEAR')
    except ValueError:
        SEX = US_temp.columns.names.index('sex')
        YEAR = US_temp.columns.names.index('year')
    
    US_t = pd.DataFrame()
    firstfound = False
    column = US_temp.columns[0][:YEAR]
    for i in range(US_temp.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/US_temp.shape[0], 1))+"%)*")
        sys.stdout.flush()
        for j in range(US_temp.shape[1]):
            CONTINUE = False
            for name in range(len(US_temp.columns.names)):
                if name != SEX and name != YEAR and US_temp.columns[j][name] != 0:
                    CONTINUE = True
                    break
            if CONTINUE == True:
                continue
            if j == 0 or US_temp.columns[j][SEX] != US_temp.columns[j-1][SEX] or US_temp.columns[j][:YEAR] != column:
                if firstfound == True:
                    new_dataframe.append(new_item_t)
                    US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
                    US_t = pd.concat([US_t, US_new], ignore_index=True)
                new_dataframe = []
                new_item_t = []
                new_index_t = ['Index', 'Label', 'order']
                prefix = str(DIY_series['ISADJUSTED'].iloc[0]['aremos_key'])
                middle = str(DIY_series['CATEGORIES'].iloc[0]['aremos_key'])
                suffix = str(DIY_series['DATA TYPES'].loc[US_temp.columns[j][SEX], 'aremos_key'])
                lab = DIY_series['DATA TYPES'].loc[US_temp.columns[j][SEX], 'dt_desc']+', '
                year = str(US_temp.index[i])
                if year == '-1':
                    lab = lab + 'Total Population'
                    year = ''
                elif year == '0':
                    lab = lab + 'Under 1 year old'
                    year ='LT1'
                elif year == '1':
                    lab = lab + '1 year old'
                elif year == '100':
                    lab = lab + '100 years old and over'
                    year = 'GE'+year
                elif year.find('LT') >= 0:
                    lab = lab + 'Under '+year.replace('LT','')+' years old'
                elif year.find('GE') >= 0:
                    lab = lab +  year.replace('GE','')+' years old and over'
                elif len(year) >= 4:
                    lab = lab +  str(int(year[:2]))+' to '+str(int(year[2:]))+' years old'
                else:
                    lab = lab + re.sub(r'[a-zA-Z_]+', "", str(US_temp.index[i])) + ' years old'
                code = prefix+middle+suffix+year
                order = DIY_series['DATA TYPES'].loc[US_temp.columns[j][SEX], 'order']
                new_item_t.extend([code, lab, order])
                firstfound = True
                column = US_temp.columns[j][:YEAR]
            new_item_t.append(US_temp.iloc[i][US_temp.columns[j]])
            new_index_t.append(US_temp.columns[j][YEAR])   
    sys.stdout.write("\n\n")
    new_dataframe.append(new_item_t)
    US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
    if US_new.empty == False:
        US_t = pd.concat([US_t, US_new], ignore_index=True)
    US_t = US_t.set_index('Index', drop=False)
    US_t = US_t.sort_values(by='order')
    label = US_t['Label']

    return US_t, label, note, footnote

def US_FAMI(prefix, middle, data_path, address, fname, sname, DIY_series, x=''):
    note = []
    footnote = []
    formnote = {}
    found = {'Both Sexes':False,'Male':False,'Female':False}

    US_temp = readExcelFile(data_path+address+fname+'.xls'+x, sheet_name_=sname, acceptNoFile=False) 
    tables = {}
    for h in range(US_temp.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((h+1)*100/US_temp.shape[0], 1))+"%)*")
        sys.stdout.flush()
        allfound = True
        if address.find('FAMI') >= 0 and str(US_temp.iloc[h][0]) in list(DIY_series['DATA TYPES']['key_desc']):
            table_head = h+1
            for i in range(table_head+2, US_temp.shape[0]):
                if str(US_temp.iloc[i][0]) in list(DIY_series['DATA TYPES']['key_desc']) or bool(re.search(r'[^0-9\(\)\s]+', str(US_temp.iloc[i][0]))):
                    table_tail = i-1
                    break
            if str(US_temp.iloc[h][0]) == 'All Families':
                use = [0,1,6]
                names = ['Year', str(US_temp.iloc[h][0]), str(US_temp.iloc[h+1][6])]
            else:
                use = [0,1]
                names = ['Year', str(US_temp.iloc[h][0])]
            tables[str(US_temp.iloc[h][0])] = readExcelFile(data_path+address+fname+'.xls'+x, header_ = 0, index_col_ = 0, skiprows_ = list(range(table_head)), nrows_ = table_tail - table_head, sheet_name_=sname, usecols_=use, names_=names)
            if tables[str(US_temp.iloc[h][0])].empty == True:
                ERROR('Sheet Not Found: '+data_path+address+fname+'.xls'+x+', sheet name: '+sname)
            index = []
            for dex in tables[str(US_temp.iloc[h][0])].index:
                dex = re.sub(r'\s+[\(0-9\)]+', "", str(dex)).strip()
                if dex.isnumeric():
                    dex = int(dex)
                index.append(dex)
            tables[str(US_temp.iloc[h][0])].index = index
        elif address.find('MADI') >= 0 and str(US_temp.iloc[h][0]).find('White') >= 0:
            table_tail = h-2
            tables['All races'] = readExcelFile(data_path+address+fname+'.xls'+x, header_ = (0,1,2,3), index_col_ = 0, skiprows_ = list(range(6)), nrows_ = table_tail-10, sheet_name_=sname)
            if tables['All races'].empty == True:
                ERROR('Sheet Not Found: '+data_path+address+fname+'.xls'+x+', sheet name: '+sname)
            index = []
            for dex in tables['All races'].index:
                dex = re.sub(r'[\.\*]+|[a-z]+$', "", str(dex)).strip()
                if dex.isnumeric():
                    dex = int(dex)
                index.append(dex)
            tables['All races'].index = index
            note2, footnote2 = US_NOTE(US_temp[0], sname, address=address)
            note = note + note2
            footnote = footnote2
            break
        elif address.find('SCEN') >= 0 and str(US_temp.iloc[h][0]).find('Year,') >= 0:
            table = readExcelFile(data_path+address+fname+'.xls'+x, header_ = (0,1), index_col_ = 0, skiprows_ = list(range(h)), sheet_name_=sname)
            note2, footnote2 = US_NOTE(US_temp[0], sname, address=address)
            note = note + note2
            footnote = footnote2
        elif address.find('SCEN') >= 0 and fname.find('tablea-1') >= 0 and re.sub(r'[0-9]+', "", str(US_temp.iloc[h][0]).strip()) in list(DIY_series['CATEGORIES']['key_desc']):
            table_head = h+1
            for i in range(table_head+2, US_temp.shape[0]):
                if str(US_temp.iloc[i][0]) in list(DIY_series['CATEGORIES']['key_desc']) or bool(re.search(r'[^0-9\'\s]+', str(US_temp.iloc[i][1]))):
                    table_tail = i
                    break
            tables[str(US_temp.iloc[h][0])] = readExcelFile(data_path+address+fname+'.xls'+x, index_col_ = 0, skiprows_ = list(range(table_head)), nrows_ = table_tail - table_head, sheet_name_=sname, header_=None)
            if tables[str(US_temp.iloc[h][0])].empty == True:
                ERROR('Sheet Not Found: '+data_path+address+fname+'.xls'+x+', sheet name: '+sname)
            tables[str(US_temp.iloc[h][0])].columns = table.columns
            index = []
            for dex in tables[str(US_temp.iloc[h][0])].index:
                dex = str(dex).replace("'",'')[:4]
                if dex.isnumeric():
                    dex = int(dex)
                index.append(dex)
            tables[str(US_temp.iloc[h][0])].index = index
        elif address.find('SCEN') >= 0 and fname.find('tablea-2') >= 0 and re.sub(r'\.+', "", str(US_temp.iloc[h][0]).strip()) in list(DIY_series['DATA TYPES']['key1']):
            key = re.sub(r'\.+', "", str(US_temp.iloc[h][0]).strip())
            found[key] = True
            table_head = h+1
            for i in range(table_head+2, US_temp.shape[0]):
                if str(US_temp.iloc[i][0]) in list(DIY_series['DATA TYPES']['key1']) or bool(re.search(r'[^0-9\.\s]+', str(US_temp.iloc[i][1]))):
                    table_tail = i
                    break
            tables[key] = readExcelFile(data_path+address+fname+'.xls'+x, index_col_ = 0, skiprows_ = list(range(table_head)), nrows_ = table_tail - table_head, sheet_name_=sname, header_=None)
            if tables[key].empty == True:
                ERROR('Sheet Not Found: '+data_path+address+fname+'.xls'+x+', sheet name: '+sname)
            tables[key].columns = table.columns
            old_data = pd.DataFrame()
            for col in range(len(tables[key].columns)):
                founded = False
                for val in range(tables[key][tables[key].columns[col]].shape[0]):
                    if str(tables[key][tables[key].columns[col]].iloc[val]).find('-') >= 0:
                        if founded == False:
                            old_data = pd.concat([old_data, tables[key][tables[key].columns[col]]], axis=1)
                            founded = True
                        tables[key].loc[tables[key].index[val], tables[key].columns[col]] = 'nan'
            new_columns = []
            for col in range(len(old_data.columns)):
                if str(old_data.columns[col][1]) == '20 and 21 years':
                    new_columns.append((old_data.columns[col][0], '20 to 24 years'))
                else:
                    new_columns.append(old_data.columns[col])
                for val in range(len(old_data.index)):
                    if str(old_data[old_data.columns[col]].loc[old_data.index[val]]).find('-') < 0:
                        old_data[old_data.columns[col]].loc[old_data.index[val]] = 'nan'
                    else:
                        old_data[old_data.columns[col]].loc[old_data.index[val]] = float(str(old_data[old_data.columns[col]].loc[old_data.index[val]]).replace('-',''))
            old_data.columns = new_columns
            tables[key] = pd.concat([tables[key], old_data], axis=1)
            index = []
            for dex in tables[key].index:
                dex = str(dex).replace("'",'')[:4]
                if dex.isnumeric():
                    dex = int(dex)
                index.append(dex)
            tables[key].index = index
            for f in found:
                if found[f] == False:
                    allfound = False
                    break
            if allfound == True:
                break
    sys.stdout.write("\n\n")
    
    US_t = pd.DataFrame()
    for key in tables:
        tables[key] = tables[key][~tables[key].index.duplicated()]
    if address.find('SCEN') >= 0:
        US_t = pd.concat([tables[key] for key in tables], axis=1, keys=[key for key in tables])
    else:
        US_t = pd.concat([tables[key] for key in tables], axis=1)
    US_t = US_t.T
    if fname.find('tablea-1') < 0:
        US_t = US_t.iloc[:, ::-1]
    
    new_index = []
    new_label = []
    new_order = []
    keys = {0:'None',1:'None',2:'None',3:'None'}
    changed = False
    for ind in range(US_t.shape[0]):
        suffix = ''
        if address.find('FAMI') >= 0:
            dex = str(US_t.index[ind])
            if dex in list(DIY_series['DATA TYPES']['key_desc']):
                suffix = str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['key_desc'] == dex].index.item())
                new_label.append(str(DIY_series['DATA TYPES'].loc[suffix, 'dt_desc']))
                new_order.append(DIY_series['DATA TYPES'].loc[suffix, 'order'])
        elif address.find('MADI') >= 0:
            middle = ''
            for j in range(len(US_t.index[ind])):
                if str(US_t.index[ind][j]).find('Unnamed') < 0:
                    n = ''
                    if bool(re.search(r'[0-9]+$', str(US_t.index[ind][j]))):
                        n = re.findall(r'[0-9]+$', str(US_t.index[ind][j]))[0]
                    keys[j] = re.sub(r'[0-9]+$', "", str(US_t.index[ind][j])).strip()
                    if j == 0:
                        changed = True
                elif j == 2 or j == 3:
                    keys[j] = 'None'
                elif j == 1 and changed == True:
                    keys[j] = 'None'
                    changed = False
            if keys[0] in list(DIY_series['DATA TYPES']['key_desc']):
                suffix = str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES']['key_desc'] == keys[0]].index.item())
                if DIY_series['CATEGORIES'].loc[(DIY_series['CATEGORIES']['key1'] == keys[1]) & (DIY_series['CATEGORIES']['key2'] == keys[2]) & (DIY_series['CATEGORIES']['key3'] == keys[3])].empty == False:
                    middle = str(DIY_series['CATEGORIES'].loc[(DIY_series['CATEGORIES']['key1'] == keys[1]) & (DIY_series['CATEGORIES']['key2'] == keys[2]) & (DIY_series['CATEGORIES']['key3'] == keys[3])].index[0])
                    new_label.append(str(DIY_series['CATEGORIES'].loc[middle, 'cat_desc'])+n)
                    new_order.append(DIY_series['CATEGORIES'].loc[middle, 'order'])
        elif address.find('SCEN') >= 0 and fname.find('tablea-1') >= 0:
            middle = ''
            for j in range(len(US_t.index[ind])):
                if str(US_t.index[ind][j]).find('Unnamed') < 0:
                    n = ''
                    keys[j] = re.sub(r'[0-9]+$', "", str(US_t.index[ind][j].strip())).strip()
                    if bool(re.search(r'[0-9]+$', str(US_t.index[ind][j]).strip())):
                        n = re.findall(r'[0-9]+$', str(US_t.index[ind][j]).strip())[0]
                        if j == 0:
                            formnote[keys[j]] = n
                            n = ''
                elif j == 2:
                    keys[j] = 'None'
            if keys[0] in list(DIY_series['CATEGORIES']['key_desc']):
                middle = str(DIY_series['CATEGORIES'].loc[DIY_series['CATEGORIES']['key_desc'] == keys[0]].index.item())
                if DIY_series['DATA TYPES'].loc[(DIY_series['DATA TYPES']['key1'] == keys[1]) & (DIY_series['DATA TYPES']['key2'] == keys[2])].empty == False:
                    suffix = str(DIY_series['DATA TYPES'].loc[(DIY_series['DATA TYPES']['key1'] == keys[1]) & (DIY_series['DATA TYPES']['key2'] == keys[2])].index[0])
                    new_label.append(str(DIY_series['DATA TYPES'].loc[suffix, 'dt_desc'])+n)
                    new_order.append(DIY_series['DATA TYPES'].loc[suffix, 'order'])
        elif address.find('SCEN') >= 0 and fname.find('tablea-2') >= 0:
            middle = ''
            for j in range(len(US_t.index[ind])):
                if str(US_t.index[ind][j]).find('Unnamed') < 0:
                    n = ''
                    keys[j] = re.sub(r'[0-9]+$', "", str(US_t.index[ind][j].strip())).strip()
                    if bool(re.search(r'[0-9]+$', str(US_t.index[ind][j]).strip())):
                        n = re.findall(r'[0-9]+$', str(US_t.index[ind][j]).strip())[0]
                        if j == 0:
                            formnote[keys[j]] = n
                            n = ''
                elif j == 2:
                    keys[j] = 'None'
            if keys[0] in list(DIY_series['DATA TYPES']['key1']):
                middle = str(DIY_series['CATEGORIES'].loc[DIY_series['CATEGORIES']['key_desc'] == 'All races'].index.item())
                if DIY_series['DATA TYPES'].loc[(DIY_series['DATA TYPES']['key1'] == keys[0]) & ((DIY_series['DATA TYPES']['key2'] == keys[1]) | (DIY_series['DATA TYPES']['key2'] == keys[2]))].empty == False:
                    suffix = str(DIY_series['DATA TYPES'].loc[(DIY_series['DATA TYPES']['key1'] == keys[0]) & ((DIY_series['DATA TYPES']['key2'] == keys[1]) | (DIY_series['DATA TYPES']['key2'] == keys[2]))].index[0])
                    new_label.append(str(DIY_series['DATA TYPES'].loc[suffix, 'dt_desc'])+n)
                    new_order.append(DIY_series['DATA TYPES'].loc[suffix, 'order'])
        if middle == '' or suffix == '':
            new_index.append('nan')
            new_label.append('nan')
            new_order.append(10000)
        else:
            new_index.append(prefix+middle+suffix)
    US_t.insert(loc=0, column='Index', value=new_index)
    US_t = US_t.set_index('Index', drop=False)
    US_t.insert(loc=1, column='Label', value=new_label)
    US_t.insert(loc=2, column='order', value=new_order)
    US_t = US_t.sort_values(by='order')
    label = US_t['Label']

    return US_t, label, note, footnote, formnote

def US_STL(US_temp, address, DIY_series, TRPT_series=None):  
    if address.find('BTS') >= 0:
        SCHEDULED = ['ASM','PSG','LDF','RPM','FLY']
        new_code = []
        for i in range(US_temp.shape[0]):
            middle = ''
            fix = ''
            if US_temp.index[i][:3] == 'TSI' or US_temp.index[i][-3:] == 'D11':
                prefix = 'A'
            else:
                prefix = 'U'
            for item in list(TRPT_series['CATEGORIES']['name']):
                if re.sub(r'^TSI|[DI1]+$', "", US_temp.index[i]) in re.split(r'//', str(item)):
                    middle = TRPT_series['CATEGORIES'].loc[TRPT_series['CATEGORIES']['name'] == item].index[0]
            if middle in SCHEDULED:
                suf = 'SATU'
            elif re.sub(r'D11$', "", US_temp.index[i]) in list(TRPT_series['DATA TYPES'].index):
                middle = 'TOT'
                suf = re.sub(r'D11$', "", US_temp.index[i])
                fix = 'T'
            elif US_temp.index[i][:3] == 'TSI':
                suf = 'TSI'
            else:
                suf = 'TRPT'
            for item in list(TRPT_series['GEO LEVELS']['name']):
                if re.sub(r'D11$', "", US_temp.index[i])[-1:] in re.split(r'//', str(item)) and middle in SCHEDULED:
                    fix = TRPT_series['GEO LEVELS'].loc[TRPT_series['GEO LEVELS']['name'] == item].index[0]
                    break
                elif middle in SCHEDULED:
                    fix = 'DMI'
            new_code.append(prefix+middle+suf+fix)
        US_temp = US_temp.reset_index()
        US_temp = US_temp.rename(columns={'index':'old_index'})
        US_temp.index = new_code
        US_temp.index.name = 'index'
        keycolumn = list(US_temp['old_index'])
    else:
        keycolumn = US_temp.index
    note = []
    footnote = []
    new_label = []
    new_form = []
    new_unit = []
    isadjusted = []
    head = 0
    for i in range(US_temp.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/US_temp.shape[0], 1))+"%)*")
        sys.stdout.flush()
        note_num = 1
        for r in range(head, len(DIY_series)):
            if DIY_series[r] == keycolumn[i]:
                for rr in range(r,len(DIY_series)):
                    if DIY_series[rr] == 'Title:':
                        if address.find('BTS') >= 0 and US_temp.index[i].find('VMT') >= 0:
                            new_label.append(TRPT_series['GEO LEVELS'].loc[US_temp.index[i][-1:], 'geo_desc']+',  '+TRPT_series['CATEGORIES'].loc[US_temp.index[i][1:4], 'cat_desc'])
                        elif address.find('BTS') >= 0 and US_temp.index[i].find('SAT') >= 0:
                            new_label.append(TRPT_series['CATEGORIES'].loc[US_temp.index[i][1:4], 'cat_desc']+',  '+TRPT_series['GEO LEVELS'].loc[US_temp.index[i][-3:], 'geo_desc'])
                        elif address.find('MCPI') >= 0 and US_temp.index[i].find('157') >= 0:
                            new_label.append(DIY_series[rr+1].strip()+',  Percent Change from Previous Month')
                        elif address.find('MCPI') >= 0 and US_temp.index[i].find('158') >= 0:
                            new_label.append(DIY_series[rr+1].strip()+',  Annualized')
                        elif address.find('MCPI') >= 0 and US_temp.index[i].find('159') >= 0:
                            new_label.append(DIY_series[rr+1].strip()+',  Percent Change from Previous Year')
                        elif address.find('IRS') >= 0:
                            m = rr+1
                            whole = ''
                            while str(DIY_series[m]) != 'Source:':
                                if str(DIY_series[m]) == 'nan':
                                    m+=1
                                    continue
                                whole = whole+str(DIY_series[m])+' '
                                m+=1
                            new_label.append(re.sub(r'\s+', " ", whole.strip()))  
                        else:
                            new_label.append(DIY_series[rr+1].strip())
                    elif DIY_series[rr] == 'Release:':
                        if address.find('IRS') >= 0:
                            new_form.append('Individual Income Tax')
                        else:
                            new_form.append(DIY_series[rr+1].strip())
                    elif DIY_series[rr] == 'Units:':
                        new_unit.append(DIY_series[rr+1].strip())
                    elif DIY_series[rr] == 'Seasonal Adjustment:':
                        if address.find('BEOL') >= 0:
                            isadjusted.append('Seasonally Adjusted')
                        else:
                            isadjusted.append(DIY_series[rr+1].strip())    
                    elif DIY_series[rr] == 'Notes:':
                        m = rr+1
                        while str(DIY_series[m]) != 'Series ID:':
                            if str(DIY_series[m]) == 'nan':
                                m+=1
                                continue
                            whole = ''
                            while True:
                                whole = whole+str(DIY_series[m])+' '
                                if m+1 >= len(DIY_series) or bool(re.search(r'\.$', str(DIY_series[m]))) or DIY_series[m+1] == 'Series ID:' or bool(re.search(r'©', str(DIY_series[m+1]))):
                                    break
                                m+=1
                            note.append([str(note_num)+US_temp.index[i]+'.', re.sub(r'\s+', " ", whole.strip())])
                            note_num += 1
                            m+=1
                            if m+1 >= len(DIY_series) or bool(re.search(r'©', str(DIY_series[m]))):
                                break  
                    elif DIY_series[rr] == 'Series ID:':
                        head = rr
                        break
                break
    sys.stdout.write("\n\n")
    US_t = US_temp.reset_index()
    US_t = US_t.set_index('index', drop=False)
    US_t = US_t.rename(columns={'index':'Index'})
    US_t.insert(loc=1, column='Label', value=new_label)
    US_t.insert(loc=2, column='form_e', value=new_form)
    US_t.insert(loc=3, column='unit', value=new_unit)
    US_t.insert(loc=4, column='is_adj', value=isadjusted)
    label = US_t['Label']
    
    return US_t, label, note, footnote

def US_DOT(Series, US_temp, fname, key, gross=False, other=False, suffix=''):
    note = []
    footnote = []
    SUFFIX = []
    if Series.loc[fname, 'Suffix'] != 'All':
        allitems = False
        SUFFIX = re.split(r', ', Series.loc[fname, 'Suffix'])
    else:
        allitems = True
    level_num = 0
    for s in range(US_temp.shape[0]):
        if US_temp.iloc[s]['Sequence Level Number'] > level_num:
            level_num = US_temp.iloc[s]['Sequence Level Number']
    
    US_t = pd.DataFrame()
    new_item_t = []
    new_index_t = []
    new_code_t = []
    new_label_t = []
    new_key_t = []
    new_level_t = []
    new_level_code_t = []
    new_level_list = []
    new_dataframe = []
    firstfound = False
    Sub_header = False
    code = ''
    level_code = ''
    for i in range(US_temp.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/US_temp.shape[0], 1))+"%)*")
        sys.stdout.flush()
        if US_temp.iloc[i]['Data Type Code']+str(US_temp.iloc[i]['Table Number'])+str(US_temp.iloc[i]['Line Code Number'])+US_temp.iloc[i]['Record Type Code']+suffix != code:
            if firstfound == True:
                new_dataframe.append(new_item_t)
                US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
                if US_new.empty == False:
                    if Sub_header == True or deal == False or US_new.dropna(axis=1).empty:
                        new_code_t.append('ZZZZZZ')
                    else:
                        new_code_t.append(code)
                    new_label_t.append(lab)
                    if allow == True:
                        new_key_t.append(Series.loc[fname, 'Other_Key'])
                    else:
                        new_key_t.append(key.replace('App', 'Applicable'))
                    new_level_t.append(level)
                    new_level_code_t.append(level_code)
                    new_level_list.append(level_list)
                    US_t = pd.concat([US_t, US_new], ignore_index=True)
                new_dataframe = []
                new_item_t = []
                new_index_t = []
            if US_temp.iloc[i]['Data Type Code'] == 'S':
                Sub_header = True
            else:
                Sub_header = False
            if allitems == False and US_temp.iloc[i]['Record Type Code'] not in SUFFIX:
                deal = False
            else:
                deal = True
            if US_temp.iloc[i]['Classification Description'] == 'Allowances':
                allow = True
            else:
                allow = False
            lab = re.sub(r'(Total)\s*\-\-.+', r"\1", re.sub(r'(Total)\s*\-\-\s*(Receipts)$', r"\1 \2", re.sub(r'(Total)\s*\-\-\s*(O[nf]+\-Budget)$', r"\1 \2", re.sub(r':$', "", US_temp.iloc[i]['Classification Description']))))
            code = US_temp.iloc[i]['Data Type Code']+str(US_temp.iloc[i]['Table Number'])+str(US_temp.iloc[i]['Line Code Number'])+US_temp.iloc[i]['Record Type Code']+suffix
            #unit = 'United States Dollars'
            level = US_temp.iloc[i]['Sequence Level Number']
            level_code = US_temp.iloc[i]['Sequence Number Code']
            level_list = [int(j) for j in re.split(r'\.', str(level_code))]
            if len(level_list) < level_num:
                level_list.extend([0 for l in range(level_num - len(level_list))])
            firstfound = True
        if re.sub(r'(Total)\s*\-\-.+', r"\1", re.sub(r'(Total)\s*\-\-\s*(Receipts)$', r"\1 \2", re.sub(r'(Total)\s*\-\-\s*(O[nf]+\-Budget)$', r"\1 \2", re.sub(r':$', "", US_temp.iloc[i]['Classification Description'])))).find(lab) < 0:
            ERROR('The code '+code+' pairs to multiple descriptions. Please check the uniqueness of the code.')
        #key = Series.loc[fname, 'Key']
        if US_temp.iloc[i]['Classification Description'] == 'Allowances':
            new_item_t.append(US_temp.iloc[i][Series.loc[fname, 'Other_Key']])
        else:
            new_item_t.append(US_temp.iloc[i][key])
        new_index_t.append(str(US_temp.iloc[i]['Calendar Year'])+'-'+str(US_temp.iloc[i]['Calendar Month Number']).rjust(2,'0')) 
    sys.stdout.write("\n\n")
    new_dataframe.append(new_item_t)
    US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
    if US_new.empty == False:
        if Sub_header == True or deal == False or US_new.dropna(axis=1).empty:
            new_code_t.append('ZZZZZZ')
        else:
            new_code_t.append(code)
        new_label_t.append(lab)
        if allow == True:
            new_key_t.append(Series.loc[fname, 'Other_Key'])
        else:
            new_key_t.append(key.replace('App', 'Applicable'))
        new_level_t.append(level)
        new_level_code_t.append(level_code)
        new_level_list.append(level_list)
        US_t = pd.concat([US_t, US_new], ignore_index=True)
    US_t = US_t.sort_index(axis=1)
    LEVEL = pd.DataFrame(new_level_list, columns=list(range(level_num)))
    US_t = pd.concat([LEVEL, US_t], axis=1)
    US_t.insert(loc=0, column='Index', value=new_code_t)
    US_t.insert(loc=1, column='Label', value=new_label_t)
    US_t.insert(loc=2, column='type', value=new_key_t)
    US_t.insert(loc=2, column='level', value=new_level_t)
    US_t.insert(loc=3, column='level_code', value=new_level_code_t)
    US_t = US_t.set_index('Index', drop=False)
    if (fname == 'MTS_OutlyAgcy_all_years' or fname == 'MTS_RcptSrc_all_years') and gross == False and other == False:
        print('Dealing with Gross Amount: '+'\n')
        US_g = US_DOT(Series, US_temp, fname, key=Series.loc[fname, 'Gross_Key'], gross=True, suffix=Series.loc[fname, 'Gross_Suffix'])
        US_t = pd.concat([US_t, US_g], ignore_index=True)
        print('Dealing with Other Amount: '+'\n')
        US_o = US_DOT(Series, US_temp, fname, key=Series.loc[fname, 'Other_Key'], other=True, suffix=Series.loc[fname, 'Other_Suffix'])
        US_t = pd.concat([US_t, US_o], ignore_index=True)
    US_t = US_t.sort_values(by=list(range(level_num)))
    label = US_t['Label']
    label_level = list(US_t['level'])
    
    if gross == True or other == True:
        #print(US_t)
        return US_t
    else:
        return US_t, label, note, footnote, label_level

def US_FTD(US_t, fname, Series, prefix, middle, suffix, freq, trans, datatype=None, AMV=None):
    PASS = ['nan', '(-)', 'Balance of Payment', 'Net Adjustments', 'Total, Census Basis', 'Total Census Basis', 'Item', 'Residual', 'Unnamed', 'Selected commodities', 'Country', 'TOTAL']
    MONTH = ['January','February','March','April','May','June','July','August','September','October','November','December']
    YEAR = ['Jan.-Dec.']
    TYPE = {'EX': 'Exports', 'IM': 'Imports'}
    EPYT = {'IM': 'Exports', 'EX': 'Imports'}
    new_columns = []
    new_index = []
    new_label = []
    new_order = []
    
    if trans == True:
        year = 0
        for ind in range(US_t.shape[1]):
            if str(US_t.columns[ind]).strip().isnumeric():
                year = str(US_t.columns[ind]).strip()
            if freq == 'A' and re.sub(r'\s+\([A-Z]+\)\s*$', "", str(US_t.columns[ind])).replace(' ', '').strip() in YEAR:
                new_columns.append(year)
            elif freq == 'M' and re.sub(r'\s+\([A-Z]+\)\s*$|\s*\.\s*$', "", str(US_t.columns[ind])).strip() in MONTH:
                new_columns.append(year+'-'+str(datetime.strptime(re.sub(r'\s+\([A-Z]+\)\s*$|\s*\.\s*$', "", str(US_t.columns[ind])).strip(),'%B').month).rjust(2,'0'))
            elif freq == 'A' and fname.find('SA') >= 0 and str(US_t.columns[ind]).strip().isnumeric():
                new_columns.append(year)
            else:
                new_columns.append('nan')
        US_t.columns = new_columns
    if fname == 'exh1' or fname == 'exhibit_history' or fname == 'exh2' or fname == 'ABOP3' or fname == 'exh9' or fname == 'petro' or fname == 'exh11' or fname == 'realpetr':    
        US_t.index = pd.MultiIndex.from_arrays([US_t.index.get_level_values(0), US_t.index.get_level_values(1).str.replace(r'\s*Census Basis.*', '', regex=True)])
        for ind in range(US_t.shape[0]):
            middle = ''
            if str(US_t.index[ind][0]) in list(Series['DATA TYPES']['dt_desc']):
                suf = Series['DATA TYPES'].loc[Series['DATA TYPES']['dt_desc'] == US_t.index[ind][0]].index[0]+suffix
                for item in list(Series['CATEGORIES']['name']):
                    if re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t.index[ind][1])) in re.split(r'//', item):
                        middle = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item].index[0]
                        new_label.append(Series['CATEGORIES'].loc[middle, 'cat_desc']+',  '+Series['DATA TYPES'].loc[suf[:2], 'dt_desc']+',  '+Series['GEO LEVELS'].loc[suffix, 'geo_desc'])
                        new_order.append(Series['CATEGORIES'].loc[middle, 'order'])
            else:
                ERROR('Item index not found in '+fname+': '+str(US_t.index[ind][0]))
            if middle == '':
                if re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t.index[ind][1])) not in PASS:
                    ERROR('Item index not found in '+fname+': '+re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t.index[ind][1])))
                else:
                    new_index.append('nan')
                    new_label.append('nan')
                    new_order.append(10000)
            else:
                new_index.append(prefix+middle+suf)
    elif fname == 'exh3' or fname == 'ASRVEXBOP' or fname == 'exh4' or fname == 'ASRVIMBOP' or fname == 'exh10' or fname == 'realexp' or fname == 'realimp' or fname == 'exh13' or fname == 'NSAEXP' or fname == 'NSAIMP' or fname == 'SAEXP' or fname == 'SAIMP':
        for ind in range(US_t.shape[0]):
            middle = ''
            for item in list(Series['CATEGORIES']['name']):
                if re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t.index[ind])) in re.split(r'//', item):
                    middle = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item].index[0]
                    new_label.append(Series['CATEGORIES'].loc[middle, 'cat_desc']+',  '+Series['DATA TYPES'].loc[suffix[:2], 'dt_desc']+',  '+Series['GEO LEVELS'].loc[suffix[2:], 'geo_desc'])
                    new_order.append(Series['CATEGORIES'].loc[middle, 'order'])
            if middle == '':
                if re.sub(r'\s+\([0-9]+\)\s*$|:\s*[0-9]+\s*$', "", str(US_t.index[ind])) not in PASS:
                    ERROR('Item index not found in '+fname+': '+re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t.index[ind])))
                else:
                    new_index.append('nan')
                    new_label.append('nan')
                    new_order.append(10000)
            else:
                new_index.append(prefix+middle+suffix)
    elif fname == 'exh5' or fname == 'AGDSCSB' or fname == 'exh12' or fname == 'UGDSCSB':
        US_t = US_t.rename(index={'Unnamed: 2_level_0': 'Balance'})
        add_value = []
        for ind in range(US_t.shape[1]):
            TBOP = str(US_t.loc[('Balance', 'Total Balance of Payments Basis'), US_t.columns[ind]])
            TCSB = str(US_t.loc[('Balance', 'Total Census Basis'), US_t.columns[ind]])
            if US_t.columns[ind] == 'nan':
                add_value.append('nan')
            elif TBOP != 'nan' and TCSB != 'nan' and TBOP.strip() != '' and TCSB.strip() != '':
                add_value.append(US_t.loc[('Balance', 'Total Balance of Payments Basis'), US_t.columns[ind]]-US_t.loc[('Balance', 'Total Census Basis'), US_t.columns[ind]])
            else:
                add_value.append('nan')
        US_new = pd.DataFrame([add_value], columns=US_t.columns, index=[('Balance', 'Net Adjustments')])
        US_t = pd.concat([US_t, US_new])
        if fname == 'exh12' or fname == 'UGDSCSB':
            US_t.index = pd.MultiIndex.from_arrays([US_t.index.get_level_values(0), US_t.index.get_level_values(1).str.replace(r'\s*Total Balance of Payments Basis.*', 'BOP', regex=True)])
        suf = ''    
        for ind in range(US_t.shape[0]):
            suffix = ''
            if str(US_t.index[ind][0]) in list(Series['DATA TYPES']['dt_desc']):
                suf = Series['DATA TYPES'].loc[Series['DATA TYPES']['dt_desc'] == US_t.index[ind][0]].index[0]
            for item in list(Series['GEO LEVELS']['name']):
                if re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t.index[ind][1])) in re.split(r'//', item):
                    suffix = suf+Series['GEO LEVELS'].loc[Series['GEO LEVELS']['name'] == item].index[0]
                    break
            if suffix == '' and str(US_t.index[ind][1]) != 'Total Balance of Payments Basis':
                ERROR('Item index not found in '+fname+': '+re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t.index[ind][1])))
            elif suffix == '' and str(US_t.index[ind][1]) == 'Total Balance of Payments Basis':
                new_index.append('nan')
                new_label.append('nan')
                new_order.append(10000)
            else:
                new_index.append(prefix+middle+suffix)
                new_label.append(Series['DATA TYPES'].loc[suffix[:2], 'dt_desc']+',  '+Series['GEO LEVELS'].loc[suffix[2:], 'geo_desc'])
                new_order.append(Series['CATEGORIES'].loc[middle, 'order'])
    elif fname == 'exh7' or fname == 'AGDSEXCSB' or fname == 'exh6_Y' or  fname == 'AGDSEXCSB_Y' or fname == 'exh8' or fname == 'AGDSIMCSB' or fname == 'exh7_Y' or fname == 'AGDSIMCSB_Y'\
         or fname == 'exh14_Y' or fname == 'exh15' or fname == 'UGDSSITC' or fname == 'UGDSSITC_Y':
        US_new = pd.DataFrame()
        for date in US_t:
            sys.stdout.write("\rLoading...("+str(round((list(US_t.keys()).index(date)+1)*100/len(US_t.keys()), 1))+"%)*")
            sys.stdout.flush()
            typeCorrect = False
            if (fname == 'exh7' or date == '7' or fname == 'exh8' or date == '8') and fname.find('_Y') < 0:
                US_t[date].columns = [re.sub(r'\s+\([A-Z]+\)\s*', "", str(US_t[date].iloc[1].iloc[m])+'-'+\
                    str(datetime.strptime(re.sub(r'\s+\([A-Z]+\)\s*', "", str(US_t[date].iloc[0].iloc[m])).strip(),'%B').month).rjust(2,'0')) for m in range(US_t[date].shape[1])]
            elif fname == 'exh6_Y' or fname == 'exh7_Y' or ((date == '6' or date == '7') and fname.find('_Y') >= 0):
                US_t[date].columns = [int(re.sub(r'\s*Annual\s*', "", str(US_t[date].columns[y]).strip())) for y in range(US_t[date].shape[1])]
            elif fname == 'exh14_Y' or date == '14':
                cols = [int(US_t[date].iloc[0].iloc[m]) for m in range(0, 6, 2)]
                if datatype == 'EX':
                    US_t[date] = US_t[date][[1, 3, 5]]
                elif datatype == 'IM':
                    US_t[date] = US_t[date][[2, 4, 6]]
                US_t[date].columns = cols
            elif fname == 'exh15' or date == '15':
                US_t[date] = US_t[date].drop(US_t[date].index[[0]])
                if str(US_t[date].iloc[0].iloc[2]).isnumeric():
                    cols = [str(US_t[date].iloc[0].iloc[0])+'-'+str(datetime.strptime(str(US_t[date].iloc[1].iloc[0]).strip(),'%B').month).rjust(2,'0')]+[str(US_t[date].iloc[0].iloc[2])+'-'+str(datetime.strptime(str(US_t[date].iloc[1].iloc[2]).strip(),'%B').month).rjust(2,'0')]
                else:
                    cols = [str(US_t[date].iloc[0].iloc[0])+'-'+str(datetime.strptime(str(US_t[date].iloc[1].iloc[m]).strip(),'%B').month).rjust(2,'0') for m in [0, 2]]
                if datatype == 'EX':
                    if TYPE[datatype] in list(US_t[date][3]) and EPYT[datatype] not in list(US_t[date][4]):
                        typeCorrect = True
                    US_t[date] = US_t[date][[1, 4]]
                elif datatype == 'IM':
                    if TYPE[datatype] in list(US_t[date][5]) and EPYT[datatype] not in list(US_t[date][6]):
                        typeCorrect = True
                    US_t[date] = US_t[date][[2, 6]]
                US_t[date].columns = cols
            elif fname == 'UGDSSITC_Y':
                if re.split(r',', date)[0] < '2010':
                    if datatype == 'EX':
                        US_t[date] = US_t[date][[1]]
                    elif datatype == 'IM':
                        US_t[date] = US_t[date][[2]]
                    US_t[date].columns = [int(date)]
                else:
                    dropcols = []
                    for yr in range(0, US_t[date].shape[1], 2):
                        if str(US_t[date].iloc[0].iloc[yr]) not in re.split(r',', date):
                            dropcols.extend([yr+1, yr+2])
                    US_t[date] = US_t[date].drop(columns=dropcols)
                    if datatype == 'EX':
                        US_t[date] = US_t[date][[US_t[date].columns[m] for m in range(0, US_t[date].shape[1], 2)]]
                    elif datatype == 'IM':
                        US_t[date] = US_t[date][[US_t[date].columns[m] for m in range(1, US_t[date].shape[1], 2)]]
                    US_t[date].columns = reversed([int(year) for year in re.split(r',', date)])
            elif fname.find('_Y') >= 0:
                for col in US_t[date].columns:
                    if col.find(date) < 0:
                        US_t[date] = US_t[date].drop(columns=[col])
                US_t[date].columns = [int(date)]
            elif fname == 'UGDSSITC' and date.find('M') < 0:
                if datatype == 'EX':
                    if TYPE[datatype] in list(US_t[date][3]) and EPYT[datatype] not in list(US_t[date][4]):
                        typeCorrect = True
                    US_t[date] = US_t[date][[4]]
                elif datatype == 'IM':
                    if TYPE[datatype] in list(US_t[date][5]) and EPYT[datatype] not in list(US_t[date][6]):
                        typeCorrect = True
                    US_t[date] = US_t[date][[6]]
                US_t[date].columns = [date]
            elif date.find('M') >= 0:
                if bool(re.search(r'M[0-9]$', date)):
                    US_t[date] = US_t[date].drop(US_t[date].index[[0]])
                cols = [date[:4]+'-'+str(datetime.strptime(str(US_t[date].iloc[0].iloc[m]).strip(),'%B').month).rjust(2,'0') for m in range(0, US_t[date].shape[1], 2)]
                if datatype == 'EX':
                    US_t[date] = US_t[date][[m for m in range(1, US_t[date].shape[1]+1, 2)]]
                elif datatype == 'IM':
                    US_t[date] = US_t[date][[m for m in range(2, US_t[date].shape[1]+1, 2)]]
                US_t[date].columns = cols
            else:
                US_t[date] = US_t[date].drop(columns=[1])
                US_t[date].columns = [date]
            US_t[date] = US_t[date].sort_index(axis=1)
            if datatype != None:
                for col in range(US_t[date].shape[1]):
                    if TYPE[datatype] not in list(US_t[date][US_t[date].columns[col]]) and typeCorrect == False:
                        print(US_t[date])
                        ERROR('Incorrect columns were chosen: '+date+' '+TYPE[datatype])
            new_ind = []
            REX = False
            for ind in range(US_t[date].shape[0]):
                found = False
                if REX == True and datatype == 'EX':
                    for item in reversed(list(Series['CATEGORIES']['name'])):
                        if re.sub(r'\s+', " ", re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t[date].index[ind]).strip())).strip()+', Re-exports' in re.split(r'//', item.strip()):
                            found = True
                            new_ind.append(Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item]['cat_desc'].item())
                            break
                elif REX == True and datatype == 'IM':
                    found = True
                    new_ind.append('nan')
                else:
                    for item in list(Series['CATEGORIES']['name']):
                        if re.sub(r'\s+', " ", re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t[date].index[ind]).strip())).strip() in re.split(r'//', item.strip()):
                            if re.sub(r'\s+', " ", re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t[date].index[ind]).strip())).strip() == 'Total':
                                new_ind.append('nan')
                            else:
                                new_ind.append(Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item]['cat_desc'].item())
                            found = True
                            break
                        elif re.sub(r'\s+', " ", re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t[date].index[ind]).strip())).strip().capitalize() == 'Re-exports' and datatype == 'EX':
                            found = True
                            REX = True
                            new_ind.append('Re-exports')
                            break
                        elif re.sub(r'\s+', " ", re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t[date].index[ind]).strip())).strip().capitalize() == 'Re-exports' and datatype == 'IM':
                            found = True
                            REX = True
                            new_ind.append('nan')
                            break
                if found == False:
                    to_pass = False
                    if str(US_t[date].iloc[ind].iloc[0]) == 'nan':
                        to_pass = True
                    for pas in PASS:
                        if str(US_t[date].index[ind]).strip().find(pas) >= 0 or str(US_t[date].index[ind]) == ' ':
                            to_pass = True
                            break
                    if to_pass == False:
                        if REX == True:
                            print('\nRe-exports')
                        ERROR('Category item code not found: '+date+'-"'+str(US_t[date].index[ind])+'"')
                    new_ind.append('nan')    
            US_t[date].index = new_ind
            US_t[date] = US_t[date][~US_t[date].index.duplicated()]
            US_new = pd.concat([US_new, US_t[date]], axis=1)
        sys.stdout.write("\n\n")
        US_t = US_new
        fix = suffix
        for ind in range(US_t.shape[0]):
            middle = ''
            description = str(US_t.index[ind])
            if description.find('Re-exports') >= 0:
                if description == 'Re-exports':
                    description = 'Goods'
                description = description.replace(', Re-exports', '')
                suffix = 'RE'+fix
            elif datatype != None:
                suffix = datatype+fix
            for item in list(Series['CATEGORIES']['cat_desc']):
                if description in re.split(r'//', item.strip()):
                    middle = Series['CATEGORIES'].loc[Series['CATEGORIES']['cat_desc'] == item].index[0]
                    new_label.append(Series['CATEGORIES'].loc[middle, 'cat_desc']+',  '+Series['DATA TYPES'].loc[suffix[:2], 'dt_desc']+',  '+Series['GEO LEVELS'].loc[suffix[2:], 'geo_desc'])
                    new_order.append(Series['CATEGORIES'].loc[middle, 'order'])
            if middle == '':
                new_index.append('nan')
                new_label.append('nan')
                new_order.append(10000)
                #ERROR('Item index not found in '+fname+': '+re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t.index[ind])))
            else:
                new_index.append(prefix+str(middle)+suffix)
    elif fname == 'country' or fname == 'ctyseasonal':
        US_t = US_t.reset_index()
        if fname == 'country':
            US_t, new_index, new_label, new_order = US_country(US_t, Series, prefix, middle, freq, name='CTYNAME', bal=True)
        elif fname == 'ctyseasonal':
            US_t = US_t.sort_values(by=['cty_desc','year'])
            US_t, new_index, new_label, new_order = US_country(US_t, Series, prefix, middle, freq, name='cty_desc')
    elif fname == 'exh16' or fname == 'UATPCSB':
        for ind in range(US_t.shape[0]):
            suf = ''
            if str(US_t.index[ind]) in list(Series['DATA TYPES']['dt_desc']):
                suf = Series['DATA TYPES'].loc[Series['DATA TYPES']['dt_desc'] == US_t.index[ind]].index[0]
                new_label.append(Series['CATEGORIES'].loc[middle, 'cat_desc']+',  '+Series['DATA TYPES'].loc[suf, 'dt_desc']+',  '+Series['GEO LEVELS'].loc[suffix, 'geo_desc'])
                new_order.append(Series['CATEGORIES'].loc[middle, 'order'])
                new_index.append(prefix+middle+suf+suffix)
            else:
                if str(US_t.index[ind]) not in PASS:
                    ERROR('Item index not found in '+fname+': '+str(US_t.index[ind]))
                else:
                    new_index.append('nan')
                    new_label.append('nan')
                    new_order.append(10000)
    elif fname == 'exh17' or fname == 'UPPCO':
        if 'Imports' in list(US_t.iloc[0]):
            new_cols = []
            ImportsFound = False
            for ind in range(US_t.shape[1]):
                if US_t.iloc[0].iloc[ind] == 'Imports':
                    ImportsFound = True
                if ImportsFound == True:
                    new_cols.append(US_t.columns[ind])
                else:
                    new_cols.append('drop')
            US_t.columns = new_cols
            US_t = US_t.drop(columns=['drop'])
        product = ''
        for ind in range(US_t.shape[0]):
            middle = ''
            if str(US_t.index[ind][0]).find('Unnamed') < 0:
                product = re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t.index[ind][0]))
            for item in range(Series['CATEGORIES'].shape[0]):
                if Series['CATEGORIES'].iloc[item]['name'] == product and Series['CATEGORIES'].iloc[item]['cat_desc'] == re.sub(r'\s+\([a-z\s]+\)\s*$', "", str(US_t.index[ind][1])):
                    middle = Series['CATEGORIES'].index[item]
                    new_label.append(Series['CATEGORIES'].loc[middle, 'cat_desc']+',  '+Series['GEO LEVELS'].loc[suffix[2:], 'geo_desc'])
                    new_order.append(Series['CATEGORIES'].loc[middle, 'order'])
                    break
            if middle == '':
                to_pass = False
                for pas in PASS:
                    if str(US_t.index[ind][0]).find(pas) >= 0 and str(US_t.index[ind][1]).find(pas) >= 0:
                        to_pass = True
                        break
                if to_pass == False:
                    ERROR('Item index not found in '+fname+': '+product+', '+re.sub(r'\s+\([a-z\s]+\)\s*$', "", str(US_t.index[ind][1])))
                else:
                    new_index.append('nan')
                    new_label.append('nan')
                    new_order.append(10000)
            else:
                new_index.append(prefix+middle+suffix)
    elif fname == 'exh17_Y' or fname == 'exh18' or fname == 'UAMVCSB' or fname == 'UAMVCSB_Y':
        US_new = pd.DataFrame()
        for date in US_t:
            sys.stdout.write("\rLoading...("+str(round((list(US_t.keys()).index(date)+1)*100/len(US_t.keys()), 1))+"%)*")
            sys.stdout.flush()
            typeCorrect = False
            new_rows = []
            typeFound = True
            for ind in range(US_t[date].shape[0]):
                if str(US_t[date].iloc[ind].iloc[0]).capitalize() == TYPE[datatype]:
                    typeFound = True
                elif str(US_t[date].iloc[ind].iloc[0]).capitalize() == EPYT[datatype]:
                    typeFound = False
                if typeFound == True:
                    new_rows.append(US_t[date].index[ind])
                elif bool(re.search(r'M[0-9]$', date)) and str(US_t[date].iloc[ind].iloc[0]) in MONTH:
                    new_rows.append(US_t[date].index[ind])
                else:
                    new_rows.append('drop')
            US_t[date].index = new_rows
            US_t[date] = US_t[date].drop(index=['drop'])
            if (TYPE[datatype] in list(US_t[date][1]) or TYPE[datatype].upper() in list(US_t[date][1])) and (EPYT[datatype] not in list(US_t[date][1]) and EPYT[datatype].upper() not in list(US_t[date][1])):
                typeCorrect = True
            dropcols = []
            cols = []
            if date.find('M') < 0 and fname.find('_Y') < 0:
                if US_t[date].shape[1]%3 != 0:
                    time_range = 2
                else:
                    time_range = 3
                for auto in range(0, US_t[date].shape[1], time_range):
                    if str(US_t[date].iloc[0].iloc[auto]) == 'Total' and AMV == 'AMV':
                        continue
                    elif str(US_t[date].iloc[0].iloc[auto]) not in re.split(r'//', str(Series['CATEGORIES'].loc[AMV, 'name']).strip()):
                        for a in range(1,time_range+1):
                            dropcols.extend([auto+a])
            else:
                if bool(re.search(r'M[0-9]$', date)):
                    US_t[date] = US_t[date].drop(US_t[date].index[[0]]) 
                for auto in range(US_t[date].shape[1]):
                    if fname == 'exh17_Y' or date == '17':
                        if str(US_t[date].iloc[1].iloc[auto]).isnumeric():
                            cols.append(int(US_t[date].iloc[1].iloc[auto]))
                    auto_found = False
                    for item in range(US_t[date].shape[0]):
                        if date.find('M') >= 0 and str(US_t[date].iloc[item].iloc[auto]).strip() in MONTH:
                            cols.append(str(US_t[date].iloc[item].iloc[auto]).strip())
                        if str(US_t[date].iloc[item].iloc[auto]) == 'Total' and AMV == 'AMV':
                            auto_found = True
                        elif str(US_t[date].iloc[item].iloc[auto]) in re.split(r'//', str(Series['CATEGORIES'].loc[AMV, 'name']).strip()):
                            auto_found = True
                    if auto_found == False:
                        dropcols.append(auto+1)
            US_t[date] = US_t[date].drop(columns=dropcols)
            if (fname == 'exh18' or date == '18') and fname.find('_Y') < 0:
                for mnth in range(US_t[date].shape[1]):
                    if re.split(r'\n',str(US_t[date].iloc[1].iloc[mnth]))[0] in MONTH and re.split(r'\n',str(US_t[date].iloc[1].iloc[mnth]))[1].isnumeric():
                        cols.append(re.split(r'\n',str(US_t[date].iloc[1].iloc[mnth]))[1]+'-'+str(datetime.strptime(re.split(r'\n',str(US_t[date].iloc[1].iloc[mnth]))[0].strip(),'%B').month).rjust(2,'0'))
                    else:
                        cols.append('drop')
                US_t[date].columns = cols
                if 'drop' in US_t[date].columns:
                    US_t[date] = US_t[date].drop(columns=['drop'])
            elif (fname == 'exh17_Y' or date == '17') and fname.find('_Y') >= 0:
                US_t[date].columns = cols
            elif fname == 'UAMVCSB' and date.find('M') < 0:
                for mnth in range(US_t[date].shape[1]):
                    if re.split(r'\n',str(US_t[date].iloc[1].iloc[mnth]))[0].strip() not in MONTH:
                        cols.append('drop')
                    elif re.split(r'\n',str(US_t[date].iloc[1].iloc[mnth]))[1]+'-'+str(datetime.strptime(re.split(r'\n',str(US_t[date].iloc[1].iloc[mnth]))[0].strip(),'%B').month).rjust(2,'0') == date:
                        cols.append(date)
                    else:
                        cols.append('drop')
                US_t[date].columns = cols
                US_t[date] = US_t[date].drop(columns=['drop'])
            elif date.find('M') >= 0:
                cols = [date[:4]+'-'+str(datetime.strptime(item,'%B').month).rjust(2,'0') for item in cols]
                US_t[date].columns = cols
            elif fname == 'UAMVCSB_Y':
                if date >= '2010':
                    US_t[date] = US_t[date][[US_t[date].columns[2]]]
                US_t[date].columns = [int(date)]
            US_t[date] = US_t[date].sort_index(axis=1)
            if TYPE[datatype] not in list(US_t[date][US_t[date].columns[0]]) and typeCorrect == False:
                print(US_t[date])
                ERROR('Incorrect indexes were chosen: '+date+' '+TYPE[datatype])
            for col in range(US_t[date].shape[1]):
                if freq == 'M' and date.find('M') < 0 and fname != 'exh18' and date != '18':
                    if datetime.strptime(US_t[date].columns[col],'%Y-%m').strftime('%B')+'\n'+date[:4] not in list(US_t[date][US_t[date].columns[col]]):
                        print(US_t[date][US_t[date].columns[col]])
                        ERROR('Incorrect month was chosen: '+date+' '+datetime.strptime(US_t[date].columns[col],'%Y-%m').strftime('%B'))
                if freq == 'A' or date.find('M') >= 0:
                    ItemCorrect = False
                    for item in re.split(r'//', str(Series['CATEGORIES'].loc[AMV, 'name']).strip()):
                        if (AMV == 'AMV' and 'Total' in list(US_t[date][US_t[date].columns[col]])) or (item in list(US_t[date][US_t[date].columns[col]])):
                            ItemCorrect = True
                            break
                    if ItemCorrect == False:
                        print(US_t[date][US_t[date].columns[col]])
                        ERROR('Incorrect column was chosen: '+date+' '+str(Series['CATEGORIES'].loc[AMV, 'cat_desc']))
            new_ind = []
            for ind in range(US_t[date].shape[0]):
                found = False
                for item in list(Series['GEO LEVELS']['name']):
                    if str(US_t[date].index[ind]).strip() in re.split(r'//', item.strip()):
                        new_ind.append(Series['GEO LEVELS'].loc[Series['GEO LEVELS']['name'] == item]['geo_desc'].item())
                        found = True
                        break
                if found == False:
                    to_pass = False
                    if str(US_t[date].iloc[ind].iloc[0]) == 'nan':
                        to_pass = True
                    for pas in PASS:
                        if str(US_t[date].index[ind]).strip().find(pas) >= 0 or str(US_t[date].index[ind]).strip() == '':
                            to_pass = True
                            break
                    if to_pass == False:
                        ERROR('Country code not found: '+date+'-"'+str(US_t[date].index[ind])+'"')
                    new_ind.append('nan')    
            US_t[date].index = new_ind
            US_t[date] = US_t[date][~US_t[date].index.duplicated()]
            US_new = pd.concat([US_new, US_t[date]], axis=1)
        sys.stdout.write("\n\n")
        US_t = US_new
        middle = AMV
        suf = datatype
        for ind in range(US_t.shape[0]):
            fix = ''
            for item in list(Series['GEO LEVELS']['geo_desc']):
                if str(US_t.index[ind]) in re.split(r'//', item.strip()):
                    fix = Series['GEO LEVELS'].loc[Series['GEO LEVELS']['geo_desc'] == item].index[0]
                    new_label.append(Series['CATEGORIES'].loc[middle, 'cat_desc']+',  '+Series['DATA TYPES'].loc[suf, 'dt_desc']+',  '+Series['GEO LEVELS'].loc[fix, 'geo_desc'])
                    new_order.append(Series['CATEGORIES'].loc[middle, 'order'])
            if fix == '':
                new_index.append('nan')
                new_label.append('nan')
                new_order.append(10000)
                #ERROR('Item index not found in '+fname+': '+re.sub(r'\s+\([0-9]+\)\s*$', "", str(US_t.index[ind])))
            else:
                new_index.append(prefix+middle+suf+fix)
    US_t = US_t.sort_index(axis=1)
    
    return US_t, new_index, new_label, new_order

def US_TICS(US_temp, Series, data_path, address, fname, start=None, find_unknown=False):
    note = []
    footnote = []
    
    US_t = pd.DataFrame()
    new_item_t = []
    new_index_t = []
    new_code_t = []
    new_label_t = []
    if fname == 'mfhhis01':
        note, footnote = US_NOTE(US_temp.index, fname=fname)
        tables = {}
        Note_suf = {}
        prefix = 'H'
        print('Item: '+Series['DATA TYPES'].loc[prefix, 'dt_desc'].strip())
        for g in range(US_temp.shape[0]):
            sys.stdout.write("\rLoading...("+str(round((g+1)*100/US_temp.shape[0], 1))+"%)*")
            sys.stdout.flush()
            if str(US_temp.index[g]) == 'Country':
                key = str(US_temp.iloc[g][0])[:4]
                table_head = g
                for i in range(g+1, US_temp.shape[0]):
                    if str(US_temp.index[i]).find('T-Bonds & Notes') >= 0:
                        table_tail = i
                        break
                tables[key] = readFile(data_path+address+fname+'.csv', header_ = [0, 1], index_col_ = 0, skiprows_ = list(range(table_head)), nrows_ = table_tail - table_head)
                if tables[key].empty == True:
                    ERROR('Table Not Found: '+key)
                cols = []
                for col in tables[key].columns:
                    try:
                        cols.append(col[1]+'-'+str(datetime.strptime(col[0].strip(),'%b').month).rjust(2,'0'))
                    except ValueError:
                        cols.append(None)
                tables[key].columns = cols
                tables[key] = tables[key].loc[:,tables[key].columns.dropna()]
                inds = []
                GRAND = ['For. Official', 'Treasury Bills' , 'T-Bonds & Notes']
                for dex in tables[key].index:
                    middle = ''
                    suffix = ''
                    if str(dex).strip() == 'nan' or str(dex).strip() == 'Of which:':
                        inds.append(None)
                        continue
                    elif str(dex).strip() in GRAND:
                        middle = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == str(dex).strip()].index[0]
                        suffix = str(Series['GEO LEVELS'].loc[Series['GEO LEVELS']['name'] == 'Grand Total'].index[0])
                    else:
                        middle = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == 'Major Foreign Holders'].index[0]
                        suf_key = re.sub(r'[0-9]+/', "", str(dex).strip()).strip()
                        for item in list(Series['GEO LEVELS']['name']):
                            if suf_key in re.split(r'//', item.strip()):
                                suffix = str(Series['GEO LEVELS'].loc[Series['GEO LEVELS']['name'] == item].index[0])
                    if middle == '' or suffix == '':
                        ERROR('Item code of '+str(dex).strip()+' not found in table: '+key)
                    inds.append(prefix+middle+suffix)
                    if bool(re.search(r'[0-9]+/', str(dex))) and suffix not in Note_suf:
                        Note_suf[suffix] = re.findall(r'[0-9]+/',str(dex))
                tables[key].index = inds
        sys.stdout.write("\n\n")
        US_t = pd.DataFrame()
        for key in tables:
            tables[key] = tables[key][~tables[key].index.duplicated()]
            US_t = pd.concat([US_t, tables[key]], axis=1)
        US_t = US_t.loc[US_t.index.dropna()]
        new_code_t = list(US_t.index)
        new_label_t = [re.sub(r'(within the), ', r"\1", Series['CATEGORIES'].loc[code[1:4], 'cat_desc'].strip()+',  '+Series['GEO LEVELS'].loc[int(code[4:]), 'geo_desc'].strip()) for code in new_code_t]
        for lab in range(len(new_label_t)):
            if new_code_t[lab][4:] in Note_suf:
                for suf_note in Note_suf[new_code_t[lab][4:]]:
                    new_label_t[lab] = new_label_t[lab]+suf_note
    elif fname == 's1_globl':
        new_dataframe = []
        firstfound = False
        for h in range(US_temp.shape[1]):
            prefix = ''
            middle = ''
            suffix = ''
            for item in list(Series['CATEGORIES']['name']):
                if str(US_temp.columns[h]) in re.split(r'//', item.strip()):
                    middle = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item].index[0]
                    if int(re.sub(r'\[|\]', "", str(US_temp.columns[h]))) <= 6:
                        prefix = 'P'
                    else:
                        prefix = 'S'
                    break
            if middle == '':
                ERROR('Item code not found: '+str(US_temp.columns[h]))
            print('Item: '+Series['DATA TYPES'].loc[prefix, 'dt_desc'].strip()+', '+Series['CATEGORIES'].loc[middle, 'cat_desc'].strip())
            for i in range(US_temp.shape[0]):
                sys.stdout.write("\rLoading...("+str(round((i+1)*100/US_temp.shape[0], 1))+"%)*")
                sys.stdout.flush()
                if US_temp.index[i][1].isnumeric() == False:
                    continue
                if US_temp.index[i][1] != suffix:
                    if firstfound == True:
                        new_dataframe.append(new_item_t)
                        US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
                        if US_new.empty == False:
                            new_code_t.append(code)
                            new_label_t.append(lab)
                            US_t = pd.concat([US_t, US_new], ignore_index=True)
                        new_dataframe = []
                        new_item_t = []
                        new_index_t = []
                    suffix = US_temp.index[i][1]
                    if str(US_temp.index[i][0]) not in re.split(r'//', Series['GEO LEVELS'].loc[int(suffix), 'name'].strip()):
                        ERROR('Country code '+suffix+' does not match country name: '+str(US_temp.index[i][0])+' in Series')
                    code = prefix+middle+suffix
                    lab = Series['CATEGORIES'].loc[middle, 'cat_desc'].strip()+',  '+Series['GEO LEVELS'].loc[int(suffix), 'geo_desc'].strip()
                    firstfound = True
                if start != None and find_unknown == False:
                    if US_temp.index[i][2] < start:
                        continue
                new_item_t.append(US_temp.iloc[i].iloc[h].replace(',',''))
                new_index_t.append(US_temp.index[i][2])  
            sys.stdout.write("\n\n")
        new_dataframe.append(new_item_t)
        US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
        if US_new.empty == False:
            new_code_t.append(code)
            new_label_t.append(lab)
            US_t = pd.concat([US_t, US_new], ignore_index=True)
    
    US_t = US_t.sort_index(axis=1)
    US_t.insert(loc=0, column='Index', value=new_code_t)
    US_t.insert(loc=1, column='Label', value=new_label_t)
    US_t = US_t.set_index('Index', drop=False)
    label = US_t['Label']

    return US_t, label, note, footnote

def US_BTSDOL(data_path, address, fname, sname, Series, header=None, index_col=None, skiprows=None, freq=None, x='', usecols=None, transpose=True, suffix=None, names=None, TRPT=None, chrome=None):
    MONTH = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    YEAR = ['Year']
    SEMI = {'1st Half':'1','2nd Half':'2'}
    QUAR = {'Q1':'1','Q2':'2','Q3':'3','Q4':'4'}
    REGION = {'Page 7':'R','Page 8':'U'}

    if address.find('BTS') >= 0:
        if fname.find('http') >= 0 and sname != 'Summary Table of Cargo Revenue Ton-Miles':
            chrome.get(fname)
            carrier = Select(chrome.find_element_by_id("CarrierList"))
            if sname.find('US') >= 0:
                carrier.select_by_value("AllUS")
            else:
                carrier.select_by_value("All")
            airport = Select(chrome.find_element_by_id("AirportList"))
            airport.select_by_value("All")
            chrome.find_element_by_id("Link_"+re.sub(r'_.+', "", sname)).click()
            search = BeautifulSoup(chrome.page_source, "html.parser")
            result = search.find(id="GridView1")
            US_t = pd.read_html(str(result), header=header, index_col=index_col)[0]
        elif fname.find('http') >= 0 and sname == 'Summary Table of Cargo Revenue Ton-Miles':
            chrome.get(fname)
            carrier = Select(chrome.find_element_by_id("Carrier"))
            carrier.select_by_value("0:All")
            search = BeautifulSoup(chrome.page_source, "html.parser")
            result = search.find_all("table", class_="largeTABLE")
            US_t = pd.read_html(str(result[2]), skiprows=[0,1], header=header, index_col=index_col)[0]
        else:
            US_t = readExcelFile(data_path+address+fname+'.xls'+x, header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=sname, acceptNoFile=False, usecols_=usecols, names_=names)
    elif address.find('DOL') >= 0:
        chrome.get(fname)
        chrome.find_element_by_xpath("//input[@aria-label='Select US total']").click()
        start_year = Select(chrome.find_element_by_name("strtyear"))
        start_year.select_by_value("1971")
        start_month = Select(chrome.find_element_by_name("strtmonth"))
        start_month.select_by_value("01/01")
        end_year = Select(chrome.find_element_by_name("endyear"))
        end_year.select_by_value(str(datetime.today().year))
        end_month = Select(chrome.find_element_by_name("endmonth"))
        end_month.select_by_value("12/31")
        chrome.find_element_by_name("submit").click()
        US_t = pd.read_html(chrome.page_source, header=[3], index_col=index_col)[0]
        US_t = US_t[US_t.columns[0:6]]
    if type(US_t) != dict and US_t.empty == True:
        ERROR('Sheet Not Found: '+data_path+address+fname+'.xls'+x+', sheet name: '+str(sname))      
    if fname.find('TVT') >= 0:
        for col in range(US_t.shape[1]):
            if 'F' not in list(US_t[US_t.columns[col]]):
                ERROR('Incorrect column selected: '+str(US_t.columns[col]))
    elif address.find('UIWC') >= 0:
        for col in US_t.columns:
            if str(col).find(names[list(US_t.columns).index(col)+1]) < 0:
                ERROR('Incorrect column selected: '+str(col))
    elif fname == 'dl201':
        drop_index = []
        for dex in range(US_t.shape[0]):
            if str(US_t.index[dex]).strip() != 'Total':
                drop_index.append(US_t.index[dex])
        US_t = US_t.drop(drop_index)
        US_t.index = ['Number of Licensed Drivers']
    else:
        US_t = US_t[~US_t.index.duplicated()]
    note_line = []
    for dex in range(len(US_t.index)):
        if type(index_col) == list:
            note_line.append(US_t.index[dex][0])
        else:
            note_line.append(US_t.index[dex])
    note, footnote = US_NOTE(note_line, sname, address=address)
    if transpose == True:
        US_t = US_t.T
    
    if fname == 'TRPT':
        US_t, label, note, footnote = US_STL(US_t, address, Series, TRPT_series=TRPT)
        unit = 'nan'
        return US_t, label, note, footnote, unit
    prefix = 'U'
    if address.find('UIWC') >= 0:
        unit = 'nan'
    else:
        unit = Series['DATA TYPES'].loc[suffix, 'dt_unit']
    if suffix.find('SAT') >= 0:
        note = []
        unit_t = search.find(id="LblHeader").text
        #unit_t = str(readExcelFile(data_path+address+fname+'.xls'+x, usecols_=[0], sheet_name_=sname).iloc[0][0]).strip()
        if bool(re.search(r'.+?\(.+?\(.+?\)\).*', unit_t)):
            unit = re.sub(r'.+?\((.+?)\(.+?\)\).*', r'\1', unit_t).strip().capitalize()
    PASS = ['Air', 'Rail']
    
    new_columns = []
    new_index = []
    new_order = []
    new_label = []
    new_note = []
    new_unit = []
    END = False
    if fname == 'Summary Table of Cargo Revenue Ton-Miles' or sname == 'Summary Table of Cargo Revenue Ton-Miles':
        footnote = []
        for ind in range(US_t.shape[1]):
            if freq == 'A' and str(US_t.columns[ind][0]).find('Total') >= 0:
                new_columns.append(int(str(US_t.columns[ind][0]).replace('Total', '').strip()))
            elif freq == 'M' and str(US_t.columns[ind][0]).isnumeric():
                new_columns.append(str(US_t.columns[ind][0])+'-'+str(datetime.strptime(str(US_t.columns[ind][1]).strip(),'%B').month).rjust(2,'0'))
            else:
                new_columns.append('nan')
        US_t.columns = new_columns
    if suffix.find('SAT') >= 0:
        suf = suffix
        for ind in range(US_t.shape[1]):
            if freq == 'A' and str(US_t.columns[ind][1]).find('TOTAL') >= 0 and str(US_t.columns[ind][0]).isnumeric():
                new_columns.append(int(str(US_t.columns[ind][0]).strip()))
            elif freq == 'M' and str(US_t.columns[ind][1]).isnumeric():
                new_columns.append(str(US_t.columns[ind][0])+'-'+str(US_t.columns[ind][1]).strip().rjust(2,'0'))
            else:
                new_columns.append('nan')
        US_t.columns = new_columns
        US_t = US_t.loc[:, ~US_t.columns.duplicated()]
    elif fname.find('TVT') >= 0:
        region = REGION[sname]
        suffix = suffix+region
        year = 0
        for ind in range(US_t.shape[1]):
            if bool(re.match(r'Year\s*\-\s*[0-9]{4}.*', str(US_t.columns[ind]).strip())):
                year = re.sub(r'Year\s*\-\s*([0-9]{4}).*', r'\1', str(US_t.columns[ind]).strip())
            if freq == 'A' and str(US_t.columns[ind]).strip() in YEAR:
                new_columns.append(year)
            elif freq == 'S' and str(US_t.columns[ind]).strip() in SEMI:
                new_columns.append(year+'-S'+SEMI[str(US_t.columns[ind]).strip()])
            elif freq == 'Q' and str(US_t.columns[ind]).strip() in QUAR:
                new_columns.append(year+'-Q'+QUAR[str(US_t.columns[ind]).strip()])
            elif freq == 'M' and str(US_t.columns[ind]).strip() in MONTH:
                new_columns.append(year+'-'+str(datetime.strptime(str(US_t.columns[ind]).strip(),'%b').month).rjust(2,'0'))
            else:
                new_columns.append('nan')
        US_t.columns = new_columns
        US_t = US_t.loc[:, ~US_t.columns.duplicated()]
    elif address.find('UIWC') >= 0:
        for ind in range(US_t.shape[1]):
            try:
                new_columns.append(datetime.strptime(str(US_t.columns[ind]).strip(), '%m/%d/%Y').strftime('%Y-%m'))
            except ValueError:
                new_columns.append('nan')
        US_t.columns = new_columns
        US_t = US_t.loc[:, ~US_t.columns.duplicated()]
    for ind in range(US_t.shape[0]):
        if fname.find('table') >= 0 or fname.find('TVT') >= 0 or fname == 'dl201' or address.find('UIWC') >= 0:
            index_key = str(US_t.index[ind])
        elif fname == 'Summary Table of Cargo Revenue Ton-Miles' or sname == 'Summary Table of Cargo Revenue Ton-Miles':
            index_key = str(US_t.index[ind][1])
            if index_key.find('Unnamed') >= 0:
                index_key = str(US_t.index[ind][0])
        elif suffix.find('SAT') >= 0:
            index_key = sname
            for item in list(Series['GEO LEVELS']['name']):
                if str(US_t.index[ind]).strip() in re.split(r'//', str(item)):
                    region = Series['GEO LEVELS'].loc[Series['GEO LEVELS']['name'] == item].index[0]
                    break
            suffix = suf+region
        if index_key.find('KEY:') >= 0 or END == True:
            new_index.append(None)
            new_label.append('nan')
            new_order.append(10000)
            new_note.append(None)
            END = True
            continue
        middle = ''
        for item in list(Series['CATEGORIES']['name']):
            match = True
            if suffix.find('SAT') >= 0:
                if index_key not in re.split(r'//', str(item)):
                    match = False
            else:
                for part in re.split(r', ', str(item)):
                    if index_key.find(part) < 0:
                        match = False
                        break
            if match == False:
                continue
            else:
                middle = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item].index[0]
                if fname.find('TVT') >= 0:
                    new_label.append(Series['GEO LEVELS'].loc[region, 'geo_desc']+',  '+Series['CATEGORIES'].loc[middle, 'cat_desc'])
                elif suffix.find('SAT') >= 0:
                    new_label.append(Series['CATEGORIES'].loc[middle, 'cat_desc']+',  '+Series['GEO LEVELS'].loc[region, 'geo_desc'])
                else:
                    new_label.append(Series['CATEGORIES'].loc[middle, 'cat_desc'])
                new_order.append(Series['CATEGORIES'].loc[middle, 'order'])
                if address.find('UIWC') >= 0:
                    new_unit.append(Series['CATEGORIES'].loc[middle, 'cat_unit'])
                break
        if middle == '':
            to_pass = False
            if str(US_t.iloc[ind].iloc[0]) == 'nan':
                to_pass = True
            if to_pass == False and index_key not in PASS:
                ERROR('Item index not found in '+fname+': '+index_key)
            else:
                new_index.append(None)
                new_label.append('nan')
                new_order.append(10000)
                new_note.append(None)
        else:
            new_index.append(prefix+middle+suffix)
            if fname.find('TVT') >= 0 or suffix.find('SAT') >= 0 or fname == 'dl201' or address.find('UIWC') >= 0:
                new_note.append('')
            elif fname == 'Summary Table of Cargo Revenue Ton-Miles' or sname == 'Summary Table of Cargo Revenue Ton-Miles':
                new_note.append(re.sub(r'.+?(/[a-z,]+/)*$', r'\1', index_key))
            elif index_key.find('total') >= 0:
                new_note.append(re.search(r'([a-z],\s*)*[a-z],\s', index_key).group().strip(', '))
            elif index_key.find('Intercity/Amtrak') >= 0:
                new_note.append(re.sub(r'Intercity/Amtrak(([a-z],)*[a-z]).*', r"\1", index_key))
            else:
                new_note.append(re.search(r'([a-z],\s*)*[a-z]$', index_key).group())
    
    US_t.insert(loc=0, column='Index', value=new_index)
    US_t = US_t.set_index('Index', drop=False)
    US_t.insert(loc=1, column='Label', value=new_label)
    US_t.insert(loc=2, column='order', value=new_order)
    if address.find('UIWC') >= 0:
        US_t.insert(loc=3, column='unit', value=new_unit)
    else:
        US_t.insert(loc=3, column='Label_note', value=new_note)
    US_t = US_t.sort_values(by='order')
    US_t = US_t.loc[US_t.index.dropna()]
    label = US_t['Label']

    return US_t, label, note, footnote, unit

def US_ISM(US_t, fname, Series):
    note = []
    footnote = []
    PASS = []
    ISADJUSTED = {'Seasonally Adjusted':'A', 'Not Seasonally Adjusted':'U'}
    prefix = ISADJUSTED[Series['ISADJUSTED'].loc[fname, 'adj_desc']]
    middle = Series['INDUSTRY'].loc[Series['INDUSTRY']['name'] == re.sub(r'([A-Z]+?)_.+', r"\1", fname)].index[0]
    for item in list(Series['DATA TYPES']['name']):
        if re.sub(r'[A-Z]+?_(.+)', r"\1", fname) in re.split(r'//', str(item)):
            suf = Series['DATA TYPES'].loc[Series['DATA TYPES']['name'] == item].index[0]
    
    new_index = []
    new_order = []
    new_label = []
    new_unit = []
    for ind in range(US_t.shape[0]):
        fix = ''
        for item in list(Series['CATEGORIES']['name']):
            if str(US_t.index[ind]).strip() in re.split(r'//', str(item)):
                fix = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item].index[0]
                new_label.append(Series['CATEGORIES'].loc[fix, 'cat_desc'])
                new_order.append(Series['CATEGORIES'].loc[fix, 'order'])
                new_unit.append(Series['CATEGORIES'].loc[fix, 'cat_unit'])
                break
        if fix == '':
            to_pass = False
            if str(US_t.iloc[ind].iloc[0]) == 'nan':
                to_pass = True
            if to_pass == False and str(US_t.index[ind]).strip() not in PASS:
                ERROR('Item index not found in '+fname+': '+str(US_t.index[ind]).strip())
            else:
                new_index.append(None)
                new_label.append('nan')
                new_order.append(10000)
                new_unit.append(None)
        else:
            new_index.append(prefix+middle+suf+fix)
    
    US_t.insert(loc=0, column='Index', value=new_index)
    US_t = US_t.set_index('Index', drop=False)
    US_t.insert(loc=1, column='Label', value=new_label)
    US_t.insert(loc=2, column='order', value=new_order)
    US_t.insert(loc=3, column='unit', value=new_unit)
    US_t = US_t.sort_values(by='order')
    US_t = US_t.loc[US_t.index.dropna()]
    label = US_t['Label']

    return US_t, label, note, footnote

def US_RCM(US_t, fname, Series):
    note = []
    footnote = []
    PASS = []
    
    new_index = []
    new_order = []
    new_label = []
    new_unit = []
    for ind in range(US_t.shape[0]):
        prefix = Series['ISADJUSTED'].loc[Series['ISADJUSTED']['name'] == re.sub(r'([a-z]+?)_.+', r"\1", str(US_t.index[ind]).strip())].index[0]
        middle = Series['INDUSTRY'].loc[Series['INDUSTRY']['name'] == re.sub(r'[a-z]+?_([a-z]+?)_.+', r"\1", str(US_t.index[ind]).strip())].index[0]
        try:
            fix = Series['PERIOD'].loc[Series['PERIOD']['name'] == re.sub(r'.+_(.+)$', r"\1", str(US_t.index[ind]).strip())].index[0]
        except IndexError:
            if re.sub(r'.+_(.+)$', r"\1", str(US_t.index[ind]).strip()) == 'composite':
                fix = 'C'
            else:
                ERROR('Incorrect index suffix: '+str(US_t.index[ind]).strip())
        suf = ''
        for item in list(Series['CATEGORIES']['name']):
            if str(US_t.index[ind]).find(item) >= 0:
                suf = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item].index[0]
                new_label.append(Series['CATEGORIES'].loc[suf, 'cat_desc'])
                new_order.append(Series['CATEGORIES'].loc[suf, 'order'])
                new_unit.append(Series['CATEGORIES'].loc[suf, 'cat_unit'])
                break
        if suf == '':
            to_pass = False
            if str(US_t.iloc[ind].iloc[0]) == 'nan':
                to_pass = True
            if to_pass == False and str(US_t.index[ind]).strip() not in PASS:
                ERROR('Item index not found: '+str(US_t.index[ind]).strip())
            else:
                new_index.append(None)
                new_label.append('nan')
                new_order.append(10000)
                new_unit.append(None)
        else:
            new_index.append(prefix+middle+suf+fix)
    
    US_t.insert(loc=0, column='Index', value=new_index)
    US_t = US_t.set_index('Index', drop=False)
    US_t.insert(loc=1, column='Label', value=new_label)
    US_t.insert(loc=2, column='order', value=new_order)
    US_t.insert(loc=3, column='unit', value=new_unit)
    US_t = US_t.sort_values(by='order')
    US_t = US_t.loc[US_t.index.dropna()]
    label = US_t['Label']

    return US_t, label, note, footnote

def US_CBS(address, fname, sname, Series, US_t=pd.DataFrame(), chrome=None):
    note = []
    footnote = []
    PASS = ['0. No Reply','10. N/A']
    sname_temp = ''

    if US_t.empty == True and address.find('NFIB') >= 0:
        chrome.get(fname)
        chrome.execute_script("document.getElementById('indicators1').setAttribute('style', 'display: block;')")
        indicator = Select(chrome.find_element_by_id('indicators1'))
        sname_temp = sname
        sname = 'Most Important Reason for Higher Earnings'
        while True:
            try:
                ActionChains(chrome).click(indicator.select_by_visible_text(sname)).send_keys(Keys.ENTER).perform()
            except NoSuchElementException:
                ERROR('Item "'+sname+'" Not Found in address: '+fname)
            chrome.execute_script("document.getElementById('grid').setAttribute('style', 'display: block;')")
            if sname_temp == '':
                print('Loading...')
                time.sleep(20)
                break
            else:
                time.sleep(3)
                sname = sname_temp
                sname_temp = ''
        while True:
            try:
                pd.read_html(chrome.page_source)
            except ValueError:
                time.sleep(2)
            else:
                break
        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/a[@data-page="2"]'))).click()
        try:
            WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/a[@class="k-link" and @data-page="1"]'))).click()
        except TimeoutException:
            time.sleep(1)
        US_t = pd.read_html(chrome.page_source)[1]
        US_t.columns = pd.read_html(chrome.page_source)[0].columns
        if sname == 'Amount of Capital Expenditures Made':
            US_t = US_t[[US_t.columns[0],US_t.columns[2],US_t.columns[4]]]
        else:
            US_t = US_t.set_index(US_t.columns[0])
        i = 2
        while(chrome.find_element_by_class_name("k-state-selected").text != chrome.find_element_by_xpath('.//a[@title="Go to the last page"]').get_attribute("data-page")):
            WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/a[@data-page="'+str(i)+'"]'))).click()
            US_temp = pd.read_html(chrome.page_source)[1]
            US_temp.columns = pd.read_html(chrome.page_source)[0].columns
            if sname == 'Amount of Capital Expenditures Made':
                US_temp = US_temp[[US_temp.columns[0],US_temp.columns[2],US_temp.columns[4]]]
                US_t = pd.concat([US_t, US_temp], ignore_index=True)
            else:
                US_temp = US_temp.set_index(US_temp.columns[0])
                US_t = pd.concat([US_t, US_temp])
            i+=1
        WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/a[@class="k-button k-button-icontext k-grid-excel"]'))).click()
        chrome.execute_script("window.open()")
        chrome.switch_to.window(chrome.window_handles[-1])
        chrome.get('chrome://downloads')
        time.sleep(1)
        excel_file = chrome.execute_script("return document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('div#content  #file-link').text")
        chrome.close()
        chrome.switch_to.window(chrome.window_handles[0])
        if sname != 'Amount of Capital Expenditures Made':
            US_t = US_t.T
            US_copy = US_t.copy()
            US_excel = readExcelFile((Path.home() / "Downloads" / excel_file).as_posix(), header_ =0, index_col_=0, sheet_name_=0).T
        else:
            US_copy = US_t.copy()
            US_excel = readExcelFile((Path.home() / "Downloads" / excel_file).as_posix(), header_ =0, sheet_name_=0, usecols_=[0,2,4])
            US_temp = US_t.sort_values(by=['Answer','Date'], ignore_index=True)
            US_t = pd.DataFrame()
            new_dataframe = []
            new_item_t = []
            new_index_t = []
            firstfound = False
            code = ''
            for i in range(US_temp.shape[0]):
                if US_temp.iloc[i]['Answer'] != code:
                    if firstfound == True:
                        new_dataframe.append(new_item_t)
                        US_new = pd.DataFrame(new_dataframe, index=[code], columns=new_index_t)
                        if US_new.empty == False:
                            US_t = pd.concat([US_t, US_new])
                        new_dataframe = []
                        new_item_t = []
                        new_index_t = []
                    code = US_temp.iloc[i]['Answer']
                    firstfound = True
                new_item_t.append(US_temp.iloc[i]['Percent'])
                new_index_t.append(US_temp.iloc[i]['Date'])  
            new_dataframe.append(new_item_t)
            US_new = pd.DataFrame(new_dataframe, index=[code], columns=new_index_t)
            if US_new.empty == False:
                US_t = pd.concat([US_t, US_new])
        if US_copy.equals(US_excel) == False:
            ERROR('Not equal: '+sname)
        try:
            (Path.home() / "Downloads" / excel_file).unlink()
        except FileNotFoundError:
            time.sleep(1)
        sys.stdout.write("\n")
    elif US_t.empty == True and address.find('OECD') >= 0:
        chrome.get(fname)
        chrome.implicitly_wait(3)
        chart = chrome.find_element_by_xpath('.//div[@class="ddp-chart indicator-main-chart normal compact-header"]')
        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/a[@class="dropdown-button light highlighted-locations-dropdown-button"]'))).click()
        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/li[@data-id="USA"]'))).click()
        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@value="_HIGHLIGHTED"]'))).click()
        if chart.get_attribute("data-show-baseline") == 'true':
            WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@class="baseline-comparison-checkbox"]'))).click()
        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/a[@class="close-btn"]'))).click()
        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="dropdown single-subject-dropdown"]'))).click()
        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/a[@data-value="AMPLITUD"]'))).click()
        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/div[@class="dropdown measures"]'))).click()
        WebDriverWait(chrome, 3).until(EC.element_to_be_clickable((By.XPATH, './/a[@data-value="LTRENDIDX"]'))).click()
        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/a[@data-value="M"]'))).click()
        if chart.get_attribute("data-use-latest-data") == 'true':
            WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/input[@class="use-latest-data-checkbox"]'))).click()
        start = chrome.find_element_by_xpath('.//div[@class="noUi-handle noUi-handle-lower"]')
        end = chrome.find_element_by_xpath('.//div[@class="noUi-handle noUi-handle-upper"]')
        start_loc = 0
        end_loc = 0
        print('Loading...')
        while True:
            if chrome.find_element_by_xpath('.//div[@class="noUi-origin noUi-background"]').get_attribute("style").find('100%') < 0:
                ActionChains(chrome).drag_and_drop_by_offset(end,end_loc,0).release(end).perform()
                end_loc+=20
            ActionChains(chrome).drag_and_drop_by_offset(start,start_loc,0).release(start).perform()
            start_loc-=20
            try:
                if chrome.find_element_by_xpath('.//div[@class="noUi-origin noUi-connect noUi-dragable"]').get_attribute("style").find(' 0%') >= 0 \
                and chrome.find_element_by_xpath('.//div[@class="noUi-origin noUi-background"]').get_attribute("style").find('100%') >= 0:
                    break
            except NoSuchElementException:
                continue
        chrome.execute_script("window.scrollTo(0,0)")
        while True:
            try:
                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/a[@class="dropdown-button dark chart-button download-btn"]'))).click()
                time.sleep(3)
                WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.XPATH, './/a[@class="download-selection-button"]'))).click()
                time.sleep(3)
            except:
                time.sleep(5)
            else:
                break
        US_t = US_WEBDRIVER(chrome, address, fname, sname, header=0, index_col=0, usecols=[5,6], csv=True).T
        sys.stdout.write("\n")

    new_index = []
    new_label = []
    new_unit = []
    new_cols = []
    if fname != 'Consumer Confidence Index' and sname != 'Consumer Confidence Index':
        for col in range(US_t.shape[1]):
            new_cols.append(datetime.strptime(str(US_t.columns[col]).strip(), '%Y/%m/%d').strftime('%Y-%m'))
        US_t.columns = new_cols
    for ind in range(US_t.shape[0]):
        if sname == 'Amount of Capital Expenditures Made':
            if str(US_t.index[ind]).strip() not in list(Series['item']):
                to_pass = False
                if str(US_t.iloc[ind].iloc[0]) == 'nan':
                    to_pass = True
                if to_pass == False and str(US_t.index[ind]).strip() not in PASS:
                    ERROR('Item index not found: '+str(US_t.index[ind]).strip())
                else:
                    new_index.append(None)
                    new_label.append('nan')
                    new_unit.append(None)
            else:
                prefix = Series.loc[Series['item'] == str(US_t.index[ind]).strip()]['prefix'].item()
                middle = Series.loc[Series['item'] == str(US_t.index[ind]).strip()]['middle'].item()
                suffix = Series.loc[Series['item'] == str(US_t.index[ind]).strip()]['suffix'].item()
                new_label.append(sname+',  '+re.sub(r'^[0-9]+\.\s*', "", str(US_t.index[ind]).strip()).title().replace('Or','or'))
                new_unit.append(Series.loc[Series['item'] == str(US_t.index[ind]).strip()]['unit'].item())
                new_index.append(prefix+middle+suffix)
        else:
            prefix = Series.loc[sname, 'prefix']
            middle = Series.loc[sname, 'middle']
            suffix = Series.loc[sname, 'suffix']
            new_label.append(sname)
            new_unit.append(Series.loc[sname, 'unit'])
            new_index.append(prefix+middle+suffix)
    
    US_t = US_t.sort_index(axis=1)
    US_t.insert(loc=0, column='Index', value=new_index)
    US_t = US_t.set_index('Index', drop=False)
    US_t.insert(loc=1, column='Label', value=new_label)
    US_t.insert(loc=2, column='unit', value=new_unit)
    US_t = US_t.loc[US_t.index.dropna()]
    label = US_t['Label']

    return US_t, label, note, footnote

def US_DOA(US_temp, Series, Table, address, fname, sname, chrome):
    MON = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
    note = []
    footnote = []

    if US_temp.empty == True:
        chrome.get(fname)
        print('Downloading File...')
        WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/select[@id="source_desc"]/option[text()="'+Table.loc[sname, 'Program']+'"]'))).click()
        WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/select[@id="sector_desc"]/option[text()="'+Table.loc[sname, 'Sector']+'"]'))).click()
        WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/select[@id="group_desc"]/option[text()="'+Table.loc[sname, 'Group']+'"]'))).click()
        WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/select[@id="commodity_desc"]/option[text()="'+Table.loc[sname, 'Commodity']+'"]'))).click()
        for item in re.split(r', ', str(Table.loc[sname, 'Data Items'])):
            WebDriverWait(chrome, 15).until(EC.element_to_be_clickable((By.XPATH, './/select[@id="statisticcat_desc"]/option[text()="'+Table.loc[sname, 'Category']+', '+item+'"]'))).click()
        if sname == 'PPITW':
            WebDriverWait(chrome, 30).until(EC.element_to_be_clickable((By.ID, 'short_desc'))).click()
            chrome.find_element_by_id("short_desc").send_keys(Keys.CONTROL, 'a')
        else:
            for item in re.split(r', ', str(Table.loc[sname, 'Data Items'])):
                WebDriverWait(chrome, 15).until(EC.element_to_be_clickable((By.XPATH, './/select[@id="short_desc"]/option[text()="'+Table.loc[sname, 'Commodity']+' - '+Table.loc[sname, 'Category']+', '+item+'"]'))).click()
        WebDriverWait(chrome, 30).until(EC.element_to_be_clickable((By.XPATH, './/select[@id="domain_desc"]/option[text()="TOTAL"]'))).click()
        WebDriverWait(chrome, 30).until(EC.element_to_be_clickable((By.XPATH, './/select[@id="agg_level_desc"]/option[text()="NATIONAL"]'))).click()
        WebDriverWait(chrome, 30).until(EC.element_to_be_clickable((By.XPATH, './/select[@id="state_name"]/option[text()="US TOTAL"]'))).click()
        chrome.find_element_by_id("year").send_keys(Keys.CONTROL, 'a')
        WebDriverWait(chrome, 30).until(EC.element_to_be_clickable((By.XPATH, './/select[@id="freq_desc"]/option[text()="MONTHLY"]'))).click()
        WebDriverWait(chrome, 30).until(EC.element_to_be_clickable((By.ID, 'reference_period_desc'))).click()
        chrome.find_element_by_id("reference_period_desc").send_keys(Keys.CONTROL, 'a')
        time.sleep(3)
        WebDriverWait(chrome, 1).until(EC.element_to_be_clickable((By.ID, 'submit001'))).click()
        WebDriverWait(chrome, 30).until(EC.element_to_be_clickable((By.XPATH, './/a[@href="javascript:download();"]'))).click()
        US_temp = US_WEBDRIVER(chrome, address, fname, sname, header=0, usecols=[1,2,16,19], csv=True)
        sys.stdout.write("\n")
    
    for i in range(US_temp.shape[0]):
        US_temp.loc[i, 'Period'] = MON[US_temp.iloc[i]['Period']]
    US_temp = US_temp.sort_values(by=['Data Item','Year','Period'], ignore_index=True)
    
    US_t = pd.DataFrame()
    new_item_t = []
    new_index_t = []
    new_code_t = []
    new_label_t = []
    new_form_t = []
    new_unit_t = []
    new_dataframe = []
    firstfound = False
    data = ''
    for i in range(US_temp.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/US_temp.shape[0], 1))+"%)*")
        sys.stdout.flush()
        if US_temp.iloc[i]['Data Item'] != data:
            if firstfound == True:
                new_dataframe.append(new_item_t)
                US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
                if US_new.empty == False:
                    new_code_t.append(code)
                    new_label_t.append(lab)
                    new_form_t.append(form)
                    new_unit_t.append(unit)
                    US_t = pd.concat([US_t, US_new], ignore_index=True)
                new_dataframe = []
                new_item_t = []
                new_index_t = []
            data = US_temp.iloc[i]['Data Item']
            prefix = ''
            middle = ''
            suffix = ''
            for item in list(Series['DATA TYPES']['name']):
                if data.find(str(item)) >= 0:
                    prefix = Series['DATA TYPES'].loc[Series['DATA TYPES']['name'] == item].index[0]
                    form = Series['DATA TYPES'].loc[prefix, 'dt_desc']
                    break
            for item in list(Series['CATEGORIES']['name']):
                if data.find(str(item)) >= 0:
                    middle = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item].index[0]
                    lab = Series['CATEGORIES'].loc[middle, 'cat_desc']
                    break
            for item in list(Series['BASE']['name']):
                if data.find(str(item)) >= 0:
                    suffix = Series['BASE'].loc[Series['BASE']['name'] == item].index[0]
                    unit = Series['BASE'].loc[suffix, 'base_desc']
                    break
            if prefix == '' or middle == '' or suffix == '':
                ERROR('Item index not found: '+data)
            else:
                code = prefix+middle+suffix
                if code in new_code_t:
                    ERROR('Item duplicated: '+data+', code = '+code)
            firstfound = True
        value = str(US_temp.iloc[i]['Value']).replace(',','')
        new_item_t.append(value)
        new_index_t.append(str(US_temp.iloc[i]['Year'])+'-'+str(US_temp.iloc[i]['Period']).rjust(2,'0'))  
    sys.stdout.write("\n\n")
    new_dataframe.append(new_item_t)
    US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
    if US_new.empty == False:
        new_code_t.append(code)
        new_label_t.append(lab)
        new_form_t.append(form)
        new_unit_t.append(unit)
        US_t = pd.concat([US_t, US_new], ignore_index=True)
    US_t = US_t.sort_index(axis=1)
    US_t.insert(loc=0, column='Index', value=new_code_t)
    US_t.insert(loc=1, column='Label', value=new_label_t)
    US_t.insert(loc=2, column='form', value=new_form_t)
    US_t.insert(loc=3, column='unit', value=new_unit_t)
    US_t = US_t.set_index('Index', drop=False)
    label = US_t['Label']

    return US_t, label, note, footnote

def US_AISI(data_path, address, fname, steelorbis_year='2019'):
    note = []
    footnote = []
    IHS = readExcelFile(data_path+address+'Historical Data.xlsx', header_=0, index_col_=0, sheet_name_=0)

    US_t = pd.DataFrame()
    new_item_t0 = []
    new_item_t1 = []
    new_index_t = []
    new_code_t = list(IHS.index)
    new_label_t = list(IHS['Label'])
    new_unit_t = list(IHS['unit'])
    new_dataframe = []
    for i in range(7):
        if (datetime.today()-timedelta(days=i)).weekday() == 5:
            start = datetime.today()-timedelta(days=i)
            break
    delta = 0
    worksheet = False
    while True:
        sys.stdout.write("\rProducing data from AISI...["+(start-timedelta(days=delta)).strftime('%Y-%m-%d')+"]*")
        sys.stdout.flush()
        response = rq.get(fname+(start-timedelta(days=delta)).strftime('%B-%d-%Y').lower().replace('-0','-'))
        search = BeautifulSoup(response.text, "html.parser")
        try:
            result2 = search.find("p").text
            if result2.find('no longer active') >= 0:
                if delta == 0:
                    delta+=7
                    continue
                else:
                    break
        except AttributeError:
            if delta == 0:
                delta+=7
                continue
            else:
                break
        production = re.sub(r'.+?([0-9,]+)\s+nt\s.+', r"\1", re.sub(r'.+?([0-9,]+)\s+net\stons.+', r"\1", result2.replace('\n',''), 1), 1)
        rate = re.sub(r'.+?utilization\srate.+?([0-9.]+)\s+percent.+', r"\1", result2.replace('\n',''), 1)
        date = (start-timedelta(days=delta)).strftime('%Y-%m-%d')
        new_item_t0.append(float(production.replace(',','')))
        new_item_t1.append(float(rate))
        new_index_t.append(date)
        #print('"'+date+'"', '"'+production+' net tons"','"'+rate+' percent"')
        delta+=7
    sys.stdout.write("\n\n")

    begin = False
    while begin == False:
        if worksheet == False and (start-timedelta(days=delta)).strftime('%Y-%m-%d') not in IHS.columns:
            sys.stdout.write("\rProducing data from steelorbis.com...["+(start-timedelta(days=delta)).strftime('%Y-%m-%d')+"]*")
            sys.stdout.flush()
            date = None
            DATE = start-timedelta(days=delta)
            date_begin = DATE
            old_date = None
            for page in range(1, 35):
                response = rq.get("https://www.steelorbis.com/steel-companies/company/companyContactSearch.do?page="+str(page)+"&searchKey=utilization%20raw%20steel&method=showArticleSearchView")
                search = BeautifulSoup(response.text, "html.parser")#2018-01-13
                result = search.select("tr")
                for res in result:
                    if date != None:
                        DATE = start-timedelta(days=delta)
                    if bool(re.search(r"^US raw|[^mdy] steel (production[,]*|mill[s']*|output) [^shltmdu]|^US steel [^me]|US[']* (weekly|domestic) (raw|steel)|Weekly (raw|steel)|raw steel|AISI|year-over-year|week-on-week|^US crude steel production|^US steel mill utilization rate", res.text.replace('\n',''))) and re.sub(r'[\n\r\t]', "", res.text)[-4:] <= str(date_begin.year) and re.sub(r'[\n\r\t]', "", res.text)[-4:] > steelorbis_year:
                        response2 = rq.get("https://www.steelorbis.com"+res.find("a")["href"])
                        search2 = BeautifulSoup(response2.text, "html.parser")
                        try:
                            result2 = search2.find("div", class_="table-responsive cofax-article-body").text
                        except AttributeError:
                            #print('Missing Data: '+res.text)
                            continue
                        production = re.sub(r'.+?([0-9,]+)\s+[nm]t[\s,\.].+', r"\1", re.sub(r'.+?([0-9,\.]+)\s+(million\s)*net\stons.+', r"\1", result2.replace('\n',''), 1), 1)
                        if bool(re.search(r'([0-9,]+)\s+mt\s', result2.replace('\n',''))):
                            production = "{:,}".format(int(float(production.replace(',',''))*1.10231/1000)*1000)
                        elif bool(re.search(r'.+?([0-9,\.]+)\smillion\snet\stons.+', result2.replace('\n',''))) and re.sub(r'.+?([0-9,\.]+)\smillion\snet\stons.+', r"\1", result2.replace('\n',''), 1) == production:
                            production = "{:,}".format(float(production)*1000000)
                        rate = re.sub(r'.+?utilization.+?([0-9.]+)\s+percent.+', r"\1", result2.replace('\n',''), 1)
                        date = re.sub(r'.+?([A-Z][a-z]+\s[0-9]+).+', r"\1", re.sub(r'.+?\(ended\s([A-Za-z]+\s[0-9]+)\).+', r"\1", re.sub(r'.+?([A-Za-z\.]+\s[0-9]+,*\s[0-9]{4}).+', r"\1", result2.replace('\n',''), 1), 1),1)
                        if bool(re.search(r'[0-9]{4}$', date)) == False:
                            if DATE != None:
                                date = date+', '+str(DATE.year)
                            else:
                                year_text = search2.find("div", class_="col-sm-8 col-md-9").text
                                date = date+', '+re.sub(r'.+?([0-9]{4}).+', r"\1", year_text.replace('\n',''), 1)
                        if old_date != None and date == old_date:
                            continue
                        try:
                            datestrip = datetime.strptime(date,'%B %d, %Y')
                        except ValueError:
                            try:
                                datestrip = datetime.strptime(date,'%B %d %Y')
                            except ValueError:    
                                datestrip = datetime.strptime(date,'%b. %d, %Y')
                        if DATE != None:
                            while datestrip.strftime('%Y-%m-%d') < DATE.strftime('%Y-%m-%d'):
                                date2 = re.sub(r'.+?([A-Z][a-z\.]+\s[0-9]+).+', r"\1", result2.replace('\n',''))
                                year_text = search2.find("div", class_="col-sm-8 col-md-9").text
                                date2 = date2+', '+re.sub(r'.+?([0-9]{4}).+', r"\1", year_text.replace('\n',''), 1)
                                try:
                                    datestrip2 = datetime.strptime(date2,'%B %d, %Y')
                                except ValueError:
                                    try:
                                        datestrip2 = datetime.strptime(date2,'%B %d %Y')
                                    except ValueError:    
                                        datestrip2 = datetime.strptime(date,'%b. %d, %Y')    
                                if datestrip2.strftime('%Y-%m-%d') != DATE.strftime('%Y-%m-%d'):
                                    found = False
                                    for t in range(1,4):
                                        if (datestrip-timedelta(days=t)).strftime('%Y-%m-%d') == DATE.strftime('%Y-%m-%d'):
                                            date = (datestrip-timedelta(days=t)).strftime('%B %d, %Y')
                                            datestrip = datestrip-timedelta(days=t)
                                            found = True
                                            break
                                        elif (datestrip+timedelta(days=t)).strftime('%Y-%m-%d') == DATE.strftime('%Y-%m-%d'):
                                            date = (datestrip+timedelta(days=t)).strftime('%B %d, %Y')
                                            datestrip = datestrip+timedelta(days=t)
                                            found = True
                                            break
                                        elif (datestrip2-timedelta(days=t)).strftime('%Y-%m-%d') == DATE.strftime('%Y-%m-%d'):
                                            date = (datestrip2-timedelta(days=t)).strftime('%B %d, %Y')
                                            datestrip = datestrip2-timedelta(days=t)
                                            found = True
                                            break
                                        elif (datestrip2+timedelta(days=t)).strftime('%Y-%m-%d') == DATE.strftime('%Y-%m-%d'):
                                            date = (datestrip2+timedelta(days=t)).strftime('%B %d, %Y')
                                            datestrip = datestrip2+timedelta(days=t)
                                            found = True
                                            break
                                    if found == False:
                                        #print('Date not found: '+DATE.strftime('%Y-%m-%d'))#2016-12-24, 2015-07-25, 2014-02-15, 2010-10-02
                                        checkDate = False
                                        if old_result.find(old_production) >= 0 or old_result.find(str(float(old_production.replace(',',''))/1000000)+' million') >= 0:
                                            if old_result.find(old_production) >= 0:
                                                old_pro = old_production
                                            elif old_result.find(str(float(old_production.replace(',',''))/1000000)+' million') >= 0:
                                                old_pro = str(float(old_production.replace(',',''))/1000000)+' million'
                                            old_ra = old_rate
                                            #print(old_result.replace('\n','')[old_result.replace('\n','').find(old_pro)+9:])
                                            old_production = re.sub(r'.+?([0-9,]+)\s+[nm]t[\s,\.].+', r"\1", re.sub(r'.+?([0-9,]+)\s+net\stons.+', r"\1", old_result.replace('\n','')[old_result.replace('\n','').find(old_pro)+9:], 1), 1)
                                            if bool(re.search(r'([0-9,]+)\s+mt\s', old_result.replace('\n',''))):
                                                old_production = "{:,}".format(int(float(old_production.replace(',',''))*1.10231/1000)*1000)
                                            old_rate = re.sub(r'.+?([0-9.]+)\s+percent.+', r"\1", old_result.replace('\n','')[old_result.replace('\n','').find(old_ra)+4:], 1)
                                            try:
                                                float(old_production.replace(',',''))
                                            except ValueError:
                                                datestrf = DATE.strftime('%Y-%m-%d')
                                                #print('Date not found: '+datestrf)
                                                delta+=7
                                            else:
                                                if old_result.find(DATE.strftime('%B %d').replace(' 0',' ')) >= 0 and DATE.strftime('%Y-%m-%d') != '2008-11-08':
                                                    checkDate = True
                                                else:
                                                    datestrf = DATE.strftime('%Y-%m-%d')
                                                    new_item_t0.append(float(old_production.replace(',','')))
                                                    new_item_t1.append(float(old_rate))
                                                    new_index_t.append(datestrf)
                                                    #print('"'+datestrf+'"', '"'+old_production+' net tons"','"'+old_rate+' percent"')
                                                    delta+=7
                                            if checkDate == True and (old_result.find(DATE.strftime('%B %d').replace(' 0',' ')) >= 0 or old_result.find((DATE+timedelta(days=2)).strftime('%B %d').replace(' 0',' ')) >= 0):
                                                DATEstrf = DATE.strftime('%B %d').replace(' 0',' ')
                                                old_production = re.sub(r'.+?([0-9,]+)\s+[nm]t\s.+', r"\1", re.sub(r'.+?([0-9,]+)\s+net\stons.+', r"\1", old_result.replace('\n','')[old_result.replace('\n','').find(DATEstrf):], 1), 1)
                                                if bool(re.search(r'([0-9,]+)\s+mt\s', old_result.replace('\n',''))):
                                                    old_production = "{:,}".format(int(float(old_production.replace(',',''))*1.10231/1000)*1000)
                                                old_rate = re.sub(r'.+?utilization.+?([0-9.]+)\s+percent.+', r"\1", old_result.replace('\n','')[old_result.replace('\n','').find(DATEstrf):], 1)
                                                datestrf = DATE.strftime('%Y-%m-%d')
                                                new_item_t0.append(float(old_production.replace(',','')))
                                                new_item_t1.append(float(old_rate))
                                                new_index_t.append(datestrf)
                                                #print('"'+datestrf+'"', '"'+old_production+' net tons"','"'+old_rate+' percent"')
                                                delta+=7
                                        else:
                                            datestrf = DATE.strftime('%Y-%m-%d')
                                            #print('Date not found: '+datestrf)
                                            delta+=7
                                else:
                                    date = date2
                                    datestrip = datestrip2
                        datestrf = datestrip.strftime('%Y-%m-%d')
                        new_item_t0.append(float(production.replace(',','')))
                        new_item_t1.append(float(rate))
                        new_index_t.append(datestrf)
                        #print('"'+datestrf+'"', '"'+production+' net tons"','"'+rate+' percent"')
                        delta+=7
                        old_result = result2
                        old_date = date
                        old_production = production
                        old_rate = rate
                    elif re.sub(r'[\n\r\t]', "", res.text)[-4:] <= steelorbis_year:
                        worksheet = True
                        break
                if worksheet == True:
                    break
        else:
            worksheet = True
            sys.stdout.write("\rProducing data from historical data...["+(start-timedelta(days=delta)).strftime('%Y-%m-%d')+"]*")
            sys.stdout.flush()    
            production = IHS[(start-timedelta(days=delta)).strftime('%Y-%m-%d')].iloc[0]
            rate = IHS[(start-timedelta(days=delta)).strftime('%Y-%m-%d')].iloc[1]
            date = (start-timedelta(days=delta)).strftime('%Y-%m-%d')
            new_item_t0.append(float(production))
            new_item_t1.append(float(rate))
            new_index_t.append(date)
            #print('"'+date+'"', '"'+"{:,}".format(production)+' net tons"','"'+str(rate)+' percent"')
            delta+=7
            if date == '1963-01-05':
                begin = True
    sys.stdout.write("\n\n")

    new_dataframe.append(new_item_t0)
    new_dataframe.append(new_item_t1)
    US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
    if US_new.empty == False:
        US_t = pd.concat([US_t, US_new], ignore_index=True)
    US_t = US_t.loc[:, ~US_t.columns.duplicated()]
    US_t = US_t.sort_index(axis=1)
    US_t.insert(loc=0, column='Index', value=new_code_t)
    US_t.insert(loc=1, column='Label', value=new_label_t)
    US_t.insert(loc=2, column='unit', value=new_unit_t)
    US_t = US_t.set_index('Index', drop=False)
    label = US_t['Label']

    US_t.to_excel(data_path+address+'Historical Data.xlsx', sheet_name='Weekly_Sat')
    return US_t, label, note, footnote

def US_EIAIRS(Series, data_path, address, fname, sname, freq, x='', header=None, index_col=None, skiprows=None, transpose=True, usecols=None, prefix=None, nrows=None, chrome=None):
    if fname.find('http') >= 0:
        US_t = {}
        revised_year = str(datetime.today().year-1)
        original_year = str(datetime.today().year)
        chrome.get(fname)
        try:
            revised = chrome.find_elements_by_xpath('.//table[caption/text()="Revised estimates"]/tbody/tr[td/text()="'+revised_year+'"]/td/a')
        except:
            print('Revised estimates data not found.')
        else:
            for element in revised:
                if element.get_attribute('href').find('week') >= 0:
                    element.click()
                    sname_r = sname+revised_year+'tot'
                    US_t[revised_year] = US_WEBDRIVER(chrome, address, fname, sname_r, header=header, index_col=index_col, skiprows=skiprows, usecols=usecols, csv=False)
                    break
        try:
            original = chrome.find_elements_by_xpath('.//table[caption/text()="Original estimates*"]/tbody/tr[td/text()="'+original_year+'"]/td/a')
        except:
            print('Revised estimates data not found.')
        else:
            for element in original:
                if element.get_attribute('href').find('week') >= 0:
                    element.click()
                    sname_o = sname+'forecast'+original_year+'tot'
                    US_t[original_year] = US_WEBDRIVER(chrome, address, fname, sname_o, header=header, index_col=index_col, skiprows=skiprows, usecols=usecols, csv=False)
                    break
    elif x == 'csv':
        US_t = readFile(data_path+address+fname+'.csv', header_=header, index_col_=index_col, skiprows_=skiprows, acceptNoFile=False, usecols_=usecols)
    else:
        US_t = readExcelFile(data_path+address+fname+'.xls'+x, header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=sname, acceptNoFile=False, usecols_=usecols, nrows_=nrows)
    if type(US_t) != dict and US_t.empty == True:
        ERROR('Sheet Not Found: '+data_path+address+fname+'.xls'+x+', sheet name: '+str(sname))  
    if fname.find('table') >= 0:
        note, footnote = US_NOTE(US_t.index, sname, address=address)
    else:
        note = []
        footnote = []
    if transpose == True:
        US_t = US_t.T
    PASS = ['state','nan']
    new_code_t = []
    new_label_t = []
    new_unit_t = []
    
    if fname.find('PET') >= 0:
        US_t.columns = [col+timedelta(days=1) if type(col) == pd._libs.tslibs.timestamps.Timestamp else col for col in US_t.columns]
        for ind in range(US_t.shape[0]):
            new_code_t.append(re.sub(r'^(.+?)NUS-Z00.+', r"\1", str(US_t.index[ind][0]).strip().replace('_', '')).strip())
            new_label_t.append(re.sub(r'^(.+?)\s+\([^\)\(]+\)$', r"\1", str(US_t.index[ind][1]).strip()).strip())
            new_unit_t.append(re.sub(r'.+?\s+\(([^\)\(]+)\)$', r"\1", str(US_t.index[ind][1]).strip()).strip())
    elif fname.find('electricity') >= 0:
        for ind in range(US_t.shape[0]):
            if str(US_t.index[ind]).find(':') < 0:
                new_code_t.append('nan')
                new_label_t.append('nan')
                new_unit_t.append('nan')
            else:
                new_code_t.append(prefix+re.sub(r'.+?\-([A-Z]+)\..+$', r"\1", US_t.iloc[ind]['source key']))
                new_label_t.append(Series['DATA TYPES'].loc[prefix, 'dt_desc']+',  '+re.sub(r'.+?:\s+(.+)$', r"\1", str(US_t.index[ind]).title()).strip())
                new_unit_t.append(US_t.iloc[ind]['units'].title())
    elif fname.find('Table') >= 0:
        for ind in range(US_t.shape[0]):
            suffix = ''
            for item in list(Series['CATEGORIES']['name']):
                if str(US_t.index[ind]).strip() in re.split(r'//', item.strip()):
                    suffix = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item].index[0]
                    new_label_t.append(Series['DATA TYPES'].loc[prefix, 'dt_desc']+',  '+Series['CATEGORIES'].loc[suffix, 'cat_desc'])
                    new_unit_t.append(re.sub(r'[\(\)]', "", str(US_t.iloc[ind].iloc[0]).strip()).strip())
                    break
            if suffix == '':
                to_pass = False
                if str(US_t.iloc[ind].iloc[0]) == 'nan':
                    to_pass = True
                for pas in PASS:
                    if str(US_t.index[ind]).strip().lower().find(pas) >= 0 or str(US_t.index[ind]) == '':
                        to_pass = True
                        break
                if to_pass == False:
                    ERROR('Category item code not found: "'+str(US_t.index[ind])+'"')
                new_code_t.append('nan')
                new_label_t.append('nan')
                new_unit_t.append('nan')
            else:
                new_code_t.append(prefix+suffix)
    elif fname.find('table') >= 0:
        US_t.columns = [str(col) for col in US_t.columns]
        for ind in range(US_t.shape[0]):
            suffix = ''
            note_suffix = ''
            if bool(re.search(r'[a-z]+[0-9]+$', str(US_t.index[ind]).strip())):
                note_suffix = '/'+re.sub(r'.+?([0-9]+)$', r"\1", str(US_t.index[ind]).strip())+'/'
            elif bool(re.search(r'[a-z]+[0-9]+$', str(US_t.iloc[0].iloc[ind]).strip())):
                note_suffix = '/'+re.sub(r'.+?([0-9]+)$', r"\1", str(US_t.iloc[0].iloc[ind]).strip())+'/'
            description = re.sub(r'[0-9]+$', "", str(US_t.index[ind]).strip())
            for item in list(Series['CATEGORIES']['name']):
                if description in re.split(r'//', item.strip()):
                    suffix = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item].index[0]
                    new_label_t.append(Series['DATA TYPES'].loc[prefix, 'dt_desc']+',  '+Series['CATEGORIES'].loc[suffix, 'cat_desc']+note_suffix)
                    new_unit_t.append(Series['DATA TYPES'].loc[prefix, 'dt_unit'])
                    break
            if suffix == '':
                to_pass = False
                if str(US_t.iloc[ind].iloc[0]) == 'nan':
                    to_pass = True
                for pas in PASS:
                    if str(US_t.index[ind]).strip().lower().find(pas) >= 0 or str(US_t.index[ind]) == '':
                        to_pass = True
                        break
                if to_pass == False:
                    ERROR('Category item code not found: "'+str(US_t.index[ind])+'"')
                new_code_t.append('nan')
                new_label_t.append('nan')
                new_unit_t.append('nan')
            else:
                new_code_t.append(prefix+suffix)
    elif (fname.find('weekprod') >= 0 or str(sname).find('weekprod') >= 0) and freq == 'W':
        US_new = pd.DataFrame()
        last_week = pd.DataFrame()
        first_concat = False
        last_remain = False
        for date in US_t:
            sys.stdout.write("\rLoading...("+str(round((list(US_t.keys()).index(date)+1)*100/len(US_t.keys()), 1))+"%)*")
            sys.stdout.flush()
            
            if fname.find('histot') >= 0 or fname.find('http') >= 0:
                year = re.sub(r'.*?([0-9]{4}).*', r"\1", date)
            else:
                year = re.sub(r'.*?([0-9]{4}).*', r"\1", fname)
            new_ind = []
            for ind in range(US_t[date].shape[0]):
                found = False
                for item in list(Series['CATEGORIES']['name']):
                    if str(US_t[date].index[ind]).strip() in re.split(r'//', item.strip()):
                        new_ind.append(Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item]['cat_desc'].item())
                        found = True
                        break
                if found == False:
                    to_pass = False
                    if str(US_t[date].iloc[ind].iloc[0]) == 'nan' or type(US_t[date].iloc[ind].iloc[0]) == datetime:
                        to_pass = True
                    for pas in PASS:
                        if str(US_t[date].index[ind]).strip().lower().find(pas) >= 0 or str(US_t[date].index[ind]) == '':
                            to_pass = True
                            break
                    if to_pass == False:
                        ERROR('Category item code not found: '+date+'-"'+str(US_t[date].index[ind])+'"')
                    if str(US_t[date].index[ind]).strip().lower() == 'state':
                        new_ind.append('Week')
                    else: 
                        new_ind.append(None)    
            US_t[date].index = new_ind
            US_t[date] = US_t[date][~US_t[date].index.duplicated()]
            US_t[date] = US_t[date].loc[US_t[date].index.dropna()]

            previous_week = ''
            NO_LAST_REMAIN = False
            if datetime.now().year - int(year) <= 1:
                for item in list(US_t[date].loc['Week']):
                    try:
                        week_num = int(re.sub(r'.*?[Ww]eek\s+([0-9]+).*', r"\1", str(item).replace('\n', '')))
                    except ValueError:
                        week_num = week_num
                date_range = pd.date_range(start=year+'-01-01',periods=week_num,freq='W-SAT').strftime('%Y-%m-%d')
            else:
                date_range = pd.date_range(start=year+'-01-01',end=year+'-12-31',freq='W-SAT').strftime('%Y-%m-%d')
            if len(date_range) == 53:
                NO_LAST_REMAIN = True
                FIT = False
                for item in list(US_t[date].loc['Week']):
                    if bool(re.search(r'[Ww]eek 53', str(item))):
                        FIT = True
                        break
                if FIT == False:
                    if last_remain == False:
                        ERROR('Length of date range does not meet the week number of year '+year)
                    first_concat = False
                    last_week.columns = [0]
                    US_t[date] = pd.concat([last_week, US_t[date]], axis=1)
                    last_remain = False
            for col in US_t[date].columns:
                WEEK = False
                for item in list(US_t[date][col]):
                    if type(item) == datetime:
                        US_t[date].loc[US_t[date].loc[US_t[date][col] == item].index[0], col] = item.strftime('%Y-%m-%d')
                    if bool(re.search(r'[Ww]eek 0*1[^0-9]*$', str(item))) and first_concat == True:
                        first_week = pd.concat([last_week, US_t[date][col]], axis=1).sum(axis=1)
                        last_remain = False
                    elif bool(re.search(r'[Ww]eek 53', str(item))) and NO_LAST_REMAIN == False:
                        last_remain = True
                        last_week = pd.DataFrame(US_t[date][col])
                        break
                    elif bool(re.search(r'[Ww]eek', str(item))):
                        WEEK = True
                        if str(item).lower() == previous_week:
                            US_t[date][previous_col] = pd.concat([US_t[date][previous_col], US_t[date][col]], axis=1).sum(axis=1)
                            WEEK = False
                            #print(US_t[date][previous_col])
                        previous_week = str(item).lower()
                        previous_col = col
                        break
                if WEEK == False:
                    US_t[date] = US_t[date].drop(columns=[col])
            #print('first_concat',first_concat)
            #print('last_remain',last_remain)
            if first_concat == True:
                if type(first_week) == type(None):
                    ERROR('Data from end week of last year('+str(int(year)-1)+') has not been concated to data for the first week of this year: '+year+'.')
                US_t[date] = pd.concat([first_week, US_t[date]], axis=1)
                first_week = None
            else:
                first_week = None
            if last_remain == False:
                last_week = pd.DataFrame()
                first_concat = False
            else:
                first_concat = True
            US_t[date].columns = date_range
            US_t[date] = US_t[date].sort_index(axis=1)
            #print(US_t[date])
            US_new = pd.concat([US_new, US_t[date]], axis=1)
        sys.stdout.write("\n\n")
        US_t = US_new
        for ind in range(US_t.shape[0]):
            suffix = ''
            description = str(US_t.index[ind])
            for item in list(Series['CATEGORIES']['cat_desc']):
                if description in re.split(r'//', item.strip()):
                    suffix = Series['CATEGORIES'].loc[Series['CATEGORIES']['cat_desc'] == item].index[0]
                    new_label_t.append(Series['DATA TYPES'].loc[prefix, 'dt_desc']+',  '+Series['CATEGORIES'].loc[suffix, 'cat_desc'])
                    new_unit_t.append(Series['DATA TYPES'].loc[prefix, 'dt_unit'])
                    break
            if suffix == '':
                new_code_t.append('nan')
                new_label_t.append('nan')
                new_unit_t.append('nan')
            else:
                new_code_t.append(prefix+suffix)
    elif fname.find('crushed') >= 0:
        US_t.columns = [int(re.sub(r'[a-z]+',"", col)) if type(col) == str else col for col in US_t.columns]
        for ind in range(US_t.shape[0]):
            new_code_t.append(prefix+'PD')
            new_label_t.append(Series['DATA TYPES'].loc[prefix, 'dt_desc'])
            new_unit_t.append(Series['DATA TYPES'].loc[prefix, 'dt_unit'])
    elif address.find('IRS') >= 0:
        new_col = []
        for col in range(US_t.shape[1]):
            if str(US_t.columns[col][0]).find('Unnamed') < 0:
                Dollar = str(US_t.columns[col][0])
            new_col.append(tuple([Dollar, US_t.columns[col][1]]))
        US_t.columns = new_col
        for col in US_t.columns:
            if col[0] != 'Current dollars':
                US_t = US_t.drop(columns=[col])
        US_t.columns = [col[1] for col in US_t.columns]
        for ind in range(US_t.shape[0]):
            suffix = ''
            description = str(US_t.index[ind])
            for item in list(Series['CATEGORIES']['name']):
                if description.strip() in re.split(r'//', item.strip()):
                    suffix = Series['CATEGORIES'].loc[Series['CATEGORIES']['name'] == item].index[0]
                    new_label_t.append(Series['CATEGORIES'].loc[suffix, 'cat_desc'])
                    new_unit_t.append(Series['CATEGORIES'].loc[suffix, 'cat_unit'])
                    break
            if suffix == '':
                new_code_t.append(None)
                new_label_t.append('nan')
                new_unit_t.append('nan')
            else:
                new_code_t.append(prefix+suffix)
    
    US_t = US_t.loc[:, ~US_t.columns.duplicated()]
    US_t = US_t.sort_index(axis=1)
    US_t.insert(loc=0, column='Index', value=new_code_t)
    US_t.insert(loc=1, column='Label', value=new_label_t)
    US_t.insert(loc=2, column='unit', value=new_unit_t)
    US_t = US_t.set_index('Index', drop=False)
    if address.find('IRS') >= 0:
        US_t = US_t[~US_t.index.duplicated()]
        US_t = US_t.loc[US_t.index.dropna()]
    label = US_t['Label']

    return US_t, label, note, footnote

def US_SEMI(data_path, address, fname, freq, chrome):
    note = []
    footnote = []
    
    US_t = pd.DataFrame()
    new_index_t = []
    new_dataframe = []
    if freq == 'M':
        IHS = readExcelFile(data_path+address+'Historical Data.xlsx', header_=0, index_col_=0, sheet_name_=0)
        new_code_t = list(IHS.index)
        new_label_t = list(IHS['Label'])
        new_unit_t = list(IHS['unit'])
        IHS.columns = [col.strftime('%Y-%m') if type(col) == datetime else col for col in IHS.columns]
        new_item_t = {'Billings':[], 'Bookings':[], 'BooktoBill':[]}
        latest = True
        Booking_latest = True
        date = None
        DATE = datetime.strptime(datetime.today().strftime('%Y-%m'), '%Y-%m')-relativedelta(months=1)
        DATA = {'Billings':None, 'Bookings':None, 'BooktoBill':None}
        begin = False
        worksheet = False
        while begin == False:
            if worksheet == False and DATE.strftime('%Y-%m') not in IHS.columns:
                for page in range(11):
                    chrome.get(fname+str(page))
                    search = BeautifulSoup(chrome.page_source, "html.parser")
                    result = search.find_all("h3", class_="resource-library-item__title")
                    for res in result:
                        sys.stdout.write("\rProducing data from www.semi.org...["+DATE.strftime('%Y-%m')+"]*")
                        sys.stdout.flush()
                        if bool(re.search(r"North American Semiconductor Equipment", res.text.replace('\n',''))):
                            chrome.get("https://www.semi.org"+res.find("a")["href"])
                            try:
                                result2 = pd.read_html(chrome.page_source)[0]
                                result2 = result2.set_index([result2.columns[0]])
                                if str(result2.columns[0]).isnumeric():
                                    result2.index = ['col' if col == 0 else result2.index[col] for col in range(len(result2.index))]
                                    result2 = result2.set_axis(result2.iloc[0], axis='columns').drop(index=['col'])
                                result2 = result2.loc[result2.index.dropna()]
                            except ValueError:
                                print('Missing Data:'+res.text.replace('\n',' '))
                                continue
                            BooktoBill = False
                            for col in range(result2.shape[1]):
                                if str(result2.columns[col]).find('Book-to-Bill') >= 0:
                                    BooktoBill = True
                                    if Booking_latest == True and BooktoBill == True:
                                        DATE = None
                                    break
                            index_list = range(result2.shape[0])
                            if (latest == True or (Booking_latest == True and BooktoBill == True)):
                                latest_index = list(reversed(range(result2.shape[0])))[0]
                                index_list = reversed(range(result2.shape[0]))
                            while True:
                                for ind in index_list:
                                    date_temp = re.sub(r'\(.+\)', "", str(result2.index[ind])).strip()
                                    if bool(re.search(r'Sept\s', date_temp)):
                                        date_temp = re.sub(r'(Sep)t\s', r"\1 ", date_temp)
                                    try:
                                        datestrp_temp = datetime.strptime(date_temp,'%B %Y')
                                    except ValueError:
                                        datestrp_temp = datetime.strptime(date_temp,'%b %Y')
                                    if ((latest == False and (Booking_latest == False or BooktoBill == False)) and datestrp_temp == DATE)\
                                        or ((latest == True or (Booking_latest == True and BooktoBill == True)) and ind == latest_index):
                                        date = date_temp
                                        datestrp = datestrp_temp
                                        for col in range(result2.iloc[ind].shape[0]):
                                            if bool(re.search(r'[0-9]+\.[0-9]+\.[0-9]+', str(result2.iloc[ind].iloc[col]))):
                                                result2.loc[result2.index[ind], result2.iloc[ind].index[col]] = re.sub(r'([0-9]+)\.([0-9]+\.[0-9]+)', r"\1,\2", str(result2.iloc[ind].iloc[col]))
                                            if bool(re.search(r'.+?\$', str(result2.iloc[ind].iloc[col]))):
                                                result2.loc[result2.index[ind], result2.iloc[ind].index[col]] = re.sub(r'.+?(\$)', r"\1", str(result2.iloc[ind].iloc[col]))
                                            if str(result2.iloc[ind].index[col]).find('Billings') >= 0 and (Booking_latest == False or BooktoBill == False):
                                                DATA['Billings'] = str(result2.iloc[ind].iloc[col]).replace('$', '')
                                            elif str(result2.iloc[ind].index[col]).find('Billings') >= 0 and Booking_latest == True and BooktoBill == True:
                                                continue
                                            elif str(result2.iloc[ind].index[col]).find('Bookings') >= 0:
                                                DATA['Bookings'] = str(result2.iloc[ind].iloc[col]).replace('$', '')
                                            elif bool(re.search(r'Book\s*\-\s*to\s*\-\s*Bill', str(result2.iloc[ind].index[col]))):
                                                DATA['BooktoBill'] = str(result2.iloc[ind].iloc[col])
                                            elif str(result2.iloc[ind].index[col]).find('Year-Over-Year') >= 0:
                                                continue
                                            else:
                                                print('Unknown Data Exists: '+str(result2.iloc[ind].index[col]))
                                        break
                                try:
                                    datestrf = datestrp.strftime('%Y-%m')
                                    new_ind = new_index_t.copy()
                                    if datestrf not in new_index_t:
                                        new_index_t.append(datestrf)
                                except ValueError:
                                    ERROR('Incorrect Date Format: '+date)
                                if DATE != None:
                                    if latest == True and datestrf < DATE.strftime('%Y-%m') and DATE.month-datestrp.month == 1:
                                        DATE = datestrp-relativedelta(months=1)
                                    elif datestrf != DATE.strftime('%Y-%m'):
                                        ERROR('Missing Date: '+DATE.strftime('%Y-%m'))
                                for key in DATA:
                                    if DATA[key] != None:
                                        if (key == 'Bookings' or key == 'BooktoBill') and (Booking_latest == True and BooktoBill == True):
                                            new_item_t[key].remove(None)
                                        new_item_t[key].append(float(str(DATA[key]).replace(',','')))
                                        DATA[key] = None
                                    elif datestrf not in new_ind:
                                        new_item_t[key].append(None)
                                #for i in range(len(script)): 
                                #    print('"'+script[i]+'"', end =" ")
                                #print()
                                if latest == True:
                                    latest_index -= 1
                                    if latest_index == 0:
                                        latest = False
                                    DATE = datestrp-relativedelta(months=1)
                                elif Booking_latest == True and BooktoBill == True:
                                    latest_index -= 1
                                    if latest_index == 0:
                                        Booking_latest = False
                                    DATE = datestrp-relativedelta(months=1)
                                else:
                                    DATE = datestrp-relativedelta(months=1)
                                    break
                            if DATE.strftime('%Y-%m') in IHS.columns:
                                worksheet = True
                                break
                    if worksheet == True:
                        break
                worksheet = True
                sys.stdout.write("\n\n")
            else:
                worksheet = True
                sys.stdout.write("\rProducing data from historical data...["+DATE.strftime('%Y-%m')+"]*")
                sys.stdout.flush()    
                DATA['Billings'] = IHS[DATE.strftime('%Y-%m')].iloc[0]
                DATA['Bookings'] = IHS[DATE.strftime('%Y-%m')].iloc[1]
                DATA['BooktoBill'] = IHS[DATE.strftime('%Y-%m')].iloc[2]
                date = DATE.strftime('%Y-%m')
                for key in DATA:
                    if DATA[key] != None:
                        new_item_t[key].append(float(DATA[key]))
                        DATA[key] = None
                    else:
                        new_item_t[key].append(None)
                new_index_t.append(date)
                #for i in range(len(script)): 
                #    print('"'+script[i]+'"', end =" ")
                #print()
                DATE = DATE-relativedelta(months=1)
                if date == '1991-01':
                    begin = True
        sys.stdout.write("\n\n")
        for key in new_item_t:
            new_dataframe.append(new_item_t[key])
    elif freq == 'Q':
        IHS = readExcelFile(data_path+address+'Historical DataQ.xlsx', header_=0, index_col_=0, sheet_name_=0)
        new_code_t = list(IHS.index)
        new_label_t = list(IHS['Label'])
        new_unit_t = list(IHS['unit'])
        IHS.columns = [pd.Timestamp(col).to_period('Q').strftime('%Y-Q%q') if type(col) == datetime else col for col in IHS.columns]
        new_item_t = []
        chrome.get(fname)
        search = BeautifulSoup(chrome.page_source, "html.parser")
        try:
            result = pd.read_html(chrome.page_source)[0]
            result = result.set_index([result.columns[0]])
        except ValueError:
            print('Table not found in '+fname)
        for ind in range(result.shape[0]):
            if str(result.index[ind]).isnumeric():
                for col in range(result.iloc[ind].shape[0]):
                    if bool(re.search(r'Q[1-4]', str(result.iloc[ind].iloc[col]))):
                        date = str(result.index[ind])+'-'+re.sub(r'.*?(Q[1-4]).*', r"\1", str(result.iloc[ind].iloc[col]))
                        shipment = re.sub(r'(Q[1-4])', "", str(result.iloc[ind].iloc[col])).strip()
                        #print('"'+date+'"', '"'+shipment+' Millions of Square Inches"')
                        new_index_t.append(date)
                        new_item_t.append(float(shipment.replace(',','')))
                    else:
                        print('Unknown Data Exists: '+str(result.iloc[ind].index[col]))
        sys.stdout.write("\n\n")
        new_dataframe.append(new_item_t)
    
    US_new = pd.DataFrame(new_dataframe, columns=new_index_t)
    if US_new.empty == False:
        US_t = pd.concat([US_t, US_new], ignore_index=True)
    US_t = US_t.loc[:, ~US_t.columns.duplicated()]
    US_t = US_t.sort_index(axis=1)
    US_t.insert(loc=0, column='Index', value=new_code_t)
    US_t.insert(loc=1, column='Label', value=new_label_t)
    US_t.insert(loc=2, column='unit', value=new_unit_t)
    US_t = US_t.set_index('Index', drop=False)
    label = US_t['Label']
    if freq == 'M':
        US_t.to_excel(data_path+address+'Historical Data.xlsx', sheet_name='Monthly')
    
    return US_t, label, note, footnote
