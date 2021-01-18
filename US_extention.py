# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from urllib.error import HTTPError
#from US_concat import CONCATE, readExcelFile

ENCODING = 'utf-8-sig'
out_path = "./output/"

def takeFirst(alist):
    return alist[0]

# 回報錯誤、儲存錯誤檔案並結束程式
def ERROR(error_text):
    print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()

def readFile(dir, default=pd.DataFrame(), acceptNoFile=False,header_=None,names_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,nrows_=None,encoding_=ENCODING,engine_='python',sep_=None):
    try:
        t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        names=names_,usecols=usecols_,nrows=nrows_,encoding=encoding_,engine=engine_,sep=sep_)
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            ERROR('找不到檔案：'+dir)
    except HTTPError as err:
        if acceptNoFile:
            return default
        else:
            ERROR(str(err))
    except:
        try: #檔案編碼格式不同
            t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,\
                        engine='python')
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, \
             header_=None,names_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,nrows_=None,sheet_name_=None):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,names=names_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_,nrows=nrows_)
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            ERROR('找不到檔案：'+dir)
    except:
        try: #檔案編碼格式不同
            t = pd.read_excel(dir, header=header_,skiprows=skiprows_,index_col=index_col_,sheet_name=sheet_name_)
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

def CONCATE(NAME, suf, data_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, DB_dict, DB_name_dict):
    
    print('Reading file: '+NAME+'key'+suf+', Time: ', int(time.time() - tStart),'s'+'\n')
    KEY_DATA_t = readExcelFile(data_path+NAME+'key'+suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
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
    KEY_DATA_t = KEY_DATA_t.sort_values(by=['name', 'db_table'], ignore_index=True)
    
    repeated = 0
    repeated_index = []
    Repeat = {}
    Repeat['key'] = []
    Repeat['start'] = []
    for i in range(1, len(KEY_DATA_t)):
        if i in Repeat['key']:
            continue
        Repeat['key'] = []
        Repeat['start'] = []
        if KEY_DATA_t.iloc[i]['name'] == KEY_DATA_t.iloc[i-1]['name']:
            j = i
            Repeat['key'].append(j-1)
            Repeat['start'].append(str(KEY_DATA_t.iloc[j-1]['start']))
            while KEY_DATA_t.iloc[j]['name'] == KEY_DATA_t.iloc[j-1]['name']:
                repeated += 1
                Repeat['key'].append(j)
                Repeat['start'].append(str(KEY_DATA_t.iloc[j]['start']))
                j += 1
                if j >= len(KEY_DATA_t):
                    break
            keep = Repeat['key'][Repeat['start'].index(min(Repeat['start']))]
            for k in Repeat['key']:
                if k != keep and Repeat['start'][Repeat['key'].index(k)] == min(Repeat['start']):
                    if (k > keep and KEY_DATA_t.iloc[k]['source'] != 'Bureau of Economic Analysis') or (k < keep and KEY_DATA_t.iloc[k]['source'] == 'Bureau of Economic Analysis'):
                        repeated_index.append(keep)
                        keep = k
                    else:
                        repeated_index.append(k)
                elif k != keep and Repeat['start'][Repeat['key'].index(k)] > min(Repeat['start']):
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

def US_NOTE(LINE, sname, LABEL=[], address='', other=False):
    note = []
    footnote = []
    if other == True:
        for n in range(LINE.shape[0]):
            line = LINE.index[n]
            if str(line).isnumeric():
                line = int(line)
            if address.find('ei/') >= 0:
                for code in LABEL['footnote_codes']:
                    footnote = str(LABEL['footnote_codes'][code])
                    if footnote.isnumeric():
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
            else:
                note.append([LINE.index[n], LINE.iloc[n]['note']])
        return note
    for n in range(len(LINE)):
        if sname != 0 and bool(re.match(r'[0-9]+\.', str(LINE[n]))):
            whole = str(LINE[n])[str(LINE[n]).find('.')+1:]
            whole = re.sub(r'\s\([0-9]\)|\s\(see\sfootnote\s[0-9]+\)',"",whole)
            if bool(re.search(r'[Ll]ine', whole)):
                whole = re.sub(r'\s\([Ll]ine\s[0-9]+\)|[Ll]ine\s[0-9]+,\s',"",whole)
                if whole.find('residual') >= 0:
                    whole = whole.replace('the first line',LABEL['1'].strip()).replace('detailed lines','detailed items')
                if bool(re.search(r'[Ll]ine\s[0-9]+', whole)) or bool(re.search(r'[Ll]ines\s[0-9]+', whole)):
                    whole = re.sub(r'\s[Ll]ine'," Item of line", whole)
                else:
                    whole = re.sub(r'\s[Ll]ine'," item", whole)
            note.append([int(str(LINE[n])[:str(LINE[n]).find('.')]),whole.strip()])
        elif address.find('BOC') >= 0 and bool(re.match(r'[0-9]+\s*[A-Z]+', str(LINE[n]).strip())):
            whole = str(LINE[n])[re.search(r'[A-Z]',str(LINE[n])).start():]
            m = n
            while str(LINE[m+1]) != 'nan' and bool(re.match(r'[0-9]+\s*[A-Z]+', str(LINE[m+1]).strip())) == False and address.find('SCEN') < 0:
                whole = whole+str(LINE[m+1])
                m+=1
                if m+1 >= len(LINE):
                    break
            note.append([int(str(LINE[n])[:re.search(r'[A-Z]',str(LINE[n])).start()]),whole.strip()])
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
        elif bool(re.search(r'Note:', str(LINE[n]))):
            whole = str(LINE[n])[str(LINE[n]).find('Note:')+5:].strip("',() ")
            m = n
            if m+1 < len(LINE):
                while str(LINE[m+1]) != 'nan' and bool(re.search(r'Note:', str(LINE[m+1]))) == False and address.find('APEP') < 0:
                    whole = whole+str(LINE[m+1])
                    m+=1
                    if m+1 >= len(LINE):
                        break
            if whole.find('Single-family') >= 0:
                key = 'ONE'
            else:
                key = 'Note'
            if whole.find('Universe') >= 0:
                whole = whole+'.'
            note.append([key, whole.replace("'",'').strip()])
        elif str(sname).find('U70206') >= 0 and str(LINE[n]) != 'nan' and str(LINE[n]).isnumeric() == False:
            whole = str(LINE[n]).replace('table are','item is').replace('This table is','This item is')
            note.append(['Note', whole.strip()])
        elif sname != 0 and str(LINE[n]) != 'nan' and str(LINE[n]).isnumeric() == False:
            footnote.append(re.split(r'[\s=:]+', str(LINE[n]), 1))
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

def DATA_SETS(TABLES, data_path, address, datasets=None, fname=None, sname=None, DIY_series=None, MONTH=None, password='', header=None, index_col=None, skiprows=None, freq=None, x='', HIES=False, usecols=None):
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
        US_t, label, note2, footnote2 = HIES_OLD(TABLES, data_path, address, fname, sname, DIY_series)
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
                str(DIY_series['CATEGORIES'].loc[US_temp.iloc[i]['cat_idx'], 'cat_code']).find(password) >= 0):
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
            if MONTH != None:
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
        US_t = readExcelFile(data_path+address+fname+'.xls'+x, header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=sname, acceptNoFile=False, usecols_=usecols)
        if US_t.empty == True:
            ERROR('Sheet Not Found: '+data_path+address+fname+'.xls'+x+', sheet name: '+sname)
        if fname != 'shiphist':
            US_t = US_t[~US_t.index.duplicated()]
        note_line = []
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
        if address.find('PRIC') >= 0 or address.find('SHIP') >= 0 or address.find('APEP') >= 0:
            footnote = footnote
        else:
            footnote = footnote + footnote2
        if fname.find('national-hmihistory') < 0 and address.find('POPT') < 0 and address.find('CBRT') < 0:
            US_t = US_t.T
        new_index = []
        new_order = []
        new_label = []
        for t in range(TABLES.shape[0]):
            if TABLES.iloc[t]['Address'] == address and TABLES.iloc[t]['File'] == fname and TABLES.iloc[t]['Sheet'] == sname:
                subword = str(TABLES.iloc[t]['subword'])
                prefix = str(TABLES.iloc[t]['prefix'])
                middle = str(TABLES.iloc[t]['middle'])
                suffix = str(TABLES.iloc[t]['suffix'])
        if address.find('CONS') >= 0:
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
        elif address.find('SHIP') >= 0:
            new_columns = []
            month = [datetime.strptime(m,'%b').strftime('%B') for m in MONTH]
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
            elif freq == 'A':
                for ind in range(US_t.shape[0]):
                    if str(US_t.index[ind]).strip() == 'Not Seasonally':
                        new_label.append(str(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix[-3:]]['dt_desc'].item()))
                        new_order.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix[-3:]]['order'].item())
                        US_t = US_HISTORYDATA(US_t.loc[US_t.index[ind]].T.reset_index(), name='Period', MONTH=month, summ=True)
                        new_index.append(prefix+middle+suffix)
                        break
        elif address.find('NAHB') >= 0:
            new_index.append(prefix+middle+suffix)
            new_label.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix[-3:]]['dt_desc'].item())
            new_order.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix[-3:]]['order'].item())
            US_t.columns = MONTH
            US_t = US_HISTORYDATA(US_t.reset_index(), name='index', MONTH=MONTH)
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
        elif address.find('CBRT') >= 0:
            US_t = US_t.rename(columns={'Label':'old_label'})
            for ind in range(US_t.shape[0]):
                new_label.append(DIY_series['DATA TYPES'].loc[DIY_series['DATA TYPES'].index == suffix]['dt_desc'].item()+', '+US_t.iloc[ind]['old_label'])
            US_t = US_t.iloc[:, ::-1]
        elif address.find('URIN') >= 0:
            US_t = US_t.rename(columns={'Label':'old_label'})
            new_label = list(US_t['old_label'])
            US_t = US_t.iloc[:, ::-1]
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
        elif address.find('HOUS') >= 0:
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
        if address.find('CBRT') < 0 and address.find('URIN') < 0:
            US_t.insert(loc=0, column='Index', value=new_index)
            US_t.insert(loc=1, column='order', value=new_order)
        US_t = US_t.set_index('Index', drop=False)
        if address.find('CONS') >= 0:
            US_t = US_t.rename(columns={'Date':'Label'})
            US_t = US_t.iloc[:, ::-1]
        elif address.find('HOUS') >= 0 or address.find('MRTS') >= 0 or address.find('APEP') >= 0 or address.find('DSCO') >= 0:
            US_t.insert(loc=1, column='Label', value=new_label)
        if address.find('DSCO') >= 0:
            US_t.insert(loc=3, column='start', value=new_start)
            US_t.insert(loc=4, column='last', value=new_last)
        US_t = US_t.sort_values(by=['order','Label'])
        label = US_t['Label']
        if address.find('MRTS') >= 0 and freq == 'Q':
            label = pd.Series(['Retail Trade and Food Services','Retail Trade'], index=['U44X7200SMR','U4400000SMR']).append(label)
        
    return US_t, label, note, footnote

def HIES_OLD(TABLES, data_path, address, fname=None, sname=None, DIY_series=None):
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
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Address'] == address and TABLES.iloc[t]['File'] == fname and TABLES.iloc[t]['Sheet'] == sname:
            prefix = str(TABLES.iloc[t]['prefix'])
            middle = str(TABLES.iloc[t]['middle'])
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

def US_BLS(US_temp, Table, freq, index_base, address, start=None, key='main'):
    MONTH = ['JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE','JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER']
    YEAR = {'main':['M13'],'ln/':['M13'],'pr/':['Q05'],'mp/':['A01'],'ec/':['Q05'],'bd/':['Q05']}
    SEMI = ['S01','S02']
    QUAR = {}
    QUAR['main'] = {'M03':'Q1','M06':'Q2','M09':'Q3','M12':'Q4'}
    QUAR['ln/'] = {'Q01':'Q1','Q02':'Q2','Q03':'Q3','Q04':'Q4'}
    QUAR['pr/'] = QUAR['ln/']
    QUAR['mp/'] = QUAR['main']
    QUAR['ec/'] = QUAR['ln/']
    QUAR['bd/'] = QUAR['ln/']
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
        if (US_temp.iloc[i]['series_id'] != code and US_temp.iloc[i]['period'] in MON and freq == 'M') or (US_temp.iloc[i]['period'] in YEAR[key] and US_temp.iloc[i]['series_id'] != code and freq == 'A') \
            or (US_temp.iloc[i]['period'] in SEMI and US_temp.iloc[i]['series_id'] != code and freq == 'S') or (US_temp.iloc[i]['period'] in QUAR[key] and US_temp.iloc[i]['series_id'] != code and freq == 'Q'):
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
            if address.find('li/') >= 0:
                lab = Table['item_code'][code]
            elif address.find('ce/') >= 0 or address.find('bd/') >= 0 or address.find('jt/') >= 0:
                lab = Table['industry_code'][code]
            elif address.find('pr/') >= 0:
                lab = Table['measure_code'][code]
            elif address.find('ec/') >= 0:
                lab = Table['group_code'][code]
            else:
                lab = Table['series_title'][code]
            month = ''
            if address.find('ln/') < 0 and address.find('ce/') < 0 and address.find('ec/') < 0 and address.find('bd/') < 0 and address.find('jt/') < 0:
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
        if key == 'bd/' and code != '':
            if Table['state_code'][code] != 0:
                continue
        if (freq == 'M' and US_temp.iloc[i]['period'] not in MON) or (freq == 'A' and US_temp.iloc[i]['period'] not in YEAR[key]) or (freq == 'S' and US_temp.iloc[i]['period'] not in SEMI)\
            or (freq == 'Q' and US_temp.iloc[i]['period'] not in QUAR[key]):
            continue
        if start != None:
            if US_temp.iloc[i]['year'] < start:
                continue
        new_item_t.append(US_temp.iloc[i]['value'])
        if freq == 'M' or freq == 'S':
            period_index = str(US_temp.iloc[i]['year'])+'-'+str(US_temp.iloc[i]['period']).replace('M','').replace('S0','S')
        elif freq == 'A':
            period_index = US_temp.iloc[i]['year']
        elif freq == 'Q':
            period_index = str(US_temp.iloc[i]['year'])+'-'+QUAR[key][str(US_temp.iloc[i]['period'])]
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

def US_FAMI(TABLES, data_path, address, fname, sname, DIY_series, x=''):
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
    for t in range(TABLES.shape[0]):
        if TABLES.iloc[t]['Address'] == address and TABLES.iloc[t]['File'] == fname and TABLES.iloc[t]['Sheet'] == sname:
            prefix = str(TABLES.iloc[t]['prefix'])
            middle = str(TABLES.iloc[t]['middle'])
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

def US_STL(US_temp, address, DIY_series):
    note = []
    footnote = []
    new_label = []
    new_unit = []
    isadjusted = []
    for i in range(US_temp.shape[0]):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/US_temp.shape[0], 1))+"%)*")
        sys.stdout.flush()
        for r in range(len(DIY_series)):
            if DIY_series[r] == US_temp.index[i]:
                for rr in range(r,len(DIY_series)):
                    if DIY_series[rr] == 'Title:':
                        new_label.append(DIY_series[rr+1].strip())
                    elif DIY_series[rr] == 'Units:':
                        new_unit.append(DIY_series[rr+1].strip())
                    elif DIY_series[rr] == 'Seasonal Adjustment:':
                        isadjusted.append(DIY_series[rr+1].strip())    
                        break
                break
    sys.stdout.write("\n\n")
    US_t = US_temp.reset_index()
    US_t = US_t.set_index('index', drop=False)
    US_t.insert(loc=1, column='Label', value=new_label)
    US_t.insert(loc=2, column='unit', value=new_unit)
    US_t.insert(loc=3, column='is_adj', value=isadjusted)
    label = US_t['Label']
    
    return US_t, label, note, footnote