# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date

ENCODING = 'utf-8-sig'
data_path = './output/'
out_path = './output/'
NAME = 'QNIA_'
NAME1 = '_rename'#_rename
NAME2 = 'renamed'
#NAME3 = 'Q'

def ERROR(error_text):
    print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()
def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, \
             header_=None,skiprows_=None,index_col_=None,sheet_name_=None):
    try:
        t = pd.read_excel(dir, header=header_,skiprows=skiprows_,index_col=index_col_,sheet_name=sheet_name_)
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

tStart = time.time()
print('Reading file: '+NAME+'key'+NAME1+', Time: ', int(time.time() - tStart),'s'+'\n')
QNIA_key = readExcelFile(data_path+NAME+'key'+NAME1+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'key'+NAME1)
print('Reading file: '+NAME+NAME2+', Time: ', int(time.time() - tStart),'s'+'\n')
QNIA_t = readExcelFile(data_path+NAME+NAME2+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+NAME2)
#print('Reading file: '+NAME+NAME3+', Time: ', int(time.time() - tStart),'s'+'\n')
#QNIA_tt = readExcelFile(data_path+NAME+NAME3+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+NAME3)
'''
QNIA_key = QNIA_key.sort_values(by=['name', 'db_table'], ignore_index=True)
key_data = list(QNIA_key['name'])
repeated = 0
repeated_index = []
for i in range(1, len(QNIA_key)):
    if key_data[i] == key_data[i-1]:
        repeated += 1
        repeated_index.append(i-1)
        #print(i,' ',i-1)
        key = QNIA_key.iloc[i-1]    
        #DATA_BASE_t[key['db_table']] = DATA_BASE_t[key['db_table']].drop(columns = key['db_code'])
    sys.stdout.write("\r"+str(repeated)+" repeated data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")
#print(key_data)
#for i in repeated_index:
#    print(key_data[i])
'''
not_found = []
possible_exist = []
replacefrom = [' -','/',' &',';',' approach',' National currency,',' (ISIC Rev.4)','$','Distrib.','transp.',"'",'scientif.','activ.','(ISIC Rev.4)','health.',' borrowing to the rest of the world','serv.','accommod.,','techn.','admn.',' activites','Comp.','empl. Total',' A10,','GFCF ',' Comparable Table, Gross domestic product,','current prices and','volume estimates and','Implicit price deflator',' (final consumption+GFCF)','Gross Domestic Product, Expenditure, Comparable Table, ','Gross Domestic Product, Activity, ']
replaceto = [',',', ',',',',','','','','dollars','Distributive','transport','','scientific','activities','','health','net borrowing','serv','accommod.','tech.','admin.',' activities','Compensation','employees','','','','current prices,','volume estimates,','deflator','','','']
#renamed = []
print('Renaming the key file, Time: ', int(time.time() - tStart),'s'+'\n')
for key in range(QNIA_key.shape[0]):
    sys.stdout.write("\rLoading...("+str(round((key+1)*100/QNIA_key.shape[0], 2))+"%), "+str(QNIA_key.loc[key, 'book'])+", Time: "+str(int(time.time() - tStart))+"s, not_found: "+str(len(not_found))+" , possible_exist: "+str(len(possible_exist))+" ############################################")
    sys.stdout.flush()
    if QNIA_key.loc[key, 'is_renamed'] == 'renamed':
        #print(QNIA_key.loc[key, 'is_renamed'])
        continue
    elif QNIA_key.loc[key, 'is_renamed'] == 'cannot_renamed':
        #print(QNIA_key.loc[key, 'is_renamed'])
        continue
    elif QNIA_key.loc[key, 'book'] == 'Bulgaria':
        QNIA_key.loc[key, 'is_renamed'] = 'cannot_renamed'
        continue
    elif QNIA_key.loc[key, 'book'] == 'Colombia':
        QNIA_key.loc[key, 'is_renamed'] = 'cannot_renamed'
        continue
    elif QNIA_key.loc[key, 'book'] == 'G7':
        QNIA_key.loc[key, 'is_renamed'] = 'cannot_renamed'
        continue
    elif QNIA_key.loc[key, 'book'] == 'Romania':
        QNIA_key.loc[key, 'is_renamed'] = 'cannot_renamed'
        continue
    name0 = str(QNIA_key.loc[key, 'name'])[:1]
    name1 = str(QNIA_key.loc[key, 'name'])[:4]
    found = False
    possible = False
    desc_e = str(QNIA_key.loc[key, 'desc_e'])
    unit = str(QNIA_key.loc[key, 'unit'])
    locu = desc_e.find(unit)-2
    desc_e = desc_e[:locu]
    for r in range(len(replacefrom)):
        desc_e = desc_e.replace(replacefrom[r],replaceto[r])
    desc_e = desc_e.lower()
    target = desc_e.split(', ')
    possible_code = []
    #if name0 == 'A':
    for code in range(QNIA_t.shape[0]):
        if QNIA_t.loc[code, 'by'] != 'used':
            name02 = str(QNIA_t.loc[code, 'code'])[:1]
            name2 = str(QNIA_t.loc[code, 'code'])[:4]
            if name0 == name02:
                if name1 == name2 or QNIA_key.loc[key, 'book'] == QNIA_t.loc[code, 'country']:
                    des = str(QNIA_t.loc[code, 'description'])
                    for r in range(len(replacefrom)):
                        des = des.replace(replacefrom[r],replaceto[r])
                    des = des.lower()
                    if des.find('(') >= 0:
                        if des.find('concept') < 0:
                            if des.find('gdp') >= 0:
                                loca =  des.find(')')-3
                                locb = des.find(')')
                                ab = des[loca:locb]
                                if str(QNIA_key.loc[key, 'form_e']).find(ab) < 0:
                                    found = False
                                    continue
                            locl = des.find('(')-1
                            locr = des.find(')')+1
                            des = des[:locl]+des[locr:]
                    if des.find('statistical discrepancy') >= 0:
                        locs = des.find('statistical discrepancy')
                        des = des[locs:]+'possible'
                    d = des
                    for word in target:
                        #print(word)
                        d = d.replace(word,'').replace(', ','')
                        if des.find(word) >= 0:
                            found = True
                        else:
                            if word == 'annual levels':
                                QNIA_key.loc[key, 'annual levels'] = QNIA_t.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            elif word == 'quarterly levels':
                                QNIA_key.loc[key, 'quarterly levels'] = QNIA_t.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            elif word == 'seasonally adjusted':
                                QNIA_key.loc[key, 'seasonally adjusted'] = QNIA_t.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            found = False
                            break
                    if found == True:
                        if d != '':
                            if d == 'annual levels':
                                QNIA_key.loc[key, 'annual levels'] = QNIA_t.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            elif d == 'quarterly levels':
                                QNIA_key.loc[key, 'quarterly levels'] = QNIA_t.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            elif d == 'seasonally adjusted':
                                QNIA_key.loc[key, 'seasonally adjusted'] = QNIA_t.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            elif d == 'possible':
                                QNIA_key.loc[key, 'others'] = QNIA_t.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            found = False
                    if found == True:
                        QNIA_key.loc[key, 'name'] = QNIA_t.loc[code, 'code']
                        QNIA_t.loc[code, 'by'] = 'used'
                        QNIA_t.loc[code, 'subject'] = ''
                        QNIA_key.loc[key, 'is_renamed'] = 'renamed'
                        QNIA_key.loc[key, 'annual levels'] = ''
                        QNIA_key.loc[key, 'quarterly levels'] = ''
                        QNIA_key.loc[key, 'seasonally adjusted'] = ''
                        possible = False
                        #found = True
                        break
                    else:
                        continue
                else:
                    continue
            else:
                continue
        else:
            continue
    """else:
        for code in range(QNIA_tt.shape[0]):
            if QNIA_tt.loc[code, 'code'] != 'used':
                name2 = str(QNIA_tt.loc[code, 'code'])[:4]
                if name1 == name2 or QNIA_key.loc[key, 'book'] == QNIA_tt.loc[code, 'country']:
                    des = str(QNIA_tt.loc[code, 'description'])
                    for r in range(len(replacefrom)):
                        des = des.replace(replacefrom[r],replaceto[r])
                    des = des.lower()
                    if des.find('(') >= 0:
                        if des.find('concept') < 0:
                            if des.find('gdp') >= 0:
                                loca =  des.find(')')-3
                                locb = des.find(')')
                                ab = des[loca:locb]
                                if str(QNIA_key.loc[key, 'form_e']).find(ab) < 0:
                                    found = False
                                    continue
                            locl = des.find('(')-1
                            locr = des.find(')')+1
                            des = des[:locl]+des[locr:]
                    d = des
                    for word in target:
                        d = d.replace(word,'').replace(', ','')
                        if des.find(word) >= 0:
                            found = True
                        else:
                            if word == 'annual levels':
                                QNIA_key.loc[key, 'annual levels'] = QNIA_tt.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            elif word == 'quarterly levels':
                                QNIA_key.loc[key, 'quarterly levels'] = QNIA_tt.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            elif word == 'seasonally adjusted':
                                QNIA_key.loc[key, 'seasonally adjusted'] = QNIA_tt.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            found = False
                            break
                    if found == True:
                        if d != '':
                            found = False
                    if found == True:
                        QNIA_key.loc[key, 'name'] = QNIA_tt.loc[code, 'code']
                        QNIA_tt.loc[code, 'by'] = 'used'
                        QNIA_key.loc[key, 'is_renamed'] = 'renamed'
                        QNIA_key.loc[key, 'annual levels'] = ''
                        QNIA_key.loc[key, 'quarterly levels'] = ''
                        QNIA_key.loc[key, 'seasonally adjusted'] = ''
                        possible = False
                        #found = True
                        break
                    else:
                        continue
                else:
                    continue
            else:
                continue"""
    if QNIA_key.loc[key, 'book'] == 'European Union – 27 countries (from 01/02/2020)':
        QNIA_key.loc[key, 'name'] = str(QNIA_key.loc[key, 'name']).replace('727', 'EUU')
    elif QNIA_key.loc[key, 'book'] == 'G20':
        QNIA_key.loc[key, 'name'] = str(QNIA_key.loc[key, 'name']).replace('920', 'G20')
    elif QNIA_key.loc[key, 'book'] == 'NAFTA':
        QNIA_key.loc[key, 'name'] = str(QNIA_key.loc[key, 'name']).replace('121', 'NAT')
    elif QNIA_key.loc[key, 'book'] == 'OECD - FORMER TOTAL':
        QNIA_key.loc[key, 'name'] = str(QNIA_key.loc[key, 'name']).replace('990', 'OTF')
    elif QNIA_key.loc[key, 'book'] == 'Euro area':
        QNIA_key.loc[key, 'name'] = str(QNIA_key.loc[key, 'name']).replace('719', 'EMU')
    if found == False:
        if desc_e.find('mineral exploration and evaluation') >= 0:
            QNIA_key.loc[key, 'is_renamed'] = 'cannot_renamed'
            continue
        elif desc_e.find('research and development') >= 0:
            QNIA_key.loc[key, 'is_renamed'] = 'cannot_renamed'
            continue
        elif desc_e.find('information and communication') >= 0:
            QNIA_key.loc[key, 'is_renamed'] = 'cannot_renamed'
            continue
        elif desc_e.find('computer software and databases') >= 0:
            QNIA_key.loc[key, 'is_renamed'] = 'cannot_renamed'
            continue
        elif desc_e.find('literary and artistic originals') >= 0:
            QNIA_key.loc[key, 'is_renamed'] = 'cannot_renamed'
            continue
        elif desc_e.find('ict equipment') >= 0:
            QNIA_key.loc[key, 'is_renamed'] = 'cannot_renamed'
            continue
        elif desc_e.find('Changes in inventories and acquisitions less disposals of valuables') >= 0:
            QNIA_key.loc[key, 'is_renamed'] = 'cannot_renamed'
            continue
        elif desc_e.find('other intellectual property products') >= 0:
            QNIA_key.loc[key, 'is_renamed'] = 'cannot_renamed'
            continue
        else:
            not_found.append(key)
    if possible == True:
        if str(QNIA_key.loc[key, 'name']) not in possible_exist:
            possible_exist.append(str(QNIA_key.loc[key, 'name']))
        for c in possible_code:
            #if name0 == 'A':
            QNIA_t.loc[c, 'subject'] = 'possible'
            #else:
            #    QNIA_tt.loc[c, 'subject'] = 'possible'
sys.stdout.write("\n\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')

not_found2 = []
print('Renaming the key file second time, Time: ', int(time.time() - tStart),'s'+'\n')
count = 0
for key in not_found:
    count+=1
    sys.stdout.write("\rLoading...("+str(round((count)*100/len(not_found), 2))+"%), "+str(QNIA_key.loc[key, 'book'])+", Time: "+str(int(time.time() - tStart))+"s, not_found2: "+str(len(not_found2))+" , possible_exist: "+str(len(possible_exist))+" ############################################")
    sys.stdout.flush()
    if QNIA_key.loc[key, 'is_renamed'] == 'renamed':
        #print(QNIA_key.loc[key, 'is_renamed'])
        continue
    name0 = str(QNIA_key.loc[key, 'name'])[:1]
    name1 = str(QNIA_key.loc[key, 'name'])[:4]
    found = False
    possible = False
    desc_e = str(QNIA_key.loc[key, 'desc_e'])
    unit = str(QNIA_key.loc[key, 'unit'])
    locu = desc_e.find(unit)-2
    desc_e = desc_e[:locu]
    for r in range(len(replacefrom)):
        desc_e = desc_e.replace(replacefrom[r],replaceto[r])
    desc_e = desc_e.lower()
    target = desc_e.split(', ')
    possible_code = []
    #if name0 == 'A':
    for code in range(QNIA_t.shape[0]):
        if QNIA_t.loc[code, 'by'] == 'used':
            name02 = str(QNIA_t.loc[code, 'code'])[:1]
            name2 = str(QNIA_t.loc[code, 'code'])[:4]
            if name0 == name02:
                if name1 == name2 or QNIA_key.loc[key, 'book'] == QNIA_t.loc[code, 'country']:
                    des = str(QNIA_t.loc[code, 'description'])
                    for r in range(len(replacefrom)):
                        des = des.replace(replacefrom[r],replaceto[r])
                    des = des.lower()
                    if des.find('(') >= 0:
                        if des.find('concept') < 0:
                            if des.find('gdp') >= 0:
                                loca =  des.find(')')-3
                                locb = des.find(')')
                                ab = des[loca:locb]
                                if str(QNIA_key.loc[key, 'form_e']).find(ab) < 0:
                                    found = False
                                    continue
                            locl = des.find('(')-1
                            locr = des.find(')')+1
                            des = des[:locl]+des[locr:]
                    d = des
                    for word in target:
                        #print(word)
                        d = d.replace(word,'').replace(', ','')
                        if des.find(word) >= 0:
                            found = True
                        else:
                            if word == 'annual levels':
                                QNIA_key.loc[key, 'annual levels'] = QNIA_t.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            elif word == 'quarterly levels':
                                QNIA_key.loc[key, 'quarterly levels'] = QNIA_t.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            elif word == 'seasonally adjusted':
                                QNIA_key.loc[key, 'seasonally adjusted'] = QNIA_t.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            found = False
                            break
                    if found == True:
                        if d != '':
                            found = False
                    if found == True:
                        error = True
                        for k in range(QNIA_key.shape[0]):
                            if QNIA_key.loc[k, 'name'] == QNIA_t.loc[code, 'code']:
                                QNIA_key.loc[k, 'repeated'] = 'repeated'
                                error = False
                                break
                        if error == True:
                            ERROR(str(QNIA_t.loc[code, 'code'])+' is not repeated')
                        QNIA_key.loc[key, 'name'] = QNIA_t.loc[code, 'code']
                        QNIA_key.loc[key, 'repeated'] = 'repeated'
                        QNIA_t.loc[code, 'by'] = 'used'
                        QNIA_t.loc[code, 'subject'] = ''
                        QNIA_key.loc[key, 'is_renamed'] = 'renamed'
                        QNIA_key.loc[key, 'annual levels'] = ''
                        QNIA_key.loc[key, 'quarterly levels'] = ''
                        QNIA_key.loc[key, 'seasonally adjusted'] = ''
                        possible = False
                        #found = True
                        break
                    else:
                        continue
                else:
                    continue
            else:
                continue
        else:
            continue
    """else:
        for code in range(QNIA_tt.shape[0]):
            if QNIA_tt.loc[code, 'code'] != 'used':
                name2 = str(QNIA_tt.loc[code, 'code'])[:4]
                if name1 == name2 or QNIA_key.loc[key, 'book'] == QNIA_tt.loc[code, 'country']:
                    des = str(QNIA_tt.loc[code, 'description'])
                    for r in range(len(replacefrom)):
                        des = des.replace(replacefrom[r],replaceto[r])
                    des = des.lower()
                    if des.find('(') >= 0:
                        if des.find('concept') < 0:
                            if des.find('gdp') >= 0:
                                loca =  des.find(')')-3
                                locb = des.find(')')
                                ab = des[loca:locb]
                                if str(QNIA_key.loc[key, 'form_e']).find(ab) < 0:
                                    found = False
                                    continue
                            locl = des.find('(')-1
                            locr = des.find(')')+1
                            des = des[:locl]+des[locr:]
                    d = des
                    for word in target:
                        d = d.replace(word,'').replace(', ','')
                        if des.find(word) >= 0:
                            found = True
                        else:
                            if word == 'annual levels':
                                QNIA_key.loc[key, 'annual levels'] = QNIA_tt.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            elif word == 'quarterly levels':
                                QNIA_key.loc[key, 'quarterly levels'] = QNIA_tt.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            elif word == 'seasonally adjusted':
                                QNIA_key.loc[key, 'seasonally adjusted'] = QNIA_tt.loc[code, 'code']
                                possible_code.append(code)
                                possible = True
                            found = False
                            break
                    if found == True:
                        if d != '':
                            found = False
                    if found == True:
                        QNIA_key.loc[key, 'name'] = QNIA_tt.loc[code, 'code']
                        QNIA_tt.loc[code, 'by'] = 'used'
                        QNIA_key.loc[key, 'is_renamed'] = 'renamed'
                        QNIA_key.loc[key, 'annual levels'] = ''
                        QNIA_key.loc[key, 'quarterly levels'] = ''
                        QNIA_key.loc[key, 'seasonally adjusted'] = ''
                        possible = False
                        #found = True
                        break
                    else:
                        continue
                else:
                    continue
            else:
                continue"""
    if QNIA_key.loc[key, 'book'] == 'European Union – 27 countries (from 01/02/2020)':
        QNIA_key.loc[key, 'name'] = str(QNIA_key.loc[key, 'name']).replace('727', 'EUU')
    elif QNIA_key.loc[key, 'book'] == 'G20':
        QNIA_key.loc[key, 'name'] = str(QNIA_key.loc[key, 'name']).replace('920', 'G20')
    elif QNIA_key.loc[key, 'book'] == 'NAFTA':
        QNIA_key.loc[key, 'name'] = str(QNIA_key.loc[key, 'name']).replace('121', 'NAT')
    elif QNIA_key.loc[key, 'book'] == 'OECD - FORMER TOTAL':
        QNIA_key.loc[key, 'name'] = str(QNIA_key.loc[key, 'name']).replace('990', 'OTF')
    elif QNIA_key.loc[key, 'book'] == 'Euro area':
        QNIA_key.loc[key, 'name'] = str(QNIA_key.loc[key, 'name']).replace('719', 'EMU')
    if found == False:
        not_found2.append(str(QNIA_key.loc[key, 'name']))
    if possible == True:
        if str(QNIA_key.loc[key, 'name']) not in possible_exist:
            possible_exist.append(str(QNIA_key.loc[key, 'name']))
        for c in possible_code:
            #if name0 == 'A':
            QNIA_t.loc[c, 'subject'] = 'possible'
            #else:
            #    QNIA_tt.loc[c, 'subject'] = 'possible'
sys.stdout.write("\n\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')

#print(not_found)
print('Name not found:', len(not_found))
print('Name not found without repeated:', len(not_found2))
print('Possible code exists:', len(possible_exist))
QNIA_key.to_excel(out_path+NAME+"key_rename.xlsx", sheet_name=NAME+'key_rename')
"""
print('Renaming the key file, Time: ', int(time.time() - tStart),'s'+'\n')
for key in range(QNIA_key.shape[0]):
    sys.stdout.write("\rLoading...("+str(round((key+1)*100/QNIA_key.shape[0], 2))+"%), "+str(QNIA_key.loc[key, 'book'])+", Time: "+str(int(time.time() - tStart))+"s ############################################")
    sys.stdout.flush()
    QNIA_key.loc[key, 'seasonally adjusted'] = ''
    QNIA_key.loc[key, 'quarterly levels'] = ''
    if QNIA_key.loc[key, 'is_renamed'] == 'renamed':
        #print(QNIA_key.loc[key, 'is_renamed'])
        for code in range(QNIA_t.shape[0]):
            if QNIA_key.loc[key, 'name'] == QNIA_t.loc[code, 'code']:
                QNIA_t.loc[code, 'by'] = 'used'
                QNIA_t.loc[code, 'subject'] = ''
                break
sys.stdout.write("\n\n")
"""
print('Outputing file, Time: ', int(time.time() - tStart),'s'+'\n')
#QNIA = pd.concat([QNIA_t, QNIA_tt], ignore_index=True)
QNIA_t.to_excel(out_path+NAME+"renamed.xlsx", sheet_name=NAME+'renamed')

print('Time: ', int(time.time() - tStart),'s'+'\n')