# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date

ENCODING = 'utf-8-sig'
out_path = './output/'
column = ['code', 'frequency', 'from', 'to', 'description', 'subject', 'by', 'unit', 'source', 'OECD_code', 'country']

"""
tStart = time.time()
print('Reading file: QNIA_key'+NAME1+', Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = readExcelFile(data_path+'QNIA_key'+NAME1+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='QNIA_key')
print('Reading file: QNIA_key'+NAME2+', Time: ', int(time.time() - tStart),'s'+'\n')
df_key = readExcelFile(data_path+'QNIA_key'+NAME2+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='QNIA_key')
#print('Reading file: MEI_database, Time: ', int(time.time() - tStart),'s'+'\n')
#DATA_BASE_t = readExcelFile(data_path+'MEI_database.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
"""
with open('./QNIA_Q.txt','r',encoding='ANSI') as f:
    lines = f.readlines()
for l in range(len(lines)):
    lines[l] = lines[l].replace('\n','')
#print(lines)
#frequency = 'DAILY'

QNIA_Q = []
g_t = []
code = ''
frequency = ''
fromt = ''
to = ''
des = ''
country = ''
subject = ''
by = ''
unit = ''
source = ''
OECD_code = ''
#note = ''
#last = ''
countS = 0
ignore = False
for l in range(len(lines)):
    #print(lines[l])
    sys.stdout.write("\rLoading...("+str(int((l+1)*100/len(lines)))+"%)*")
    sys.stdout.flush()
    if l+1 >= len(lines):
        QNIA_Q.append(g_t)
        break
    if not lines[l] or lines[l] == ' ':
        if lines[l+1].find('SERIES') >= 0:
            if g_t != []:
                QNIA_Q.append(g_t)
            g_t = []
            code = ''
            frequency = ''
            fromt = ''
            to = ''
            des = ''
            country = ''
            subject = ''
            by = ''
            unit = ''
            source = ''
            OECD_code = ''
            #note = ''
            loc7 = -1
            loc8 = loc7
            loc9 = loc7
            loc10 = loc7
            loc11 = loc7
            loc12 = loc7
            ignore = False
    elif ignore == True:
        continue
    elif lines[l].find('#') >= 0:
        continue
    else:
        if lines[l].find('SERIES') >= 0:
            countS+=1
            loc1 = lines[l].find(':')+1
            loc2 = lines[l].find(' ', loc1)
            code = lines[l][loc1:loc2]
            g_t.append(code)
        elif lines[l].find('Data for') >= 0:
            locf1 = lines[l].find('Data')-1
            frequency = lines[l][:locf1]
            g_t.append(frequency)
            loc3 = lines[l].find('from')+5
            loc4 = lines[l].find('to')-2
            loc5 = lines[l].find('to')+3
            loc6 = loc5+6
            fromt = lines[l][loc3:loc4]
            to = lines[l][loc5:loc6]
            if frequency == 'ANNUAL':
                #print(lines[l])
                try:
                    fromt = int(fromt)
                    to = int(to)
                except:
                    fromt = fromt
                    to = to
            g_t.append(fromt)
            g_t.append(to)
        else:
            d = lines[l]
            des = ''
            m = l
            first = True
            while lines[m+1].find('SERIES') < 0 and lines[l].find('#') < 0:
                if first == True:
                    des = des+d+'@'
                    first = False
                else:
                    des = des+d+'/'
                m+=1
                d = lines[m]
                if m+1 >= len(lines):
                    break
            
            if des.replace('/','').replace('@','').replace('"','').find('SOURCE:') >= 0:
                loc7 = des.replace('/','').replace('"','').find('@')+1
                #loc8 = des.find('/',loc7)
                loc9 = des.replace('/','').replace('"','').find('MILLIONS')-1
                loc91 = des.replace('/','').replace('"','').find('BILLIONS')-1
                loc8 = des.replace('/','').replace('"','').find('SOURCE')-1
                loc10 = des.replace('/','').replace('@','').replace('"','').find('MILLIONS')
                loc101 = des.replace('/','').replace('@','').replace('"','').find('BILLIONS')
                loc11 = des.replace('/','').replace('@','').replace('"','').find('SOURCE')
                loc12 = des.replace('/','').replace('@','').replace('"','').find(':',loc11)+1
                loc13 = des.replace('/','').replace('@','').replace('"','').find('.',loc11)-3
                country = des.replace('"','')[:loc7].replace('@','').replace('#Name?','').title()
                #subject = des[loc7+1:loc8].replace('/','')
                #by = des[loc8+1:loc9].replace('/','')
                if loc10 >= 0:
                    unit = des.replace('/','').replace('@','').replace('"','')[loc10:loc11]
                elif loc101 >= 0:
                    unit = des.replace('/','').replace('@','').replace('"','')[loc101:loc11]
                else:
                    unit = ''
                source = des.replace('/','').replace('@','').replace('"','')[loc12:loc13]
                OECD_code = des.replace('/','').replace('@','').replace('"','')[loc13:]
                if OECD_code == 'UNTS':
                    source = des.replace('/','').replace('@','').replace('"','')[loc12:]
                    OECD_code = ''
                if loc9 >= 0:
                    des = des.replace('/','').replace('"','')[loc7:loc9].replace('#Name?','').replace('  Current', ', Current').replace('  Chained', ', Chained').replace('  Constant', ', Constant').replace(' Impicit', ', Implicit')
                elif loc91 >= 0:
                    des = des.replace('/','').replace('"','')[loc7:loc91].replace('#Name?','').replace('  Current', ', Current').replace('  Chained', ', Chained').replace('  Constant', ', Constant').replace(' Impicit', ', Implicit')
                else:
                    des = des.replace('/','').replace('"','')[loc7:loc8].replace('#Name?','').replace('  Current', ', Current').replace('  Chained', ', Chained').replace('  Constant', ', Constant').replace(' Impicit', ', Implicit')
                r = re.findall('([a-z][A-Z])',des)
                s = re.findall('([0-9][A-Z])',des)
                for w in r:
                    rr = w[0]+', '+w[1]
                    des = des.replace(w, rr)
                for w in s:
                    ss = w[0]+', '+w[1]
                    des = des.replace(w, ss)
            elif des.replace('/','').replace('@','').replace('"','').find(' - ') >= 0:
                loc8 = des.replace('/','').replace('@','').replace('"','').find(',')
                #loc9 = loc8 + 2
                #loc10 = des.find(',',loc8)
                #loc11 = loc10 + 2
                #loc10 = des.find(',',loc10)
                loc7 = des.replace('/','').replace('@','').replace('"','').find(' - ', loc8)+3
                loc9 = des.replace('/','').replace('@','').replace('"','').find(' - ', loc8)
                country = des.replace('/','').replace('@','').replace('"','')[loc7:].replace('#Name?','').title()
                des = des.replace('/','').replace('@','').replace('"','')[:loc9].replace('#Name?','')
                #subject = des[:loc8]
                #currency = des[loc9:loc10]
            elif des.replace('/','').replace('@','').find('#') >= 0:
                loc9 = des.replace('/','').replace('@','').replace('"','').find('#')-1
                des = des.replace('/','').replace('@','').replace('"','')[:loc9].replace('#Name?','')
            else:
                print(des)
            des = des.replace('/','').replace('@','').replace('S.A.', 'seasonally adjusted')
            g_t.append(des)
            g_t.append(subject)
            g_t.append(by)
            g_t.append(unit)
            g_t.append(source)
            g_t.append(OECD_code)
            g_t.append(country)
            
            ignore = True
        #else:
        #    g_t.append(lines[l])
        
    #last = l
sys.stdout.write("\n\n")
#print(QNIA_Q)
"""
for g in QNIA_Q:
    if len(g) > 7:
        print(g)
""" 
print(countS)
ger = pd.DataFrame(QNIA_Q, columns=column)
print(ger)
ger.to_excel(out_path+"QNIA_Q.xlsx", sheet_name='QNIA_Q')
