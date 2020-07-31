#!/usr/bin/env python
# coding: utf-8
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date
# In[80]:

import pandasdmx
from pandasdmx import Request

tStart = time.time()

oecd = Request('OECD')
print('Time: ', int(time.time() - tStart),'s'+'\n')

data_response = oecd.data(resource_id='MEI_BTS_COS', key='all?startTime=1995')
print('Time: ', int(time.time() - tStart),'s'+'\n')

df = data_response.to_pandas()
print('Time: ', int(time.time() - tStart),'s'+'\n')

#df.to_csv('c:\\Temp\\test_lei.txt', sep='\t')
path = 'C:/Users/lawre/Desktop'

# In[82]:


#df = data_response.to_pandas()
#type(data_response.data)
df.to_csv(path+'test_lei.csv')
print('Time: ', int(time.time() - tStart),'s'+'\n')

# In[93]:


#type(oecd)
#oecd.get( resource_type='data', resource_id='MEI_BTS_COS')

