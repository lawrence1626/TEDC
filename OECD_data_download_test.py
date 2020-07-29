#!/usr/bin/env python
# coding: utf-8

# In[80]:


from pandasdmx import Request
oecd = Request('OECD')
data_response = oecd.data(resource_id='MEI_BTS_COS', key='all?startTime=1995')
df = data_response.to_pandas()
#df.to_csv('c:\\Temp\\test_lei.txt', sep='\t')


# In[82]:


#df = data_response.to_pandas()
#type(data_response.data)
df.to_csv('test_lei.csv')


# In[93]:


#type(oecd)
#oecd.get( resource_type='data', resource_id='MEI_BTS_COS')

