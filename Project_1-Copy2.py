#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import numpy as np


# In[10]:


df = pd.read_csv('100k_a.csv')


# In[11]:


df.head(10)


# In[12]:


total = df.shape[0]
print("total:",total)


# In[13]:


print(df.head())


# In[15]:


pip install pandas pyarrow


# In[1]:


file_path = '100k_a.csv'
columns_names = ['User_Id','Streaming_Id','Streamer_Username','Time_Start','Time_Stop']


# In[4]:


df = pd.read_csv(file_path, names = columns_names)


# In[5]:


print(df.head(10))


# In[7]:


distinct_id = df['User_Id'].drop_duplicates().head(10)


# In[8]:


print(distinct_id)


# In[ ]:




