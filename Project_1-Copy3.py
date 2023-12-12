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


# In[4]:


file_path = '100k_a.csv'
columns_names = ['User_Id','Streaming_Id','Streamer_Username','Time_Start','Time_Stop']


# In[5]:


df = pd.read_csv(file_path, names = columns_names)


# In[6]:


print(df.head(10))


# In[7]:


distinct_id = df['User_Id'].drop_duplicates().head(10)


# In[8]:


print(distinct_id)


# In[8]:


counts = df['Streamer_Username'].value_counts()


# In[9]:


print(counts)


# In[13]:


sum(counts)


# In[14]:


total = df.shape[0]
print("total:", total)


# In[19]:


least_rated_item = df.loc[df['Streaming_Id'].idxmin()]


# In[20]:


print(least_rated_item)


# In[21]:


most_rated_item = df.loc[df['Streaming_Id'].idxmax()]


# In[22]:


print(most_rated_item)


# In[23]:


print(df.head(10))


# In[24]:


df['result'] = df['Time_Stop'] - df['Time_Start']


# In[25]:


df_diff = df.sort_values(by='result', ascending=False)


# In[26]:


longest_Review =df_diff.head(7)


# In[27]:


print(longest_Review)


# In[34]:


average_rating = df.groupby('Streaming_Id')['result'].mean()


# In[30]:


print(average_rating.head(7))


# In[37]:


parquet_file = df.to_parquet(engine='pyarrow')

