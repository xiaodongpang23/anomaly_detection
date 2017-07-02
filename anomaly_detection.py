
# coding: utf-8

# In[1]:

import numpy as np
import pandas as pd
import networkx as nx
import json
import sys


# ## Step1: build the initial state of the entire user network, as well as the purchae history of the users
# Input: sample_dataset/batch_log.json

# In[2]:

batchlogfile = 'sample_dataset/batch_log.json'
df_batch = pd.read_json(batchlogfile, lines=True)


# In[3]:

#df_batch.head()


# In[4]:

#df_batch.describe()


# In[5]:

# Read D and T
df_DT=df_batch[df_batch['D'].notnull()]
df_DT=df_DT[['D','T']]
D = df_DT.values[0][0]
T = df_DT.values[0][1]
#print(D)
#print(T)
#df_DT.head()


# In[6]:

# check D and T values
if D < 1:
    print('Program terminated because of D < 1')
    sys.exit()
if T < 2:
    print('Program terminated because of T < 2')
    sys.exit()


# In[7]:

#for possible_value in set(df['event_type'].tolist()):
#    print(possible_value)


# In[8]:

df_purchase = df_batch[df_batch['event_type']=='purchase']
df_purchase = df_purchase[['event_type','id','timestamp','amount']]
# If sort on the timestamp is needed, commentout the following line
# df_purchase = df_purchase.sort_values('timestamp')
#df_purchase.shape


# In[9]:

df_friend=df_batch[(df_batch['event_type']=='befriend') | (df_batch['event_type']=='unfriend')]
df_friend=df_friend[['event_type','id1','id2','timestamp']]
# If sort on the timestamp is needed, commentout the following line
#df_friend=df_friend.sort_values('timestamp')
#df_friend.shape


# In[10]:

G = nx.Graph()


# In[11]:

id1list = df_friend.id1.tolist()
id2list = df_friend.id2.tolist()
idlist = set(id1list + id2list)
G.add_nodes_from(idlist)
#len(list(G.nodes()))


# In[12]:

def Add_edges(data):
    for row in data.itertuples():
        id10 = row.id1
        id20 = row.id2
        event_type0 = row.event_type
        if event_type0 == 'befriend':
            G.add_edge(id10,id20)
        if event_type0 == 'unfriend':
            if G.has_edge(id10,id20):
                G.remove_edge(id10,id20)  


# In[13]:

Add_edges(df_friend)


# In[14]:

#len(list(G.edges()))


# In[15]:

#G[10.0]


# In[16]:

#G.number_of_nodes()


# In[17]:

#G.number_of_edges()


# In[18]:

# define a function to calcualte the mean and sd for userid's network
def Get_Mean_SD(userid):
    Nodes = list(nx.ego_graph(G, userid, D, center=False))
    df_Nodes = df_purchase.loc[df_purchase['id'].isin(Nodes)]
    if len(df_Nodes) >= 2:    
        if len(df_Nodes) > T:
            df_Nodes = df_Nodes.sort_values('timestamp').iloc[-int(T):]
        df_Nodes.shape
        mean = df_Nodes.amount.mean()
        sd = df_Nodes.amount.std()
        mean = float("{0:.2f}".format(mean))
        sd = float("{0:.2f}".format(sd))
    else:
        mean=np.nan
        sd=np.nan
    
    return mean, sd


# In[19]:

#Get_Mean_SD(0.0)


# ## Step2: Determine whether a purchase is anomalous 
# input file: sample_dataset/stream_log.json

# In[20]:

# read in the stream_log.json
streamlogfile = 'sample_dataset/stream_log.json'
df_stream = pd.read_json(streamlogfile, lines=True)
# If sort on the timestamp is needed, commentout the following line
#df_stream = df_stream.sort_values('timestamp')

# open output file flagged_purchases.json
flaggedfile = 'log_output/flagged_purchases.json'
f = open(flaggedfile, 'w')


# In[21]:

# Determine whether a purchase is anomalous; update purchase history; update social network
index = list(['event_type','id','timestamp','amount'])
for row in df_stream.itertuples():
    event_type = row.event_type
    if event_type == 'purchase':
        # update purchase history
        df_purchase = df_purchase.append(pd.Series([row[2], row[3], row[6], row[1]],index=index),ignore_index=True)
        timestamp = row.timestamp
        timestamp = str(timestamp)
        userid = row.id
        amount = row.amount
        mean, sd = Get_Mean_SD(userid)
        if mean != np.nan:
            mean_3sd = mean + (3*sd)
            if amount > mean_3sd:
                f.write('{{"event_type":"{0:s}", "timestamp":"{1:s}", "id": "{2:.0f}", "amount": "{3:.2f}", "mean": "{4:.2f}", "sd": "{5:.2f}"}}\n'.format(event_type, timestamp, userid, amount, mean, sd))
    # update social network
    if event_type == 'befriend':
        id1 = row.id1
        id2 = row.id2
        G.add_edge(id1,id2)
    if event_type == 'unfriend':
        id1 = row.id1
        id2 = row.id2
        if G.has_edge(id1,id2):
            G.remove_edge(id1,id2)  
    


# In[22]:

f.close() 

