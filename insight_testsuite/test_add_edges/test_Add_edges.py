import numpy as np
import pandas as pd
import networkx as nx
import json
import sys

batchlogfile = 'sample_dataset/batch_log.json'
df_batch = pd.read_json(batchlogfile, lines=True)

index_purchase = ['event_type','id','timestamp','amount']
index_friend = ['event_type','id1','id2','timestamp']

df_purchase = df_batch[df_batch['event_type']=='purchase']
df_purchase = df_purchase[index_purchase]
df_purchase = df_purchase.dropna(how='any')
# If sort on the timestamp is needed, commentout the following line
# df_purchase = df_purchase.sort_values('timestamp')
#df_purchase.shape

df_friend = df_batch[(df_batch['event_type']=='befriend') | (df_batch['event_type']=='unfriend')]
df_friend = df_friend[index_friend]
df_friend = df_friend.dropna(how='any') 
# If sort on the timestamp is needed, commentout the following line
#df_friend=df_friend.sort_values('timestamp')
#df_friend.shape

G = nx.Graph()

def Add_nodes(data):
	idlist = data.id.tolist()
	idlist = set(idlist)
	G.add_nodes_from(idlist)
	#len(list(G.nodes()))



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
								
def test_Add_edges():
	Add_edges(df_friend)
	


def test_Add_nodes():
	Add_nodes(df_purchase) 
