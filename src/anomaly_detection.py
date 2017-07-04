import sys
import numpy as np
import pandas as pd
import networkx as nx
import json
import sys

def main(argv):
	# ## Step1: build the initial state of the entire user network, as well as the purchae history of the users
	# Input: sample_dataset/batch_log.json
	
	#batchlogfile = 'sample_dataset/batch_log.json'
	batchlogfile = sys.argv[1]
	df_batch = pd.read_json(batchlogfile, lines=True)

	index_purchase = ['event_type','id','timestamp','amount']
	index_friend = ['event_type','id1','id2','timestamp']
	
	# Read D and T
	df_DT=df_batch[df_batch['D'].notnull()]
	df_DT=df_DT[['D','T']]
	D = df_DT.values[0][0]
	T = df_DT.values[0][1]
	
	# check D and T values
	if D < 1:
		print('Program terminated because of D < 1')
		sys.exit()
	if T < 2:
		print('Program terminated because of T < 2')
		sys.exit()
	
	
	df_purchase = df_batch[df_batch['event_type']=='purchase']
	df_purchase = df_purchase[index_purchase]
	df_purchase = df_purchase.dropna(how='any')
	# If sort on the timestamp is needed, commentout the following line
	# df_purchase = df_purchase.sort_values('timestamp')
	#df_purchase.shape
	
	
	df_friend=df_batch[(df_batch['event_type']=='befriend') | (df_batch['event_type']=='unfriend')]
	df_friend=df_friend[index_friend]
	df_friend=df_friend.dropna(how='any')
	# If sort on the timestamp is needed, commentout the following line
	#df_friend=df_friend.sort_values('timestamp')
	#df_friend.shape
	
	
	# Define a network G
	G = nx.Graph()
	
	idlist = set(df_purchase.id.tolist())
	G.add_nodes_from(idlist)
	#len(list(G.nodes()))
	
	
	# Define a function Add_edges to add edges to G
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
	
	
	Add_edges(df_friend)
	
	#G.number_of_nodes()
	#G.number_of_edges()
	
	
	# define a function to calcualte the mean and sd for userid's network
	def Get_Mean_SD(userid):
		Nodes = list(nx.ego_graph(G, userid, D, center=False))
		df_Nodes = df_purchase.loc[df_purchase['id'].isin(Nodes)]
		if len(df_Nodes) >= 2:    
			if len(df_Nodes) > T:
				df_Nodes = df_Nodes.sort_values('timestamp').iloc[-int(T):]
			#df_Nodes.shape
			#the std from pd is different from np; np is correct
			#mean = df_Nodes.amount.mean()
			#sd = df_Nodes.amount.std()
			mean = np.mean(df_Nodes['amount'])
			sd = np.std(df_Nodes['amount'])
			mean = float("{0:.2f}".format(mean))
			sd = float("{0:.2f}".format(sd))
		else:
			mean=np.nan
			sd=np.nan
		
		return mean, sd
	
	
	#Get_Mean_SD(0.0)
	
	# read in the stream_log.json
	#streamlogfile = 'sample_dataset/stream_log.json'
	streamlogfile = sys.argv[2]
	df_stream = pd.read_json(streamlogfile, lines=True)
	# If sort on the timestamp is needed, commentout the following line
	#df_stream = df_stream.sort_values('timestamp')
	
	# open output file flagged_purchases.json
	#flaggedfile = 'log_output/flagged_purchases.json'
	flaggedfile = sys.argv[3]
	f = open(flaggedfile, 'w')
	
	# Determine whether a purchase is anomalous; update purchase history; update social network
	for i in range(0, len(df_stream)):
		datai = df_stream.iloc[i]
		event_type = datai['event_type']
		if (event_type == 'purchase') & (not datai[index_purchase].isnull().any()):
			# update purchase history
			df_purchase = df_purchase.append(datai[index_purchase])
			timestamp = datai['timestamp']
			timestamp = str(timestamp)
			userid = datai['id']
			amount = datai['amount']
			mean, sd = Get_Mean_SD(userid)
			if mean != np.nan:
				mean_3sd = mean + (3*sd)
				if amount > mean_3sd:
					f.write('{{"event_type":"{0:s}", "timestamp":"{1:s}", "id": "{2:.0f}", "amount": "{3:.2f}", "mean": "{4:.2f}", "sd": "{5:.2f}"}}\n'.format(event_type, timestamp, userid, amount, mean, sd))
		# update social network
		if (event_type == 'befriend') & (not datai[index_friend].isnull().any()):
			df_friend=df_friend.append(datai[index_friend])
			id1 = datai['id1']
			id2 = datai['id2']
			G.add_edge(id1,id2)
		if (event_type == 'unfriend') & (not datai[index_friend].isnull().any()):
			df_friend=df_friend.append(datai[index_friend])
			id1 = datai['id1']
			id2 = datai['id2']
			if G.has_edge(id1,id2):
				G.remove_edge(id1,id2)  
	

	f.close() 

	

if __name__ == "__main__":
	main(sys.argv)


