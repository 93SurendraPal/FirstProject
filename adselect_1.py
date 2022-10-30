import MySQLdb
import pandas as pd
import sys
from sklearn.cluster import KMeans
import json
import numpy as np
from math import sqrt
import operator
#------------------------- DB CONNECTION -------------------------#
host = '139.59.73.142'
uname = 'services'
passw = 'services@1234'
dbname = 'db_cms_fb'
cxn = MySQLdb.connect(host=host,user=uname,passwd=passw,db=dbname)
cursor = cxn.cursor()
input_camp = sys.argv[1]
volflag = sys.argv[2]
cpaflag = sys.argv[3]
tbl_name = 'ADSELECT_RUN_HISTORY'
select_query_adsets = "select distinct(adset_id) from {0} where campaign_id={1};".format(tbl_name,input_camp)
cursor.execute(select_query_adsets)
# creating the iterable by adset id
adset_iter_temp = np.array(cursor.fetchall()).tolist()
adset_iter = []
adset_iter1 = []
for i in adset_iter_temp:
	for j in i:
		adset_iter1.append(j)


# #------------------------- DATA IMPORT -------------------------#
final_list = []
adsets_dict = {}
final_dict = {}
adset_iter = [int(a) for a in adset_iter1]
for i in adset_iter:#Iterating over adsets
	pauseads = {}
	select_query_process = "select run_date,ad_id,sum(spend) as spend,sum(results) as results,sum(impressions) as impressions from {0} where adset_id={1} group by run_date,ad_id;".format(tbl_name,i)
	# print select_query_process
	cursor.execute(select_query_process)
	df = pd.DataFrame(list(cursor.fetchall()))#storing all data from the adset id in df
	df.columns = ["date","ad_id","spend","actions","impressions"]
	df = df.replace(0,np.NaN)
	df = df.dropna(inplace=False)
	df['cpa'] = df['spend']/df['actions']
	df['date'] = pd.to_datetime(df['date'])
	df["ad_id"] = df["ad_id"].astype('category')
	# print i
	# print len(df['ad_id'].unique())
	itervar = df['ad_id'].unique() #extracting unique ad_ids
	if len(itervar) < 2: #Checking how many ads are within adset
		pauseads[str(i)] = ["No Action"]
		continue
	#Creating generator of ad_id subsets within df
	test_gen = (df.loc[df['ad_id']==i,] for i in itervar)# if len(df.loc[df['ad_id']==i,'ad_id'].unique()) > 1)
	a = list(test_gen)
	
	use = {} #Dictionary storing metrics for each ad_id
	metric_admean = {} #metric adsetmean
	 #list of metrics for adset
	metric_flatlist = [] #flattening above list
	adkeep = []
	adtest = []
	volat_dict = {}
	lolists = []
	for j in range(len(a)): #ITERATING OVER EACH AD ID, CALCULATING METRIC FOR ADS RUN FOR OVER 4 DAYS AND STORING IN USE
		temp = a[j]
		temp = temp.sort_values('date')
		if(len(temp)<4): 								#NOT CONSIDERING LESS THAN 4 DAYS OF DATA
			continue		
		# temp['act_rate'] = temp['actions']/temp['impressions']
		temp['metric'] = temp['actions']/temp['cpa']#(temp['act_rate']*temp['actions'])/temp['cpa']
		metric_list = temp['metric'].tolist()
		use[temp['ad_id'].iloc[0]] = metric_list

	if bool(use)==False: #IF bool is empty exit the iteration
		continue
	if len(use.keys()) <2: #NOT CONSIDERING ADSETS WITH ONLY ONE AD
		continue

	for k in use.keys():
		for l in use[k]:
			metric_flatlist.append(l) #FLATTENING THE LIST OF METRICS

	adset_mean = sum(metric_flatlist)/len(metric_flatlist) #FINDING OUT MEAN OF CURRENT ADSET
	for m in use:
		metric_admean[m]=float(sum(use[m])/len(use[m])) #CHECKING WHICH ADS ARE BELOW AND ABOVE THE MEAN AND SEGRATING
		# print metric_admean[m]
		# print adset_mean
		if metric_admean[m]>adset_mean:
			adkeep.append(m)
		else:
			adtest.append(m)
	if len(adtest)<=2: # IF BELOW MEAN IS <=2 THEN THEY ARE DIRECTLY PAUSED 
		pauseads[str(i)] = adtest
		final_list.append(pauseads)
		continue
	for n in use: # ELSE THE VOLATILITY IS CALCULATED
		dev_list=[]
		if(n in adtest):
			metlist = use[n]
			for o in range(1,len(metlist)):
				if o == len(metlist):
					dev_list[o] = dev_list[o-1]
				dev_list.append(metlist[o]/metlist[o-1])
			adj_volat = (pd.to_numeric(pd.Series(dev_list)).std())*sqrt(len(dev_list))
			volat_dict[n] = adj_volat
		else:
			continue

	
	newd = {k:round(v,2) for k,v in volat_dict.items()}
	arrofarr = np.asarray(newd.values())
	for h in arrofarr:
		lolists.append([h]) # FLATTENING THE LIST OF VOLATILITIES FOR EACH AD WITHIN ADSET
	
	if len(arrofarr)<=4:
		pauseads = {str(i):max(newd.iteritems(), key=operator.itemgetter(1))[0]}
		final_list.append(pauseads) # IF THERE ARE <= 4 ITEMS IN ARRAY PAUSE THE ONE WITH THE MAXIMUM VOLATILITY
	else: # ELSE CLUSTER THEM AND PAUSE THE WEAK CLUSTER
		kmeans = KMeans(n_clusters=2)
		x = kmeans.fit_predict(lolists)
		volvals = pd.Series(arrofarr)
		finaldf = pd.DataFrame({'ad_id':newd.keys(),'Volatility':volvals,'Cluster':x})#.reset_index()
		pauseclust = int(finaldf.loc[finaldf['Volatility']==max(finaldf['Volatility']),'Cluster'])
		pauseads = {str(i):finaldf.loc[finaldf['Cluster']==pauseclust,'ad_id'].tolist()}
		final_list.append(pauseads)
		# print pauseads

#CREATING FINAL RESULT
final_dict["campaign_id"] = input_camp
final_dict["adsets"] = final_list
result = json.dumps(final_dict)
print result


