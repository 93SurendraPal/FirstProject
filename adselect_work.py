import pandas as pd
import numpy as np
from math import sqrt 
from sklearn.cluster import KMeans
import sys

#------------------------- DB CONNECTION -------------------------#
host = '139.59.73.142'
uname = 'services'
passw = 'services@1234'
dbname = 'db_cms_fb'
cxn = MySQLdb.connect(host=host,user=uname,passwd=passw,db=dbname)
cursor = cxn.cursor()
tbl_name = 'ADSELECT_RUN_HISTORY'
input_camp = sys.argv[1]
select_query = "select run_date,ad_name,results,spend,impressions from {0};".format(input_camp)
cursor.execute(select_query)
df = pd.DataFrame(list(cursor.fetchall()))
df['Cost.per.Results'] = df['spend']/df['results']
df.columns = ["Date","Ad.Name","Results","Amount.Spent..INR.","Impressions","Cost.per.Results"]

df["Date"] = pd.to_datetime(df["Date"])
df["Ad.Name"] = df["Ad.Name"].astype('category')
df = df.replace(0,np.NaN)
df = df.dropna(inplace=False)
#--Variables---#
itervar = df['Ad.Name'].unique()
finallist = []



test_gen = (df.loc[df['Ad.Name']==i] for i in itervar)
a = list(test_gen)

use = {}
metric_admean = {}
metric_list = []
metric_flatlist = []
adkeep = []
adtest = []
volat_dict = {}
lolists = [] #List of lists


for i in range(len(a)):
	temp = a[i]
	temp = temp.sort_values('Date')
	if(len(temp)<4):
		# print temp['Ad.Name']
		continue
	temp['Act_rate'] = temp['Results']/temp['Impressions']
	temp['Metric'] = (temp['Act_rate']*temp['Results'])/temp['Cost.per.Results']
	metric_list.append(temp['Metric'].tolist())
	use[temp['Ad.Name'].iloc[0]] = temp['Metric'].tolist()

 
# meanmetric = temp['Metric'].mean()
for i in use.keys():
	for j in use[i]:
		metric_flatlist.append(j)

adset_mean = sum(metric_flatlist)/len(metric_flatlist)

for i in use:
	metric_admean[i]=sum(use[i])/len(use[i])
	if metric_admean[i]>adset_mean:
		adkeep.append(i)
	else:
		adtest.append(i)


for i in use:
	dev_list=[]
	if(i in adtest):
		metlist = use[i]
		for j in range(1,len(metlist)):
			if j == len(metlist):
				dev_list[j] = dev_list[j-1]
			dev_list.append(metlist[j]/metlist[j-1])
		adj_volat = (pd.Series(dev_list).std())*sqrt(len(dev_list))
		volat_dict[i] = adj_volat
	else:
		continue



newd = {k:round(v,2) for k,v in volat_dict.items()}

arrofarr = np.asarray(newd.values())

for i in arrofarr:
	lolists.append([i])


if len(arrofarr)<=4:
	pauseads = {'PauseAds':np.amax(arrofarr).tolist()}
	print pauseads
else:
	kmeans = KMeans(n_clusters=2)
	x = kmeans.fit_predict(lolists)
	volvals = pd.Series(arrofarr)
	finaldf = pd.DataFrame({'Ad.Name':newd.keys(),'Volatility':volvals,'Cluster':x})#.reset_index()
	pauseclust = int(finaldf.loc[finaldf['Volatility']==max(finaldf['Volatility']),'Cluster'])
	pauseads = { 'PauseAds':finaldf.loc[finaldf['Cluster']==pauseclust,'Ad.Name'].tolist()}
	print pauseads
