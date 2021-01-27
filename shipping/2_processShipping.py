"""
    Gather All Shipping Information For All Projects Found In MasterListing
    Kickstarter Project
--------------------------------------------------------------------------------
change log:
    v0.0.1  Tues 22 Sep 2020
-------------------------------------------------------------------------------
notes: This program no longer runs

--------------------------------------------------------------------------------
contributors:
    Kevin:
        name:       Kevin Williams
        email:      kevin.williams@yale.edu
--------------------------------------------------------------------------------
Copyright 2020 Yale University
"""

######################################################################
######### IMPORT REQUIRED PACKAGES
######################################################################
import re
import csv
import pandas as pd
import datetime
import time
import random
import glob
import json
import multiprocessing as mp



######################################################################
######### DEFINE PRIMATIVES
######################################################################
pathMain                               = "/gpfs/project/kickstarter/"
pathIn                                 = pathMain + "shipping/raw/"
pathOut                                = pathMain + "agg/"

usa 			= 23424977 # code for usa shopping

# this is the master listing file
files 			= glob.glob(pathIn + "*.json")


######################################################################
######### DEFINE FUNCTIONS
######################################################################
def gatherCosts(fname):
	try:
		pid 							= int(re.split("_",fname)[1])
		with open(fname) as f:
		    data 						= json.load(f)
		    # if shipping is not enabled, then the shipping field is equivalent to free
		    if data['shipping_enabled'] == False:
		    	min_cost 				= 0
		    	max_cost 				= 0
		    	usa_cost				= 0
		    # if shipping is enabled, we need to look for shipping costs
		    elif data['shipping_enabled'] == True:
		    	costData 				= data['shipping_rules']
		    	min_cost 				= min(list(map(float, list(map(lambda d: d.get('cost', '0'), costData)))))
		    	max_cost 				= max(list(map(float, list(map(lambda d: d.get('cost', '0'), costData)))))
		    	for c in costData:
		    		if c['location_id'] == usa:
		    			usa_cost 		= float(c['cost'])
		    # if USA cost does not exist, create it
		    try:
		    	usa_cost
		    except NameError:
		    	usa_cost = 0
		    # now track the price and id
		    rewardID = data['id']
		    price 	 = data['minimum']
		return [pid,rewardID,price,usa_cost, min_cost, max_cost]
	except Exception:
		return []

# process all files
def processAll(files):
    p                                   = mp.Pool()
    results                             = p.map(gatherCosts,files)
    p.close()
    p.join()
    return results


if __name__ == '__main__':
	results = processAll(files)

	pd.DataFrame(results).to_csv(pathOut + "shippingQuotes.csv")
