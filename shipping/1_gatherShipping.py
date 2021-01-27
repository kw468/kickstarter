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
import requests
import re
import csv
import pandas as pd
import datetime
import time
import random



######################################################################
######### DEFINE PRIMATIVES
######################################################################
pathMain1 			= "/mnt/data0/kickstarter/data/"
pathMain2 			= "/mnt/data0/kickstarter2/data/"
pathIn 				= "/mnt/data0/kickstarter2/data/projects/raw/"        # this golder is pid-12-hr specific
pathOut 			= "/mnt/data0/kickstarter2/data/shipping/raw/"    # this folder is pid-specific

# this is the master listing file
master1 			= pathMain1 + "masterListing.txt"
master2 			= pathMain2 + "masterListing.txt"


timeStamp 			= datetime.datetime.now().strftime('%Y%m%d%H%M%S')


from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

proxy_host 			= "<<REMOVED>>"
proxy_port 			= "<<REMOVED>>"
proxy_auth 			= "<<REMOVED>>:"  # Make sure to incl


######################################################################
######### DEFINE FUNCTIONS
######################################################################

# read master listing files
def openMaster(master):
    with open(master, 'r') as csvfile:
        reader 			= csv.reader(csvfile, delimiter="|")
        X 				= []
        for row in reader:
            X.append(row)
    return X                        


# writes the results of the pid to folder
def writeResults(pid,reward,result,timeStamp):
    # write results to file
    with open(pathOut + "Shipping_" + pid + "_" + reward + "_" + timeStamp + ".json", 'w') as f:
        f.write(result.content)  


def gatherPrices(pid,link):
	proxies 			= {"https": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
               				"http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}
	s 					= requests.Session()
	s.proxies.update(proxies)
	try:
		mainPage		= s.get(link, verify = False)
		# now find the required links
		if mainPage.status_code == 200:
			sep 		= re.split(";",mainPage.content)
			urls 		= [u for u in sep if "https://api.kickstarter.com/" in u]
			urls 		= [u for u in urls if "reward" in u]
			print("found " + str(len(urls)) + " pages to download for this pid: " + pid)
			for u in urls:
				result 			= s.get(u, verify = False)
				if result.status_code == 200:
					print(u)
					bucket 		= re.split("/|\?", u)[7]
					#time.sleep(random.uniform(0, 3))
					writeResults(pid,bucket,result,timeStamp)
				else:
					print "THERE IS SOMETHING WRONG"
	except Exception:
		print("BAD PROJECT")
		pass


if __name__ == '__main__':
    Y1               =   openMaster(master1)                  # open master listing
    Y2               =   openMaster(master2)                  # open master listing
    Y=Y1+Y2
    X = []
    for y in Y:
        print("working on index " + str(Y.index(y)))
        pid 	     = y[0]
        link 		 = y[-1]
        gatherPrices(pid,link)
        X.append(y)


