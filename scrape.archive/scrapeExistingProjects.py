#!/home/krwra/Applications/anaconda/bin/python

##### THIS PROGRAM SCRAPES LISTING-LEVEL DETAILS FROM KICKSTARTER.COM
# Written by:  Kevin Williams
# Modified on: 3/8/2017

######################################
##### THIS SCRIPT RUNS AT 1:30:00AM and 1:30:00PM
##### AT MOST 5000 PAGES ARE DOWNLOADED AT ONE TIME (OR THE MAX # OBS ON KICKSTARTER)
######################################


######################################################################
######### IMPORT REQUIRED PACKAGES
######################################################################
from selenium import webdriver
import unicodedata
import datetime
import csv
import glob
import time
import random
import requests
import smtplib

######################################################################
######### DEFINE PRIMATIVES
######################################################################
mainUrl = "https://www.kickstarter.com/"
pathOut = "/mnt/data0/kickstarter2/data/existingprojects/"
pathMain = "/mnt/data0/kickstarter2/data/"

# this is the master listing file
master = pathMain + "masterListing.txt"

# define email log in
FROM = "<<REMOVED>>"
TO = "<<REMOVED>>"
LOGIN = ["<<REMOVED>>", "<<REMOVED>>"]
SUBJECT = "Kickstarter scraping: successfully scraped existing projects"


######################################################################
######### DEFINE FUNCTIONS
######################################################################

# OPEN MAIN CSV
def openCSV(master):
    with open(master, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter="|")
        X = []
        for row in reader:
            X.append(row)
    return X   

# REQUEST SPECIFIC URL
def requestWeb(url):
    user_agent ={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    #user_agent ={'User-Agent': 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'}
    r = requests.get(url, headers = user_agent)
    return r

# WRITE PROJECT DETAILS TO FILE
def writeResults(pid,contents,timeStamp):
	# write results to file
	with open(pathOut + "Listing_" + pid + "_" + timeStamp + ".html", 'w') as f:
		f.write(contents)		
	print "page write complete, " + pid +", " + timeStamp

# DOWNLOAD ALL LISTINGS
def downloadAll():
    listings = openCSV(master)
    a = len(listings)
    b = 0
    for x in listings:
        if datetime.date(int(x[1]), int(x[2]), int(x[3])) +  datetime.timedelta(7) >= datetime.date.today():
            b +=1
            time.sleep(random.uniform(0,2))     # go to sleep for a little bit
    	    pid = x[0]                          # extract project id
    	    url = x[-1]                         # extract url to download
            print "working on " + pid
    	    r=requestWeb(url)			# download page
    	    source = r.text        # obtain page source
    	    content = unicodedata.normalize('NFKD', source).encode('ascii','ignore') # convert unicode to ascii
            timeStamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')        # record time stamp
    	    writeResults(pid,content,timeStamp)                                      # write results to file
    return a,b

######################################################################
######### EXECUTE
######################################################################
if __name__ == "__main__":
    a,b=downloadAll()
    aa = "Download complete: there are " + str(a) + " projects in the master"
    bb = "Of those, " + str(b) + " are determined to be active"
    print aa
    print bb
    TEXT = "\n".join([aa,bb])
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.ehlo()
    server.starttls()
    server.login(LOGIN[0],LOGIN[1])
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    server.sendmail(FROM, TO, message)         
    server.quit()
