#!/home/krwra/Applications/anaconda/bin/python
##### THIS PROGRAM GATHERS THE NEW PROJECTS POSTED TO KICKSTARTER.COM
##### IT PARSES THE FILE AND OUTPUTS THE CSV

######################################
##### THIS SCRIPT RUNS EVERY 5 MINUTES
######################################

# Written by:  Kevin Williams
# Modified on: 3/8/2017


######################################################################
######### IMPORT REQUIRED PACKAGES
######################################################################
from selenium import webdriver
import unicodedata
import datetime
from bs4 import BeautifulSoup
import csv
import re
import smtplib
import requests
import json


# define email log in
FROM = "<<REMOVED>>"
TO = "<<REMOVED>>"
LOGIN = ["<<REMOVED>>", "<<REMOVED>>"]
SUBJECT = "Error in Kickstarter data collection"



######################################################################
######### DEFINE PRIMATIVES
######################################################################
timeStamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
url = "https://www.kickstarter.com/discover/advanced?woe_id=0&sort=newest"
pathOut = "/mnt/data0/kickstarter2/data/newprojects/raw/"
pathParsed = "/mnt/data0/kickstarter2/data/newprojects/parsed/"

######################################################################
######### DEFINE FUNCTIONS
######################################################################
# REQUEST SPECIFIC URL
def requestWeb(url):
    user_agent ={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    #user_agent ={'User-Agent': 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'}
    r = requests.get(url, headers = user_agent)
    return r


# DOWNLOAD THE NEW TOP PROJECTS
def downloadPage(url):
	# open the browser
	#browser = webdriver.Firefox(executable_path='/usr/local/bin/geckodriver')
	# get the page
	#browser.get(url)
	# gather page source
	#source = browser.page_source
	# convert unicode for writing to file
	r=requestWeb(url)                       # download page
	source = r.text        # obtain page source	
	content = unicodedata.normalize('NFKD', source).encode('ascii','ignore')
	# close the browser
	#browser.quit()
	return content


# WRITE HTML TO FILE
def writeResults(contents,timeStamp):
	# write results to file
	with open(pathOut + "newProjectsTop_" + timeStamp + ".html", 'w') as f:
		f.write(contents)		
	print "page write complete, " + timeStamp


# CONVERT TIMESTAMP TO LIST
def extractTimeStamp(timeStamp):
    li = re.split("_", timeStamp)
    return li

# PARSE HTML TO CSV
#def parseFile(contents,timeStamp):
#    #f = open(fname)
#    soup = BeautifulSoup(contents,'html.parser')			# open the file and make some soup
#    #f.close()
#    data=soup.findAll("h6", { "class" : "project-title" })	# project IDs are stored in heading 6 (20 per page)
#    ts = extractTimeStamp(timeStamp)
#    X = []		# storage list for results
#    for d in data:
#        pName = unicodedata.normalize('NFKD', d.string).encode('ascii','ignore')		# this is the project name
#        pName = pName.replace('|', '')  # replace | with '' so we do not have any write errors 
#        pid = d.a['data-pid']			# project id is pid
#        link = d.a['href']				# href to each project
#        li = [pName,pid,link] + ts		# vectorize result
#        X.append(li)					# append to X
#    return X

# updated for july
def parseFile(contents,timeStamp):
    #f = open(fname)
    soup = BeautifulSoup(contents,'html.parser')            # open the file and make some soup
    #f.close()
    data=soup.findAll("div", { "data-ref" : "newest" })  # project IDs are stored in heading 6 (20 per page)
    ts = extractTimeStamp(timeStamp)
    X = []      # storage list for results
    for d in data:
        pName = str(d['id'])
        #pName = unicodedata.normalize('NFKD', str(d['id'])).encode('ascii','ignore')        # this is the project name
        pName = pName.replace('|', '')  # replace | with '' so we do not have any write errors 
        pid = str(d['data-pid'])           # project id is pid
        link = str(json.loads(d["data-project"])['urls']['web']['project'])	#d.a['href']              # href to each project
        li = [pName,pid,link] + ts      # vectorize result
        X.append(li)                    # append to X
    return X

# WRITE CSV TO FILE
def writeParsedResults(X,timeStamp):
    g = open(pathParsed+'newProjectsListings_'+timeStamp+'.txt', 'wb')
    csvWriter = csv.writer(g, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    for x in X:
        csvWriter.writerow(x)
    g.close()



######################################################################
######### EXECUTE
######################################################################
if __name__ == "__main__":
    contents = downloadPage(url)			#download most recent projects and gather contents
    writeResults(contents,timeStamp)		#write contents (html) to file
    X=parseFile(contents,timeStamp)			#parse results to csv
    if X == []:
        a= "there is an error in the data collection process"
        TEXT = a
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(LOGIN[0],LOGIN[1])
        message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
        server.sendmail(FROM, TO, message)         
        server.quit()
    writeParsedResults(X,timeStamp)			#save csv to file
    print "completed with top 20 pull"
