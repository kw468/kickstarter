##### THIS PROGRAM GATHERS THE FRONT PAGE OF KICKSTARTER.COM
# Written by:  Kevin Williams
# Modified on: 3/8/2017


######################################################################
######### IMPORT REQUIRED PACKAGES
######################################################################
from selenium import webdriver
import unicodedata
import datetime


######################################################################
######### DEFINE PRIMATIVES
######################################################################
timeStamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
url = "https://www.kickstarter.com/"
pathOut = "/Users/kw468/Dropbox/CrowdfundingJKA/tempStorage/"
#pathOut = "/Users/kevin/Dropbox/CrowdfundingJKA/tempStorage/"

#pathOut = "/mnt/data0/Kickstarter/downloads/frontPage/"


######################################################################
######### DEFINE FUNCTIONS
######################################################################
def downloadPage(url):

	# open the browser
	browser = webdriver.Firefox(executable_path='/usr/local/bin/geckodriver')

	# get the page
	browser.get(url)

	# gather page source
	source = browser.page_source

	# convert unicode for writing to file
	content = unicodedata.normalize('NFKD', source).encode('ascii','ignore')

	# close the browser
	browser.quit()

	return content


def writeResults(contents,timeStamp):

	# write results to file
	with open(pathOut + "mainPage_" + timeStamp + ".html", 'w') as f:
		f.write(contents)		

	print "page write complete, " + timeStamp


######################################################################
######### EXECUTE
######################################################################
if __name__ == "__main__":
    contents = downloadPage(url)
    writeResults(contents,timeStamp)