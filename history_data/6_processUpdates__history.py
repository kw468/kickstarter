"""
    Clean History HTML Files For All Kickstarter Project Updates Gathered From allSuccessfulProjects
    Kickstarter Project
--------------------------------------------------------------------------------
change log:
    v0.0.1  Tues 22 Sep 2020
-------------------------------------------------------------------------------
notes:

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
from bs4 import BeautifulSoup
import datetime
import re
import glob
import csv
import bleach
import multiprocessing as mp


######################################################################
######### DEFINE PRIMATIVES
######################################################################
pathMain                               = "/gpfs/project/kickstarter/"
pathIn                                 = pathMain + "comments_and_updates/updates_details/"
pathOut                                = pathMain + "agg/"

######################################################################
######### FUNCTION CALLS
######################################################################

# writes the results of the pid to folder
def writeResults(X):
    g = open(pathOut + "projectUpdateData_history.csv", 'w')
    csvWriter = csv.writer(g, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    for x in X:
        if x != None:
            csvWriter.writerow(x)
    g.close()


# the main parsing function
def parseFile(fname):
    # open the html and parse to soup
    f               = open(fname)
    soup            = BeautifulSoup(f,'html.parser')           # open the file and make some soup
    f.close()
    # extract the pid id
    pidDetails      = list(map(int, re.split("_",fname)[4:5]))
    # check to see if the pid is under copyright dispute
    if len(soup.findAll("div", { "id" : "hidden_project" })) != 0:
        print("this project is subject to an IP dispute; ignore")
    else:
        try:
            # we'll extract the update number, uppdate timestamp, number of comments, number of likes, and the text
            updateNum	            = int(str(soup.find("a", { "class" : "green no-wrap" }).contents[0]).replace("\n","").replace("Update #",""))
            updateTime 	            = soup.find("p", { "class" : "published f6 grey-dark" }).find("time")['datetime']
            tsList                  = list(map(int, re.split("-|T|:", updateTime)))
            try:
                numComments	        = int(re.findall('\d+', str(soup.find("div", { "class" : "native-hide mobile-hide" }).find("a").contents[0]))[0])
            except AttributeError:
                numComments         = int(re.findall('\d+', str(soup.find("span", { "class" : "comments" }).contents[0]))[0])    
            numLikes                = soup.find("data", {"itemprop" : "Post[post_likes]"}).contents
            if numLikes == []:
            	likeCount           = 0
            else:
            	likeCount           = int(re.findall('\d+', str(numLikes[0]))[0])
            # extract the title of the post
            title 		            = soup.find("meta", {"property" : "og:title"})["content"].replace("|","")
            descrip 	            = re.sub('\s+', ' ',soup.find("meta", {"property" : "og:description"})["content"].replace("|",""))
            numFigures 	            = len(soup.findAll("figure"))
            text		            = soup.find("div", {"class" : "body readability responsive-media formatted-lists"})
            text                    = str(text).replace("<", " <")
            updateText              = re.sub(r'[^\w]', ' ', re.sub('\s+', ' ', bleach.clean(text, tags=[], strip=True)).replace("|",""))
            updateText              = ' '.join(updateText.split())
            success  	            = True
        except Exception:
            print(fname)
            success                 = False
        if success == True:
            return pidDetails + tsList + [updateNum,numComments,likeCount,numFigures, len(title), len(descrip), len(updateText)] + [title,descrip,updateText]
        elif success == False:
            pass

# process all files
def processAll(inds):
    p                                   = mp.Pool()
    results                             = p.map(parseFile,inds)
    p.close()
    p.join()
    return results


if __name__ == '__main__':
    inds            = glob.glob(pathIn + "*.html")
    result          = processAll(inds)
    writeResults(result)
    print("program complete")

