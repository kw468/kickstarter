"""
    Clean History HTML Files For All Kickstarter Project Comments Gathered From allSuccessfulProjects
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
import os
import bleach
import multiprocessing as mp
import itertools


######################################################################
######### DEFINE PRIMATIVES
######################################################################
pathMain                               = "/gpfs/project/kickstarter/"
pathIn                                 = pathMain + "comments_and_updates/comments_main/"
pathOut                                = pathMain + "comments_and_updates/comments_clean/"

######################################################################
######### FUNCTION CALLS
######################################################################

# writes the results of the pid to folder
def writeResults(X,pidDetails,num):
    g           = open(pathOut + "projectUpdate_" + str(pidDetails[0]) + "_" + num + ".csv", 'w')
    csvWriter   = csv.writer(g, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    for x in X:
        if x != None:
            csvWriter.writerow(x)
    g.close()


# replaces sequential white spaces with a single space
def replace_whiteSpace(string):
    while '  ' in string:
        string = string.replace('  ', ' ')
    return string


# replaces html links with text
def replace_links(string):
    try:
        return string.get_text()
    except AttributeError:
        return string


# the main parsing function
def parseFile(fname):
    # open the html and parse to soup
    f               = open(fname)
    soup            = BeautifulSoup(f,'html.parser')           # open the file and make some soup
    f.close()
    pidDetails      = list(map(int, re.split("_",fname)[4:5]))
    # extract all the comments and the number of the comments page
    comments        = soup.findAll("li", { "class" : "NS_comments__comment comment item py3 mb3" })
    num             = re.split("_|\.",fname)[-2]
    if comments == []:
        pass                # if no comments, skip
    else:
        # we'll extract the text, author and timestamp for each comment
        text=[] ; authors=[]; times=[];
        for c in comments:
            # AUTHOR BLOCK
            try: 
                au                      = c.find("a", { "class" : "author" }).contents
            except AttributeError:
                au                      = None      # we'll replace attribute errors with None and empty sets with N/A
            if au != None:
                if au == []:
                    au                  = ["N/A"]
                authors.append(au)
                # TEXT BLOCK
                try:
                    text.append(c.find('p').contents[0])        # extract the comment
                except IndexError:
                    text.append("N/A")
                    # TIME BLOCK
                try:
                    times.append(c.find("a", { "class" : "grey-dark" }).time['datetime'])
                # if there is a time error, it could be become comments were made "hours" or "days" ago. So let's adjust for that.
                except TypeError:
                    ostime              = datetime.datetime.fromtimestamp(os.path.getctime(fname))#.strftime('%Y-%m-%dT%H:%M:%S-05:00')
                    adjustment          = c.find("a", { "class" : "grey-dark" }).contents[0]
                    if "days ago" in adjustment:
                        ostime          = ostime - datetime.timedelta(days=int(re.findall('\d+', adjustment)[0]))
                    elif "hours ago" in adjustment:
                        ostime          = ostime - datetime.timedelta(hours=int(re.findall('\d+', adjustment)[0]))
                    else:
                        pass
                    ostime              = ostime.strftime('%Y-%m-%dT%H:%M:%S-05:00')
                    times.append(ostime)
        # now let's clean up the results. We'll make a list of the others and replace html tags and characters in the text.
        authors                         = list(itertools.chain.from_iterable(authors))
        text                            = [replace_links(t) for t in text]
        text                            = [re.sub(r'[^\w]@', ' ', re.sub('\s+', ' ', bleach.clean(t, tags=[], strip=True)).replace("|","")) for t in text]
        text                            = [replace_whiteSpace(string) for string in text]
        # if lengths disappear, we'll ignore the PID
        if len(text) != len(authors) != len(times):
            print("issue with this file")
            print(fname)
        else:
            pid                         = [pidDetails[0]]*len(text)
            zips                        = [list(l) for l in zip(pid,authors,times,text)]
            # write results to file
            writeResults(zips,pidDetails,num)


# this uses pool() to go over all files
def processAll(inds):
    p                                   = mp.Pool()
    res_list                            = []
    results                             = p.map(parseFile,inds)
    p.close()
    p.join()
    return res_list


# run the program
if __name__ == '__main__':
    inds            = glob.glob(pathIn + "*.html")
    result          = processAll(inds)
    print("program complete")




