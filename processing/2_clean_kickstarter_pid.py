"""
    This Script Parses All Of The Projects Found In MasterListing
    Kickstarter Project
--------------------------------------------------------------------------------
change log:
    v0.0.2  Wed 30 Sep 2020
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
import numpy as np
import pandas as pd
import multiprocessing as mp
from functools import partial
np.seterr(divide='ignore', invalid='ignore')
from shutil import copyfile, copy2
import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
import sys


######################################################################
######### DEFINE PRIMATIVES
######################################################################
pathMain                               = "/gpfs/project/kickstarter/"
pathIn                                 = pathMain + "projects/raw/"
pathOut                                = pathMain + "projects/cleaned/"
pathErrors                             = pathMain + "projects/errors/"

######################################################################
######### DEFINE FUNCTIONS
######################################################################


# OPEN MASTER FILE
def openListings():
    # projects that have been canceled, or contain issues have a URL marked as "00"
    return pd.read_csv(pathMain + "agg/masterListing.txt", delimiter = "|", header=None)


def findAddOns(soup):
    found = 0
    matches = ["add-ons", "add-on", "add ons", "add ons", "stretch goals", "stretch goal", "stretch-goal", "optional buys", "optional buy"]
    try:
        if any(x in soup.text for x in matches): found = 1
    except Exception:
        pass
    return found



# PARSE FILE (PID-12-HR SPECIFIC), AND RETURN LIST OF KEY DETAILS
def parseFile(fname, dd, pid):
    endTime         =       datetime.datetime(dd[0], dd[1], dd[2], dd[3])
    # for all files gathered, process gathered times, key details, parse file, and append to X
    times           =   list(map(int, re.split("_",fname)[2:6]))
    
    curTime         =   datetime.datetime(times[0], times[1], times[2], times[3])
    
    vec1            =   [int(pid),float((endTime-curTime).total_seconds()/datetime.timedelta(days=1).total_seconds())] \
                        + dd[0:5] \
                        + list(map(int, re.split("_",fname)[2:7]))
    # pass vec1, which contains curTime, endTime, and pid information to the main processing script
    g = open(fname)
    soup = BeautifulSoup(g,'html.parser')           # open the file and make some soup
    g.close()
    
    try:
        currency    = soup.findAll("h2", { "class" : "pledge__amount" })[-1]
        usd2        = locale.atof(re.findall('\d*\.?\,?\d+',str(currency.find("span", { "class" : None }).text))[0])
        local2      = locale.atof(re.findall('\d*\.?\,?\d+',str(currency.find("span", { "class" : "money" }).text))[0])
        adjustment  = usd2/local2
        #adjustment  = min(adjustment, adjust2)
    except Exception:
        print("cannot find currency, default to USD")
        adjustment = 1 # just keep currency the same, and replace later if we see a different currency
    
    htmlType = 0
    try:
        # This data block obtains the basic summary results of the campaign;
        # Two versions of the html occurred during the data collection.
        dataP               =       soup.find("div", { "class" : "num nowrap" }) # this contains aggregate backer information
        if dataP == None:
            dataP           =       soup.find("div", { "class" : "NS_campaigns__spotlight_stats" })
            
            goal            =       locale.atof(re.findall('\d*\.?\,?\d+',str(soup.find("div", { "class" : "type-12 medium navy-500" }).find('span').contents[0]))[0])
            
            pledged         =       locale.atof(re.findall('\d*\.?\,?\d+',str(dataP.find('span').contents[0]))[0])
            
            # goal and pledged may need adjustment, so review to column
            totalBackers    =       float(''.join(filter(str.isdigit, str(dataP.find('b').contents[0])))) #float(filter(str.isdigit, str(dataP.find('b').contents[0])))
            
            pp              =       pledged/goal
            #
            htmlType = 2
        else:
            try:
                pp              =       float(dataP['data-percent-raised'])      # extract timestamp for completion, such as 2017-04-06T10:04:06-04:00'
                
                goal            =       float(dataP['data-goal'])                # goal of the project, needs adjustment
                
                pledged         =       float(dataP['data-pledged'])             # current pledge amount, needs adjustment
                
                totalBackers    =       int(soup.find("div", {"id" : "backers_count"})["data-backers-count"]) # total number of backers
                #
                htmlType        =       1
            except KeyError:
                dataP           =       soup.find("div", { "class" : "flex flex-column-lg mb4 mb5-sm" })
                #
                try:
                    totalBackers=       locale.atof(dataP.find("div", { "class" : "block type-16 type-24-md medium soft-black" }).find("span").contents[0])
                except AttributeError:
                    totalBackers=       locale.atof(dataP.find("div", { "class" : "block type-16 type-28-md bold dark-grey-500" }).find("span").contents[0])
                
                goal            =       locale.atof(re.findall('\d*\.?\,?\d+', dataP.find("span", { "class" : "money" }).contents[0])[0])
                #
                pledged         =       locale.atof(re.findall('\d*\.?\,?\d+', dataP.find("span", { "class" : "ksr-green-700" }).find("span").contents[0])[0])
                #
                pp              =       pledged/goal
                #
                htmlType        =       3
        # still need to adjust goal, pledged, and prices according to adjustment critiera
        
        # Extract the number of days the project is tracked.
        # Two different versions occur in the data.
        tSoups = soup.findAll("span")
        if tSoups != None:
            success = False
            for t in tSoups:
                if success == False:
                    try:
                        start=int(t['data-duration'])
                        success = True
                    except Exception:
                        pass
        try:
            start
        except NameError: 
            try:
                start =  float(''.join(filter(str.isdigit, str(re.split("\(|\)", soup.find("p", { "class" : "f5" }).text)[-2])))) #float(filter(str.isdigit, str(re.split("\(|\)", soup.find("p", { "class" : "f5" }).text)[-2])))
            except Exception:
                start = -1 # start of -1 means we don't know the length of the project and they should be dropped
        
        ###########
        # FIND OUT IF IT IS A PROJECT THAT WE LOVE
        ###########
        love = int("Project We Love" in str(soup))
        
        ###########
        # NOW EXTRACT BUCKET-LEVEL INFORMATION
        ###########
        rewardId    = [int(d['data-reward-id']) for d in soup.findAll("li", {"data-reward-id" : re.compile(r'\d+')})]
        rewardId    = [r for r in rewardId if r != 0] # remove donation bucket, which is marked as 0, if active
        
        # backers per bucket
        backers     = [int(''.join(filter(str.isdigit, str(b.contents[0])))) for b in soup.findAll("span", { "class" : "pledge__backer-count" })]
        
        # prices per bucket
        #prices      = [float(filter(str.isdigit, str(p.contents))) for p in soup.findAll("span", { "class" : "money" })[-len(backers):]]
        #prices      = [float(re.findall('\d*\.?\d+', str(p.contents))[0]) for p in soup.findAll("span", { "class" : "money" })[-len(backers):]]
        prices      = [locale.atof(re.findall('\d*\.?\,?\d+', str(p.contents))[0]) for p in soup.findAll("span", { "class" : "money" })[-len(backers):]]
        if prices  == []:
            prices      = [float(''.join(filter(str.isdigit, str(p.contents)))) for p in soup.findAll("span", { "class" : "money" })[-len(backers):]]
        
        # revenue over buckets
        rev         = np.sum(np.array(backers)*np.array(prices))
        
        #capacity per bucket
        capList = soup.findAll("div", { "class" : "pledge__backer-stats" })
        it = 0
        if capList != []:
            caps                    =   [""]*len(capList)
            for c in capList:
                try:
                    caps[it]        =   str(c.find("span", { "class" : "pledge__limit" }).contents[0]).replace('\n', '')
                except AttributeError:
                    pass
                if caps[it] == '':
                    caps[it]        =   "NA" # adjust no constraints to N/A
                it +=1
        
        numBackers  =   np.array(backers).sum()
        
        avgBacker   =   rev/numBackers
        
        # Now extract the campaign category. Two versions occur.
        dataCat     =   soup.find("div", { "class" : "block-lg hide" })
        
        if dataCat != None:
            try:
                catTag      =   str(dataCat.find("a").contents[1]).strip()
            except AttributeError:
                pass
        # if the category tag is not found, try it another way
        try:
            catTag
        except NameError:
            try:
                catTag          =       str(soup.findAll("a", { "href" : re.compile("/discover/categories/") })[-1].text.strip())
            except AttributeError:
                catTag          =       "-1"
            
        
    except Exception: # If there is an error, set everything to -1
        pp                      = -1.0
        goal                    = -1.0
        pledged                 = -1.0
        start                   = -1
        numBackers              = -1
        prices                  = -1
        avgBacker               = -1
        rev                     = -1
        totalBackers            = -1
        catTag                  = "-1" # not dropped for this
        capList                 = []
        rewardId                = []
        htmlType                = -1
    
    # now, problematic files have -1, but there may be more
    # Prices and Reward IDs must line up.
    cont = True
    if prices != -1:
        if len(rewardId)    !=      len(prices): #make adjustment if any reward_ids are missing
            print("reward length failure")
            rewardId        =       [-1]*len(prices)
            cont = False
    
    # If we are missing either rewards or capacity vector, set result to []
    if (capList == []) or (rewardId == []):
        print("capacity constraint failure")
        cont = False
    
    try:
        if len(capList) != len(prices) != len(backers):
            print("len capacity != len prices != len backers failure")
            cont = False
    except TypeError:
        cont = False
    
    if pp == -1.0:
        cont = False
    
    add_on = findAddOns(soup)
    X = []
    if cont == True:
        for i in range(len(caps)):
            if pp != -1.0:
                X.append(vec1+[catTag,pp,goal,pledged,start] + [rev,totalBackers,numBackers,avgBacker] \
                    + [caps[i]] + [prices[i]] + [backers[i]]  + [rewardId[i]] + [adjustment] + [love] + [htmlType] + [add_on])
    if X == []:
        copy2(fname, pathErrors + re.split("/", fname)[-1])
        print(fname)
    return X


# WRITE RESULTS PER PID (this is j,t for t = 1,...T)
def writeResults(df,pid):
    g = pathOut + pid + ".csv"
    df.to_csv(g, sep = "|", index = False)



# CORRECT END TIME FOR PROJECTS
def determineEndTime(fname):
    # open the file
    g       = open(fname)
    soup    = BeautifulSoup(g,'html.parser')           # open the file and make some soup
    g.close()
    
    try:
        dataT           =           soup.find("p", { "class" : "mb3 mb0-lg type-12" })
        ts              =           dataT.time['datetime']      # extract timestamp for completion, such as 2017-04-06T10:04:06-04:00'
        tsList          =           re.split("-|T|:", ts)
    
    except Exception:   # if the project gets cancelled or something happens, set the date to be 1/1/2000
        
        try:
            dataT       =           soup.find("span", { "id" : "project_duration_data" })
            ts          =           dataT["data-end_time"]     # extract timestamp for completion, such as 2017-04-06T10:04:06-04:00'
            tsList      =           re.split("-|T|:", ts)
            #ex: <span data-duration="43" data-end_time="2018-02-27T20:00:00-05:00" data-hours-remaining="532" id="project_duration_data"></span>
        
        except Exception:
            #print "error with time stamp in file " + fname
            tsList      =           ["2000","01","01","00","00","00","00","00","00"]
    
    return list(map(int, tsList[0:5]))


# PROCESS ALL FILES FOR A GIVEN PID
def process(pid,flist):
    
    #files           =       [line for line in flist if "_"+y+"_" in line]   # these are all T files for pid
    checker         =       False                                               # have we found correct end date?
    
    # adjust end times if at some point the project went to draft
    if len(flist) > 1: # skip over pids with just a single observation
        for f in flist:
            if checker == False:
                dd              =       determineEndTime(f)
                if dd[0] > 2000:
                    checker     =       True
    
    if checker == False:
        print("this pid has an end date failure")
    if (flist != []) & (checker == True): # only process files where the end time has been specified correctly
        
        X 				= 		[parseFile(f,dd,pid) for f in flist]
        df              =       pd.DataFrame([item for sublist in X for item in sublist])
        writeResults(df,pid)
    else: pass


# RUN THE MAIN PROGRAM
if __name__ == '__main__':
    try: 
        ind = int(sys.argv[1]) - 1
        Y                      = openListings().values                                  # open master listings as pd
        pid 				   = str(Y[ind][0])
        print("working on PID: " + pid)
        flist                  =   glob.glob(pathIn + pid + "/*.html")                  # glob all result files
        print("number of files in flist: " + str(len(flist)))
        process(pid,flist)
        print("\nprogram complete for PID: " + pid)
    except IndexError:
        pass
        print("this is not a value PID index")

