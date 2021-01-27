"""
    Combines All Cleaned PID CSVs To Single Parquet
    Kickstarter Project
--------------------------------------------------------------------------------
change log:
    v0.0.1  Thurs 24 Sep 2020
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

import numpy as np
import pandas as pd
import scipy
import sys
import csv
import time
import argparse
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
import subprocess
import statsmodels.api as sm
lowess = sm.nonparametric.lowess
import statsmodels.stats.api as sms
import statsmodels.formula.api as smf
import glob
import os
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from lifelines import KaplanMeierFitter
from functools import reduce
from sklearn.linear_model import LogisticRegression
from scipy import sparse
from sklearn import preprocessing
from sklearn.model_selection import GroupShuffleSplit
from sklearn.preprocessing import OneHotEncoder
import scipy as sp



pathMain                               = "/mnt/data0/kickstarter/"
#pathMain                               = "/gpfs/project/kickstarter/"
pathIn                                 = pathMain + "agg/"
pathOut                                = pathMain + "output/"
pathPlot                               = pathOut  + "plots/" 
pathStats                              = pathOut  + "sumStats/" 
pathRepdata                            = pathOut  + "repData/"

shippingFile = pathIn + "shippingQuotes.csv"
catFile      = pathIn + "categories.csv"


######################################################################
######### the keepAll input defines if entire data is used for analysis or "co" is dropped at the bottom/top
######################################################################
keepAll 			=	False 	# False or True, which creates file names that contain the word All
co 					= 	.005 	# percent trimmed off top and bottom
logFile 			= 	True 	# True or False

######################################################################
######### Define the shipping adjustment being made
######################################################################
shipAdjust 			= 	"US" 	# min_cost or max_cost or US


######################################################################
######### DEFINE OUTPUT AND LOGGING FILES
######################################################################

sumFile     	= pathStats + 'summary_numbers_for_paper'
if keepAll == True:
    sumFile 	= sumFile + "All"

# turn on logging or not:
if logFile:
    logger      = open(sumFile + "_" + shipAdjust + ".log","w")


######################################################################
######### DEFINE MAIN DATA FUNCTIONS
######################################################################

# Open the parsed kickstarter data after files have been appended
def openData(file):
    if file == "master":
        # columns in master file
        # df.rename(columns={ 0:'pid',
        #                 1:'t',
        #                 2:'edate_year',
        #                 3:'edate_month',
        #                 4:'edate_day',
        #                 5:'edate_hour',
        #                 6:'edate_min',
        #                 7:'sdate_year',
        #                 8:'sdate_month',
        #                 9:'sdate_day',
        #                 10:'sdate_hour',
        #                 11:'sdate_min',
        #                 12:'category',
        #                 13:'cumfracG',
        #                 14:'goal',
        #                 15:'cumTotalRev',
        #                 16:'length',
        #                 17:'cumBackerRev',
        #                 18:'cumTotalN',
        #                 19:'cumBackerN',
        #                 20:'avgBackerRev',
        #                 21:'bucketCap',
        #                 22:'bucketPrice',
        #                 23:'bucketBackerN',
        #                 24:'rewardId',
        #                 25:'currencyConv',
        #                 26:'love',
        #                 27:'htmlType',
        #                 28:'add_on',
        #                 }, inplace=True)
        name = 'masterListings.parquet.gzip'
        return pd.read_parquet(pathIn+name, engine="pyarrow")


# CONVERT DATA TO DF
def convertData(df,shipAdjust):
    df["add_on"]   = df.groupby("pid").add_on.transform("max")
    df             = df.loc[df.add_on == 0].reset_index(drop=True)
    cols                            = ["goal", "cumTotalRev", "cumBackerRev", "avgBackerRev", "bucketPrice"]
    for cc in cols:
        df[cc] = df[cc]*df.currency
    
    # now make some modifications to the df, convert to float
    df.loc[df.bucketCap.isnull(), 'bucketCap']                          = ""
    df.loc[df.bucketCap  == "Reward no longer available", 'bucketCap']  = "0 0"
    # now process bucket information
    bucketTemp1                                         =   df.bucketCap.str.replace('Limited |\)|\(| left of', '', case=False)
    bucketTemp2                                         =   bucketTemp1.str.split(' ', expand=True)
    df                                                  =   df.drop('bucketCap', 1)
    df["bucketCap"]                                     =   bucketTemp2[1]
    df["bucketRemain"]                                  =   bucketTemp2[0]
    #df.loc[:, df.columns != 'category'] = df.loc[:, df.columns != 'category'].apply(pd.to_numeric)
    df['category']                                      =   df['category'].astype('category')
    df['bucketPrice']                                   =   df.bucketPrice.astype("float64")
    df.loc[df.bucketRemain == "", "bucketRemain"]       =   np.nan
    df["bucketRemain"]                                  =   df.bucketRemain.astype("float64")
    df.loc[df.bucketCap.isnull(), "bucketCap"]          =   np.nan
    df["bucketCap"]                                     =   df.bucketCap.astype("float64")
    
    # There are >= 24 projects in which the html displays bucket rev as less than amount towards the goal
    # We believe this is due to a lag in the goal beind updated. It corrects within 2 days.
    if logFile:
        logger.write("Number of projects with cumFracG inversions: {:10.0f}".format(df.loc[df.cumBackerRev > df.cumTotalRev].pid.nunique()) + "\n")
    #df.loc[df.cumBackerRev > df.cumTotalRev, "cumfracG"]    = df.cumBackerRev/df.goal
    #df.loc[df.cumBackerRev > df.cumTotalRev, "cumTotalRev"] = df.cumBackerRev
    df                                                  =   df.loc[df.cumBackerRev < df.cumTotalRev]
    # now process the shipping adjustment
    dfShip                                              = pd.read_csv(shippingFile)
    dfShip.rename(columns={ '0':'pid',
                        '1':'rewardId',
                        '2':'price',
                        '3':'usa_cost',
                        '4':'min_cost',
                        '5':'max_cost',
                        }, inplace=True)
    if shipAdjust == "US":
        df                                          = df.merge(dfShip[["usa_cost","pid","rewardId"]].drop_duplicates(), on=["pid", "rewardId"], how="left")
        df['shipCost']                              = df['usa_cost'].fillna(0)
        df['shipCost']                              = df.shipCost*df.currency
    elif shipAdjust == "min_cost":
        df                                          = df.merge(dfShip[["min_cost","pid","rewardId"]].drop_duplicates(), on=["pid", "rewardId"], how="left")
        df['shipCost']                              = df['min_cost'].fillna(0)
        df['shipCost']                              = df.shipCost*df.currency
    elif shipAdjust == "max_cost":
        df                                          = df.merge(dfShip[["max_cost","pid","rewardId"]].drop_duplicates(), on=["pid", "rewardId"], how="left")
        df['shipCost']                              = df['max_cost'].fillna(0)
        df['shipCost']                              = df.shipCost*df.currency
    
    # convert end date and search date to datetime
    df['edate']             = pd.to_datetime(dict(year=df.edate_year, month=df.edate_month, day=df.edate_day, hour=df.edate_hour, minute=df.edate_min))
    df['sdate']             = pd.to_datetime(dict(year=df.sdate_year, month=df.sdate_month, day=df.sdate_day, hour=df.sdate_hour, minute=df.sdate_min))
    df['datedif']           = df.edate-df.sdate
    
    # add in the shipping adjustment
    df['shippingTotal']     = df.bucketBackerN*df.shipCost
    df['cumShippingTotal']  = np.minimum( df.groupby(["pid", "datedif"])['shippingTotal'].transform("sum") , df.cumTotalRev-df.cumBackerRev )
    if logFile:
        logger.write("% of obs with shipping errors: {:10.9f}".format(100*df.loc[(df.cumShippingTotal < 0)].shape[0]/float(df.shape[0])) + "\n")
    
    # Remove obs with neg t, these are projects which were cancelled
    if logFile:
        logger.write("Number of projects before cancel drop: {:10.0f}".format(df.pid.nunique()) + "\n")
    #df                      = df.loc[df.t > -100]
    df                      = df.reset_index(drop = True)
    if logFile:
        logger.write("Number of projects after cancel drop: {:10.0f}".format(df.pid.nunique()) + "\n")
    df['cumDonRev']         = df.cumTotalRev-df.cumBackerRev - df.cumShippingTotal
    df['cumDonN']           = df.cumTotalN-df.cumBackerN
    # adjust backer and prices now
    df['cumBackerRev']      = df.cumBackerRev + df.cumShippingTotal
    df['bucketPrice']       = df.bucketPrice + df.shipCost
    
    # replace few rows of negative donations data; also adjusts for potential negative due to max shipping
    if logFile:
        logger.write("Number of obs with donation rev inversion: {:10.0f}".format(df.loc[df.cumDonRev     < 0, 'cumDonRev']     .shape[0]) + "\n")
        logger.write("Number of obs with donation rev inversion: {:10.0f}".format(df.loc[df.cumDonN       < 0, 'cumDonN']       .shape[0]) + "\n")
        logger.write("Number of obs with donation rev inversion: {:10.0f}".format(df.loc[df.cumBackerRev  < 0, "cumBackerRev"]  .shape[0]) + "\n")
        logger.write("Number of obs with donation rev inversion: {:10.0f}".format(df.loc[df.cumBackerN    < 0, "cumBackerN"]    .shape[0]) + "\n")        
    df.loc[df.cumDonRev     < 0, 'cumDonRev']       = 0
    df.loc[df.cumDonN       < 0, 'cumDonN']         = 0
    df.loc[df.cumBackerRev  < 0, "cumBackerRev"]    = 0
    df.loc[df.cumBackerN    < 0, "cumBackerN"]      = 0
    
    # sort projects by pid, and then descending in t (towards end date)
    df                                              =       df.sort_values(by = ["pid", "datedif"], ascending = [True,False])
    
    # single project is observed to have a length is 87. Not possible.
    df                                              =       df.loc[df.length <= 60]
    df                                              =       df.reset_index(drop=True)
    
    return df


# CREATE TIME INDEX FOR DF
def createTimeIndex(df,keepAll=False):
    # define df that is just dates and pids,  cheap length and goal for drop below. They are not needed otherwise
    dateData                            = df[["pid", "edate", "sdate", "datedif", "length", "goal"]].drop_duplicates()
    
        # determine %tile
    if logFile:
        logger.write( "Cuttoff threshold for project goal is equal to " + str(co) + "\n")
    lowerbound                          = dateData.drop_duplicates(["pid", "goal"])["goal"].quantile(co)
    upperbound                          = dateData.drop_duplicates(["pid", "goal"])["goal"].quantile(1-co)
    if keepAll == True:
        lowerbound = 0.0 ; upperbound = 1e10
    if logFile:
        logger.write( "Lower bound for cutoff: {:10.0f}".format(lowerbound) + "\n")
        logger.write( "Upper bound for cutoff: {:10.0f}".format(upperbound) + "\n")
        
        logger.write( "Number of projects before revenue drop: {:10.0f}".format(dateData.pid.nunique()) + "\n")
    
    dateData                            = dateData.loc[dateData.goal > lowerbound]
    dateData                            = dateData.loc[dateData.goal < upperbound]      
    dateData                            = dateData.reset_index(drop=True)
    if logFile:
        logger.write(  "Number of projects after revenue drop: {:10.0f}".format(dateData.pid.nunique()) + "\n")
    
    # define the difference between sdate and edate in hours; ositive means still more time left; negative means after completion
    dateData['difHours']                = dateData.datedif / np.timedelta64(1, 'h')
    
    # mark the first data pull, relative to length of project
    dateData['firstObs']                = dateData.groupby("pid")["difHours"].transform("max") - dateData.length*24.0 
    
    # mark the last time a project was observed
    dateData['lastObs']                 = dateData.groupby("pid")["difHours"].transform("min") 
    
    if logFile:
        logger.write( "Number of projects before time drop: {:10.0f}".format(dateData.pid.nunique()) + "\n")
    # drop if first obs is not within 24 hrs of project starting time
    dateData                            = dateData.loc[dateData.firstObs > -24]
    # drop if last obs is not within 24 hrs of project ending time
    dateData                            = dateData.loc[dateData.lastObs < 24]
    if logFile:
        logger.write( "Number of projects after time drop: {:10.0f}".format(dateData.pid.nunique()) + "\n")
    
    # reset time index
    dateData                            = dateData.reset_index(drop=True)
    
    # determine the number of hours between obs
    #dateData['timeHours'] 		        = dateData.firstObs.apply(np.abs)
    dateData.sort_values(["pid", "datedif"],ascending = [True,False], inplace=True) # changed timeHours to datedif; and a true to false
    dateData 					        = dateData.reset_index(drop=True)
    #dateData['hourShift'] 		        = dateData.groupby('pid').datedif.shift()
    #dateData['hourDif'] 		        = (dateData.hourShift-dateData.datedif)/np.timedelta64(1, 'D') * 24.0
    #dateData['hourDifRound'] 	        = (dateData.hourDif/12).round()
    dateData["idx"]                     = dateData.datedif/np.timedelta64(1, 'D') # this is the countdown as float, e.g. 35.4 days to go
    dateData["dcm"]                     = dateData.idx - np.fix(dateData.idx)     # this converts 35.4
    dateData["adjust"]                  = 0
    dateData.loc[(dateData.dcm >0) & (dateData.dcm >.5), "adjust"] = 1            # this will cause 35.8 -> 36.0
    dateData.loc[(dateData.dcm >0) & (dateData.dcm <.5), "adjust"] = .5           # this will cause 35.4 -> 35.5
    dateData.loc[(dateData.dcm <0), "adjust"]                      = 0            # this will cause any neg number to be <0, but will create dupes
    
    dateData["timeIndex"]               = np.fix(dateData["idx"]) + dateData.adjust
    # now adjust for ending of projects, set anything <0 -> 0
    lastObs                             = dateData.groupby("pid").timeIndex.min().reset_index()
    lastObs                             = lastObs.loc[lastObs.timeIndex > 0]
    
    dateData.loc[dateData.timeIndex < 0, "timeIndex"] = 0
    # if the last obs is not timeIndex < 0, select the last one and set equal to 0. That is, 3 hrs before deadline goes to 0
    dateData.loc[(dateData.pid.isin(lastObs.pid)) & (dateData.timeIndex.isin(lastObs.timeIndex)), "timeIndex"] = 0
    
    # not do dup drop
    dateData                            = dateData.drop_duplicates(["pid", "timeIndex"], keep = "last")
    
    grid1                               = dateData[["pid"]].drop_duplicates()
    grid1["ones"]                       = 1
    grid2                               = pd.DataFrame(np.arange(0,61,.5))
    grid2.rename(columns = {0 : "timeIndex"}, inplace = True)
    grid2["ones"]                       = 1
    grid                                = grid1.merge(grid2, on = "ones", how = "outer")
    
    grid                                = dateData[["pid", "edate", "sdate", "timeIndex"]].merge(grid[["pid", "timeIndex"]], how = "right", on = ["pid", "timeIndex"])
    
    maxT = grid.loc[grid.sdate.notnull()].groupby("pid").timeIndex.max().reset_index().rename(columns = {"timeIndex" : "maxT"})
    minT = grid.loc[grid.sdate.notnull()].groupby("pid").timeIndex.min().reset_index().rename(columns = {"timeIndex" : "minT"})
    
    grid                                = grid.merge(maxT, on = "pid", how = "inner")
    grid                                = grid.merge(minT, on = "pid", how = "inner")
    grid                                = grid.loc[grid.timeIndex >= grid.minT]
    grid                                = grid.loc[grid.timeIndex <= grid.maxT]
    grid                                = grid.merge(dateData[["pid", "length"]].drop_duplicates(), on = ["pid"], how = "inner")
    grid                                = grid.loc[grid.timeIndex <= grid.length]
    
    grid.sort_values(["pid", "timeIndex"],ascending = [True,False], inplace=True)
    grid                                = grid.reset_index(drop=True)
    grid["edate"]                       = grid.groupby("pid").edate.ffill()
    grid["sdate"]                       = grid.groupby("pid").sdate.ffill()
    # # create the time index
    # dateData['index'] 			        = np.nan
    # dateData.loc[dateData.hourDifRound.isnull()==True, 'index'] = dateData.length - ((dateData.timeHours/12).astype(np.double)).round()
    # dfTmp 						        = dateData[["pid", "hourDifRound", "index"]].fillna(0).groupby("pid").cumsum()
    # dateData["timeIndex"] 		        = (dfTmp["hourDifRound"] * (-0.5)) + dfTmp["index"]
    
    # # drop duplicate obs, first is kept
    # dateData 					        = dateData.drop_duplicates(["pid", "timeIndex"])
    # # 1 project is observed to have initial time index greater than length of project. Delete them.
    # # this looks like a timestamp issue for
    # # https://www.kickstarter.com/projects/445939737/the-worlds-first-and-only-color-changing-bedding/comments
    # #
    # dateData                            = dateData.loc[dateData.groupby("pid")["timeIndex"].transform("max") <= 60]
    # # remove scraping results such that project is tracked more than 1 week after completion
    # dateData                            = dateData.loc[dateData.timeIndex > -7]
    # dateData 					        = dateData.reset_index(drop=True)
    
    return grid[["pid", "edate", "sdate", "timeIndex"]], upperbound, lowerbound


# GATHER INFORMATION FOR TSFILL
def tsFill(df,dateData):
    df1 	= dateData.merge(df, how="left", on=['pid', 'edate', 'sdate'])
    subset 	= df1[["pid", "timeIndex", "cumfracG", "goal", "cumTotalRev", \
                "cumTotalN", "cumBackerN", "avgBackerRev", "cumDonRev", \
                "cumDonN", "cumBackerRev", "length", "rewardId", "love"]]
    subset 	= subset.drop_duplicates(["pid", "rewardId", "timeIndex"])
    # ss                              = subset.copy()
    # ss                              = ss[["pid", "rewardId", "timeIndex"]]
    # dfTimes                         = pd.DataFrame()
    # dfTimes["timeIndex"]            = np.arange(ss.timeIndex.min(), ss.timeIndex.max(), .5)
    # dfIndex                         = ss[["pid", "rewardId"]].copy().drop_duplicates()
    # dfTimes["ones"]                 = 1
    # dfIndex["ones"]                 = 1
    # dfIndex                         = dfTimes.merge(dfIndex, on='ones')
    # dfIndex.drop(columns            = "ones", inplace = True)
    # ss["ind"]                       = 1
    # dfIndex                         = dfIndex.merge(ss, on = ["pid", "rewardId", "timeIndex"], how = "left")
    # tmp                             = dfIndex.loc[dfIndex['ind'].notnull() == True].groupby(["pid", "rewardId"]).timeIndex.max().reset_index(drop=False)
    # tmp.rename(columns              = {"timeIndex" : "maxT"}, inplace=True)
    # dfIndex                         = dfIndex.merge(tmp, on = ["pid", "rewardId"], how = "left")
    # dfIndex                         = dfIndex.loc[dfIndex.timeIndex <= dfIndex.maxT].reset_index(drop=True)
    # dfIndex.drop(columns            = "maxT", inplace = True)
    
    # tmp                             = dfIndex.loc[dfIndex['ind'].notnull() == True].groupby(["pid", "rewardId"]).timeIndex.min().reset_index(drop=False)
    # tmp.rename(columns              = {"timeIndex" : "minT"}, inplace=True)
    # dfIndex                         = dfIndex.merge(tmp, on = ["pid", "rewardId"], how = "left")
    # dfIndex                         = dfIndex.loc[dfIndex.timeIndex >= dfIndex.minT].reset_index(drop=True)
    # dfIndex.drop(columns            = "minT", inplace = True)
    # dfIndex.drop(columns            = "ind", inplace = True)
    # subset                          = subset.merge(dfIndex, on = ["pid", "rewardId", "timeIndex"], how = "right")
    # subset                          = subset.sort_values(["pid", "rewardId", "timeIndex"], ascending = False).reset_index(drop=True)
    # subset                          = subset.groupby(["pid", "rewardId"], as_index=False).apply(lambda group: group.ffill())
    return subset




# COLLAPSE BUCKET LEVEL INFORMATION TO PID-DAY LEVEL INFORMATION
def createDailyDF(df1):
    dfAgg = df1[["pid",
            "timeIndex",
            "cumfracG", 
            "goal", 
            "cumTotalRev", 
            "cumTotalN", 
            "cumBackerN", 
            "avgBackerRev", 
            "cumDonRev", 
            "cumDonN", 
            "cumBackerRev", 
            "length",
            "love"]].drop_duplicates(["pid", "timeIndex"])
    dfAgg                   = dfAgg.reset_index(drop=True)
    dfAgg                   = dfAgg.sort_values(["pid", "timeIndex"])
    dfAgg                   = dfAgg.reset_index(drop=True)
    
    # now find daily amounts for donations, backers, total
    dfAgg['d_N']            = dfAgg.cumDonN - dfAgg.groupby(['pid'])['cumDonN'].shift(-1)
    dfAgg['b_N']            = dfAgg.cumBackerN - dfAgg.groupby(['pid'])['cumBackerN'].shift(-1)
    dfAgg['d_R']            = dfAgg.cumDonRev - dfAgg.groupby(['pid'])['cumDonRev'].shift(-1)
    dfAgg['b_R']            = dfAgg.cumBackerRev - dfAgg.groupby(['pid'])['cumBackerRev'].shift(-1)
    dfAgg['rev']            = dfAgg.cumTotalRev - dfAgg.groupby(['pid'])['cumTotalRev'].shift(-1)
    dfAgg['N']              = dfAgg.cumTotalN - dfAgg.groupby(['pid'])['cumTotalN'].shift(-1)
    
    # fix any potential negative values due to measurement error
    dfAgg.loc[dfAgg.d_N < 0, 'd_N'] = 0
    dfAgg.loc[dfAgg.b_N < 0, 'b_N'] = 0
    dfAgg.loc[dfAgg.d_R < 0, 'd_R'] = 0
    dfAgg.loc[dfAgg.b_R < 0, 'b_R'] = 0
    dfAgg.loc[dfAgg.rev < 0, 'rev'] = 0
    dfAgg.loc[dfAgg.N   < 0, 'N']  	= 0
    
    dfAgg                                   = dfAgg.loc[dfAgg.N.notnull()].reset_index(drop=True)
    dfAgg["success"] 		                = 0
    dfAgg.loc[dfAgg.cumfracG >= 1, 'success'] = 1
    dfAgg["successEver"] 	                = dfAgg.groupby("pid")["success"].transform("max")
    
    temp 					                = dfAgg.loc[dfAgg.success == 1].groupby("pid")["timeIndex"].max()
    temp 					                = temp.reset_index()
    temp 					                = temp.rename(columns={  "timeIndex":'hitTime'})
    dfAgg 					                = dfAgg.merge(temp, how="left", on="pid")
    dfAgg["relativeHit"] 	                = dfAgg.hitTime - dfAgg.timeIndex
    
    # totalRev is only defined for positive timeIndex
    dfAgg["totalRev"]                       = dfAgg.cumTotalRev
    dfAgg.loc[dfAgg.timeIndex >= 0, "totalRev"] = dfAgg.groupby("pid")["cumTotalRev"].transform(max)
    # replace period rev for instances in which totals disagree due to lagged recordings of sales
    dfAgg["rev"]                            = dfAgg.b_R + dfAgg.d_R
    dfAgg["fracB_totalR"]                   = dfAgg.b_R/dfAgg.totalRev
    dfAgg["fracD_totalR"]                   = dfAgg.d_R/dfAgg.totalRev
    dfAgg["fracB_periodR"]                  = dfAgg.b_R/dfAgg.rev
    dfAgg["fracD_periodR"]                  = dfAgg.d_R/dfAgg.rev
    # replace nans with 0s
    dfAgg.loc[dfAgg.fracB_periodR.isnull(), "fracB_periodR"] = 0
    dfAgg.loc[dfAgg.fracD_periodR.isnull(), "fracD_periodR"] = 0
    dfAgg.loc[dfAgg.fracB_totalR.isnull(), "fracB_totalR"]   = 0
    dfAgg.loc[dfAgg.fracD_totalR.isnull(), "fracD_totalR"]   = 0
    return dfAgg


# PROCESS ALL DATA CLEANING SCRIPTS
def cleanData(keepAll=False,shipAdjust="US"):
    print("opening the master data file")
    df                  = openData("master")
    
    print("converting the raw data to df format")
    df                  = convertData(df,shipAdjust)
    
    print("creating time index and trimming data based on thresholds")
    dateData, uB, lB    = createTimeIndex(df,keepAll)
    
    print("filling in any gaps with the data using tsfill")
    df1                 = tsFill(df,dateData)
    
    print("aggregating data to the daily level")
    df2                 = createDailyDF(df1)
    df2                 = df2.replace([np.inf, -np.inf], np.nan)
    
    # save files
    keepName = ""
    if keepAll == True: keepName = "All"
    df.to_parquet(pathRepdata + "kickstarter_DF"+shipAdjust+keepName+".parquet.gzip", engine='fastparquet', compression="gzip")
    df1.to_parquet(pathRepdata + "kickstarter_DF1"+shipAdjust+keepName+".parquet.gzip", engine='fastparquet', compression="gzip")
    df2.columns = df2.columns.values.astype("str")
    df2.to_parquet(pathRepdata + "kickstarter_DF2"+shipAdjust+keepName+".parquet.gzip", engine='fastparquet', compression="gzip")
    
    return df, df1, df2, uB, lB


######################################################################
######### DEFINE MAIN PLOT FUNCTIONS
######################################################################

# fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
# csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
# palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]

# sns.set(style="white",color_codes=False)
# L                       = plt.legend()
# plt.setp(L.texts, family='Liberation Serif', fontsize = 14)

# plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
# plt.xticks(fontname = "Liberation Serif", fontsize = 14) 

# NUMBER OF BACKERS FOR EARLY, MIDDLE, LATE PROJECTS, 30 DAYS
def backerTimePlot(df2,keepAll,shipAdjust):
    fname 				= "meanCountsOverTime"
    if keepAll == True: fname = fname + "All"
    df3 				= df2.copy()
    df3 				= df3[(df3.length==30) & (df3.timeIndex>0)]
    df3["timeIndex"] 	= 30-df3.timeIndex
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    z1                     = df3.loc[(df3.length==30) & (df3.timeIndex>=0)]
    plt.plot(z1.groupby("timeIndex")["N"].mean(), label="Mean Total", color=palette[3], linewidth=4, linestyle="--")    
    plt.xlabel("Time Index (0 is first period, 30 is deadline)" ,**csfont)
    plt.ylabel("Mean Contribution Count" ,**csfont)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    leg = plt.legend(loc=1)
    for ll in range(len(leg.get_lines())):
        leg.get_lines()[ll].set_linewidth(3)
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.savefig(pathPlot + fname + shipAdjust+ ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()


# HISTOGRAM OF GOALS FOR ALL PROJECTS
def histGoals(df2,keepAll,shipAdjust):
    fname 				= "histGoals"
    if keepAll == True: fname = fname + "All"
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    plt.hist(df2[df2.goal < 80000].drop_duplicates("pid")["goal"], bins=50,color=palette[3])
    plt.xlabel("Goals",**csfont)
    plt.ylabel("Frequency",**csfont)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.savefig(pathPlot + fname + shipAdjust+".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()


# HISTOGRAM OF GOALS FOR ALL PROJECTS
def histLove(df2,keepAll,shipAdjust):
    fname               = "histLove"
    if keepAll == True: fname = fname + "All"
    df3                     =          df2.copy()
    df3                     =          df3.loc[df3.love == 1]
    df3                     =          df3.loc[df3.timeIndex >= 0]
    idx                     =          df3.groupby(['pid'])['timeIndex'].transform(max) == df3['timeIndex']
    df3                     =          df3[idx]
    df3["frac"]             =          df3.timeIndex/df3.length
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    plt.hist(100*df3["frac"], bins=10,color=palette[3])
    plt.xlabel("Percent of Time Remaining (u)",**csfont)
    plt.ylabel("Frequency",**csfont)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.savefig(pathPlot + fname + shipAdjust+".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()


# HISTOGRAM OF TOTAL REVENUE / GOAL AT ENDING TIME
def completionRtoG(df2,keepAll,shipAdjust):
    fname 				= "completionRtoG"
    if keepAll == True: fname = fname + "All"
    colors 				= ["windows blue", "greyish", "black"]
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    plt.hist(df2.groupby('pid')["cumfracG"].max()[df2.groupby('pid')["cumfracG"].max() < 2], bins=50,color=palette[3])
    plt.xlabel("Total Revenue / Goal",**csfont)
    plt.ylabel("Frequency",**csfont)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.savefig(pathPlot + fname + shipAdjust+".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()


# HISTOGRAM OF COMPLETED TIME FOR 30 DAY PROJECTS
def completionTime(df2,keepAll,shipAdjust):
    fname 				= "completionTime"
    if keepAll == True: fname = fname + "All"
    df3 				= df2.copy()
    df3 				= df3[(df3.length==30) & (df3.timeIndex>=0) & (df3.timeIndex <= 30)].reset_index()
    df3["timeIndex"] 	= 30-df3.timeIndex
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    plt.hist(df3[(df3.cumfracG >=1)].groupby("pid")["timeIndex"].min(), bins=30,color=palette[3])
    plt.xlabel("Success Time (0 is first period, 30 is deadline)",**csfont)
    plt.ylabel("Frequency",**csfont)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.savefig(pathPlot + fname + shipAdjust+ ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()


# BUYER VS DONATION REVENUE FOR 30 DAY PROJECTS, E,M,L
def buyerDonorPlot(df2,keepAll,shipAdjust):
    fname 				= "Buyer_Donor_Contributions_Over_Time"
    if keepAll == True: fname = fname + "All"
    df3 				= df2.copy()
    df3 				= df3[(df3.length==30) & (df3.timeIndex>=0)]
    df3["timeIndex"] = 30-df3.timeIndex
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    
    plt.xlabel("Time Index (0 is first period, 30 is deadline)",**csfont)
    plt.ylabel("Donor Contribution (% of Revenue)",**csfont)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    
    sub1 				= df3[(df3.length==30) & (df3.hitTime >= 27) & (df3.timeIndex >= 0)][["timeIndex","fracD_totalR" ]].sort_values("timeIndex").reset_index(drop=True)
    sub2 				= df3[(df3.length==30) & (df3.hitTime > 3) & (df3.hitTime < 27) & (df3.timeIndex >= 0)][["timeIndex","fracD_totalR" ]].sort_values("timeIndex").reset_index(drop=True)
    sub3 				= df3[(df3.length==30) & (df3.hitTime <= 3) & (df3.timeIndex >= 0)][["timeIndex","fracD_totalR" ]].sort_values("timeIndex").reset_index(drop=True)
    
    sub1                = sub1.dropna().reset_index(drop=True)
    sub2                = sub2.dropna().reset_index(drop=True)
    sub3                = sub3.dropna().reset_index(drop=True)
    
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub1.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub1["fracD_totalR"].values[:, np.newaxis])
    y1 = 100*model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub2.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub2["fracD_totalR"].values[:, np.newaxis])
    y2 = 100*model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub3.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub3["fracD_totalR"].values[:, np.newaxis])
    y3 = 100*model.predict(x_poly)
    
    plt.plot(sub1.timeIndex, y1, label="Early Finishers",linewidth=4, linestyle="-.",color=palette[1])
    plt.plot(sub2.timeIndex, y2, label="Middle Finishers",linewidth=4, linestyle="-",color=palette[2])
    plt.plot(sub3.timeIndex, y3, label="Late Finishers",linewidth=4, linestyle=":",color=palette[3])
        
    leg = plt.legend()
    for ll in range(len(leg.get_lines())):
        leg.get_lines()[ll].set_linewidth(3)
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.savefig(pathPlot + fname + shipAdjust+"1.pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.clf()
    plt.close()
    
    # Now do second plot
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    plt.xlabel("Time Index (0 is first period, 30 is deadline)",**csfont)
    plt.ylabel("Buyer Contribution (% of Revenue)",**csfont)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    
    sub1 				= df3[(df3.length==30) & (df3.hitTime >= 27) & (df3.timeIndex >= 0)][["timeIndex","fracB_totalR" ]].sort_values("timeIndex").reset_index(drop=True)
    sub2 				= df3[(df3.length==30) & (df3.hitTime > 3) & (df3.hitTime < 27) & (df3.timeIndex >= 0)][["timeIndex","fracB_totalR" ]].sort_values("timeIndex").reset_index(drop=True)
    sub3 				= df3[(df3.length==30) & (df3.hitTime <= 3) & (df3.timeIndex >= 0)][["timeIndex","fracB_totalR" ]].sort_values("timeIndex").reset_index(drop=True)
    
    sub1                = sub1.dropna().reset_index(drop=True)
    sub2                = sub2.dropna().reset_index(drop=True)
    sub3                = sub3.dropna().reset_index(drop=True)
    
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub1.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub1["fracB_totalR"].values[:, np.newaxis])
    y1 = 100*model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub2.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub2["fracB_totalR"].values[:, np.newaxis])
    y2 = 100*model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub3.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub3["fracB_totalR"].values[:, np.newaxis])
    y3 = 100*model.predict(x_poly)
    
    plt.plot(sub1.timeIndex, y1, label="Early Finishers",linewidth=4, linestyle="-.",color=palette[1])
    plt.plot(sub2.timeIndex, y2, label="Middle Finishers",linewidth=4, linestyle="-",color=palette[2])
    plt.plot(sub3.timeIndex, y3, label="Late Finishers",linewidth=4, linestyle=":",color=palette[3])
    
    leg = plt.legend()
    for ll in range(len(leg.get_lines())):
        leg.get_lines()[ll].set_linewidth(3)
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.savefig(pathPlot + fname + shipAdjust+"2.pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()
    
    if logFile:
        logger.write(  "Number of projects early: " + str(df3[(df3.length==30) & (df3.hitTime >= 27) & (df3.timeIndex >= 0)].pid.nunique()) + "\n")
        logger.write(  "Number of projects middle: " + str(df3[(df3.length==30) & (df3.hitTime > 3) & (df3.hitTime < 27) & (df3.timeIndex >= 0)].pid.nunique()) + "\n")
        logger.write(  "Number of projects late: " + str(df3[(df3.length==30) & (df3.hitTime <= 3) & (df3.timeIndex >= 0)].pid.nunique()) + "\n")
    # end of func


# PLOT OF HITTING TIME
def plotHitTimeCompare(df2,keepAll,shipAdjust,timeCut=3):
    fname 				= "Relative_Hit_Time_3day"
    if logFile:
        logger.write( "information for " + fname +  ":\n")
    if keepAll == True: fname = fname + "All"
    df3 				= df2[(df2.length==30) & (df2.hitTime >= timeCut) & (df2.hitTime <= 30-timeCut) &(df2.relativeHit >= -3) &(df2.relativeHit <= 3)]
    df3                 = df3.reset_index(drop=True)
    
    if logFile:
        logger.write( "Number of projects in hit time graph: " + str(df3.pid.nunique())  + "\n")
    df3["rev"]          = df3.rev / df3.goal
    df3["d_R"]          = df3.d_R / df3.goal
    df3["b_R"]          = df3.b_R / df3.goal
    df4 				= df3.groupby("relativeHit")["rev", "d_R", "b_R"].mean()
    
    if logFile:
        logger.write( "Donation rev prior: " + str(df3[df3.relativeHit < 0].d_R.mean()) + "\n")
        logger.write( "Donation rev post: " + str(df3[df3.relativeHit > 0].d_R.mean()) + "\n")
    
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    
    df4 				= df4.reset_index(drop=False)    
    plt.plot(df4.relativeHit,df4.rev ,label="Mean Total Revenue",linewidth=4, linestyle="-",color=palette[1])
    plt.plot(df4.relativeHit,df4.d_R ,label="Mean Donor Revenue",linewidth=4, linestyle="-.",color=palette[2])
    plt.plot(df4.relativeHit,df4.b_R ,label="Mean Buyer Revenue",linewidth=4, linestyle=":",color=palette[3])
    
    plt.axvline(x=0, color="black", linewidth=3, linestyle="-")
    #plt.grid(alpha = 0.25)
    leg = plt.legend()
    for ll in range(len(leg.get_lines())):
        leg.get_lines()[ll].set_linewidth(3)
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.xlabel("Days relative to success time",**csfont)
    plt.ylabel("Contribution Revenue / Goal",**csfont)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    if timeCut == 3 : timeCut = ""
    plt.savefig(pathPlot + fname + shipAdjust+str(timeCut) + ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()
    # NOW RUN REG
    dfRegs = df3.copy()#df2.loc[(df2.hitTime <= 30-timeCut) &(df2.relativeHit >= -3) &(df2.relativeHit <= 3)].copy().reset_index(drop=True)
    dfRegs = dfRegs.loc[dfRegs.relativeHit.notnull()]
    dfRegs["RH"] = dfRegs.relativeHit.astype("str")
    cols = [str(t) for t in sorted([float(r) for r in pd.get_dummies(dfRegs.RH).columns.values])]
    mtx = pd.get_dummies(dfRegs.RH)
    m1 = sm.OLS(dfRegs.d_R.values, pd.get_dummies(dfRegs.RH)[cols].values).fit(cov_type="cluster",cov_kwds={"groups":dfRegs.pid})
    r1=m1.summary(alpha=0.05)
    with open(pathStats + fname + shipAdjust+str(timeCut) + '_regsummary.txt', 'w') as fh:
        fh.write(r1.as_text())
    # donations before / donations after for successful pids
    st = ((dfRegs.loc[dfRegs.relativeHit > 0].groupby("pid").d_R.sum()) / (dfRegs.loc[dfRegs.relativeHit <=  0].groupby("pid").d_R.sum())).describe()
    if logFile:
        logger.write("\npercent sum donations before and after success \n")
        st.to_csv(sumFile + "_" + shipAdjust + ".log", mode='a', header=False)



def buyerDonorPanels(df2,type,keepAll):
    fname = "contr_panels_"
    if type == "buyer":
        col = "b_N"
        name = "Buyer"
        col2 = "b_R"
    elif type == "donor":
        col = "d_N"
        name = "Donor"
        col2 = "d_R"
    df3                 = df2.copy()
    df3                 = df3[(df3.length==30) & (df3.timeIndex>=0)]
    df3["timeIndex"] = 30-df3.timeIndex
    #df3.loc[df3[col].isnull() == True, col] = 0
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    #
    df3["totalProdRev"]              = df3.groupby("pid")[col2].transform("sum")
    df3["frac_period_rev"]           = df3[col2] / df3.totalProdRev
    df3.loc[df3[col] > 0, col] = 100
    df3.loc[df3[col] <= 0, col] = 0
    df3[col] = df3[col] ##*df3.frac_period_rev
    sub1                = df3[(df3.length==30) & (df3.hitTime >= 27) & (df3.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    sub2                = df3[(df3.length==30) & (df3.hitTime > 3) & (df3.hitTime < 27) & (df3.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    sub3                = df3[(df3.length==30) & (df3.hitTime <= 3) & (df3.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    sub4                = df3[(df3.length==30) & (df3.successEver == 0) & (df3.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    sub1                = sub1.dropna().reset_index(drop=True)
    sub2                = sub2.dropna().reset_index(drop=True)
    sub3                = sub3.dropna().reset_index(drop=True)
    sub4                = sub4.dropna().reset_index(drop=True)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub1.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub1[col].values[:, np.newaxis])
    y1 = model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub2.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub2[col].values[:, np.newaxis])
    y2 = model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub3.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub3[col].values[:, np.newaxis])
    y3 = model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub4.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub4[col].values[:, np.newaxis])
    y4 = model.predict(x_poly)
    plt.xlabel("Time Index (0 is first period, 30 is deadline)",**csfont)
    if type == "donor":
        plt.ylabel("Fraction of projects with donations",**csfont)
    else:
        plt.ylabel("Fraction of projects with buyers",**csfont)
    plt.plot(sub1.timeIndex, y1, label="Early Finishers",linewidth=4, linestyle="-.",color=palette[1])
    plt.plot(sub2.timeIndex, y2, label="Middle Finishers",linewidth=4, linestyle="-",color=palette[2])
    plt.plot(sub3.timeIndex, y3, label="Late Finishers",linewidth=4, linestyle=":",color=palette[3])
    plt.plot(sub4.timeIndex, y4, label="Not Successful",linewidth=4, linestyle="--",color=palette[0])
    leg = plt.legend()
    for ll in range(len(leg.get_lines())):
        leg.get_lines()[ll].set_linewidth(3)
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    #ax.grid(alpha = 0.25)
    plt.ylim((0,100))
    keepAd = ""
    if keepAll == True: keepAd = "All"
    plt.savefig(pathPlot + fname + col + "_"  + keepAd + "_" + shipAdjust + ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()


def buyerDonorWeightedPanels(df2,type,keepAll):
    fname = "weighted_contr_panels_"
    if type == "buyer":
        col = "b_N"
        name = "Buyer"
        col2 = "b_R"
    elif type == "donor":
        col = "d_N"
        name = "Donor"
        col2 = "d_R"
    df3                 = df2.copy()
    df3                 = df3[(df3.length==30) & (df3.timeIndex>=0)]
    df3["timeIndex"] = 30-df3.timeIndex
    #df3.loc[df3[col].isnull() == True, col] = 0
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    #
    df3["totalProdRev"]              = df3.groupby("pid")[col2].transform("sum")
    df3["frac_period_rev"]           = df3[col2] / df3.totalProdRev
    df3.loc[df3[col] > 0, col] = 100
    df3.loc[df3[col] <= 0, col] = 0
    df3[col] = df3[col]*df3.frac_period_rev
    sub1                = df3[(df3.length==30) & (df3.hitTime >= 27) & (df3.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    sub2                = df3[(df3.length==30) & (df3.hitTime > 3) & (df3.hitTime < 27) & (df3.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    sub3                = df3[(df3.length==30) & (df3.hitTime <= 3) & (df3.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    sub4                = df3[(df3.length==30) & (df3.successEver == 0) & (df3.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    sub1                = sub1.dropna().reset_index(drop=True)
    sub2                = sub2.dropna().reset_index(drop=True)
    sub3                = sub3.dropna().reset_index(drop=True)
    sub4                = sub4.dropna().reset_index(drop=True)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub1.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub1[col].values[:, np.newaxis])
    y1 = model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub2.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub2[col].values[:, np.newaxis])
    y2 = model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub3.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub3[col].values[:, np.newaxis])
    y3 = model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub4.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub4[col].values[:, np.newaxis])
    y4 = model.predict(x_poly)
    plt.xlabel("Time Index (0 is first period, 30 is deadline)",**csfont)
    if type == "donor":
        plt.ylabel("Fraction of projects with donations",**csfont)
    else:
        plt.ylabel("Fraction of projects with buyers",**csfont)
    plt.plot(sub1.timeIndex, y1, label="Early Finishers",linewidth=4, linestyle="-.",color=palette[1])
    plt.plot(sub2.timeIndex, y2, label="Middle Finishers",linewidth=4, linestyle="-",color=palette[2])
    plt.plot(sub3.timeIndex, y3, label="Late Finishers",linewidth=4, linestyle=":",color=palette[3])
    plt.plot(sub4.timeIndex, y4, label="Not Successful",linewidth=4, linestyle="--",color=palette[0])
    leg = plt.legend()
    for ll in range(len(leg.get_lines())):
        leg.get_lines()[ll].set_linewidth(3)
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.ylim((0,15))
    keepAd = ""
    if keepAll == True: keepAd = "All"
    plt.savefig(pathPlot + fname + col + "_"  + keepAd + "_" + shipAdjust + ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()


def completionTimePercents(df2,keepAll):
    fname = "completionTimePercents"
    df3 = df2.copy()
    df3 = df3.loc[df3.success == 1]
    df3 = df3.loc[df3.hitTime >= 0]
    df3 = df3.loc[df3.timeIndex >= 0]
    df3 = df3.loc[df3.relativeHit == 0]
    df3["frac"] = df3.hitTime/df3.length
    df3["r"]   = np.round(df3.frac*100,0)
    df3["r2"] = df3.r*df3.r
    df3["r3"] = df3.r2*df3.r
    mod = smf.quantreg('fracB_periodR ~ r + r2 + r3', df3)
    # Quantile regression for 5 quantiles
    quantiles = [.25, .50, .75]
    # get all result instances in a list
    res_all = [mod.fit(q=q) for q in quantiles]
    res_ols = smf.ols('fracB_periodR ~ r + r2 + r3', df3).fit()
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FCB07E", "#6B717E", "#3A6EA5", "#FF6700", "#070707"]
    sns.set(style="white",color_codes=False)
    #
    # create x for prediction
    x_p = np.linspace(df3.r.min(), df3.r.max(), 50)
    df_p = pd.DataFrame({'r': x_p})
    df_p["r2"] = df_p.r**2
    df_p["r3"] = df_p.r2*df_p.r
    i = 0
    LS=["-.", "--", ":", "-"]
    labels=["25th Percentile","50th Percentile", "75th Percentile","Mean"]
    for qm, res in zip(quantiles, res_all):
        # get prediction for the model and plot
        # here we use a dict which works the same way as the df in ols
        plt.plot(x_p, 100*res.predict(df_p), linestyle=LS[i], lw=3, color=palette[i], label = labels[i])
        i +=1
    
    y_ols_predicted = res_ols.predict(df_p)
    plt.plot(x_p, 100*y_ols_predicted, linestyle=LS[3], lw=3, color=palette[3], label=labels[i])
    plt.ylim((0, 105))
    plt.xlabel("% of Time Remaining at Success Time(u)",**csfont)
    plt.ylabel("% of Rev. from Buyers at Success Time",**csfont)
    plt.legend()
    leg = plt.legend()
    for ll in range(len(leg.get_lines())):
        leg.get_lines()[ll].set_linewidth(3)
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    keepAd = ""
    if keepAll == True: keepAd = "All"
    plt.savefig(pathPlot + fname + "_"  + keepAd + "_" + shipAdjust + ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()


def completionPeriodFinalRev(df2,keepAll):
    fname = "completionPeriodFinalRev"
    df3 = df2.copy()
    df3 = df3.loc[df3.success == 1]
    df3 = df3.loc[df3.hitTime >= 0]
    df3 = df3.loc[df3.timeIndex >= 0]
    df3 = df3.loc[df3.relativeHit == 0]
    df3["frac"] = df3.hitTime/df3.length
    df3["r"]   = np.round(df3.frac*100,0)
    df3 = df3[df3.groupby(['pid'])['timeIndex'].transform(min) == df3['timeIndex']].reset_index(drop=True)
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    #
    z = lowess(df3.cumfracG, df3.r, frac=1./3)  
    plt.plot(z[:,0], z[:,1], linestyle="-", lw=3, color=palette[3], label = "LOESS") 
    plt.xlabel("% of Time Remaining at Success Time (u)",**csfont)
    plt.ylabel("Revenue / Goal at Deadline",**csfont)
    plt.legend()
    leg = plt.legend()
    for ll in range(len(leg.get_lines())):
        leg.get_lines()[ll].set_linewidth(3)
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    keepAd = ""
    if keepAll == True: keepAd = "All"
    plt.savefig(pathPlot + fname + "_"  + keepAd + "_" + shipAdjust + ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()



def initialDoverL(df2,keepAll):
    fname = "initialDoverL"
    df3 = df2.copy()
    df3 = df3.loc[df3.timeIndex >= 0]
    df3 = df3[df3.groupby(['pid'])['timeIndex'].transform(max) == df3['timeIndex']].reset_index(drop=True)
    df3["dg"] = df3.d_R/df3.goal
    df3["l2"] = df3.length*df3.length
    df3["l3"] = df3.l2*df3.length
    mod = smf.quantreg('dg ~ length + l2 + l3', df3)
    # Quantile regression for 5 quantiles
    quantiles = [.5, .75, .9]
    # get all result instances in a list
    res_all = [mod.fit(q=q) for q in quantiles]
    res_ols = smf.ols('dg ~ length + l2 + l3', df3).fit()
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FCB07E", "#6B717E", "#3A6EA5", "#FF6700", "#070707"]
    sns.set(style="white",color_codes=False)
    #
    # create x for prediction
    x_p = np.linspace(df3.length.min(), df3.length.max(), 15)
    df_p = pd.DataFrame({'length': x_p})
    df_p["l2"] = df_p.length**2
    df_p["l3"] = df_p.l2*df_p.length
    labels=["50th", "75th", "90th"]
    labels = [l + " Quantile" for l in labels]
    y_ols_predicted = res_ols.predict(df_p)
    plt.plot(x_p, 100*y_ols_predicted, linestyle="-", lw=3, color=palette[3], label="Mean")
    plt.xlabel("Length of Project",**csfont)
    plt.ylabel("Inital Donation / Goal (%)",**csfont)
    i = 0
    lss = [":", "--", "-."]
    for qm, res in zip(quantiles, res_all):
        # get prediction for the model and plot
        # here we use a dict which works the same way as the df in ols
        plt.plot(x_p, 100*res.predict(df_p), linestyle=lss[i], lw=1, color=palette[4], label = labels[i])
        i +=1
    
    leg = plt.legend()
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    keepAd = ""
    if keepAll == True: keepAd = "All"
    plt.savefig(pathPlot + fname + "_"  + keepAd + "_" + shipAdjust + ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()


def ortho_poly_fit(x, degree = 1):
    n = degree + 1
    x = np.asarray(x).flatten()
    if(degree >= len(np.unique(x))):
            stop("'degree' must be less than number of unique points")
    xbar = np.mean(x)
    x = x - xbar
    X = np.fliplr(np.vander(x, n))
    q,r = np.linalg.qr(X)
    z = np.diag(np.diag(r))
    raw = np.dot(q, z)
    norm2 = np.sum(raw**2, axis=0)
    alpha = (np.sum((raw**2)*np.reshape(x,(-1,1)), axis=0)/norm2 + xbar)[:degree]
    Z = raw / np.sqrt(norm2)
    return Z, norm2, alpha

def ortho_poly_predict(x, alpha, norm2, degree = 1):
    x = np.asarray(x).flatten()
    n = degree + 1
    Z = np.empty((len(x), n))
    Z[:,0] = 1
    if degree > 0:
        Z[:, 1] = x - alpha[0]
    if degree > 1:
      for i in np.arange(1,degree):
          Z[:, i+1] = (x - alpha[i]) * Z[:, i] - (norm2[i] / norm2[i-1]) * Z[:, i-1]
    Z /= np.sqrt(norm2)
    return Z

def initialDoverG(df2,keepAll):
    fname = "initialDoverG"
    df3 = df2.copy()
    df3 = df3.loc[df3.timeIndex >= 0]
    df3 = df3.loc[df3.length == 30]
    df3 = df3.loc[df3.goal <= df3.goal.quantile(.9)]
    df3 = df3[df3.groupby(['pid'])['timeIndex'].transform(max) == df3['timeIndex']].reset_index(drop=True)
    OP, n, al = ortho_poly_fit(df3.goal, degree = 3)
    df3["dg"] = df3.d_R#/df3.goal
    df3["l1"]  = OP[:,1]
    df3["l2"] = OP[:,2]
    df3["l3"] = OP[:,3]
    mod = smf.quantreg('dg ~ l1 + l2 + l3', df3)
    # Quantile regression for 5 quantiles
    quantiles = [.5, .75, .9]
    # get all result instances in a list
    res_all = [mod.fit(q=q) for q in quantiles]
    res_ols = smf.ols('dg ~ l1 + l2 + l3', df3).fit()
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FCB07E", "#6B717E", "#3A6EA5", "#FF6700", "#070707"]
    sns.set(style="white",color_codes=False)
            #
            # create x for prediction
    x_p = np.linspace(df3.goal.min(), df3.goal.max(), 100)
    df_p = pd.DataFrame({'goal': x_p})
    phat = ortho_poly_predict(df_p.goal, al, n, degree = 3)
    df_p["l1"] = phat[:,1]
    df_p["l2"] = phat[:,2]
    df_p["l3"] = phat[:,3]
    labels=["50th", "75th", "90th"]
    labels = [l + " Quantile" for l in labels]
    y_ols_predicted = res_ols.predict(df_p)
    plt.plot(x_p, 100*y_ols_predicted, linestyle="-", lw=3, color=palette[3], label="Mean")
    plt.xlabel("Goal of Project",**csfont)
    plt.ylabel("Inital Donation",**csfont)
    i = 0
    lss = [":", "--", "-."]
    for qm, res in zip(quantiles, res_all):
        # get prediction for the model and plot
        # here we use a dict which works the same way as the df in ols
        plt.plot(x_p, 100*res.predict(df_p), linestyle=lss[i], lw=1, color=palette[4], label = labels[i])
        i +=1
    
    leg = plt.legend()
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    keepAd = ""
    if keepAll == True: keepAd = "All"
    
    plt.savefig(pathPlot + fname + "_"  + keepAd + "_" + shipAdjust + ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()




def ProjectsWeLove(df2,keepAll,shipAdjust):
    fname               = "Projects_We_Love_Over_Time"
    if keepAll == True: fname = fname + "All"
    df3                     =          df2.copy()
    df3                     =          df3.loc[df3.timeIndex >= 0]
    df3["frac"]             =          df3.timeIndex/df3.length
    df3["l_frac"]           =          df3.frac*df3.love
    df3["mark"]             =          df3.groupby("pid")["l_frac"].transform("max")
    df3["ind"]              =          0
    df3.loc[df3.mark > .9, "ind"]      = 2
    df3.loc[(df3.mark <= .9) & (df3.mark > 0), "ind"]      = 1
    df3 = df3.loc[df3.length==30]
    df3["timeIndex"] = 30-df3.timeIndex
    df3 = df3.loc[df3.successEver == 1]
    
    sub1 = df3.loc[df3.ind == 0].sort_values("timeIndex").reset_index(drop=True)
    sub2 = df3.loc[df3.ind == 1].sort_values("timeIndex").reset_index(drop=True)
    sub3 = df3.loc[df3.ind == 2].sort_values("timeIndex").reset_index(drop=True)
    col = "b_R"
    z1 = lowess(sub1[col], sub1.timeIndex, frac=1./3) 
    z2 = lowess(sub2[col], sub2.timeIndex, frac=1./3) 
    z3 = lowess(sub3[col], sub3.timeIndex, frac=1./3) 
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    #
    plt.plot(z1[:,0], z1[:,1], label="Not Loved",linewidth=4, linestyle="-.",color=palette[1])
    plt.plot(z2[:,0], z2[:,1], label="Project We Love",linewidth=4, linestyle="-",color=palette[2])
    plt.plot(z3[:,0], z3[:,1], label="Project We Love - Early",linewidth=4, linestyle=":",color=palette[3])
    leg = plt.legend()
    for ll in range(len(leg.get_lines())):
        leg.get_lines()[ll].set_linewidth(3)
    
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xlim((0,30))
    keepAd = ""
    if keepAll == True: keepAd = "All"
    
    plt.xlabel("Time Index (0 is first period, 30 is deadline)",**csfont)
    plt.ylabel("Mean Buyer Count, per project, per period",**csfont)
    plt.savefig(pathPlot + fname + col + "_"  + keepAd + "_" + shipAdjust + ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()


def f(x):
    x.loc[x.idx == 1, 'e'] = 1
    a = x.e.notnull()
    x.e = a.cumsum()-a.cumsum().where(~a).ffill().fillna(0).astype(int)
    return (x)


def hazardPlot(df2,keepAll,shipAdjust):
    fname               = "No_Donation_Hazard"
    df3 = df2.copy()
    df3 = df3.loc[df3.successEver == 1]
    df3 = df3.loc[df3.success == 0]
    df3 = df3.loc[df3.timeIndex >= 0]
    
    df3["idx"] = np.nan
    df3.loc[df3.b_R > 0, "idx"] = 0
    df3.loc[df3.b_R == 0, "idx"] = 1
    
    df3["mean"] = df3.groupby("pid")["idx"].transform("mean")
    df3 = df3.loc[df3["mean"] != 0]
    df3 = df3.loc[df3["mean"] != 1]
    df3 = df3.reset_index(drop=True)
    
    df3 = df3.sort_values(["pid", "timeIndex"], ascending =False).reset_index(drop=True)
    
    df4 = df3.groupby('pid').apply(f).reset_index(drop=True)
    df4["d"] = 1
    df4.loc[df4.d_N > 0, "d"] = 0
    df4 = df4.loc[df4.length == 30]
    
    kmf = KaplanMeierFitter()
    kmf.fit(df4.e, df4.d, label="No Donation")
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    #
    kmf.plot(lw = 4,color=palette[3])
    plt.xlabel("Time Since Last Purchase, in 12-hour periods",**csfont)
    plt.ylabel("Probability of No Donation",**csfont)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    keepAd = ""
    if keepAll == True: keepAd = "All"
    
    plt.savefig(pathPlot + fname + keepAd + "_" + shipAdjust + ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()



def failurePlot(X_ohe,model,enc2,keepAll,shipAdjust):
    fname               = "ProbFailure_overU"
    if keepAll == True: fname = fname + "All"
    
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    #
    
    percents 				=  np.arange(.01,1,.01)
    lp 						=  len(percents)
    X_oheT_p 				=  sparse.lil_matrix((lp,X_ohe.shape[1]))
    X_ohe2_p 				=  enc2.transform(29*np.ones(lp).reshape(-1, 1))
    XT_p 					=  (X_ohe2_p.toarray()*percents[:,np.newaxis])
    XT_p 					=  sp.sparse.hstack((XT_p, X_oheT_p ))
    
    plt.plot(model.predict_proba(XT_p)[:,0], label = "u = 29",linestyle="-.", lw=3, color=palette[1])
    
    X_oheT_p 				=  sparse.lil_matrix((lp,X_ohe.shape[1]))
    X_ohe2_p 				=  enc2.transform(15*np.ones(lp).reshape(-1, 1))
    XT_p 					=  (X_ohe2_p.toarray()*percents[:,np.newaxis])
    XT_p 					= sp.sparse.hstack((XT_p, X_oheT_p ))
    
    plt.plot(model.predict_proba(XT_p)[:,0], label = "u = 15",linestyle="-", lw=3, color=palette[2])
    
    X_oheT_p 				=  sparse.lil_matrix((lp,X_ohe.shape[1]))
    X_ohe2_p				=  enc2.transform(1*np.ones(lp).reshape(-1, 1))
    XT_p 					=  (X_ohe2_p.toarray()*percents[:,np.newaxis])
    XT_p 					=  sp.sparse.hstack((XT_p, X_oheT_p ))
    
    plt.plot(model.predict_proba(XT_p)[:,0], label = "u = 1",linestyle=":", lw=3, color=palette[3])
    
    leg 					= 	plt.legend()
    plt.xlabel("Percent of Goal Raised (R/G)",**csfont)
    plt.ylabel("Probability of Failure",**csfont)
    #plt.grid(alpha = 0.25)
    for ll in range(len(leg.get_lines())):
        leg.get_lines()[ll].set_linewidth(3)
    
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    keepAd = ""
    if keepAll == True: keepAd = "All"
    plt.savefig(pathPlot + fname + "_"  + keepAd + "_" + shipAdjust + ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()


def probSuccessPlot(XT,model,test,keepAll,shipAdjust):
    fname               = "ProbSuccess_overT"
    if keepAll == True: fname = fname + "All"
    df_t         = test.copy()
    df_t["pred"] = model.predict(XT)
    df_t["probF"] = model.predict_proba(XT)[:,0]
    df_t["probS"] = model.predict_proba(XT)[:,1]
    
    failures = df_t.loc[df_t.successEver == 0].reset_index(drop=True)
    failures["timeIndex"] = 30-failures.timeIndex.values
    
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    #
    plt.plot(1*failures.groupby("timeIndex")["probS"].mean(), label="Mean",linewidth=4, linestyle="-.",color=palette[0])
    plt.plot(1*failures.groupby("timeIndex")["probS"].quantile(.5), label = "Median", linewidth=4, linestyle="-", color=palette[2])
    plt.plot(1*failures.groupby("timeIndex")["probS"].quantile(.9), label="90th Percentile",linewidth=4, linestyle=":", color=palette[3])
    leg = plt.legend()
    plt.ylabel("Probability of Success",**csfont)
    plt.xlabel("Time Index (0 is first period, 30 is deadline)",**csfont)
    #plt.grid(alpha = 0.25)
    for ll in range(len(leg.get_lines())):
        leg.get_lines()[ll].set_linewidth(3)
    
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    keepAd = ""
    if keepAll == True: keepAd = "All"
    plt.savefig(pathPlot + fname + "_"  + keepAd + "_" + shipAdjust + ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()


def myround(x, base=3):
    return base * round(x/base)



def histofXi(df2,test,model,XT,keepAll,shipAdjust):
    fname               = "histofXi"
    if keepAll == True: fname = fname + "All"
    df_t = test.copy()
    df_t["pred"] = model.predict(XT)
    df_t["probF"] = model.predict_proba(XT)[:,0]
    df_t["probS"] = model.predict_proba(XT)[:,1]
    df_t = df_t.loc[df_t.successEver == 0]
    pids = df_t[["pid"]].drop_duplicates().reset_index(drop=True)
    df_t = df_t.loc[df_t.probS > .1]
    df_t = df_t.groupby("pid")["timeIndex"].min().reset_index(drop=False)
    df_t = df_t.merge(pids, on = "pid", how="right")
    df_t = df_t.loc[df_t.timeIndex.notnull()]
    
    print("fraction of immediate deaths" + str(1-df_t.pid.nunique()/len(pids)))
    death = df_t.copy()
    death.rename(columns = {"timeIndex":"deathTime"}, inplace = True)
    death = death.merge(df2, on = ["pid"], how = "inner")
    deathCor = np.corrcoef(30 - death.groupby("pid").deathTime.mean().values, death.groupby("pid").cumDonRev.max().values)[0,1]

    df_t["timeIndex"] = myround(df_t["timeIndex"], base=3)
    df_t = df_t.groupby("timeIndex")["pid"].count().reset_index(drop=False)
    df_t["pid"] = df_t.pid / np.sum(df_t.pid )
    
    fig                       = plt.figure(figsize=(6.4, 4.8*(1.6/2)))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    #
    barlist = plt.bar(df_t.timeIndex, df_t.pid, width=3,color=palette[3])
    #a = ax.get_xticks().tolist()
    #a[8]='Never'
    #ax.set_xticklabels(a,ha="center")
    #barlist[-1].set_color(sns.xkcd_palette(colors)[1])
    plt.ylabel("Fraction of Projects",**csfont)
    plt.xlabel("Last time (u) in which Pr(success) > 0.1",**csfont)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    keepAd = ""
    if keepAll == True: keepAd = "All"
    plt.savefig(pathPlot + fname + "_"  + keepAd + "_" + shipAdjust + ".pdf",bbox_inches='tight',format= "pdf",dpi=600)
    plt.close()



def runLassoLogit(df2):
    df4 = df2.copy()
    df4 = df4.loc[df4.timeIndex >= 0]
    df4 = df4.loc[df4.length == 30]
    df4 = df4.loc[df4.timeIndex <= 30]
    df4 = df4.reset_index(drop=True)
    df4["cumfracGc"] = np.minimum(df4.cumfracG, 1)
    
    train_inds, test_inds = next(GroupShuffleSplit(test_size=.20, n_splits=2, random_state = 7).split(df4, groups=df4['pid']))
    
    train = df4.iloc[train_inds]
    test = df4.iloc[test_inds]
    
    y = train.successEver
    enc1 = preprocessing.OneHotEncoder(categories='auto')
    X_ohe = enc1.fit_transform(train.pid.to_numpy().reshape(-1, 1))
    enc2 = preprocessing.OneHotEncoder(categories='auto')
    X_ohe2 = enc2.fit_transform(train.timeIndex.to_numpy().reshape(-1, 1))
    X =(X_ohe2.toarray()*train.cumfracGc[:,np.newaxis])
    X = sp.sparse.hstack((X, X_ohe ))
    
    SA = sparse.csr_matrix(X) 
    
    C = np.arange(.01,3,.25)
    R = []
    for c in C:
        model = LogisticRegression(penalty='l1', C=c, solver='liblinear')
        model.fit(SA, y)
        X_oheT =  sparse.lil_matrix((test.shape[0],X_ohe.shape[1])) #sparse.csr_matrix(np.zeros((test.shape[0],X_ohe.shape[1])))
        X_ohe2T = enc2.transform(test.timeIndex.to_numpy().reshape(-1, 1))
        XT =(X_ohe2T.toarray()*test.cumfracGc[:,np.newaxis])
        XT = sp.sparse.hstack((XT, X_oheT ))
        result = model.score(XT, test.successEver)
        print("Accuracy: %.2f%%" % (result*100.0))
        R.append([result])
    
    res = np.array(R).argmax()
    c = C[res]
    
    model = LogisticRegression(penalty='l1', C=c, solver='liblinear')
    model.fit(SA, y)
    X_oheT =  sparse.lil_matrix((test.shape[0],X_ohe.shape[1])) #sparse.csr_matrix(np.zeros((test.shape[0],X_ohe.shape[1])))
    X_ohe2T = enc2.transform(test.timeIndex.to_numpy().reshape(-1, 1))
    XT =(X_ohe2T.toarray()*test.cumfracGc[:,np.newaxis])
    XT = sp.sparse.hstack((XT, X_oheT ))
    result = model.score(XT, test.successEver)
    print("Accuracy: %.2f%%" % (result*100.0))
    
    failurePlot(X_ohe,model,enc2,keepAll,shipAdjust)
    probSuccessPlot(XT,model,test,keepAll,shipAdjust)
    histofXi(df2,test,model,XT,keepAll,shipAdjust)


######################################################################
######### DEFINE MAIN STATS FUNCTIONS
######################################################################

def meanSummaryStats(df,df2,df1,keepAll,shipAdjust):
    a = df2[["pid", "length"]].drop_duplicates()['length']
    b = df2[["pid", "goal"]].drop_duplicates()['goal']
    c = df2[["pid", "successEver"]].drop_duplicates()['successEver']
    d = df2.d_R
    e = df2.b_R
    f = df[["pid", "rewardId"]].drop_duplicates().groupby("pid")["rewardId"].nunique()
    g = 100*df2[df2.timeIndex == 0].cumDonRev/df2[df2.timeIndex == 0].cumTotalRev
    
    a1 = df2[["pid", "length", "successEver"]].drop_duplicates().groupby("successEver")['length'].mean()
    b1 = df2[["pid", "goal", "successEver"]].drop_duplicates().groupby("successEver")['goal'].mean()
    f1 = df[["pid", "rewardId"]].drop_duplicates().merge(df2[["pid", "successEver"]].drop_duplicates(), on="pid",how="left").groupby(["pid", "successEver"])["rewardId"].nunique().reset_index().groupby("successEver")["rewardId"].mean()
    d1 = df2[["d_R", "successEver"]].groupby("successEver")['d_R'].mean()
    e1 = df2[["b_R", "successEver"]].groupby("successEver")['b_R'].mean()
    g1 = df2[df2.timeIndex == 0].reset_index()
    g1["pD"] = 100*g1.cumDonRev/g1.cumTotalRev
    g1 = g1.groupby("successEver")["pD"].mean()   
    
    p1 = "Project Length" + "&" + str("{0:.1f}".format(a.mean())) + "&" + str("{0:.1f}".format(a1[0])) + "&" + str("{0:.1f}".format(a1[1])) + "&" + str("{0:.1f}".format(a.median())) + "&"+ str("{0:.1f}".format(a.quantile(q=0.05))) + "&" + str("{0:.1f}".format(a.quantile(q=0.95))) + "\\\\[.5ex]"
    p2 = "Goal (\$)" + "&" + str("{0:.1f}".format(b.mean())) + "&" + str("{0:.1f}".format(b1[0])) + "&" + str("{0:.1f}".format(b1[1])) + "&" + str("{0:.1f}".format(b.median())) + "&"+ str("{0:.1f}".format(b.quantile(q=0.05))) + "&" + str("{0:.1f}".format(b.quantile(q=0.95))) + "\\\\[.5ex]"
    p3 = "Number of Rewards" + "&" + str("{0:.1f}".format(f.mean())) + "&" + str("{0:.1f}".format(f1[0])) + "&" + str("{0:.1f}".format(f1[1])) + "&" + str("{0:.1f}".format(f.median())) + "&"+ str("{0:.1f}".format(f[f>=0].quantile(q=0.05))) + "&" + str("{0:.1f}".format(f[f>=0].quantile(q=0.95))) + "\\\\[.5ex]"
    p4 = "Donor Revenue (per period)" + "&" + str("{0:.1f}".format(d.mean())) + "&" + str("{0:.1f}".format(d1[0])) + "&" + str("{0:.1f}".format(d1[1])) + "&" + str("{0:.1f}".format(d.median())) + "&"+ str("{0:.1f}".format(d[d>=0].quantile(q=0.05))) + "&" + str("{0:.1f}".format(d[d>=0].quantile(q=0.95))) + "\\\\[.5ex]"
    p5 = "Buyer Revenue (per period)" + "&" + str("{0:.1f}".format(e.mean())) + "&" + str("{0:.1f}".format(e1[0])) + "&" + str("{0:.1f}".format(e1[1])) + "&" + str("{0:.1f}".format(e.median())) + "&"+ str("{0:.1f}".format(e[e>=0].quantile(q=0.05))) + "&" + str("{0:.1f}".format(e[e>=0].quantile(q=0.95))) + "\\\\[.5ex]"
    p6 = "Percent Donations at Deadline" + "&" + str("{0:.1f}".format(g.mean()))  + "&" + str("{0:.1f}".format(g1[0])) + "&" + str("{0:.1f}".format(g1[1])) + "&" + str("{0:.1f}".format(g.median())) + "&"+ str("{0:.1f}".format(g[g>=0].quantile(q=0.05))) + "&" + str("{0:.1f}".format(g[g>=0].quantile(q=0.95))) + "\\\\[.5ex]"
    p7 = "Number of Projects" + "&" + str("{0:.0f}".format(c.count())) + "&" + str("{0:.0f}".format(c[c==0].count())) + "&" + str("{0:.0f}".format(c[c==1].count())) + "&$-$&$-$&$-$\\\\[.5ex]"
    
    keepAd = ""
    if keepAll == True: keepAd = "All"
    with open(pathStats + 'summaryStatsTable'+keepAd+shipAdjust+'.txt', 'w') as f:
        f.writelines([line + "\n" for line in [p1,p2,p3,p4,p5,p6,p7]])

# obtain the number of projects by category.
def get_top_cat(g):
    return g['category'].value_counts().idxmax()

# top 4 cat summary stat
def meanCatSummaryStats(df2,df1,df,keepAll,shipAdjust):
    cats    =  pd.read_csv(catFile, delimiter=",")
    cats.rename(columns={ 'categories':'category'}, inplace=True)
    temp    =   df[df.category != "Project We Love"]
    temp    =   temp.groupby('pid').apply(get_top_cat)
    temp    =   temp.reset_index(drop=False)
    temp    =   temp.rename(columns={ 0:'category'})
    temp    =   temp.merge(cats, on="category", how="left")
    temp2   =   temp.groupby("cat_main")["pid"].count()
    temp2   =   temp2.sort_values(ascending=False)
    temp2   =   temp2[:4] # pick out the top 4 categories
    temp2   =   temp2.reset_index()
    temp2   =   temp2.drop('pid', axis=1)
    topCats =   temp.merge(temp2, how="right", on="cat_main")
    
    a 			= (df2[["pid", "length"]].merge(topCats, on="pid", how="inner").drop_duplicates()).reset_index(drop=False)[['length', 'cat_main']]
    b 			= (df2[["pid", "goal"]].merge(topCats, on="pid", how="inner").drop_duplicates()).reset_index(drop=False)[['goal', 'cat_main']]
    c 			= (df2[["pid", "successEver"]].merge(topCats, on="pid", how="inner").drop_duplicates()).reset_index(drop=False)[['successEver', 'cat_main']]
    d 			= df2[["pid", "d_R"]].merge(topCats, on="pid", how="inner")[['d_R','cat_main']]
    e 			= df2[["pid", "b_R"]].merge(topCats, on="pid", how="inner")[['b_R','cat_main']]
    f 			= df[["pid", "rewardId"]].merge(topCats, on="pid", how="inner").drop_duplicates().groupby(["pid", "cat_main"])["rewardId"].nunique()
    g1 			= df2[df2.timeIndex == 0].reset_index(drop=False)
    g1["frac"]  = 100*g1.cumDonRev/g1.cumTotalRev
    h1          = df2[df2.timeIndex == 0].reset_index(drop=False)
    h1["frac"]  = 100*g1.cumDonRev/g1.goal
    donResult 	= g1[["pid", "frac"]].merge(temp, on="pid", how="inner")[["frac", "cat_main"]]
    donResult 	= donResult.groupby("cat_main")["frac"].mean()
    donResult 	= donResult.reset_index().values.tolist()
    donResult 	= [[str(it) for it in line] for line in donResult]
    donResult2  = h1[["pid", "frac"]].merge(temp, on="pid", how="inner")[["frac", "cat_main"]]
    donResult2  = donResult2.groupby("cat_main")["frac"].mean()
    donResult2  = donResult2.reset_index().values.tolist()
    donResult2  = [[str(it) for it in line] for line in donResult2]
    if logFile:
        logger.write("percent donations at deadline by category \n")
        logger.writelines([",".join(line) + "\n" for line in donResult])
    if logFile:
        logger.write("percent donations at deadline (of goal) by category \n")
        logger.writelines([",".join(line) + "\n" for line in donResult2])
    g 			= g1[["pid", "frac"]].merge(topCats, on="pid", how="inner")[["frac", "cat_main"]]
    h           = h1[["pid", "frac"]].merge(topCats, on="pid", how="inner")[["frac", "cat_main"]]
    
    labels 		= "&" +  " & ".join(a.groupby("cat_main").mean().index.values.tolist()) + "\\\\"
    spacer 		= "\\hline\\\\[-1ex]"
    
    p1 			= "Project Length" + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(a.groupby("cat_main").mean().values)) + "\\\\"
    p11 		= "&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(a.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p2 			= "Goal (\$)" + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(b.groupby("cat_main").mean().values)) + "\\\\"
    p22 		= "&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(b.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p3 			= "Number of Rewards" + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(f.groupby("cat_main").mean().values)) + "\\\\"
    p33 		= "&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(f.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p4 			= "Donor Revenue" + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(d.groupby("cat_main").mean().values)) + "\\\\"
    p44 		= " (per period)&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(d.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p5 			= "Buyer Revenue " + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(e.groupby("cat_main").mean().values)) + "\\\\"
    p55 		= "(per period)&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(e.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p6 			= "Percent Donations " + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(g.groupby("cat_main").mean().values)) + "\\\\"
    p66 		= "at Deadline&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(g.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p6_2        = "Percent Donations " + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(h.groupby("cat_main").mean().values)) + "\\\\"
    p66_2       = "of Goal&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(h.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p7 			= "Percent Successful" + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(c.groupby("cat_main").mean().values*100)) + "\\\\[1ex]"
    
    p8 			= "Number of Projects" + "&" + " & ".join(str("{0:.0f}".format(float(it)))for it in list(a.groupby("cat_main")["length"].count().values)) + "\\\\[1ex]"
    
    keepAd 		= ""
    if keepAll == True: keepAd = "All"
    with open(pathStats + 'catSummaryStatsTable'+keepAd+shipAdjust+'.txt', 'w') as f:
        f.writelines([line + "\n" for line in [labels,spacer,p1,p11,p2,p22,p3,p33,p4,p44,p5,p55,p6,p66,p6_2,p66_2,p7,p8]])



# WHAT HAPPENS TO BUCKET PRICES ACROSS SUCCESS?
def meanBucketPrices_aroundSuccess(df,keepAll,shipAdjust):
    # first occ of success
    temp1  = df[(df.cumfracG >= 1) & (df.datedif/np.timedelta64(1, 'D') > 0)].groupby("pid")['datedif'].min().reset_index()
    # last occ before success
    temp2  = df[(df.cumfracG < 1) & (df.datedif/np.timedelta64(1, 'D') > 0)].groupby("pid")['datedif'].max().reset_index()
    
    # merge bucket information with time information (after)
    after  = df.merge(temp1, on=["pid", "datedif"], how="inner")
    after  = after[["pid", "bucketPrice", "bucketCap", "bucketRemain","bucketBackerN"]]
    after.rename(columns={ 'bucketCap':'bucketCap1',
                            'bucketRemain':'bucketRemain1',
                            'bucketBackerN':'bucketBackerN1',}, inplace=True)
    
    # merge bucket information with time information (before)
    before = df.merge(temp2, on=["pid", "datedif"], how="inner")
    before = before.merge(after[['pid']].drop_duplicates(), on="pid", how="inner")
    after  = after.merge(before[['pid']].drop_duplicates(), on="pid", how="inner")   # ensure that pids are same
    
    # make sure all columns are floats
    after  = after.sort_values(["pid", "bucketPrice"])
    before = before.sort_values(["pid", "bucketPrice"])
    after  = after.astype("float64")
    before['bucketPrice'] = before.bucketPrice.astype("float64")
    before['bucketRemain'] = before.bucketRemain.astype("float64")
    cap1   = str(len(before[before.bucketRemain== 0])/float(len(before)))
    cap2   = str(len(after[after.bucketRemain1 == 0])/float(len(after)))
    if logFile:
        logger.write("percent of buckets filled before \n")
        logger.write(cap1+"\n")
        logger.write("percent of buckets filled after \n")
        logger.write(cap2+"\n")
    
    for version in range(3):
        if version == 0:
            collapse_after=after.groupby("pid")['bucketPrice'].mean().reset_index()
            collapse_before=before.groupby("pid")['bucketPrice'].mean().reset_index()
        if version == 1:
            collapse_after=after[(after.bucketBackerN1 >= 0) & (after.bucketRemain1 != 0)].groupby("pid")['bucketPrice'].mean().reset_index()
            collapse_before=before[(before.bucketBackerN >= 0) & (before.bucketRemain != 0)].groupby("pid")['bucketPrice'].mean().reset_index()
        if version == 2:
            collapse_after=after[(after.bucketBackerN1 > 0) & (after.bucketRemain1 != 0)].groupby("pid")['bucketPrice'].mean().reset_index()
            collapse_before=before[(before.bucketBackerN > 0) & (before.bucketRemain != 0)].groupby("pid")['bucketPrice'].mean().reset_index()
        result_merge=collapse_after.merge(collapse_before, on="pid", how="inner")
        naming = ["all", "remaining capacity", "remaining capacity, used"]
        p1= "Differences in mean and median bucket prices for scenario: " + naming[version]
        p2= "Mean " + str((result_merge.bucketPrice_x - result_merge.bucketPrice_y).mean())
        p3= "Median " + str((result_merge.bucketPrice_x - result_merge.bucketPrice_y).median())
        p4= "Mean price before " + str(result_merge.bucketPrice_y.mean())
        p5= "Mean price after " + str(result_merge.bucketPrice_x.mean())
        if logFile:
            logger.writelines([line + "\n" for line in [p1,p2,p3,p4,p5]])


# print summary stats based on completion time, at the ending time
def printStats_segments(df2,variable):
    if variable == "RG":
        df2[variable] = df2.totalRev/df2.goal
    sub1 = df2[(df2.length==30) & (df2.hitTime >= 27) & (df2.timeIndex == 0)][[variable]].describe().to_string()
    sub2 = df2[(df2.length==30) & (df2.hitTime > 3) & (df2.hitTime < 27) & (df2.timeIndex == 0)][[variable ]].describe().to_string()
    sub3 = df2[(df2.length==30) & (df2.hitTime <= 3) & (df2.timeIndex == 0)][[variable ]].describe().to_string()
    ll   = ["\n\n\n segment stats for " + variable,"early: " + sub1 , "middle: "+ sub2, "late: " + sub3]
    if logFile:
        logger.writelines([line + "\n" for line in ll])

# print off some statistics that are mentioned in the paper
def stats_in_paper(df,df1,df2,keepAll,shipAdjust):
    # correlation between goal and length
    if logFile:
        logger.write("correlation between goal and length\n")
        logger.write(str(df2.drop_duplicates(["pid", "goal", "length"])[["goal", "length"]].corr().iloc[0]['length']) + "\n")
        # bucket stats
        logger.write("fraction of buckets without capacity" + "\n")
        logger.write(str(100*float(df[["pid", "bucketPrice", "bucketCap"]].drop_duplicates()["bucketCap"].isnull().sum())/df[["pid", "bucketPrice", "bucketCap"]].drop_duplicates()['pid'].count()) + "\n")
        logger.write("fraction of buckets that fill to capacity" + "\n")
        logger.write(str(100*float((df[["pid", "bucketPrice", "bucketCap"]].drop_duplicates()["bucketCap"] == 0).sum())/(df[["pid", "bucketPrice", "bucketCap"]].drop_duplicates()["bucketCap"] > 0).sum()) + "\n")
    # segment statistics
    printStats_segments(df2,"cumDonN")
    printStats_segments(df2,"cumBackerN")
    printStats_segments(df2,"goal")
    printStats_segments(df2,"RG")
    # early goal revenue for early finishers
    temp1 			 = df2[(df2.length==30) & (df2.hitTime <= 3) & (df2.timeIndex == 28)][["pid","cumTotalRev"]]
    temp2 			 = df2[(df2.length==30) & (df2.hitTime <= 3) & (df2.timeIndex == 0)][["pid","cumTotalRev"]]
    temp3 			 = df2[(df2.length==30) & (df2.hitTime <= 3) & (df2.timeIndex == 28)][["pid","cumTotalRev", "cumBackerRev"]]
    temp3["fracRev"] = temp3.cumBackerRev/temp3.cumTotalRev
    temp1 			 = temp1.merge(temp2, on="pid", how="inner")
    temp1["frac"]    = temp1.cumTotalRev_x/temp1.cumTotalRev_y
    if logFile:
        logger.write("fraction of revenue in first day for early finishers" + "\n")
        logger.write(temp1.frac.describe().to_string())
        logger.write("fraction of backer rev for early finishers" + "\n")
        logger.write(temp3.fracRev.describe().to_string())
        logger.write("donor count stats, conditional on positive donations" + "\n")
        logger.write(df2[df2.d_N > 0].d_N.describe().to_string())

def early_middle_late_sumstats(df,df1,df2,keepAll,shipAdjust):
    pid_e = df2[(df2.length==30) & (df2.hitTime >= 27)]['pid'].drop_duplicates().reset_index(drop=True)
    pid_m = df2[(df2.length==30) & (df2.hitTime > 3) & (df2.hitTime < 27)]['pid'].drop_duplicates().reset_index(drop=True)
    pid_l = df2[(df2.length==30) & (df2.hitTime <= 3)]['pid'].drop_duplicates().reset_index(drop=True)
    
    sub_e = df2[df2['pid'].isin(pid_e)].reset_index(drop=True)
    sub_m = df2[df2['pid'].isin(pid_m)].reset_index(drop=True)
    sub_l = df2[df2['pid'].isin(pid_l)].reset_index(drop=True)
    
    a1    = sub_e[["pid", "goal"]].drop_duplicates()['goal']
    a2    = sub_m[["pid", "goal"]].drop_duplicates()['goal']
    a3    = sub_l[["pid", "goal"]].drop_duplicates()['goal']
    
    sub_e["RG"] = sub_e.totalRev/sub_e.goal
    sub_m["RG"] = sub_m.totalRev/sub_m.goal
    sub_l["RG"] = sub_l.totalRev/sub_l.goal
    
    d1 = sub_e[sub_e.timeIndex == 0]["RG"]
    d2 = sub_m[sub_m.timeIndex == 0]["RG"]
    d3 = sub_l[sub_l.timeIndex == 0]["RG"]
    
    dd1 = 100*sub_e[sub_e.timeIndex == 0]["cumDonRev"]/sub_e[sub_e.timeIndex == 0]["totalRev"]
    dd2 = 100*sub_m[sub_m.timeIndex == 0]["cumDonRev"]/sub_m[sub_m.timeIndex == 0]["totalRev"]
    dd3 = 100*sub_l[sub_l.timeIndex == 0]["cumDonRev"]/sub_l[sub_l.timeIndex == 0]["totalRev"]
    
    ddd1 = 100*sub_e[sub_e.timeIndex == 0]["cumDonRev"]/sub_e[sub_e.timeIndex == 0]["goal"]
    ddd2 = 100*sub_m[sub_m.timeIndex == 0]["cumDonRev"]/sub_m[sub_m.timeIndex == 0]["goal"]
    ddd3 = 100*sub_l[sub_l.timeIndex == 0]["cumDonRev"]/sub_l[sub_l.timeIndex == 0]["goal"]
    
    # reset which df we are working on
    sub_e=df[df['pid'].isin(pid_e)].reset_index(drop=True)
    sub_m=df[df['pid'].isin(pid_m)].reset_index(drop=True)
    sub_l=df[df['pid'].isin(pid_l)].reset_index(drop=True)
    
    b1=sub_e[["pid", "rewardId"]].drop_duplicates().groupby("pid")["rewardId"].nunique()
    b2=sub_m[["pid", "rewardId"]].drop_duplicates().groupby("pid")["rewardId"].nunique()
    b3=sub_l[["pid", "rewardId"]].drop_duplicates().groupby("pid")["rewardId"].nunique()
    
    c1=sub_e[["pid", "bucketPrice"]].drop_duplicates().groupby("pid")["bucketPrice"].apply(np.mean)
    c2=sub_m[["pid", "bucketPrice"]].drop_duplicates().groupby("pid")["bucketPrice"].apply(np.mean)
    c3=sub_l[["pid", "bucketPrice"]].drop_duplicates().groupby("pid")["bucketPrice"].apply(np.mean)
    
    cats = pd.read_csv(catFile, delimiter=",")
    cats.rename(columns={ 'categories':'category'}, inplace=True)
    
    cat_e=sub_e[["pid", "category"]].drop_duplicates().reset_index(drop=True).merge(cats, on="category", how="left").groupby("cat_main")["pid"].count().sort_values(ascending=False).reset_index()
    cat_m=sub_m[["pid", "category"]].drop_duplicates().reset_index(drop=True).merge(cats, on="category", how="left").groupby("cat_main")["pid"].count().sort_values(ascending=False).reset_index()
    cat_l=sub_l[["pid", "category"]].drop_duplicates().reset_index(drop=True).merge(cats, on="category", how="left").groupby("cat_main")["pid"].count().sort_values(ascending=False).reset_index()
    
    p1= "Goal (\$)" + "&" + str("{0:.1f}".format(a1.mean())) + "&" + str("{0:.1f}".format(a2.mean())) + "&" + str("{0:.1f}".format(a3.mean())) + "\\\\"
    p2= "&(" + str("{0:.1f}".format(a1.std())) + ")&(" + str("{0:.1f}".format(a2.std())) + ")&(" + str("{0:.1f}".format(a3.std())) + ")\\\\[1ex]"
    
    p3= "Number of Rewards" + "&" + str("{0:.1f}".format(b1.mean())) + "&" + str("{0:.1f}".format(b2.mean())) + "&" + str("{0:.1f}".format(b3.mean())) + "\\\\"
    p4= "&(" + str("{0:.1f}".format(b1.std())) + ")&(" + str("{0:.1f}".format(b2.std())) + ")&(" + str("{0:.1f}".format(b3.std())) + ")\\\\[1ex]"
    
    p5= "Average Price" + "&" + str("{0:.1f}".format(c1.mean())) + "&" + str("{0:.1f}".format(c2.mean())) + "&" + str("{0:.1f}".format(c3.mean())) + "\\\\"
    p6= "&(" + str("{0:.1f}".format(c1.std())) + ")&(" + str("{0:.1f}".format(c2.std())) + ")&(" + str("{0:.1f}".format(c3.std())) + ")\\\\[1ex]"
    
    p7= "$R/G$" + "&" + str("{0:.1f}".format(d1.mean())) + "&" + str("{0:.1f}".format(d2.mean())) + "&" + str("{0:.1f}".format(d3.mean())) + "\\\\"
    p8= "&(" + str("{0:.1f}".format(d1.std())) + ")&(" + str("{0:.1f}".format(d2.std())) + ")&(" + str("{0:.1f}".format(d3.std())) + ")\\\\[1ex]"
    
    p9= "$D/R (\%)$" + "&" + str("{0:.1f}".format(dd1.mean())) + "&" + str("{0:.1f}".format(dd2.mean())) + "&" + str("{0:.1f}".format(dd3.mean())) + "\\\\"
    p10= "&(" + str("{0:.1f}".format(dd1.std())) + ")&(" + str("{0:.1f}".format(dd2.std())) + ")&(" + str("{0:.1f}".format(dd3.std())) + ")\\\\[1ex]"
    
    p9_2= "$D/G (\%)$" + "&" + str("{0:.1f}".format(ddd1.mean())) + "&" + str("{0:.1f}".format(ddd2.mean())) + "&" + str("{0:.1f}".format(ddd3.mean())) + "\\\\"
    p10_2= "&(" + str("{0:.1f}".format(ddd1.std())) + ")&(" + str("{0:.1f}".format(ddd2.std())) + ")&(" + str("{0:.1f}".format(ddd3.std())) + ")\\\\[1ex]"
    
    p10_1 = "Number of Projects" + "&" + str("{0:.0f}".format(sub_e.groupby("pid")['pid'].nunique().count())) + "&" + str("{0:.0f}".format(sub_m.groupby("pid")['pid'].nunique().count())) + "&" + str("{0:.0f}".format(sub_l.groupby("pid")['pid'].nunique().count())) + "\\\\[1ex]"
    
    p11 = "Top Categories" + "&" + cat_e.iloc[0]['cat_main'] + "&" + cat_m.iloc[0]['cat_main'] + "&" + cat_l.iloc[0]['cat_main'] +"\\\\"
    p12 = "&" + cat_e.iloc[1]['cat_main'] + "&" + cat_m.iloc[1]['cat_main'] + "&" + cat_l.iloc[1]['cat_main'] +"\\\\"
    p13 = "&" + cat_e.iloc[2]['cat_main'] + "&" + cat_m.iloc[2]['cat_main'] + "&" + cat_l.iloc[2]['cat_main'] +"\\\\"
    
    keepAd = ""
    if keepAll == True: keepAd = "All"
    with open(pathStats + 'EMLStatsTable'+keepAd+shipAdjust+'.txt', 'w') as f:
        f.writelines([line + "\n" for line in [p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p9_2,p10_2,p10_1,p11,p12,p13]])


def empiricalPredictions(df,df1,df2,keepAll,shipAdjust):
    df3       =           df2.copy()
    df3       =           df3.loc[(df3.hitTime==0) & (df3.timeIndex==0)]
    # EML stats
    pid_l = df2[(df2.length==30) & (df2.hitTime <= 3)]['pid'].drop_duplicates().reset_index(drop=True)
    sub_l = df2[df2['pid'].isin(pid_l)].reset_index(drop=True)
    mR    = sub_l.loc[(sub_l.timeIndex < 27) & (sub_l.timeIndex > 3)].d_R.sum()
    lR    = sub_l.loc[sub_l.timeIndex <= 3].d_R.sum()
    eR    = sub_l.loc[sub_l.timeIndex >= 27].d_R.sum()
    if logFile:
        logger.write("cumfracG at deadline for projects that finish at deadline" + "\n")
        logger.write(df3.cumfracG.describe().to_string())
        logger.write("weighted version" + "\n")
        logger.write(str((1/df3.goal.sum() * df3.goal * df3.cumfracG).sum()))
        #EML stats
        logger.write("frac of donations for late finishes E, M, L" + "\n")
        logger.write(str(eR / (mR + lR + eR)) + "\n")
        logger.write(str(mR / (mR + lR + eR)) + "\n")
        logger.write(str(lR / (mR + lR + eR)) + "\n")


######################################################################
# RUN THE MAIN PROGRAM
######################################################################

if __name__ == '__main__':
    if logFile:
        logger.write("Keeping all observations in the sample: " + str(keepAll) + "\n\n")
        logger.write("Using the shipping adjustment: " + shipAdjust + "\n")
    
    df, df1, df2, uB, lB =  cleanData(keepAll,shipAdjust)
    if logFile:
        logger.write("Numer of observations in sample: " + str(df2.shape[0]) + "\n\n")
    print("now working on plots")
    backerTimePlot(df2,keepAll,shipAdjust)
    histGoals(df2,keepAll,shipAdjust)
    histLove(df2,keepAll,shipAdjust)
    completionRtoG(df2,keepAll,shipAdjust)
    completionTime(df2,keepAll,shipAdjust)
    ProjectsWeLove(df2,keepAll,shipAdjust)
    plotHitTimeCompare(df2,keepAll,shipAdjust) # need to check SE code
    hazardPlot(df2,keepAll,shipAdjust)
    runLassoLogit(df2) #theck .loc
    completionTimePercents(df2,keepAll)
    completionPeriodFinalRev(df2,keepAll)
    buyerDonorPanels(df2,"donor",keepAll)
    buyerDonorPanels(df2,"buyer",keepAll)
    buyerDonorWeightedPanels(df2,"donor",keepAll)
    buyerDonorWeightedPanels(df2,"buyer",keepAll)
    buyerDonorPlot(df2,keepAll,shipAdjust)
    initialDoverL(df2,keepAll)
    initialDoverG(df2,keepAll)
    print("now working on stats")
    meanSummaryStats(df,df2,df1,keepAll,shipAdjust)
    meanCatSummaryStats(df2,df1,df,keepAll,shipAdjust)
    meanBucketPrices_aroundSuccess(df,keepAll,shipAdjust)
    print("now working on statistics mentioned in the paper")
    stats_in_paper(df,df1,df2,keepAll,shipAdjust)
    early_middle_late_sumstats(df,df1,df2,keepAll,shipAdjust)
    empiricalPredictions(df,df1,df2,keepAll,shipAdjust)
    if logFile:
        logger.close()



