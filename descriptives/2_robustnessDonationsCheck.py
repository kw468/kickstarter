"""
    Create Robustness Plots For Appendix
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
keepAll             =   False   # False or True, which creates file names that contain the word All
co                  =   .005     # percent trimmed off top and bottom
logFile             =   True    # True or False

######################################################################
######### Define the shipping adjustment being made
######################################################################
shipAdjust          =   "US"    # min_cost or max_cost or US


######################################################################
######### DEFINE OUTPUT AND LOGGING FILES
######################################################################

sumFile         = pathStats + 'robustness_numbers_for_paper'
if keepAll == True:
    sumFile     = sumFile + "All"

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
def convertData(df,donorAdjust,shipAdjust):
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
    df = df.reset_index(drop=True)
    # THIS IS WHERE WE ADD DONATION ADJUSTMENTS
    if (donorAdjust == "all") or (donorAdjust == "topP"):
        temp = df[["pid","datedif","bucketPrice", "rewardId"]].\
            sort_values(["pid","datedif","bucketPrice"], ascending=False)
        temp                            = temp.groupby(["pid","datedif"]).first()
        temp                            = temp.reset_index(drop=False)
        temp["ident"]                   = 1
        
        df                              = df.merge(temp,on=["pid","datedif", "rewardId", "bucketPrice"], how="left")
        
        df                  = df.loc[df.ident != 1].reset_index(drop=True)
        df.drop("ident", axis=1,inplace=True)
        df["bucketRev"]  = df.bucketBackerN * df.bucketPrice 
        df["cumBackerRev"] = df.groupby(["pid", "edate", "sdate"])["bucketRev"].transform("sum")
        df["cumBackerN"]   = df.groupby(["pid", "edate", "sdate"])["bucketBackerN"].transform("sum")
        df["avgBackerRev"] = df.cumBackerRev / df.cumBackerN
        df.drop("bucketRev", axis=1,inplace=True)
        
    if (donorAdjust == "all") or (donorAdjust == "bottomP"):
        temp = df[["pid","datedif","bucketPrice", "rewardId"]].\
            sort_values(["pid","datedif","bucketPrice"], ascending=True)
        temp                            = temp.groupby(["pid","datedif"]).first()
        temp                            = temp.reset_index(drop=False)
        temp["ident"]                   = 1
        
        df                              = df.merge(temp,on=["pid","datedif", "rewardId", "bucketPrice"], how="left")
        
        df                  = df.loc[df.ident != 1].reset_index(drop=True)
        df.drop("ident", axis=1,inplace=True)
        df["bucketRev"]  = df.bucketBackerN * df.bucketPrice 
        df["cumBackerRev"] = df.groupby(["pid", "edate", "sdate"])["bucketRev"].transform("sum")
        df["cumBackerN"]   = df.groupby(["pid", "edate", "sdate"])["bucketBackerN"].transform("sum")
        df["avgBackerRev"] = df.cumBackerRev / df.cumBackerN
        df.drop("bucketRev", axis=1,inplace=True)
    
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
    #dateData['timeHours']              = dateData.firstObs.apply(np.abs)
    dateData.sort_values(["pid", "datedif"],ascending = [True,False], inplace=True) # changed timeHours to datedif; and a true to false
    dateData                            = dateData.reset_index(drop=True)
    #dateData['hourShift']              = dateData.groupby('pid').datedif.shift()
    #dateData['hourDif']                = (dateData.hourShift-dateData.datedif)/np.timedelta64(1, 'D') * 24.0
    #dateData['hourDifRound']           = (dateData.hourDif/12).round()
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
    # dateData['index']                     = np.nan
    # dateData.loc[dateData.hourDifRound.isnull()==True, 'index'] = dateData.length - ((dateData.timeHours/12).astype(np.double)).round()
    # dfTmp                                 = dateData[["pid", "hourDifRound", "index"]].fillna(0).groupby("pid").cumsum()
    # dateData["timeIndex"]                 = (dfTmp["hourDifRound"] * (-0.5)) + dfTmp["index"]
    
    # # drop duplicate obs, first is kept
    # dateData                          = dateData.drop_duplicates(["pid", "timeIndex"])
    # # 1 project is observed to have initial time index greater than length of project. Delete them.
    # # this looks like a timestamp issue for
    # # https://www.kickstarter.com/projects/445939737/the-worlds-first-and-only-color-changing-bedding/comments
    # #
    # dateData                            = dateData.loc[dateData.groupby("pid")["timeIndex"].transform("max") <= 60]
    # # remove scraping results such that project is tracked more than 1 week after completion
    # dateData                            = dateData.loc[dateData.timeIndex > -7]
    # dateData                          = dateData.reset_index(drop=True)
    
    return grid[["pid", "edate", "sdate", "timeIndex"]], upperbound, lowerbound


# GATHER INFORMATION FOR TSFILL
def tsFill(df,dateData):
    df1     = dateData.merge(df, how="left", on=['pid', 'edate', 'sdate'])
    subset  = df1[["pid", "timeIndex", "cumfracG", "goal", "cumTotalRev", \
                "cumTotalN", "cumBackerN", "avgBackerRev", "cumDonRev", \
                "cumDonN", "cumBackerRev", "length", "rewardId", "love"]]
    subset  = subset.drop_duplicates(["pid", "rewardId", "timeIndex"])
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
    dfAgg.loc[dfAgg.N   < 0, 'N']   = 0
    
    dfAgg                                   = dfAgg.loc[dfAgg.N.notnull()].reset_index(drop=True)
    dfAgg["success"]                        = 0
    dfAgg.loc[dfAgg.cumfracG >= 1, 'success'] = 1
    dfAgg["successEver"]                    = dfAgg.groupby("pid")["success"].transform("max")
    
    temp                                    = dfAgg.loc[dfAgg.success == 1].groupby("pid")["timeIndex"].max()
    temp                                    = temp.reset_index()
    temp                                    = temp.rename(columns={  "timeIndex":'hitTime'})
    dfAgg                                   = dfAgg.merge(temp, how="left", on="pid")
    dfAgg["relativeHit"]                    = dfAgg.hitTime - dfAgg.timeIndex
    
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
def cleanData(keepAll,donorAdjust,shipAdjust):
    if logFile:
        logger.write("WORKING ON ROBUSTNESS USING " + donorAdjust + " AS DONATIONS\n")
    print("WORKING ON ROBUSTNESS USING " + donorAdjust + " AS DONATIONS\n")
    print("working on donor adjustment equal to " + donorAdjust)
    print("opening the master data file")
    df = openData("master")
    
    print("converting the raw data to df format")
    df = convertData(df,donorAdjust,shipAdjust)
    
    print("creating time index and trimming data based on thresholds")
    dateData, uB, lB = createTimeIndex(df,keepAll)
    
    print("filling in any gaps with the data using tsfill")
    df1 = tsFill(df,dateData)
    
    print("aggregating data to the daily level")
    df2 = createDailyDF(df1)
    df2 = df2.replace([np.inf, -np.inf], np.nan)
    
    df2.to_parquet(pathRepdata + "kickstarter_DF2"+donorAdjust+".parquet.gzip", engine='pyarrow', compression="gzip")
    
    # run the stats in the paper, robustness
    print("DONOR ADUSTMENT\n")
    print(donorAdjust)
    print("now working on stats")
    meanSummaryStats                                        (df,df2,df1,keepAll,shipAdjust,donorAdjust)
    meanCatSummaryStats                                     (df2,df1,df,keepAll,shipAdjust,donorAdjust)
    print("now working on statistics mentioned in the paper")
    stats_in_paper                                          (df,df1,df2,keepAll,shipAdjust)
    early_middle_late_sumstats                              (df,df1,df2,keepAll,shipAdjust,donorAdjust)
    return df2



######################################################################
######### DEFINE MAIN STATS FUNCTIONS
######################################################################

def meanSummaryStats(df,df2,df1,keepAll,shipAdjust,donorAdjust):
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
    with open(pathStats + 'robustness_summaryStatsTable'+keepAd+shipAdjust+donorAdjust+'.txt', 'w') as f:
        f.writelines([line + "\n" for line in [p1,p2,p3,p4,p5,p6,p7]])

# obtain the number of projects by category.
def get_top_cat(g):
    return g['category'].value_counts().idxmax()

# top 4 cat summary stat
def meanCatSummaryStats(df2,df1,df,keepAll,shipAdjust,donorAdjust):
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
    
    a           = (df2[["pid", "length"]].merge(topCats, on="pid", how="inner").drop_duplicates()).reset_index(drop=False)[['length', 'cat_main']]
    b           = (df2[["pid", "goal"]].merge(topCats, on="pid", how="inner").drop_duplicates()).reset_index(drop=False)[['goal', 'cat_main']]
    c           = (df2[["pid", "successEver"]].merge(topCats, on="pid", how="inner").drop_duplicates()).reset_index(drop=False)[['successEver', 'cat_main']]
    d           = df2[["pid", "d_R"]].merge(topCats, on="pid", how="inner")[['d_R','cat_main']]
    e           = df2[["pid", "b_R"]].merge(topCats, on="pid", how="inner")[['b_R','cat_main']]
    f           = df[["pid", "rewardId"]].merge(topCats, on="pid", how="inner").drop_duplicates().groupby(["pid", "cat_main"])["rewardId"].nunique()
    g1          = df2[df2.timeIndex == 0].reset_index(drop=False)
    g1["frac"]  = 100*g1.cumDonRev/g1.cumTotalRev
    h1          = df2[df2.timeIndex == 0].reset_index(drop=False)
    h1["frac"]  = 100*g1.cumDonRev/g1.goal
    donResult   = g1[["pid", "frac"]].merge(temp, on="pid", how="inner")[["frac", "cat_main"]]
    donResult   = donResult.groupby("cat_main")["frac"].mean()
    donResult   = donResult.reset_index().values.tolist()
    donResult   = [[str(it) for it in line] for line in donResult]
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
    g           = g1[["pid", "frac"]].merge(topCats, on="pid", how="inner")[["frac", "cat_main"]]
    h           = h1[["pid", "frac"]].merge(topCats, on="pid", how="inner")[["frac", "cat_main"]]
    
    labels      = "&" +  " & ".join(a.groupby("cat_main").mean().index.values.tolist()) + "\\\\"
    spacer      = "\\hline\\\\[-1ex]"
    
    p1          = "Project Length" + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(a.groupby("cat_main").mean().values)) + "\\\\"
    p11         = "&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(a.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p2          = "Goal (\$)" + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(b.groupby("cat_main").mean().values)) + "\\\\"
    p22         = "&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(b.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p3          = "Number of Rewards" + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(f.groupby("cat_main").mean().values)) + "\\\\"
    p33         = "&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(f.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p4          = "Donor Revenue" + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(d.groupby("cat_main").mean().values)) + "\\\\"
    p44         = " (per period)&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(d.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p5          = "Buyer Revenue " + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(e.groupby("cat_main").mean().values)) + "\\\\"
    p55         = "(per period)&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(e.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p6          = "Percent Donations " + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(g.groupby("cat_main").mean().values)) + "\\\\"
    p66         = "at Deadline&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(g.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p6_2        = "Percent Donations " + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(h.groupby("cat_main").mean().values)) + "\\\\"
    p66_2       = "of Goal&" + " & ".join(str("({0:.1f})".format(float(it)))for it in list(h.groupby("cat_main").std().values)) + "\\\\[1ex]"
    
    p7          = "Percent Successful" + "&" + " & ".join(str("{0:.1f}".format(float(it)))for it in list(c.groupby("cat_main").mean().values*100)) + "\\\\[1ex]"
    
    p8          = "Number of Projects" + "&" + " & ".join(str("{0:.0f}".format(float(it)))for it in list(a.groupby("cat_main")["length"].count().values)) + "\\\\[1ex]"
    
    keepAd      = ""
    if keepAll == True: keepAd = "All"
    with open(pathStats + 'robustness_catSummaryStatsTable'+keepAd+shipAdjust+donorAdjust+'.txt', 'w') as f:
        f.writelines([line + "\n" for line in [labels,spacer,p1,p11,p2,p22,p3,p33,p4,p44,p5,p55,p6,p66,p6_2,p66_2,p7,p8]])


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
    temp1            = df2[(df2.length==30) & (df2.hitTime <= 3) & (df2.timeIndex == 28)][["pid","cumTotalRev"]]
    temp2            = df2[(df2.length==30) & (df2.hitTime <= 3) & (df2.timeIndex == 0)][["pid","cumTotalRev"]]
    temp3            = df2[(df2.length==30) & (df2.hitTime <= 3) & (df2.timeIndex == 28)][["pid","cumTotalRev", "cumBackerRev"]]
    temp3["fracRev"] = temp3.cumBackerRev/temp3.cumTotalRev
    temp1            = temp1.merge(temp2, on="pid", how="inner")
    temp1["frac"]    = temp1.cumTotalRev_x/temp1.cumTotalRev_y
    if logFile:
        logger.write("fraction of revenue in first day for early finishers" + "\n")
        logger.write(temp1.frac.describe().to_string())
        logger.write("fraction of backer rev for early finishers" + "\n")
        logger.write(temp3.fracRev.describe().to_string())
        logger.write("donor count stats, conditional on positive donations" + "\n")
        logger.write(df2[df2.d_N > 0].d_N.describe().to_string())

def early_middle_late_sumstats(df,df1,df2,keepAll,shipAdjust,donorAdjust):
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
    
    p1= "Goal (\$)" + "&" + str("{0:.2f}".format(a1.mean())) + "&" + str("{0:.2f}".format(a2.mean())) + "&" + str("{0:.2f}".format(a3.mean())) + "\\\\"
    p2= "&(" + str("{0:.2f}".format(a1.std())) + ")&(" + str("{0:.2f}".format(a2.std())) + ")&(" + str("{0:.2f}".format(a3.std())) + ")\\\\[1ex]"
    
    p3= "Number of Rewards" + "&" + str("{0:.2f}".format(b1.mean())) + "&" + str("{0:.2f}".format(b2.mean())) + "&" + str("{0:.2f}".format(b3.mean())) + "\\\\"
    p4= "&(" + str("{0:.2f}".format(b1.std())) + ")&(" + str("{0:.2f}".format(b2.std())) + ")&(" + str("{0:.2f}".format(b3.std())) + ")\\\\[1ex]"
    
    p5= "Average Price" + "&" + str("{0:.2f}".format(c1.mean())) + "&" + str("{0:.2f}".format(c2.mean())) + "&" + str("{0:.2f}".format(c3.mean())) + "\\\\"
    p6= "&(" + str("{0:.2f}".format(c1.std())) + ")&(" + str("{0:.2f}".format(c2.std())) + ")&(" + str("{0:.2f}".format(c3.std())) + ")\\\\[1ex]"
    
    p7= "$R/G$" + "&" + str("{0:.2f}".format(d1.mean())) + "&" + str("{0:.2f}".format(d2.mean())) + "&" + str("{0:.2f}".format(d3.mean())) + "\\\\"
    p8= "&(" + str("{0:.2f}".format(d1.std())) + ")&(" + str("{0:.2f}".format(d2.std())) + ")&(" + str("{0:.2f}".format(d3.std())) + ")\\\\[1ex]"
    
    p9= "$D/R (\%)$" + "&" + str("{0:.2f}".format(dd1.mean())) + "&" + str("{0:.2f}".format(dd2.mean())) + "&" + str("{0:.2f}".format(dd3.mean())) + "\\\\"
    p10= "&(" + str("{0:.2f}".format(dd1.std())) + ")&(" + str("{0:.2f}".format(dd2.std())) + ")&(" + str("{0:.2f}".format(dd3.std())) + ")\\\\[1ex]"
    
    p9_2= "$D/G (\%)$" + "&" + str("{0:.2f}".format(ddd1.mean())) + "&" + str("{0:.2f}".format(ddd2.mean())) + "&" + str("{0:.2f}".format(ddd3.mean())) + "\\\\"
    p10_2= "&(" + str("{0:.2f}".format(ddd1.std())) + ")&(" + str("{0:.2f}".format(ddd2.std())) + ")&(" + str("{0:.2f}".format(ddd3.std())) + ")\\\\[1ex]"
    
    p10_1 = "Number of Projects" + "&" + str("{0:.0f}".format(sub_e.groupby("pid")['pid'].nunique().count())) + "&" + str("{0:.0f}".format(sub_m.groupby("pid")['pid'].nunique().count())) + "&" + str("{0:.0f}".format(sub_l.groupby("pid")['pid'].nunique().count())) + "\\\\[1ex]"
    
    p11 = "Top Categories" + "&" + cat_e.iloc[0]['cat_main'] + "&" + cat_m.iloc[0]['cat_main'] + "&" + cat_l.iloc[0]['cat_main'] +"\\\\"
    p12 = "&" + cat_e.iloc[1]['cat_main'] + "&" + cat_m.iloc[1]['cat_main'] + "&" + cat_l.iloc[1]['cat_main'] +"\\\\"
    p13 = "&" + cat_e.iloc[2]['cat_main'] + "&" + cat_m.iloc[2]['cat_main'] + "&" + cat_l.iloc[2]['cat_main'] +"\\\\"
    
    keepAd = ""
    if keepAll == True: keepAd = "All"
    with open(pathStats + 'robustness_EMLStatsTable'+keepAd+shipAdjust+donorAdjust+'.txt', 'w') as f:
        f.writelines([line + "\n" for line in [p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p9_2,p10_2,p10_1,p11,p12,p13]])







######################################################################
######### DEFINE MAIN PLOT FUNCTIONS
######################################################################


def plot1(df2both,df2,df2bottom,df2top,keepAll,shipAdjust, type):
    if type == "donor":
        fname = "Donor_Contributions_Over_Time"
        col = "fracD_totalR"
    if type == "buyer":
        fname = "Buyer_Contributions_Over_Time"
        col = "fracB_totalR"
    df2             = df2.loc[(df2.length==30) & (df2.timeIndex>=0)].reset_index(drop=True)
    df2both         = df2both.loc[(df2both.length==30) & (df2both.timeIndex>=0)].reset_index(drop=True)
    df2top          = df2top.loc[(df2top.length==30) & (df2top.timeIndex>=0)].reset_index(drop=True)
    df2bottom       = df2bottom.loc[(df2bottom.length==30) & (df2bottom.timeIndex>=0)].reset_index(drop=True)
    df2["timeIndex"]        = 30-df2.timeIndex
    df2both["timeIndex"]    = 30-df2both.timeIndex
    df2top["timeIndex"]     = 30-df2top.timeIndex
    df2bottom["timeIndex"]  = 30-df2bottom.timeIndex
    
    fig = plt.figure(figsize=(14,5))
    csfont              = {'fontname':"Liberation Serif", 'fontsize':14}
    palette             = ["#FF6700", "#FCB07E", "#6B717E", "#3A6EA5", "#004E98", "#070707"]
    sns.set(style="white",color_codes=False)
    ax = fig.add_subplot(111)    # The big subplot
    
    ax.spines['top'].set_color('none')
    ax.spines['bottom'].set_color('none')
    ax.spines['left'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.tick_params(labelcolor='w', top='off', bottom='off', left='off', right='off')
    
    ax.set_xlabel("Success Time (0 is first period, 30 is deadline)",**csfont)
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    
    ax1 = fig.add_subplot(141)
    if type == "donor":
        ax1.set_ylabel("Donor Contribution (% of Revenue)",**csfont)
    if type == "buyer":
        ax1.set_ylabel("Buyer Contribution (% of Revenue)",**csfont)
    
    sub1 = df2[(df2.length==30) & (df2.hitTime >= 27) & (df2.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    #z1 = pd.DataFrame(lowess(sub1.fracD_totalR, sub1.timeIndex,frac=1./10))
    sub2 = df2[(df2.length==30) & (df2.hitTime > 3) & (df2.hitTime < 27) & (df2.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    #z2 = pd.DataFrame(lowess(sub2.fracD_totalR, sub2.timeIndex,frac=1./10))
    sub3 = df2[(df2.length==30) & (df2.hitTime <= 3) & (df2.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    #z3 = pd.DataFrame(lowess(sub3.fracD_totalR, sub3.timeIndex,frac=1./10))
    sub1                = sub1.dropna().reset_index(drop=True)
    sub2                = sub2.dropna().reset_index(drop=True)
    sub3                = sub3.dropna().reset_index(drop=True)
    
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub1.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub1[col].values[:, np.newaxis])
    y1 = 100*model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub2.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub2[col].values[:, np.newaxis])
    y2 = 100*model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub3.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub3[col].values[:, np.newaxis])
    y3 = 100*model.predict(x_poly)
    
    ax1.plot(sub1.timeIndex, y1, label="Early Finishers",linewidth=4, linestyle="-.", color=palette[1])
    ax1.plot(sub2.timeIndex, y2, label="Middle Finishers",linewidth=4, linestyle="-", color=palette[2])
    ax1.plot(sub3.timeIndex, y3, label="Late Finishers",linewidth=4, linestyle=":", color=palette[3])
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    
    
    ax2 = fig.add_subplot(142)
    sub1 = df2bottom[(df2bottom.length==30) & (df2bottom.hitTime >= 27) & (df2bottom.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    #z1 = pd.DataFrame(lowess(sub1.fracD_totalR, sub1.timeIndex,frac=1./10))
    sub2 = df2bottom[(df2bottom.length==30) & (df2bottom.hitTime > 3) & (df2bottom.hitTime < 27) & (df2bottom.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    #z2 = pd.DataFrame(lowess(sub2.fracD_totalR, sub2.timeIndex,frac=1./10))
    sub3 = df2bottom[(df2bottom.length==30) & (df2bottom.hitTime <= 3) & (df2bottom.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    #z3 = pd.DataFrame(lowess(sub3.fracD_totalR, sub3.timeIndex,frac=1./10))
    sub1                = sub1.dropna().reset_index(drop=True)
    sub2                = sub2.dropna().reset_index(drop=True)
    sub3                = sub3.dropna().reset_index(drop=True)
    
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub1.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub1[col].values[:, np.newaxis])
    y1 = 100*model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub2.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub2[col].values[:, np.newaxis])
    y2 = 100*model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub3.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub3[col].values[:, np.newaxis])
    y3 = 100*model.predict(x_poly)
    
    ax2.plot(sub1.timeIndex, y1, label="Early Finishers",linewidth=4, linestyle="-.", color=palette[1])
    ax2.plot(sub2.timeIndex, y2, label="Middle Finishers",linewidth=4, linestyle="-", color=palette[2])
    ax2.plot(sub3.timeIndex, y3, label="Late Finishers",linewidth=4, linestyle=":", color=palette[3])
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    
    ax3 = fig.add_subplot(143)
    sub1 = df2top[(df2top.length==30) & (df2top.hitTime >= 27) & (df2top.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    #z1 = pd.DataFrame(lowess(sub1.fracD_totalR, sub1.timeIndex,frac=1./10))
    sub2 = df2top[(df2top.length==30) & (df2top.hitTime > 3) & (df2top.hitTime < 27) & (df2top.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    #z2 = pd.DataFrame(lowess(sub2.fracD_totalR, sub2.timeIndex,frac=1./10))
    sub3 = df2top[(df2top.length==30) & (df2top.hitTime <= 3) & (df2top.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    #z3 = pd.DataFrame(lowess(sub3.fracD_totalR, sub3.timeIndex,frac=1./10))
    sub1                = sub1.dropna().reset_index(drop=True)
    sub2                = sub2.dropna().reset_index(drop=True)
    sub3                = sub3.dropna().reset_index(drop=True)
    
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub1.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub1[col].values[:, np.newaxis])
    y1 = 100*model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub2.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub2[col].values[:, np.newaxis])
    y2 = 100*model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub3.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub3[col].values[:, np.newaxis])
    y3 = 100*model.predict(x_poly)
    
    ax3.plot(sub1.timeIndex, y1, label="Early Finishers",linewidth=4, linestyle="-.", color=palette[1])
    ax3.plot(sub2.timeIndex, y2, label="Middle Finishers",linewidth=4, linestyle="-", color=palette[2])
    ax3.plot(sub3.timeIndex, y3, label="Late Finishers",linewidth=4, linestyle=":", color=palette[3])
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    
    ax4 = fig.add_subplot(144)
    sub1 = df2both[(df2both.length==30) & (df2both.hitTime >= 27) & (df2both.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    #z1 = pd.DataFrame(lowess(sub1.fracD_totalR, sub1.timeIndex,frac=1./10))
    sub2 = df2both[(df2both.length==30) & (df2both.hitTime > 3) & (df2both.hitTime < 27) & (df2both.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    #z2 = pd.DataFrame(lowess(sub2.fracD_totalR, sub2.timeIndex,frac=1./10))
    sub3 = df2both[(df2both.length==30) & (df2both.hitTime <= 3) & (df2both.timeIndex >= 0)][["timeIndex",col ]].sort_values("timeIndex").reset_index(drop=True)
    #z3 = pd.DataFrame(lowess(sub3.fracD_totalR, sub3.timeIndex,frac=1./10))
    sub1                = sub1.dropna().reset_index(drop=True)
    sub2                = sub2.dropna().reset_index(drop=True)
    sub3                = sub3.dropna().reset_index(drop=True)
    
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub1.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub1[col].values[:, np.newaxis])
    y1 = 100*model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub2.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub2[col].values[:, np.newaxis])
    y2 = 100*model.predict(x_poly)
    polynomial_features= PolynomialFeatures(degree=5)
    x_poly = polynomial_features.fit_transform(sub3.timeIndex.values[:, np.newaxis])
    model = LinearRegression()
    model.fit(x_poly, sub3[col].values[:, np.newaxis])
    y3 = 100*model.predict(x_poly)
    
    ax4.plot(sub1.timeIndex, y1, label="Early Finishers",linewidth=4, linestyle="-.", color=palette[1])
    ax4.plot(sub2.timeIndex, y2, label="Middle Finishers",linewidth=4, linestyle="-", color=palette[2])
    ax4.plot(sub3.timeIndex, y3, label="Late Finishers",linewidth=4, linestyle=":", color=palette[3])
    plt.yticks(fontname = "Liberation Serif", fontsize = 14) 
    plt.xticks(fontname = "Liberation Serif", fontsize = 14) 
    
    #ax1.grid(alpha = 0.25)
    #ax2.grid(alpha = 0.25)
    #ax3.grid(alpha = 0.25)
    #ax4.grid(alpha = 0.25)
    if type == "donor":
        ax1.set_ylim([0, 100*.05])
        ax2.set_ylim([0, 100*.05])
        ax3.set_ylim([0, 100*.05])
        ax4.set_ylim([0, 100*.05])
    if type == "buyer":
        ax1.set_ylim([0, 100*.07])
        ax2.set_ylim([0, 100*.07])
        ax3.set_ylim([0, 100*.07])
        ax4.set_ylim([0, 100*.07])        
    leg = ax4.legend()
    for ll in range(len(leg.get_lines())):
        leg.get_lines()[ll].set_linewidth(3)
    plt.setp(leg.texts, family='Liberation Serif', fontsize = 14)
    ax1.set_title('Raw Data',**csfont)
    ax2.set_title('Bottom as Donation',**csfont)
    ax3.set_title('Top as Donation',**csfont)
    ax4.set_title('Both as Donation',**csfont)
    ax2.set_yticks([])
    ax3.set_yticks([])
    ax4.set_yticks([])
    #
    keepAd = ""
    if keepAll == True: keepAd = "All"
    plt.savefig(pathPlot + fname + "_"+ keepAd + "_" + shipAdjust + "_compare_all.pdf",bbox_inches='tight',format= "pdf",dpi=600)
    fig.clf()



######################################################################
# RUN THE MAIN PROGRAM
######################################################################

if __name__ == '__main__':
    df2both =  cleanData(keepAll,"all",shipAdjust)
    df2     =  cleanData(keepAll,"none",shipAdjust)
    df2bottom     =  cleanData(keepAll,"bottomP",shipAdjust)
    df2top     =  cleanData(keepAll,"topP",shipAdjust)
    plot1(df2both,df2,df2bottom,df2top,keepAll,shipAdjust, "donor")
    plot1(df2both,df2,df2bottom,df2top,keepAll,shipAdjust, "buyer")
    if logFile: logger.close()



