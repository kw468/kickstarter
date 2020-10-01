#!/bin/bash
#"""
#    COmbines All Comments Gathered From allSuccessfulProjects
#    Kickstarter Project
#--------------------------------------------------------------------------------
#change log:
#    v0.0.1  Tues 23 Sep 2020
#-------------------------------------------------------------------------------
#notes:
#
#--------------------------------------------------------------------------------
#contributors:
#    Kevin:
#        name:       Kevin Williams
#        email:      kevin.williams@yale.edu
#--------------------------------------------------------------------------------
#Copyright 2020 Yale University
#"""

cd "/gpfs/project/kickstarter/comments_and_updates/comments_clean"
find -type f -name '*.csv' -exec cat {} \; > "/gpfs/project/kickstarter/agg/projectCommentData_history.csv"