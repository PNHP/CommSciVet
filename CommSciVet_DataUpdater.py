# Name: Community Science Vetting Database Updater
# Purpose: Take iNat exports and compare records to those that are already in the CommSciVet iNat feature class for new,
# updated, and unchanged records. Load new records into FC, update records that have been updated since they were loaded,
# ignore records that are unchanged.
# Author:
# Created: 01/24/2023
# Updates:
#-------------------------------------------------------------------------------

# Import modules
import arcpy
import datetime
import numpy as np
import os
import sys
import time
from getpass import getuser
import pandas as pd

# Set tools to overwrite existing outputs
arcpy.env.overwriteOutput = True
arcpy.env.workspace = "memory"

# set path for iNat download - must be .csv
inat_csv = r"W:\Heritage\Heritage_Data\CommSciVet\iNatDataTest.csv"

# set path for existing CommSciVet FC
comm_fc = r"W:\\Heritage\\Heritage_Data\\CommSciVet\\CommSciVet GDB\\CommSciVet.gdb\\CommSciVet"

# import iNat .csv file as Pandas dataframe
inat_df = pd.read_csv(inat_csv)

# create new fields for feature_latitude and feature_longitude in inat_df dataframe with private lat/longs where available
inat_df['feature_latitude'] = inat_df['private_latitude']
inat_df['feature_longitude'] = inat_df['private_longitude']

# fill NaN values in feature_latitude and feature_longitude with public latitude and longitude values
inat_df['feature_latitude'] = inat_df['feature_latitude'].fillna(inat_df['latitude'])
inat_df['feature_longitude'] = inat_df['feature_longitude'].fillna(inat_df['longitude'])

# add 'record_status' column to inat_df Pandas dataframe with no data in it
inat_df.insert(loc = 1, column = "record_status", value = "")

# get current date in 'mm/dd/yyyy' format
today = datetime.date.today()
day = today.strftime("%m/%d/%y")

# add 'import_date' column to inat_df Pandas dataframe filled with current date
inat_df.insert(loc = 2, column = "import_date", value = day)

# create temporary FC in memory workspace from inat_df Pandas dataframe - not sure how to do this




# get list of ids from old data
old_ids = sorted({row[0] for row in arcpy.da.SearchCursor(comm_fc,"id")})
# get list of ids from new data
new_ids = "<enter search cursor for ids once temp fc is created>"


# create dictionary of ids with corresponding data from new data from iNat export - LOOK AT old_et_dict below for example code
















###########################################################
### BELOW IS FROM A DIFFERENT SCRIPT THAT COMPARES VALUES.. IT SHOULD BE USEFUL FOR COMPARING NEW TO OLD VALUES
###########################################################

# records in new_elsubids that are not in old_elsubids, so therefore classified as added
arcpy.AddMessage("Checking for ELSUBID additions")
added_elsubids = np.setdiff1d(new_elsubids, old_elsubids)
for a in added_elsubids:
    values = [a, "ELSUBID addition", None, a, export_date]
    with arcpy.da.InsertCursor(et_change, insert_fields) as cursor:
        cursor.insertRow(values)

# records in old_elsubids that are not in new_elsubids, so therefore classified as deleted
arcpy.AddMessage("Checking for ELSUBID deletions")
deleted_elsubids = np.setdiff1d(old_elsubids, new_elsubids)
for d in deleted_elsubids:
    values = [d, "ELSUBID deletion", d, None, export_date]
    with arcpy.da.InsertCursor(et_change, insert_fields) as cursor:
        cursor.insertRow(values)

old_et_dict = {int(row[0]): [row[1:]] for row in arcpy.da.SearchCursor(et_old,
                                                                       ["ELSUBID", "ELCODE", "SNAME", "SCOMNAME",
                                                                        "GRANK", "SRANK", "EO_Track", "USESA", "SPROT",
                                                                        "PBSSTATUS", "SGCN", "SENSITV_SP", "ER_RULE"])}

change_fields = ["ELCODE", "SNAME", "SCOMNAME", "GRANK", "SRANK", "EO_Track", "USESA", "SPROT", "PBSSTATUS", "SGCN",
                 "SENSITV_SP", "ER_RULE"]
dict_value_index = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
for c, i in zip(change_fields, dict_value_index):
    arcpy.AddMessage("Checking for changes in " + c)
    with arcpy.da.SearchCursor(et_new, ["ELSUBID", c]) as cursor:
        for row in cursor:
            for k, v in old_et_dict.items():
                if k == int(row[0]):
                    if row[1] == v[0][i]:
                        pass
                    else:
                        values = [int(row[0]), c, v[0][i], row[1], export_date]
                        with arcpy.da.InsertCursor(et_change, insert_fields) as cursor:
                            cursor.insertRow(values)
