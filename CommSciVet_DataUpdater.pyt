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
import pandas as pd
from arcgis.features import GeoAccessor

# Set tools to overwrite existing outputs
arcpy.env.overwriteOutput = True
arcpy.env.workspace = "memory"

# Begin toolbox
class Toolbox(object):
    def __init__(self):
        self.label = "CommSciVet_Updates"
        self.alias = "CommSciVet_Updates"
        self.canRunInBackground = False

        # List of tool classes associated with this toolbox
        self.tools = [CommSciVet_update]

class CommSciVet_update(object):
    def __init__(self):
        self.label = "Update CommSciVet Data "
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        csv = arcpy.Parameter(
            displayName = "csv",
            name = "csv",
            datatype = "DEFile",
            parameterType = "Required",
            direction = "Input")
        params = [csv]
        return params

    def execute(self, params, messages):
        """The source code of the tool."""
        inat_csv = params[0].valueAsText

        # set path for iNat download - must be .csv
        inat_fields = ["id", "observed_on", "user_id", "user_login", "created_at", "updated_at", "license", "url",
                       "image_url", "sound_url", "tag_list", "description", "captive_cultivated", "place_guess",
                       "latitude", "longitude", "positional_accuracy", "private_place_guess", "private_latitude",
                       "private_longitude", "public_positional_accuracy", "geoprivacy", "taxon_geoprivacy",
                       "coordinates_obscured", "positioning_method", "positioning_device", "place_county_name",
                       "scientific_name", "common_name", "iconic_taxon_name", "taxon_id", "taxon_order_name",
                       "taxon_family_name", "field_count", "SHAPE@"]

        # set path for existing CommSciVet FC
        comm_fc = r"W:\\Heritage\\Heritage_Data\\CommSciVet\\CommSciVet GDB\\CommSciVet.gdb\\CommSciVet"
        record_fields = ["record_status", "import_date"]
        comm_fields = inat_fields + record_fields

        # set path for iNat Changes table
        inat_changes = r"W:\\Heritage\\Heritage_Data\\CommSciVet\\CommSciVet GDB\\CommSciVet.gdb\\inat_changes"
        insert_fields = ["id", "change", "old_value", "new_value", "updated_at"]

        # import iNat .csv file as Pandas dataframe
        inat_df = pd.read_csv(inat_csv)

        # create new fields for feature_latitude and feature_longitude in inat_df dataframe with private lat/longs where available
        inat_df['feature_latitude'] = inat_df['private_latitude']
        inat_df['feature_longitude'] = inat_df['private_longitude']

        # fill NaN values in feature_latitude and feature_longitude with public latitude and longitude values
        inat_df['feature_latitude'] = inat_df['feature_latitude'].fillna(inat_df['latitude'])
        inat_df['feature_longitude'] = inat_df['feature_longitude'].fillna(inat_df['longitude'])

        # get current date in 'mm/dd/yyyy' format
        today = datetime.date.today()
        import_day = today.strftime("%m/%d/%y")

        # create spatially enabled dataframe and then export to feature class
        inat_sdf = GeoAccessor.from_xy(inat_df, "feature_longitude", "feature_latitude", sr=4326)
        inat_sdf.rename(columns={'field:count': 'field_count'},
                        inplace=True)  # rename field:count to deal with bad character
        # change all date fields to datetime format instead of string
        inat_sdf['created_at'] = pd.to_datetime(inat_sdf['created_at'])
        inat_sdf['updated_at'] = pd.to_datetime(inat_sdf['updated_at'])
        inat_sdf['observed_on'] = pd.to_datetime(inat_sdf['observed_on'])

        # export spatially enabled df to feature class
        temp_inat_import = inat_sdf.spatial.to_featureclass(
            location=os.path.join(os.path.dirname(comm_fc), "temp_inat_import"))

        # project into custom albers using existing CommSciVet layer
        sref = arcpy.Describe(comm_fc).spatialReference
        temp_inat_import = arcpy.Project_management(temp_inat_import,
                                                    os.path.join(os.path.dirname(comm_fc), "temp_inat_import_albers"),
                                                    sref)

        # get list of ids from existing data in the CommSciVet feature layer
        old_ids = sorted({row[0] for row in arcpy.da.SearchCursor(comm_fc, "id")})
        # get list of ids from new data
        new_ids = sorted({row[0] for row in arcpy.da.SearchCursor(temp_inat_import, "id")})

        # records in new_ids that are not in old_ids, so therefore classified as added
        arcpy.AddMessage("Checking for additions...")
        # gets list of added ids
        added_ids = np.setdiff1d(new_ids, old_ids)

        # check to see if there are added records, if no report that there are not added records. if so, move on to next steps
        if len(added_ids) == 0:
            arcpy.AddMessage("There are no new records to import")
        else:
            # for all added ids, insert a change record in the inat change table
            for a in added_ids:
                values = [a, "addition", None, a, import_day]
                with arcpy.da.InsertCursor(inat_changes, insert_fields) as cursor:
                    cursor.insertRow(values)

            # get dictionary of all data for added records
            added_dict = {int(row[0]): row[0:] for row in arcpy.da.SearchCursor(temp_inat_import, inat_fields) if
                          row[0] in added_ids}
            # add record tracking values and insert new records into CommSciVet layer
            arcpy.AddMessage("The following records were added:")
            for k, v in added_dict.items():
                record_tup = ("added", import_day)
                v = v + record_tup
                arcpy.AddMessage(v)
                with arcpy.da.InsertCursor(comm_fc, comm_fields) as cursor:
                    cursor.insertRow(v)

        # create dictionary of all data in iNat import that was not classified as added
        inat_import_dict = {int(row[0]): [row[0:]] for row in arcpy.da.SearchCursor(temp_inat_import,
                                                                                    inat_fields) if
                            row[0] not in added_ids}

        # get list of dictionary index values for the length of fields
        dict_value_index = range(0, len(inat_fields))

        # loop through each field and check for changes
        for c, i in zip(inat_fields, dict_value_index):
            arcpy.AddMessage("Checking for changes in " + c)
            with arcpy.da.SearchCursor(comm_fc, ["id", c]) as cursor:
                for row in cursor:
                    for k, v in inat_import_dict.items():
                        if k == int(row[0]):  # check if id matches row
                            if row[1] == v[0][i]:  # check if new and existing values match - if match, then pass
                                pass
                            else:
                                # get values and deal with shape token issue (shape token cannot be written to text field)
                                if c == "SHAPE@":
                                    values = [int(row[0]), c, "updated_geometry", "updated_geometry", import_day]
                                else:
                                    values = [int(row[0]), c, row[1], str(v[0][i]), import_day]
                                # insert changes into iNat change table
                                with arcpy.da.InsertCursor(inat_changes, insert_fields) as cursor:
                                    cursor.insertRow(values)

            # get updated values and update those values in CommSciVet layer
            with arcpy.da.UpdateCursor(comm_fc, ["id", c, "record_status", "import_date"]) as cursor:
                for row in cursor:
                    for k, v in inat_import_dict.items():
                        if k == int(row[0]):
                            if row[1] == v[0][i]:
                                pass
                            else:
                                arcpy.AddMessage("The was an update to " + c + " for ID# " + str(row[0]))
                                row[1] = v[0][i]
                                row[2] = "updated"
                                row[3] = import_day
                                cursor.updateRow(row)

        return
