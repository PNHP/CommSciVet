# title: "iNat Project"
# output: pdf_document

# load packages
library(rinat)
library(here)
library(readxl)
library(tidyverse)
library(sf)
library(arcgisbinding)
library(filelock)
arc.check_product()

# set working paths
et <- "W://Heritage//Heritage_Data//Biotics_datasets.gdb//ET"

# read in ET table from Biotics export gdb
et <- arc.open(et)
element_tracking <- arc.select(et)

# get list of SNAMEs for tracked and watchlist species to use in iNat query
inat_species <- element_tracking[which(element_tracking$EO_TRACK == "Y" | element_tracking$EO_TRACK == "W"),]$SNAME

################################################################################
# THIS IS PULLING ALL DATA IN PA WHERE SNAME MATCHES TRACKED AND WATCHLIST SPECIES FROM CURRENT ET EXPORT
################################################################################
# Function to pull data from iNaturalist
a <- list()
k <- NULL

for(x in 1:length(inat_species)){
  #get metadata on the number of occurrences
  print(paste("getting metadata from iNaturalist for ",inat_species[x],".", sep="") )
  try(k <- get_inat_obs(taxon_name=inat_species[x], bounds=c(39.7198, -80.519891, 42.26986,	-74.689516) , geo=TRUE, meta=TRUE) ) # this step first queries iNat to see if there are any records present, if there are it actually downloads them.
  Sys.sleep(10) # this is too throttle our requests so we don't overload their servers
  if(is.list(k)){
    print(paste("There are ", k$meta$found, " records on iNaturalist", sep=""))
    if(k$meta$found>0){
      a[[x]] <- get_inat_obs(taxon_name=inat_species[x], bounds=c(39.7198, -80.519891, 42.26986,	-74.689516) , geo=TRUE, maxresults = k$meta$found) 
      k <- NULL
    } else {}
  } else {
    print("No records found")
  }
}

################################################################################
# THIS PULLS ALL RECORDS FROM PNHP INAT PROBJECT
################################################################################
# function pulling data from PNHP project page
r_project_data = get_inat_obs_project("pennsylvania-natural-heritage-program",type="observations")

# reformatting tag_list column to change from list to characters
r_project_data$tag_list = as.character(r_project_data$tag_list)
r_project_data$photos = NA
r_project_data[r_project_data == "character(0)"] = NA
r_project_data$tag_list = gsub("^c\\(|\\)$|\"","",r_project_data$tag_list)

# writing r data to csv
write.csv(r_project_data,file="R_iNatProject_data.csv",row.names = FALSE)

# reading in manual data
manual_project_data = read_csv("observations-252324.csv")

# comparing field presence in different downloads
only_r = setdiff(names(r_project_data),names(manual_project_data))
only_manual = setdiff(names(manual_project_data),names(r_project_data))
both = intersect(names(manual_project_data),names(r_project_data))

# writing fields list to csv 
max_length = max(c(length(only_r),length(only_manual),length(both)))
project_data_fields = data.frame(
  only_r = c(only_r,rep(NA,max_length-length(only_r))),
  only_manual = c(only_manual,rep(NA,max_length-length(only_manual))),
  both = c(both,rep(NA,max_length-length(both)))
)
write.csv(project_data_fields,file="Project_data_fields.csv",row.names = FALSE)

# Organizing downloaded data - csv of missing species and found species
empty_inat = rep(NA,length(a))
for(ii in 1:length(a)){
  empty_inat[ii] = is_empty(a[[ii]])
}
missing_inat = inat_species[which(empty_inat == TRUE)]
found_inat = inat_species[which(empty_inat == FALSE)]
found_index = which(empty_inat == FALSE)

write.csv(missing_inat,file="Missing_Species.csv",row.names = FALSE)
write.csv(found_inat,file="Found_Species.csv",row.names = FALSE)

# Creating dataframe of downloaded data
downloaded_data = data.frame(matrix(ncol = ncol(a[[1]]), nrow = 0))
colnames(downloaded_data) = colnames(a[[1]])
for(ii in found_index){
  downloaded_data = rbind(downloaded_data,a[[ii]])
}

#replacing blanks with NA for easy data management
downloaded_data[downloaded_data == "" | downloaded_data == " "] = NA

#removing duplicate records that were downloaded
unique_inat_data = downloaded_data[!duplicated(downloaded_data),]


write.csv(unique_inat_data,file="Downloaded_Data.csv",row.names = FALSE)

# Save an object to a file - was used for the initial download from iNat - not really needed
saveRDS(a, file = "inat-data.rds")
# Restore the object
readRDS(file = "inat-data.rds")

# custom albers projection
customalbers <- "+proj=aea +lat_1=40 +lat_2=42 +lat_0=39 +lon_0=-78 +x_0=0 +y_0=0 +ellps=GRS80 +units=m +no_defs"

# creating feature class of downloaded data
inat_sf <- st_as_sf(unique_inat_data, coords=c("longitude","latitude"), crs="+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
inat_sf_test <- st_transform(inat_sf, crs=customalbers) # reproject to the custom albers

arc.write("C://Users/nsingh/Documents/ArcGIS/Projects/iNaturalistData/iNaturalistData.gdb/inat_data",inat_sf_test,overwrite = TRUE)

#Downloading data from ArcGIS
arc_open = arc.open("C://Users/nsingh/Documents/ArcGIS/Projects/iNaturalistData/iNaturalistData.gdb/inat_data")
arc_sf = arc.select(arc_open)

#Converting ArcGIS to good format
arc_database = data.frame(head(arc_sf, 6)) # only have head() for test
arc_database = arc_database[,-1]
arc_database[] = lapply(arc_database, as.character)

#Converting downloaded data to good format
inat_download = read_csv("Downloaded_Data.csv")
numeric_columns = names(which(sapply(inat_download,class) == "numeric"))
inat_download = data.frame(head(unique_inat_data,6)) # only have head() for test
inat_download[] = lapply(inat_download, as.character)
inat_download[inat_download == "" | inat_download == " "] = NA


################################################################################
## USING DATA DOWNLOADED FROM PETE
################################################################################

#Converting Pete's data into good format
pete_data = read_xlsx("iNat fields.xlsx",sheet = "example data")
pete_num_cols = names(which(sapply(pete_data,class) == "numeric"))

col_reorder = na.exclude(match(names(inat_download),names(pete_data)))
col_reorder = c(col_reorder,setdiff(1:ncol(pete_data),col_reorder))
pete_data = pete_data[,col_reorder]

# filling in private coordinates 
for(ii in 1:ncol(pete_data)){
  if(is.na(pete_data[ii,"private_latitude"] &
           pete_data[ii,"private_longitude"])){
    pete_data[ii,"private_latitude"] = pete_data[ii,"latitude"]
    pete_data[ii,"private_longitude"] = pete_data[ii, "longitude"]
  }
}

#New data added in for test - purely done for tests
test_id = inat_download[2,12]
new_row = c("test")
inat_download[2,] = new_row
inat_download[2,5:6] = c(40.464228, -79.976351)
inat_download[2,12] = test_id
inat_download[2,31] = "2022-06-20 16:44:03 UTC"
inat_download = rbind(inat_download, new_row)
inat_download[7,12] = 123456
inat_download[7,5:6] = c(40.464228, -79.976351)


#Finding new inat data
new_test = rep(NA,nrow(inat_download))
index = 1
for(ii in inat_download$id){
  new_test[index] = is.element(ii,arc_database$id)
  index = index+1
}

new_inat_data = inat_download[which(new_test == FALSE),]
new_inat_data[numeric_columns] = sapply(new_inat_data[numeric_columns],as.numeric)

#Finding updated inat data
old_inat_data = inat_download[which(new_test == TRUE),]
updated_test = rep(NA,nrow(old_inat_data))
index = 1
for(ii in old_inat_data$id){
  arc_row = which(arc_database$id == ii)
  inat_row = which(old_inat_data$id == ii)
  equal_test = setequal(arc_database[arc_row,1:34],
                        old_inat_data[inat_row,c(1:4,7:36)])
  if(equal_test==FALSE){
    date_test = as.POSIXlt(arc_database[arc_row,29],
                           format="%Y-%m-%d %H:%M:%S",tz="UTC") < old_inat_data[inat_row,31]
    if(date_test == TRUE){
      updated_test[index] = FALSE
    } else {
      updated_test[index] = TRUE
    } 
  } else {
    updated_test[index] = TRUE
  }
  index = index+1
}
updated_inat_data = old_inat_data[which(updated_test == FALSE),]
updated_inat_data[numeric_columns] = sapply(updated_inat_data[numeric_columns],as.numeric)


#date_time = gsub("-|:| ","_",as.character(Sys.time())) - was needed for overwrite issue

# Creating new inat data feature class
new_inat <- st_as_sf(new_inat_data, coords=c("longitude","latitude"), crs="+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
new_inat <- st_transform(new_inat, crs=customalbers) # reproject to the custom albers
arc.write("C://Users/nsingh/Documents/ArcGIS/Projects/iNaturalistData/iNaturalistData.gdb/new_inat",new_inat,overwrite = TRUE,validate=TRUE)


# Creating updated inat data feature class
updated_inat <- st_as_sf(updated_inat_data, coords=c("longitude","latitude"), crs="+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
updated_inat <- st_transform(updated_inat, crs=customalbers) # reproject to the custom albers
arc.write("C://Users/nsingh/Documents/ArcGIS/Projects/iNaturalistData/iNaturalistData.gdb/updated_inat",updated_inat,overwrite = TRUE,validate=TRUE)


# Creating test database feature class - purely for tests
test_db = data.frame(head(unique_inat_data,6))
test_db[integer_columns] = sapply(test_db[integer_columns],as.integer)
test_inat_db <- st_as_sf(test_db, coords=c("longitude","latitude"), crs="+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
test_inat_db <- st_transform(test_inat_db, crs=customalbers) # reproject to the custom albers
arc.write("C://Users/nsingh/Documents/ArcGIS/Projects/iNaturalistData/iNaturalistData.gdb/test_inat_db",test_inat_db,overwrite = TRUE,validate=TRUE)


# Python script
---------------
  
  #Adding new data
  arcpy.management.Append('new_inat', 'test_inat_db',schema_type="NO_TEST")

---------------
  #Updating data
  def a(path):
  field_names = []
fields = arcpy.ListFields(path)
for field in fields:
  field_names.append(field.name)
return field_names

iNat_fields = a("C://Users/nsingh/Documents/ArcGIS/Projects/Practice - iNaturalist data/updated_inat.shp")

updated_inat = "C://Users/nsingh/Documents/ArcGIS/Projects/Practice - iNaturalist data/updated_inat.shp"

test_inat_db = "C://Users/nsingh/Documents/ArcGIS/Projects/Practice - iNaturalist data/test_inat_db.shp"

iNat_dict = {}  

with arcpy.da.SearchCursor(updated_inat, iNat_fields) as cursor:
  for row in cursor:
  iNat_dict[row[11]] = row[1:]

with arcpy.da.UpdateCursor(test_inat_db, iNat_fields) as cursor:
  for row in cursor:
  for k, v in iNat_dict.items():
  if k == row[11]:
  row[1] = v[0]
row[2] = v[1]
row[3] = v[2]
row[4] = v[3]
row[5] = v[4]
row[6] = v[5]
row[7] = v[6]
row[8] = v[7]
row[9] = v[8]
row[10] = v[9]
row[11] = v[10]
row[12] = v[11]
row[13] = v[12]
row[14] = v[13]
row[15] = v[14]
row[16] = v[15]
row[17] = v[16]
row[18] = v[17]
row[19] = v[18]
row[20] = v[19]
row[21] = v[20]
row[22] = v[21]
row[23] = v[22]
row[24] = v[23]
row[25] = v[24]
row[26] = v[25]
row[27] = v[26]
row[28] = v[27]
row[29] = v[28]
row[30] = v[29]
row[31] = v[30]
row[32] = v[31]
row[33] = v[32]
row[34] = v[33]
row[35] = v[34]
cursor.updateRow(row)
```