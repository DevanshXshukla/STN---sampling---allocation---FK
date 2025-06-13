setwd("path")
rm(list = ls())
library(RMySQL)
library(lubridate)
library(stringr)
library(dplyr)
library(data.table)
library(googlesheets4)
library(readxl)
library(fst)
library(arrow)
library(ggplot2)
library(readr)
library(tidyr)
library(splitstackshape)
library(googleCloudStorageR)
library(googledrive)
library(googlesheets4)
library(httr)

options(scipen = 99999)

gcs_save_rds <- function(input, output) {
  saveRDS(input, output)
}

gcs_save_csv <- function(input, output){
  write.csv(input, output, row.names = FALSE)
}

# googlesheets4::gs4_auth(
#   path = "\\\\172.29.66.90\\Brain\\Config Engine\\config_run_auto\\myntra-planning-81e3bc0e8693.json",
#   scopes = "https://www.googleapis.com/auth/spreadsheets"
# )


project_id <- "fks-ip-azkaban"
auth_file <- "fks-ip-azkaban-sak.json"
gcs_bucket <- "fk-ipc-data-sandbox"
gcs_bucket2 <- "fk-ipc-adhoc-data-sandbox"
gcs_auth(json_file = auth_file)
gcs_global_bucket("fk-ipc-data-sandbox")
Sys.setenv("GCS_DEFAULT_BUCKET" = gcs_bucket, "GCS_AUTH_FILE" = auth_file)
options(googleCloudStorageR.upload_limit = 5000000L)
gcs_get_object

##################### READ THE FILES 


#Put your input file here:
input_df= data.table(fread("path"))
# sh_code = data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/sourcing_hub.csv", bucket = gcs_bucket2))
# sh_code[,`:=`(facility_id = NULL)]
# setnames(sh_code , old='city',new='City')
# input_df = sh_code[input_df, on=c('City')]
# input_df = input_df[,.(FSN,business_zone,code,round(stn_creation_sh))]
setnames(input_df, old=c('Dest_code','Src_Code','Units'),new=c('darkstore','FC','quantity'))

###### Seller Removal Patch getting added 
prod_cat = gcs_get_object("ipc/Nav/IPC_Hyperlocal/prod_cat.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
prod_cat<-read.csv("temp_file.csv")

setnames(prod_cat, old='product_id',new='fsn')
prod_cat = unique(prod_cat)
setDT(prod_cat)
prod_cat[,`:=`(flag =1)]
prod_cat[,`:=`(cu =cumsum(flag)),by=.(fsn)]
prod_cat = prod_cat[cu ==1]
prod_cat[,`:=`(cu = NULL,flag = NULL)]
setnames(prod_cat, old='fsn',new='FSN')
input_df =prod_cat[input_df, on =c('FSN')]

missing = input_df[is.na(cms_vertical)]
missing = unique(missing[,.(FSN)])
#gcs_upload(missing, name = "ipc/Nav/Hyperlocal_IPC/missing_fsn_mle.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

#input files
vertical_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/seller_inputs/TCR_HL_Seller_Mapping - vertical.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
vertical_master<-read.csv("temp_file.csv")
setDT(vertical_master)
vertical_master[,`:=`(VERTICAL = tolower(VERTICAL))]

brand_vertical=gcs_get_object("ipc/Nav/IPC_Hyperlocal/seller_inputs/TCR_HL_Seller_Mapping - brand_vertical.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
brand_vertical<-read.csv("temp_file.csv")

setDT(brand_vertical)
brand_vertical[,`:=`(vertical = tolower(vertical),brand =tolower(brand))]
fsn_master=gcs_get_object("ipc/Nav/IPC_Hyperlocal/seller_inputs/fdp_exclusion.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
fsn_master<-read.csv("temp_file.csv")

setDT(fsn_master)

input_df = input_df[,`:=`(cms_vertical = tolower(cms_vertical))]
input_df = input_df[,`:=`(brand = tolower(brand))]

input_df = input_df[,`:=`(cms_flag = ifelse(cms_vertical %in% vertical_master$VERTICAL,1,0))]
input_df = input_df[,`:=`(cms_flag = ifelse(is.na(cms_flag),0,cms_flag))]

input_df[,`:=`(brand_vertical_flag = ifelse(cms_vertical %in% unique(brand_vertical$vertical) & brand %in% unique(brand_vertical$brand),1,0))]
input_df[,`:=`(brand_vertical_flag = ifelse(is.na(brand_vertical_flag),0,brand_vertical_flag))]

input_df[,`:=`(FSN_tag = ifelse(FSN %in% unique(fsn_master$a.fsn),1,0))]
input_df[,`:=`(FSN_tag = ifelse(is.na(FSN_tag),0,FSN_tag))]
input_df[,`:=`(final = FSN_tag + cms_flag + brand_vertical_flag)]
input_df[,`:=`(final = ifelse(final>0,1,final))]


#fwrite(input_df, "D://Users//divanshu.d//Downloads//April//Large STNs 11th April Script and details//lfc_req_updated_after_seller_fps_all_data.csv")

exclusion_list=gcs_get_object("ipc/Nav/IPC_Hyperlocal/seller_inputs/exclusion_list.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
exclusion_list<-read.csv("temp_file.csv")

setDT(exclusion_list)

exclusion_list = exclusion_list[date < Sys.Date()]
exclusion_list[,`:=`(to_exclude =1)]
exclusion_list[,`:=`(date = NULL)]
exclusion_list = unique(exclusion_list)
exclusion_list[,`:=`(fsn = as.character(fsn),DS =as.character(DS))]
setnames(exclusion_list, old='fsn',new='FSN')
setnames(input_df, old='darkstore',new='DS')
exclusion_list_all= exclusion_list[DS =='all']
exclusion_list = exclusion_list[DS !='all']

input_df =exclusion_list[input_df, on=c('FSN','DS')]
input_df[,`:=`(to_exclude = ifelse(is.na(to_exclude),0,to_exclude))]
input_df[,`:=`(to_exclude = ifelse(FSN %in% unique(exclusion_list_all$FSN),1,to_exclude))]
input_df[,`:=`(final=ifelse(to_exclude ==1,0,final))]
input_df = input_df[final ==0]

###################### excluded fsn extracted
excluded_fsn_df <- input_df[final == 1]
excluded_fsn_df <- unique(excluded_fsn_df[, .(fsn, fc, darkstore)])
fwrite(excluded_fsn_df, "excluded_fsn.csv")
######################

inventory = data.table(gcs_get_object("ipc/Nav/Hyperlocal_IPC/Inventory_Snapshot.csv", bucket = gcs_bucket,saveToDisk = "temp_file.csv",overwrite = TRUE))
inventory<-read.csv("temp_file.csv")
setDT(inventory)
inventory = inventory [fc_area =='store']

whitelist = data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/Whitelisted_Sellerlist.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE))
whitelist<-read.csv("temp_file.csv")
#setnames(input_df, old='darkstore',new='DS')
input_df = input_df[,.(FSN,FC,quantity,DS)]
###################### INPUTNULL###################### INPUT PROCESSING

input_df <- input_df[quantity > 0]

setnames(input_df, c('FSN',  'FC', 'quantity'), c('fsn', 'fc', 'Quantity'))

input_df[, fsn := toupper(trimws(fsn))]
input_df[, fc := tolower(trimws(fc))]
setnames(input_df, old='DS',new='darkstore')
input_df[, darkstore := tolower(trimws(darkstore))]

# Group by fsn, darkstore, and fc and calculate sum of Quantity
grouped_input <- input_df[, .(Quantity = sum(Quantity)), by = .(fsn, darkstore, fc)]

head(grouped_input)

###################### INVENTORY PROCESSING

inventory[, fsn := toupper(trimws(fsn))]
inventory[, fc := tolower(trimws(fc))]
inventory[, fc_area := tolower(trimws(fc_area))]
inventory[, atp := as.numeric(atp)]
inventory[is.na(atp), atp := 0]

valid_fcs <- grouped_input$fc
filtered_inventory <- inventory[fc_area == 'store' & fc %in% valid_fcs]
setDT(whitelist)
whitelist[,`:=`(seller_id = as.character(seller_id))]
filtered_inventory[, seller_id := as.character(seller_id)]
filtered_inventory <- filtered_inventory[seller_id %in% whitelist$seller_id]
filtered_inventory[, inv_index := .I]
###################### ALLOCATION LOGIC

# For unique referencing


# Allocation logic
allocations <- list()

for (i in seq_len(nrow(grouped_input))) {
  fsn_val <- grouped_input$fsn[i]
  darkstore_val <- grouped_input$darkstore[i]
  fc_val <- grouped_input$fc[i]
  qty <- grouped_input$Quantity[i]
  allocated <- 0
  
  matching_inventory <- filtered_inventory[fsn == fsn_val & fc == fc_val & atp > 0]
  
  for (j in seq_len(nrow(matching_inventory))) {
    if (allocated >= qty) break
    
    to_allocate <- min(matching_inventory$atp[j], qty - allocated)
    
    if (to_allocate > 0) {
      allocations <- append(allocations, list(list(
        darkstore = darkstore_val,
        fsn = fsn_val,
        source = fc_val,
        Quantity = to_allocate,
        lot_id = matching_inventory$lot_id[j],
        seller_id = matching_inventory$seller_id[j]
      )))
      
      inv_idx <- matching_inventory$inv_index[j]
      filtered_inventory[inv_idx, atp := atp - to_allocate]
      allocated <- allocated + to_allocate
    }
  }
}

alloc_df <- rbindlist(allocations)

quantity_allocated <- sum(alloc_df$Quantity)
quantity_input <- sum(grouped_input$Quantity)

DS_Master = data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE))
DS_Master<-read.csv("temp_file.csv")
setDT(DS_Master)
DS_Master = DS_Master[,.(DS,Cron_Timing)]
setnames(DS_Master , old='DS',new='darkstore')
alloc_df = DS_Master[alloc_df, on=c('darkstore')]

morning = alloc_df[Cron_Timing %in% c('M','ME')]
evening = alloc_df[Cron_Timing %in% c('E','ME')]


#output
fwrite(alloc_df,"path")

