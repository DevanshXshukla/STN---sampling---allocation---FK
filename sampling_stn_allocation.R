#################################

# SAMPLING MASTER

#################################

# Objective: This script creates sampling master same as the DS Target Master
# Created Date: 04-06-2024

##################################################################################

setwd("path")

##################################################################################

#                             IMPORTING LIBRARIES                                #

##################################################################################

library(tidyverse)
library(data.table)
library(stringr)
library(dplyr)
library(readxl)
library(googleCloudStorageR)
library(googledrive)
library(googlesheets4)
library(reshape2)
library(httr)
library(gmailr)
library(rmarkdown)
library(readr)
library(parallel)
library(purrr)
library(tidyr)
library(tibble)
library(readr)

##################################################################################

#                             CONFIGURATION  FILES                               #

##################################################################################

gcs_save_rds <- function(input, output) {
  saveRDS(input, output)
}

gcs_save_csv <- function(input, output){
  write.csv(input, output, row.names = FALSE)
}

project_id <- "fks-ip-azkaban"
auth_file <- "fks-ip-azkaban-sak.json"
gcs_bucket <- "fk-ipc-data-sandbox"
gcs_bucket2 <- "fk-ipc-adhoc-data-sandbox"
gcs_auth(json_file = auth_file)
gcs_global_bucket("fk-ipc-data-sandbox")
gcs_global_bucket(gcs_bucket2)
options(googleCloudStorageR.upload_limit = 2000000000L)
Sys.setenv("GCS_DEFAULT_BUCKET" = gcs_bucket2, "GCS_AUTH_FILE" = auth_file)

print(1)

##################################################################################

#                             GETTING INPUT FILES FROM GCP                       #

##################################################################################

dsit <- gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_IT.csv", bucket = gcs_bucket, saveToDisk = "dsit_data_.csv", overwrite = TRUE)
dsit <- fread("dsit_data_.csv")
dsit = dsit[Inwarding_Status %in% c('INITIATED','IN_TRANSIT','INWARDING')]
dsit = dsit[,.(IWIT = sum(IWIT_Intransit)),by=.(Dest_FC,fsn)]
setnames(dsit, old =c('Dest_FC'),new=c('Darkstore'))
setnames(dsit, old =c('fsn'),new=c('FSN'))

inventory_snapshot <- gcs_get_object("ipc/Nav/Hyperlocal_IPC/Inventory_Snapshot.csv", bucket = gcs_bucket, saveToDisk = "inventory_snapshot2503.csv", overwrite = TRUE)
inv_snapshot <- fread("inventory_snapshot2503.csv")
store_inv <- inv_snapshot %>% filter(fc_area == "store")
setDT(store_inv)
store_inv = store_inv[,.(atp = sum(atp)),by=.(fsn,fc)]

ds_master <- gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2, saveToDisk = "dsss_master.csv", overwrite = TRUE)
ds_master <- fread("dsss_master.csv")

live_ds_master <- ds_master %>% filter(Live == "1")
live_ds_master <- live_ds_master %>% mutate(City = tolower(City))
live_ds_master <- live_ds_master %>% group_by(City) %>% mutate(darkstore_count = n_distinct(DS))


ds_master_new <- gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master_New.csv", bucket = gcs_bucket2, saveToDisk = "ds_master_new.csv", overwrite = TRUE)
ds_master_new <- fread("ds_master_new.csv")
ds_master_new = ds_master_new[,.(DS,sh_code)]

sh_master <- gcs_get_object("ipc/Nav/IPC_Hyperlocal/sourcing_hub.csv", bucket = gcs_bucket2, saveToDisk = "sh_master.csv", overwrite = TRUE)
sh_master <- fread("sh_master.csv")
# 
# ds_norms <- gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_Target_Master.csv", bucket = gcs_bucket, saveToDisk = "ds_norms.csv", overwrite = TRUE)
# ds_norms <- fread("ds_norms.csv")

fdp_exclusion <- fread("fdp_exclusion_new.csv")
exclusion_list <- fread("exclusion_list_new.csv")

sh_ds<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Network_Master.csv",bucket = gcs_bucket2,saveToDisk = "DS_Network_Master.csv",overwrite = TRUE)
sh_ds<- read.csv("DS_Network_Master.csv")

sh_ds <- sh_ds %>% mutate(sh_code = ifelse(SH_P1 == "", SH_P0, SH_P1))

print(2)

##################################################################################

#         READING INPUT FILES FROM THE MINUTES SAMPLING TRACKER SHEET            #

##################################################################################

sampling_sheet <- "https://docs.google.com/spreadsheets/d/1E4_hWb1KPl2vY08ItK496ns8Eh6xSBd_O53ekFnmHd8/edit?gid=1433017225#gid=1433017225"

input_file <- read_sheet(sampling_sheet, sheet = "sampling_input_file")
input_file <- input_file %>% filter(!is.na(FSN))
input_file <- input_file %>% filter(!is.na(casepacksize))
input_file <- input_file %>% mutate(City = tolower(City))

input_file <- input_file %>% distinct(`Input Date`, FSN, City, Qty, `Input From`, Reasons, casepacksize, .keep_all = TRUE) %>%
  select(`Input Date`, `Start Date`, `End Date`, FSN, City, Qty, `Input From`, Reasons, casepacksize, case_quantity)

print(sum(input_file$Qty))

ask_summary <- input_file %>%
  group_by(Reasons) %>%
  summarise(
    Qty = sum(Qty, na.rm = TRUE)
  ) %>% arrange(desc(Qty))

grandtotal_summary <- input_file %>%
  summarise(
    Qty = sum(Qty, na.rm = TRUE)
  ) %>% arrange(desc(Qty))

ask_summary__ <- bind_rows(ask_summary, grandtotal_summary)

print(ask_summary__)

print(3)

##################################################################################

#                GETTING LIVE DARKSTORES, SRC_FC, SH_ATP, DS_ATP, IWIT           #

##################################################################################

input_file <- input_file %>%
  filter(grepl("^\\d+(\\.\\d+)?$", as.character(casepacksize))) %>%
  mutate(casepacksize = as.numeric(casepacksize))

input_file <- left_join(input_file, live_ds_master %>% select(City, DS, Cron_Timing, Live, darkstore_count), by = c("City"))
input_file <- input_file %>% filter(Live == "1")

input_file <- left_join(input_file, sh_ds %>% select(DS, sh_code), by = "DS")

master <- merge(input_file, store_inv, by.x = c("FSN", "sh_code"), by.y = c("fsn", "fc"), all.x = TRUE)
master <- master %>% rename(sh_atp = atp)

master <- merge(master, store_inv, by.x = c("FSN", "DS"), by.y = c("fsn", "fc"), all.x = TRUE)
master <- master %>% rename(ds_atp = atp)

master <- merge(master, dsit, by.x = c("FSN", "DS"), by.y = c("FSN", "Darkstore"), all.x = TRUE)

master <- master%>% mutate(
  sh_atp = ifelse(is.na(sh_atp), 0, sh_atp),
  ds_atp = ifelse(is.na(ds_atp), 0, ds_atp),
  IWIT = ifelse(is.na(IWIT), 0, IWIT),
  ATP_IT = ds_atp + IWIT,
  ATP_IT = ifelse(is.na(ATP_IT), 0, ATP_IT)
)

master <- master %>% mutate(
  sh_atp_pivot = as.integer(sh_atp/darkstore_count)
  
)

print(4)

##################################################################################

#        GETTING MINUTES REALISTIC DEMAND OUTLOOK TO GET DEMAND WEIGHT           #

##################################################################################

minutes_demand_playbook = (gcs_get_object("ipc/Nav/IPC_Hyperlocal/DRR/Minutes Demand Playbook.csv", bucket = gcs_bucket2, saveToDisk = "minutes_demand_.csv", overwrite = TRUE))
minutes_demand_playbook <- fread("minutes_demand_.csv")
minutes_demand_playbook =  minutes_demand_playbook[-(1:9), ]   
colnames(minutes_demand_playbook) <- as.character(minutes_demand_playbook[1, ])
minutes_demand_playbook <- minutes_demand_playbook[-1, ]
minutes_demand_playbook <- minutes_demand_playbook[-1, ]
minutes_demand_playbook<- melt(minutes_demand_playbook, id.vars = c("Store code", "CIty", "site", "SiteID", "Go Live Date"), 
                               variable.name = "Date", value.name = "demand")
setDT(minutes_demand_playbook)
# Remove commas and convert to numeric
minutes_demand_playbook$demand <- as.numeric(gsub(",", "", minutes_demand_playbook$demand))
minutes_demand_playbook[,demand := as.numeric(demand)]
#minutes_demand_playbook[,'Store code' := ifelse(is.na('Store code'),SiteID,'Store code')]
#minutes_demand_playbook[,Date := as.character(Date)]
minutes_demand_playbook[,`:=`(`Store code` = ifelse(is.na(`Store code`),SiteID,`Store code`))]
minutes_demand_playbook[,Date :=(Date =as.Date(Date, format = "%m-%d-%Y"))]
ds_1 =ds_master[Live == 1]
minutes_demand_playbook <- minutes_demand_playbook %>%  filter(`Store code` %in% unique(ds_1$DS))
minutes_demand_playbook = minutes_demand_playbook[minutes_demand_playbook$Date == Sys.Date() + 14, ]
minutes_demand_playbook[,`:=`(demand = ifelse(is.na(demand),0,demand))]

minutes_demand_playbook <- merge(minutes_demand_playbook, sh_ds %>% select(DS, sh_code), by.x = c("Store code"), by.y = c("DS"), all.x = TRUE)

minutes_demand_playbook <- minutes_demand_playbook %>% mutate(CIty = tolower(CIty))

minutes_demand_playbook <- minutes_demand_playbook %>% 
  group_by(CIty) %>% 
  mutate(total_demand = sum(demand)) %>% 
  mutate(demand_weight = demand/total_demand) %>% 
  ungroup()

master <- left_join(master, minutes_demand_playbook %>% select(`Store code`, demand, total_demand, demand_weight), by = c("DS"= "Store code"))
# 
# master <- master %>% mutate(
#   required_quantity = as.integer(Qty * demand_weight),
#   required_quantity = ifelse(is.na(required_quantity), 0, required_quantity),
#   stn_quantity = pmax(required_quantity - ATP_IT, 0),
#   stn_quantity = ifelse(is.na(stn_quantity), 0, stn_quantity)
# )
# # 
# # master <- master %>%
#   mutate(
#     stn_quantity = as.numeric(unlist(stn_quantity)),
#     casepacksize = map_dbl(casepacksize, ~ as.numeric(.x[1])),  # safely extract 1st value or NA
#     final_stn_quantity = ifelse(
#       !is.na(casepacksize) & casepacksize > 0,
#       ceiling(stn_quantity / casepacksize) * casepacksize,
#       0
#     )
#   )

master <- master %>%
  mutate(
    required_quantity = as.integer(Qty * demand_weight),
    required_quantity = ifelse(is.na(required_quantity), 0, required_quantity),
    stn_quantity = pmax(required_quantity - ATP_IT, 0),
    stn_quantity = ifelse(is.na(stn_quantity), 0, stn_quantity),
    final_stn_quantity = ceiling(stn_quantity/casepacksize) * casepacksize
  )
# 
# write.csv(master, "SAMPLING_MASTER_1106_evening.csv", row.names = FALSE)
# 
# sampling_url <- "https://docs.google.com/spreadsheets/d/1E4_hWb1KPl2vY08ItK496ns8Eh6xSBd_O53ekFnmHd8/edit?gid=807137337#gid=807137337"
# sheet_write(master, ss = sampling_url, sheet = "Sampling_Norm")
#  
# cat("Dashboard is successfully written in the Google Sheet/n")

##################################################################################

#           CREATING DASHBOARD SUMMARY TO KNOW THE CREATION QUANTITY             #

##################################################################################
# 
# sampling_summary <- master %>%
#   group_by(sh_code) %>%
#   summarise(
#     Qty = sum(Qty, na.rm = TRUE),
#     ATP_IT = sum(ds_atp, na.rm = TRUE) + sum(IWIT, na.rm = TRUE),
#     deficit = sum(deficit, na.rm = TRUE),
#     MSTN_ATP = sum(MSTN_ATP, na.rm = TRUE),
#     req_qty = sum(req_qty, na.rm = TRUE),
#     .groups = "drop"
#   )
# 
# grand_total_sampling <- master %>%
#   summarise(
#     Qty = sum(Qty, na.rm = TRUE),
#     ATP_IT = sum(ds_atp, na.rm = TRUE) + sum(IWIT, na.rm = TRUE),
#     deficit = sum(deficit, na.rm = TRUE),
#     MSTN_ATP = sum(MSTN_ATP, na.rm = TRUE),
#     req_qty = sum(req_qty, na.rm = TRUE),
#     .groups = "drop"
#   )
# 
# 
# sampling_summary__ <- bind_rows(sampling_summary, grand_total_sampling)
# 
# print(5)
# sampling_summary_shATP <- master %>%
#   filter(sh_atp >0) %>%
#   group_by(sh_code) %>%
#   summarise(
#     qty = sum(qty, na.rm = TRUE),
#     ds_atp = sum(ds_atp, na.rm = TRUE),
#     IWIT = sum(IWIT, na.rm= TRUE),
#     ATP_IT = ds_atp + IWIT,
#     req_qty = sum(req_qty, na.rm = TRUE),
#     .groups = "drop"
#   )
# 
# grand_total_sampling_shATP <- master %>%
#   filter(sh_atp > 0) %>%
#   summarise(
#     qty = sum(qty, na.rm = TRUE),
#     ds_atp = sum(ds_atp, na.rm = TRUE),
#     IWIT = sum(IWIT, na.rm= TRUE),
#     ATP_IT = ds_atp + IWIT,
#     req_qty = sum(req_qty, na.rm = TRUE),
#     .groups = "drop"
#   )

# sampling_summary__SHATP <- bind_rows(sampling_summary_shATP, grand_total_sampling_shATP)

###################################################################################

#            TRACKING SALES DATA FOR LAST 14 DAYS AND 30 DAYS                     #

###################################################################################

sales_L14D <- gcs_get_object("ipc/Nav/Hyperlocal_IPC/Sales_L14D.csv",bucket = gcs_bucket,saveToDisk = "Sales_L14D.csv",overwrite = TRUE)
sales_L14D <- fread("Sales_L14D.csv")

sales_L14D <- sales_L14D %>% rename(FSN = fuf.product_id, DS = business_zone)

sales_L14D <- sales_L14D %>%
  group_by(FSN, DS) %>%
  summarise(
    units = sum(units, na.rm = TRUE),
    .groups = "drop"
  )

sales_L30D<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/Sales_L30D.csv",bucket = gcs_bucket,saveToDisk = "Sales_L30D.csv",overwrite = TRUE)
sales_L30D <- fread("Sales_L30D.csv")

sales_L30D <- sales_L30D %>% rename(FSN = fuf.product_id, DS = business_zone)

sales_L30D <- sales_L30D %>%
  group_by(FSN, DS) %>%
  summarise(
    units = sum(units, na.rm = TRUE),
    .groups = "drop"
  )

master <- left_join(master, sales_L14D, by = c("FSN", "DS"))
master <- master %>% rename(sales_14D = units)
master <- left_join(master, sales_L30D, by = c("FSN", "DS"))
master <- master %>% rename(sales_30D = units)

master <- master %>% mutate(sales_14D = ifelse(is.na(sales_14D), 0, sales_14D),
                            sales_30D = ifelse(is.na(sales_30D), 0, sales_30D))
                            
##################################################################################

#     SPLITTING THE STN CREATION REQUIRED QUANTITY INTO MORNING AND EVENING      #

##################################################################################

project <- "sampling_"
date_str <- format(Sys.Date(), "%d%m")
current_time <- format(Sys.time(), "%H:%M")
morning_start <- "04:00"
morning_end   <- "10:00"

morning_creation_ <- master %>% filter(Cron_Timing %in% c("M"),
                                       final_stn_quantity>0, sh_atp_pivot>0) %>% select(FSN, DS, sh_code, final_stn_quantity, Cron_Timing, casepacksize)
evening_creation_ <- master %>% filter(Cron_Timing %in% c("E", "ME"),
                                       final_stn_quantity >0, sh_atp_pivot>0) %>% select(FSN, DS, sh_code, final_stn_quantity, Cron_Timing, casepacksize)

filename_M <- paste0("STN_Output_Morning_", project , date_str, ".csv")
filename_E <- paste0("STN_Output_Evening_", project , date_str, ".csv")

write.csv(morning_creation_, file = filename_M, row.names = FALSE)
write.csv(evening_creation_, file = filename_E, row.names = FALSE)

current_time <- format(Sys.time(), "%H:%M")
morning_start <- "04:00"
morning_end   <- "10:00"

# Choose file based on time
if (current_time >= morning_start & current_time < morning_end) {
  selected_file <- filename_M
  cat("Using Morning File: ", selected_file)
} else {
  selected_file <- filename_E
  cat("Using Evening File: ", selected_file)
}

# Read the selected file
if (file.exists(selected_file)) {
  input_df <- read.csv(selected_file)
  print(head(input_df))
  write.csv(input_df, "STN_Output.csv", row.names = FALSE)
} else {
  stop("Selected file does not exist: ", selected_file)
}

print(6)

##################################################################################

#                WRITING THE DASHBOARD SUMMARY TO GOOGLE SHEETS                  #

##################################################################################
# 
# # gs4_auth()  
# sampling_url <- "https://docs.google.com/spreadsheets/d/1E4_hWb1KPl2vY08ItK496ns8Eh6xSBd_O53ekFnmHd8/edit?gid=807137337#gid=807137337"
# sheet_write(master, ss = sampling_url, sheet = "Sampling Tracker_")
# # sheet_write(sampling_summary__, ss = sampling_url, sheet = "Sampling Summary")
# sheet_write(ask_summary__, ss = sampling_url, sheet = "Sampling_Summary")
# sheet_write(sampling_master, ss = sampling_url, sheet = "Sampling_Norm")
# 
# cat("Dashboard is successfully written in the Google Sheet/n")

##################################################################################

#            LOG FILE CREATION AND TRACKER UPLOADING TO GCS                     #

##################################################################################

today_date <- format(Sys.Date(), "%Y-%m-%d")
gcs_file_name <- paste0("Gyan/Sampling_Tracker/sampling_tracker_", today_date, ".csv")

filename <- paste0("sampling_tracker_", format(Sys.Date(), "%Y-%m-%d"), ".csv")
write.csv(filename, "sampling_tracker.csv", row.names = FALSE)

print(paste("File saved successfully as", gcs_file_name))

gcs_upload(file = "SAMPLING_MASTER_.csv", name = gcs_file_name, predefinedAcl = "bucketLevel")

cat("Uploaded to GCP as:", gcs_file_name)

#============= Writing Log to Sampling Tracker Sheet in the Tab: Logs =============

tryCatch({
  file_info <- file.info(filename)
  file_size_Bytes <- file_info$size
  log_sheet_url <- "https://docs.google.com/spreadsheets/d/1E4_hWb1KPl2vY08ItK496ns8Eh6xSBd_O53ekFnmHd8/edit?gid=250324420#gid=250324420"
  log_status <- "Success"
  log_message <- "Script has successfully uploaded Sampling Tracker to GCS!"
  
  log_entry <- data.frame(
    timestamp = Sys.time(),
    script = "Sampling_Master",
    local_file = filename,
    gcs_file = gcs_file_name,
    status = log_status,
    message = log_message,
    user = "Gyan",
    file_size_Bytes = file_info$size
  )
  
  sheet_append(ss = log_sheet_url, data = log_entry, sheet = "Logs")
  cat("Log entry written to Google Sheet.")
  
  gcs_upload(file = log_entry, name = "Gyan/Sampling_Tracker/Logs/", predefinedAcl = "bucketlevel")
  
},  error = function(e) {
  log_status <<- "Failed"
  log_message <<- e$message
  base::message("Error occurred: ", e$message)
})


##################################################################################

#                     STN CREATTION LOGIC STARTS HERE                            #

##################################################################################

input_df <- input_df %>% select(FSN, DS, sh_code, final_stn_quantity, casepacksize) %>% rename(case_pack = casepacksize)
setnames(input_df, old=c('DS','sh_code','final_stn_quantity'),new=c('darkstore','FC','quantity'))

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

inventory = data.table(gcs_get_object("ipc/Nav/Hyperlocal_IPC/Inventory_Snapshot.csv", bucket = gcs_bucket,saveToDisk = "temp_file.csv",overwrite = TRUE))
inventory<-read.csv("temp_file.csv")
setDT(inventory)
inventory = inventory [fc_area =='store']

whitelist = data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/Whitelisted_Sellerlist.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE))
whitelist<-read.csv("temp_file.csv")
#setnames(input_df, old='darkstore',new='DS')
input_df = input_df[,.(FSN,FC,quantity,DS, case_pack)]


###################### INPUTNULL###################### INPUT PROCESSING

input_df <- input_df[quantity > 0]

setnames(input_df, c('FSN',  'FC', 'quantity'), c('fsn', 'fc', 'Quantity'))

input_df[, fsn := toupper(trimws(fsn))]
input_df[, fc := tolower(trimws(fc))]
setnames(input_df, old='DS',new='darkstore')
input_df[, darkstore := tolower(trimws(darkstore))]

# Group by fsn, darkstore, and fc and calculate sum of Quantity
# grouped_input <- input_df[, .(Quantity = sum(Quantity)), by = .(fsn, darkstore, fc)]

grouped_input <- input_df[, .(Quantity = sum(Quantity), case_pack = max(case_pack, na.rm = TRUE)), by = .(fsn, darkstore, fc)]

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

allocations <- list()

for (i in seq_len(nrow(grouped_input))) {
  fsn_val <- grouped_input$fsn[i]
  darkstore_val <- grouped_input$darkstore[i]
  fc_val <- grouped_input$fc[i]
  qty <- grouped_input$Quantity[i]
  case_pack_val <- grouped_input$case_pack[i]
  allocated <- 0
  
  
  if (is.na(case_pack_val) | case_pack_val < 1) case_pack_val <- 1
  
  
  adjusted_qty <- if (case_pack_val > 1 & qty >= case_pack_val) {
    ceiling(qty / case_pack_val) * case_pack_val
  } else {
    qty
  }
  
  matching_inventory <- filtered_inventory[fsn == fsn_val & fc == fc_val & atp > 0]
  
  for (j in seq_len(nrow(matching_inventory))) {
    if (allocated >= adjusted_qty) break
    
    remaining_needed <- adjusted_qty - allocated
    available <- matching_inventory$atp[j]
    
    to_allocate <- min(available, remaining_needed)
    
    
    to_allocate <- floor(to_allocate / case_pack_val) * case_pack_val
    
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

# # Allocation logic
# allocations <- list()
# 
# for (i in seq_len(nrow(grouped_input))) {
#   fsn_val <- grouped_input$fsn[i]
#   darkstore_val <- grouped_input$darkstore[i]
#   fc_val <- grouped_input$fc[i]
#   qty <- grouped_input$Quantity[i]
#   allocated <- 0
#   
#   if (is.na(case_pack_val) | case_pack_val < 1) case_pack_val <- 1
# 
#   adjusted_qty <- if (case_pack_val > 1 & qty >= case_pack_val) {
#     ceiling(qty / case_pack_val) * case_pack_val
#   } else {
#     qty
#   }
#   
#   matching_inventory <- filtered_inventory[fsn == fsn_val & fc == fc_val & atp > 0]
#   
#   for (j in seq_len(nrow(matching_inventory))) {
#     if (allocated >= adjusted_qty) break
#     
#     to_allocate <- min(matching_inventory$atp[j], adjusted_qty - allocated)
#     
#     if (to_allocate > 0) {
#       allocations <- append(allocations, list(list(
#         darkstore = darkstore_val,
#         fsn = fsn_val,
#         source = fc_val,
#         Quantity = to_allocate,
#         lot_id = matching_inventory$lot_id[j],
#         seller_id = matching_inventory$seller_id[j]
#       )))
#       
#       inv_idx <- matching_inventory$inv_index[j]
#       filtered_inventory[inv_idx, atp := atp - to_allocate]
#       allocated <- allocated + to_allocate
#     }
#   }
# }

alloc_df <- rbindlist(allocations)

quantity_allocated <- sum(alloc_df$Quantity)
quantity_input <- sum(grouped_input$Quantity)

DS_Master = data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE))
DS_Master<-read.csv("temp_file.csv")
setDT(DS_Master)
DS_Master = DS_Master[,.(DS,Cron_Timing)]
setnames(DS_Master , old='DS',new='darkstore')
alloc_df = DS_Master[alloc_df,on=c('darkstore')]

morning = alloc_df[Cron_Timing %in% c('M','ME')]
evening = alloc_df[Cron_Timing %in% c('E','ME')]

##output

fwrite(morning,"E:/gyan work/Working file/scripts/R/STN_Output_morning.csv")
fwrite(evening,"E:/gyan work/Working file/scripts/R/STN_Output_evening.csv")

##############################################################


