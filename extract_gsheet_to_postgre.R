#Set Directory
setwd("C:/Users/User/Documents/Cedea/R Scripts")

#Install Pacakages
if(!require(googledrive))install.packages("googledrive");library(googledrive)
if(!require(googlesheets4))install.packages("googlesheets4");library(googlesheets4)
if(!require(readxl))install.packages("readxl");library(readxl)
if(!require(readxlsb))install.packages("readxlsb");library(readxlsb)
if(!require(RODBC)) install.packages('RODBC'); require(RODBC)
if(!require(tidyverse))install.packages("tidyverse");library(tidyverse)
if(!require(lubridate))install.packages("lubridate");library(lubridate)
if(!require(DBI))install.packages("DBI");library(DBI) #to execute SQL query
if(!require(RPostgres))install.packages("RPostgres");library(RPostgres)#to connect RPostgres
if(!require(lubridate))install.packages("lubridate");library(lubridate)#to deal with date format
if(!require(zoo))install.packages("zoo");library(zoo)
if(!require(tools))install.packages("tools");library(tools)

#Data Connections
## Connection to PostgreSQL
con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "xxx",
  host = "yyy",
  port = "5432",
  user = "postgres",
  password = "zzz"
)

## Get Gdrive and Gsheets Credentials
options(gargle_oauth_cache  = ".secrets")
googlesheets4::gs4_auth()
googledrive::drive_auth()

## Google Drive and Spreadsheet Authentifications
googledrive::drive_auth(
                        email = " ",
                        cache = ".secrets",
                        use_oob = TRUE
                        )

googlesheets4::gs4_auth( 
                        email = " ",
                        cache = ".secrets",
                        use_oob = TRUE
                        )

#Access to Folder in GDrive
folder_url <- "https://drive.google.com/drive/"

#List Files in Gdrive and Local
folder_list <- drive_ls(folder_url)
local_list <- list.files(path= "C:/Users/User/Documents/Cedea/R Scripts/downloaded_files")
print(local_list)

#Filter New Files in GDrive to Download
folder_list <- folder_list %>% 
                    distinct()
folder_new_file <- folder_list %>%
                   filter(!name %in% local_list)

#Download the Files
for(file_name in folder_new_file$name){
  file_id <- folder_new_file$id[folder_new_file$name == file_name]
  local_file_path <- paste0("C:/Users/User/Documents/Cedea/R Scripts/downloaded_files/", file_name)
  drive_download(as_id(file_id), path = local_file_path)
}

#Read All of the Downloaded Files
data_list <- list()
local_list_2 <- list.files(path= "C:/Users/User/Documents/Cedea/R Scripts/downloaded_files")
for(file_name in local_list_2){
  local_file_path <- paste0("C:/Users/User/Documents/Cedea/R Scripts/downloaded_files/", file_name)
  file_exte <- tools::file_ext(file_name)
  if(file_exte =="xlsb"){
    data_list[[file_name]] <- read_xlsb(local_file_path, 1)
  } else{
    data_list[[file_name]] <- read_xlsx(local_file_path)
  }
}

#Create new column source file and warehouse
for(file_name in names(data_list)){
  data_list[[file_name]] <- data_list[[file_name]][1:13] # Make sure that all list have a same column numbers (15 columns)
  data_list[[file_name]]$source <- file_name
  
  # Custom function to convert string to sentence case
  to_sentence_case <- function(string) {
    words <- strsplit(tolower(string), " ")[[1]]
    words <- paste(toupper(substring(words, 1, 1)), substring(words, 2), sep = "", collapse = " ")
    return(words)
  }

  warehouse <- to_sentence_case(gsub("^.*?-\\s*(.*?)\\s+.*$", "\\1", data_list[[file_name]]$source))
  
  #Add condition if the warehouse is Jakarta, then it should be replaced to Muara Baru
  if (warehouse == "Jakarta") {
    warehouse <- "1100#FG_CEDEA"
  } else if (warehouse == "Majalengka") {
    warehouse <- "5100#MAJALENGKA"
  } else if (warehouse == "Semarang") {
    warehouse <- "7100#SEMARANG"
  }
  data_list[[file_name]]$warehouse <- warehouse
}

# Add date
for (file_name in names(data_list)) {
  current_df <- data_list[[file_name]]
  
  #Set the blank data as NA
  current_df[current_df == ""] <- NA
  
  # Set 'date' column to NA for the entire data frame
  current_df$date <- NA
  file_exte <- tools::file_ext(file_name)
  if(file_exte =="xlsb"){
    
    # Extract numbers from the second character to the end of the string
    numeric_part <- gsub("[^0-9]", "", colnames(data_list[[file_name]])[10])
    
    #Set the date in the first row
    date_value <- as.Date( as.numeric(numeric_part), origin = "1899-12-30")
    current_df[1,"date"] <- date_value
    
  } else{
    current_df[1, "date"] <- as.Date(as.numeric(colnames(data_list[[file_name]])[10]), format = "%Y-%m-%d", origin = "1899-12-30") #add date (extract from the column name) for each lists in data_list
  }
  
  # Set the date for the second row and onwards
  if(file_exte =="xlsb"){
    for (i in 1:nrow(current_df)) {
      if(all(is.na(current_df[i, 7:9]))) {
        print(current_df[i, ])
        if (!is.na(current_df[i, 10])) {
          # Only update 'date' column for specific rows where condition is met
          numeric_part <- gsub("[^0-9]", "", current_df[i,10])
          date_value <- as.Date(as.numeric(numeric_part), origin = "1899-12-30")
          current_df[i, "date"] <- date_value
          print(as.Date(as.numeric(current_df[i, 10])))
        }
      }
    }
  }
  else{#xlsx
    for (i in 1:nrow(current_df)) {
      if(all(is.na(current_df[i, 7:9]))) {
        print(current_df[i, ])
        if (!is.na(current_df[i, 10])) {
          # Only update 'date' column for specific rows where condition is met
          current_df[i, "date"] <- as.Date(as.numeric(current_df[i, 10]), format = "%Y-%m-%d", origin = "1899-12-30")
          print(as.Date(as.numeric(current_df[i, 10])))
        }
      }
    }
  }
  data_list[[file_name]] <- current_df
}

for (file_name in names(data_list)) {
  # Check if there are any non-NA values in the 'date' column
  if (any(!is.na(data_list[[file_name]]$date))) {
    # Apply na.locf() only if there are non-NA values
    data_list[[file_name]]$date <- zoo::na.locf(data_list[[file_name]]$date)
  }
}

#Exclude the column names and make the second row as a column name
lsDailyPlans <- list()
for(file_name in names(data_list)){
  data <- data_list[[file_name]]
  col_names <- unname(data[1,])
  data <- data [-1, ]
  colnames(data) <- col_names
  lsDailyPlans[[file_name]] <- data  
  print(nrow(data))
}

# Change the column names and remove the NA rows
for(file_name in names(lsDailyPlans)){
  colnames(lsDailyPlans[[file_name]])[ncol(lsDailyPlans[[file_name]])] <- "date"
  colnames(lsDailyPlans[[file_name]])[ncol(lsDailyPlans[[file_name]])-1] <- "warehouse"
  colnames(lsDailyPlans[[file_name]])[ncol(lsDailyPlans[[file_name]])-2] <- "source"
  }

# Convert data types in each dataframe of lsDailyPlans
for (file_name in names(lsDailyPlans)) {
  # Convert columns 1 to 5 and 14 to character
  lsDailyPlans[[file_name]][, c(1:5, 14:15)] <- sapply(lsDailyPlans[[file_name]][, c(1:5, 14:15)], as.character)
  
  # Convert columns 6 to 13 to numeric
  lsDailyPlans[[file_name]][, 6:13] <- lapply(lsDailyPlans[[file_name]][, 6:13], as.numeric)
  
  #Convert column 15 to a date
  lsDailyPlans[[file_name]][16] <- lapply(lsDailyPlans[[file_name]][16], as.Date)
}

# Remove NA values from the 'item_code' column
for (file_name in names(lsDailyPlans)) {
  lsDailyPlans[[file_name]] <- na.omit(lsDailyPlans[[file_name]], cols = "item_code")
}

# Rename the column names
for (file_name in names(lsDailyPlans)) {
  colnames(lsDailyPlans[[file_name]])[1] <- "line"
  colnames(lsDailyPlans[[file_name]])[2] <- "item_code"
  colnames(lsDailyPlans[[file_name]])[3] <- "brand"
  colnames(lsDailyPlans[[file_name]])[4] <- "category"
  colnames(lsDailyPlans[[file_name]])[5] <- "item_name"
  colnames(lsDailyPlans[[file_name]])[6] <- "rm"
  colnames(lsDailyPlans[[file_name]])[7] <- "kg_per_pak"
  colnames(lsDailyPlans[[file_name]])[8] <- "pak_per_mc"
  colnames(lsDailyPlans[[file_name]])[9] <- "yield"
  colnames(lsDailyPlans[[file_name]])[10] <- "mc"
  colnames(lsDailyPlans[[file_name]])[11] <- "pak"
  colnames(lsDailyPlans[[file_name]])[12] <- "net_kg"
  colnames(lsDailyPlans[[file_name]])[13] <- "gross_kg"
}

# Combine all data frames into a single data frame
dfAllPlans <- data.frame()

#  Combine all data frames in lsDailyPlans into a single data frame 
dfAllPlans <- do.call(rbind, lsDailyPlans)

#Write in postgres
dbExecute(con, 'DELETE FROM production_plan')
dbWriteTable(con, 'production_plan', 
             dfAllPlans, 
             append=T)
