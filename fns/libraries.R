packages <- c("curl", "DBI", "dplyr", "EnvStats", "stats", "countrycode", "ggplot2", 
              "jsonlite","lubridate", "maps", "matrixStats", "purrr", "readr", "readxl",
              "rmarkdown", "remotes", "rvest", "sjmisc", "sparklyr", "stringr", "tidyr",
              "xml2", "wppExplorer", "zoo")

# Inclusive of raster packages
# packages <- c(
#   "curl", "DBI", "EnvStats", "stats", "countrycode", "exactextractr",
#   "ggplot2", "jsonlite","lubridate", "maps", "matrixStats", "purrr", "raster",
#   "readr", "readxl", "rgdal", "rmarkdown", "rvest", "sf", "sjmisc", "sparklyr",
#   "stringr", "tidyr", "xml2", "wppExplorer", "zoo")

lapply(packages, function(p) {
  if (!require(p, character.only = T, quietly = T)) {
    install.packages(p, destdir = "libs")
  }
  library(p, character.only = T, quietly = T)
})

#loading dplyr last to prevent masking select()
library('dplyr')

source("fns/helpers.R")

## Direct Github location (data folder)
#---------------------------------
github <- "https://raw.githubusercontent.com/bennotkin/compoundriskdata/master/"
#---------------------------------

countrylist <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv")) %>%
  dplyr::select(-X) %>%
  arrange(Country)

indicators_list <- as.data.frame(read.csv("indicators-list.csv")) %>%
  subset(active == T)

## Set up Spark
# sc <- spark_connect(master = "local") # This is only for when running locally
# sc <- spark_connect(method = "databricks")
# DBI::dbSendQuery(sc,"CREATE DATABASE IF NOT EXISTS crm")
# sparklyr::tbl_change_db(sc, "crm")
# setwd("../../../dbfs/mnt/CompoundRiskMonitor")