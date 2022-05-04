packages <- c("curl", "DBI", "EnvStats", "exactextractr", "countrycode", "ggplot2", 
              "jsonlite","lubridate", "maps", "matrixStats", "purrr", "raster",
              "readr", "readxl", "remotes", "rvest", "sf",
              "sjmisc", "stats", "stringr", "tidyr", "xml2",
              "wppExplorer", "zoo")
              
# packages <- c("curl", "DBI", "EnvStats", "exactextractr", "countrycode", "ggplot2", 
#               "jsonlite","lubridate", "maps", "matrixStats", "purrr", "raster",
#               "readr", "readxl", "remotes", "rgdal", "rmarkdown", "rvest", "sf",
#               "sjmisc", "sparklyr", "stats", "stringr", "tidyr", "xml2",
#               "wppExplorer", "zoo")

.libPaths(c("lib", .libPaths()))

lapply(packages, function(p) {
  # if (!require(p, character.only = T, quietly = T)) {
  #   install.packages(p, lib = "lib")
  # }
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
  dplyr::arrange(Country)

indicators_list <- as.data.frame(read.csv("indicators-list.csv")) %>%
  subset(active == T)

## Set up Spark
# sc <- spark_connect(master = "local") # This is only for when running locally
# sc <- spark_connect(method = "databricks")
# DBI::dbSendQuery(sc,"CREATE DATABASE IF NOT EXISTS crm")
# sparklyr::tbl_change_db(sc, "crm")
# setwd("../../../dbfs/mnt/CompoundRiskMonitor")