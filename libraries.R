packages <- c("curl", "DBI", "dplyr", "EnvStats", "stats", "countrycode", "ggplot2", 
              "jsonlite","lubridate", "maps", "matrixStats", "readr", "readxl", "rmarkdown",
              "rvest", "sjmisc", "sparklyr", "stringr", "tidyr", "xml2", "zoo")
invisible(lapply(packages, require, quietly = TRUE, character.only = TRUE))

## Set up Spark
# sc <- spark_connect(master = "local") # This is only for when running locally
# sc <- spark_connect(method = "databricks")
# DBI::dbSendQuery(sc,"CREATE DATABASE IF NOT EXISTS crm")
# sparklyr::tbl_change_db(sc, "crm")
# setwd("../../../dbfs/mnt/CompoundRiskMonitor")

## Direct Github location (data folder)
#---------------------------------
github <- "https://raw.githubusercontent.com/bennotkin/compoundriskdata/master/"
#---------------------------------
