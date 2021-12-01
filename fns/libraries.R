packages <- c("curl", "DBI", "dplyr", "EnvStats", "stats", "countrycode", "ggplot2", 
              "jsonlite","lubridate", "maps", "matrixStats", "purrr", "readr", "readxl", "rmarkdown",
              "rvest", "sjmisc", "sparklyr", "stringr", "tidyr", "xml2", "zoo")

lapply(packages, function(p) {
  if (!require(p, character.only = T, quietly = T)) {
    install.packages(p, destdir = "libs")
  }
  library(p, character.only = T, quietly = T)
})

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

slugify <- function(x, non_alphanum_replace="", space_replace="_", tolower=TRUE, toupper = FALSE) {
  x <- gsub("[^[:alnum:] ]", non_alphanum_replace, x)
  x <- gsub(" ", space_replace, x)
  if (tolower) {
    x <- tolower(x)
  }
  if (toupper) {
    x <- toupper(x)
  }
  return(x)
}

`%ni%` <- Negate(`%in%`)
