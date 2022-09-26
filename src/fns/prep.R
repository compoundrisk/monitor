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

if (dir.exists("/dbfs")) {
  .libPaths(c("/dbfs/mnt/CompoundRiskMonitor/lib", .libPaths()))
} else {
  .libPaths(c("lib", .libPaths()))
}

lapply(packages, function(p) {
  # if (!require(p, character.only = T, quietly = T)) {
  #   install.packages(p, lib = "lib")
  # }
  library(p, character.only = T, quietly = T)
})

#loading dplyr last to prevent masking select()
library('dplyr')

source("src/fns/helpers.R")

## Direct Github location (data folder)
#---------------------------------
github <- "https://raw.githubusercontent.com/bennotkin/compoundriskdata/master/"
#---------------------------------

countrylist <- read.csv("src/countrylist.csv") %>%
  dplyr::select(-X) %>%
  dplyr::arrange(Country)

# country_groups <- {
#   # Source file has changed; saving in case it reverts soon
#   # codes <- curl_and_delete("http://databank.worldbank.org/data/download/site-content/CLASS.xls",
#   #   FUN = read_xls, sheet = 1, range = "C5:I224")[-1,]
#   codes <- curl_and_delete("http://databank.worldbank.org/data/download/site-content/CLASS.xlsx",
#     FUN = read_xlsx, sheet = 1, range = "A1:F219")
#   region_codes <- curl_and_delete("http://databank.worldbank.org/data/download/site-content/CLASS.xlsx",
#     FUN = read_xlsx, sheet = "Groups")[-1,] %>% 
#     select(region_code = GroupCode, GroupName) %>%
#     distinct() %>%
#     subset(GroupName %in% codes$Region)
#   left_join(codes, region_codes, by = c("Region" = "GroupName"))
# }
# regions <- select(country_groups, iso = Code, region = Region, region_code)

# regions$region_code <- regions$region_code %>%
#   str_replace_all(c(
#     "LCN" = "LAC",
#     "SAS" = "SAR",
#     "SSF" = "SSA",
#     "MEA" = "MNA",
#     "EAS" = "EAP",
#     "NAC" = "NAR",
#     "ECS" = "ECA"))

indicators_list <- as.data.frame(read.csv("src/indicators-list.csv")) %>%
  subset(active == T)

## Set up Spark
# sc <- spark_connect(master = "local") # This is only for when running locally
# sc <- spark_connect(method = "databricks")
# DBI::dbSendQuery(sc,"CREATE DATABASE IF NOT EXISTS crm")
# sparklyr::tbl_change_db(sc, "crm")
# setwd("../../../dbfs/mnt/CompoundRiskMonitor")