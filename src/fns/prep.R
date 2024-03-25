packages <- c("curl", "DBI", "EnvStats", "exactextractr", "countrycode", "httr",
              "httr2", "ggplot2", "jsonlite","lubridate", "maps", "matrixStats", "purrr", 
              "pdftools", "raster", "readr", "readxl", "remotes", "rvest", "sf",
              "sjmisc", "stats", "stringr", "tidyr", "xml2",
              "wppExplorer", "zoo")

if (dir.exists("/dbfs")) {
  .libPaths(c("/dbfs/mnt/CompoundRiskMonitor/lib", .libPaths()))
} else {
  .libPaths(c("lib", .libPaths()))
}

invisible(
  sapply(packages, function(p) {
    # if (!require(p, character.only = T, quietly = T)) {
    # install.packages(p, lib = "lib")
    # }
    suppressMessages(library(p, character.only = T, quietly = T))
    return(NULL)
  })
)

#loading dplyr last to prevent masking select()
suppressMessages(library('dplyr'))

source("src/fns/helpers.R")

#---------------------------------

country_groups <- tryCatch(
  {
    wb_countries_collect <- function() {
      # Source file has changed; saving in case it reverts soon
      # codes <- curl_and_delete("http://databank.worldbank.org/data/download/site-content/CLASS.xls",
      #   FUN = read_xls, sheet = 1, range = "C5:I224")[-1,]
      codes <- curl_and_delete("http://databank.worldbank.org/data/download/site-content/CLASS.xlsx",
        FUN = read_xlsx, sheet = 1, range = "A1:F219")
      group_codes <- curl_and_delete("http://databank.worldbank.org/data/download/site-content/CLASS.xlsx",
        FUN = read_xlsx, sheet = "Groups")
      region_codes <- group_codes %>%
        select(region_code_all = GroupCode, GroupName) %>%
        distinct() %>%
        subset(GroupName %in% codes$Region)
      no_high_income <- group_codes %>%
        subset(str_detect(GroupName, "excluding high income")) %>%
        select(region_code_no_high = GroupCode, Code = CountryCode)
      country_groups <- left_join(codes, region_codes, by = c("Region" = "GroupName")) %>%
        left_join(no_high_income, by = "Code") %>%
        mutate(region_code = str_replace_all(region_code_all, c(
          "LCN" = "LAC",
          "SAS" = "SAR",
          "SSF" = "SSA",
          "MEA" = "MNA",
          "EAS" = "EAP",
          "NAC" = "NAR",
          "ECS" = "ECA")))
      write.csv(country_groups, "src/country-groups.csv", row.names = F)
      return(country_groups)
    }
    wb_countries_collect()
  },
  error = function(e) {
    print("Unable to download country groups file from databank.worldbank.org")
    df <- read_csv("src/country-groups.csv", col_types = "cccc")
    return(df)
  })

countrylist <- country_groups %>% 
  select(Countryname = Economy, Country = Code) %>%
  arrange(Country)

regions <- select(country_groups, iso = Code, region = Region, region_code)

indicators_list <- as.data.frame(read.csv("src/indicators-list.csv")) %>%
  subset(active == T)

## Set up Spark
# sc <- spark_connect(master = "local") # This is only for when running locally
# sc <- spark_connect(method = "databricks")
# DBI::dbSendQuery(sc,"CREATE DATABASE IF NOT EXISTS crm")
# sparklyr::tbl_change_db(sc, "crm")
# setwd("../../../dbfs/mnt/CompoundRiskMonitor")