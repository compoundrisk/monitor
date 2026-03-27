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
      class_urls <- c(
        "https://databank.worldbank.org/data/download/site-content/CLASS.xlsx",
        "http://databank.worldbank.org/data/download/site-content/CLASS.xlsx"
      )

      fetch_class_sheet <- function(sheet, range = NULL) {
        last_error <- NULL
        for (u in class_urls) {
          result <- tryCatch(
            {
              if (is.null(range)) {
                curl_and_delete(u, FUN = read_xlsx, sheet = sheet)
              } else {
                curl_and_delete(u, FUN = read_xlsx, sheet = sheet, range = range)
              }
            },
            error = function(e) {
              last_error <<- e
              NULL
            }
          )
          if (!is.null(result)) return(result)
        }
        stop(last_error)
      }

      codes <- fetch_class_sheet(sheet = 1, range = "A1:F219")
      group_codes <- fetch_class_sheet(sheet = "Groups")
      region_codes <- group_codes %>%
        dplyr::select(region_code_all = GroupCode, GroupName) %>%
        distinct() %>%
        subset(GroupName %in% codes$Region)
      no_high_income <- group_codes %>%
        subset(str_detect(GroupName, "excluding high income")) %>%
        dplyr::select(region_code_no_high = GroupCode, Code = CountryCode)
      country_groups <- dplyr::left_join(codes, region_codes, by = c("Region" = "GroupName")) %>%
        dplyr::left_join(no_high_income, by = "Code") %>%
        dplyr::mutate(region_code = str_replace_all(region_code_all, c(
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
    warning("Unable to download country groups file from databank.worldbank.org; using local src/country-groups.csv")
    if (file.exists("src/country-groups.csv")) {
      return(read_csv("src/country-groups.csv", col_types = "ccccccccc"))
    }
    stop("Country groups download failed and local src/country-groups.csv was not found.")
  })

countrylist <- country_groups %>% 
  dplyr::select(Countryname = Economy, Country = Code) %>%
  dplyr::arrange(Country)

regions <- dplyr::select(country_groups, iso = Code, region = Region, region_code)

indicators_list <- as.data.frame(read.csv("src/indicators-list.csv")) %>%
  subset(active == T)

## Set up Spark
# sc <- spark_connect(master = "local") # This is only for when running locally
# sc <- spark_connect(method = "databricks")
# DBI::dbSendQuery(sc,"CREATE DATABASE IF NOT EXISTS crm")
# sparklyr::tbl_change_db(sc, "crm")
# setwd("../../../dbfs/mnt/CompoundRiskMonitor")