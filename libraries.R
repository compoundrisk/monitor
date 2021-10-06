packages <- c("curl", "DBI", "dplyr", "EnvStats", "stats", "countrycode", "ggplot2", 
              "jsonlite","lubridate", "matrixStats", "readr", "readxl", "rmarkdown",
              "rvest", "sjmisc", "sparklyr", "stringr", "tidyr", "xml2", "zoo")
invisible(lapply(packages, require, quietly = TRUE, character.only = TRUE))
