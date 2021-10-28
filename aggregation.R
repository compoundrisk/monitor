## Direct Github location (data folder)
#---------------------------------
# github <- "https://raw.githubusercontent.com/bennotkin/compoundriskdata/master/"
#---------------------------------

#
##
### ********************************************************************************************
####    SAVE RISK SHEETS TO EXTERNAL VOLUME ----
### ********************************************************************************************
##
#

# Don't think I need this, as I can re-create risk sheets at any time,
# and risk-sheets is just a pass-through from process-indicators.R to
# the next step (`aggregate_inds()`?)
archive_risk_sheets <- function() {
  # file.copy("Risk_sheets", "external", recursive = T)
  # Put dated copy in /risk-sheets/archive
  if (!dir.exists("archives")) dir.create("archives")
  # dir.create("external/risk-sheets")
  # dir.create("external/risk-sheets/archive")
  file.copy("Risk_sheets", "archives", recursive = T)
  # file.copy("Risk_sheets", "external/risk-sheets", recursive = T)
  setwd("archives/Risk_sheets")
  file.rename(list.files(),
              paste0(Sys.Date(), "-", list.files()))
  setwd("../..")
  file.copy(paste0("archives/Risk_sheets/", list.files("archives/Risk_sheets")),
            paste0("output/risk-sheets/archive/", list.files("archives/Risk_sheets")),
            overwrite = TRUE)
}

externalize_risk_sheets <- function() {
  file.copy(paste0("Risk_sheets/", list.files("Risk_sheets")),
            paste0("output/risk-sheets/", list.files("Risk_sheets")),
            overwrite = TRUE)
}

#
##
### ********************************************************************************************
####    WRITE MINIMAL DIMENSION SHEETS ----
### ********************************************************************************************
##
#

write_minimal_dim_sheets <- function() {
  # Saves CSVs with only used indicators (normalized, not raw) to crm_excel folder
  # Delete. Now included in `libraries.R`
  # countrylist <- read.csv("https://raw.githubusercontent.com/ljonestz/compoundriskdata/update_socio_eco/Indicator_dataset/countrylist.csv")
  # countrylist <- countrylist %>%
  # dplyr::select(-X) %>% 
  # arrange(Country)
  
  # Load dimension / data dictionary
  indicators_list <- as.data.frame(read.csv("indicators-list.csv"))
  
  health_sheet <- read.csv("output/risk-sheets/health-sheet.csv")[,-1] # drops first column, X, which is row number
  food_sheet <- read.csv("output/risk-sheets/food-sheet.csv")[,-1]
  fragility_sheet <- read.csv("output/risk-sheets/fragility-sheet.csv")[,-1]
  macro_sheet <- read.csv("output/risk-sheets/macro-sheet.csv")[,-1]
  natural_hazards_sheet <- read.csv("output/risk-sheets/natural_hazards-sheet.csv")[,-1]
  socio_sheet <- read.csv("output/risk-sheets/socio-sheet.csv")[,-1]
  
  # Compile list of all dimension data for Map function
  # List names should match dimension names in `indicator` data frame
  unique(indicators_list$Dimension)
  sheetList <- list("Health" = health_sheet,
                    "Food Security" = food_sheet,
                    "Conflict and Fragility" = fragility_sheet, # Technical note uses various names; what do we want to use? Fragility and Conflict, Conflict and Fragility, Conflict Fragility and Institutional Risk
                    "Macro Fiscal" = macro_sheet,
                    "Natural Hazard" = natural_hazards_sheet,
                    "Socioeconomic Vulnerability" = socio_sheet
  )
  
  # Make directory from within script to enable volume sharing with docker
  # (All contents are emptied when volume is shared)
  # dir.create("external/crm-excel")
  # dir.create("external/crm-excel/archive")
  
  # —Write function to apply to each sheet ----
  used_indicators <- countrylist
  
  slugify <- function(x, alphanum_replace="", space_replace="-", tolower=TRUE) {
    x <- gsub("[^[:alnum:] ]", alphanum_replace, x)
    x <- gsub(" ", space_replace, x)
    if (tolower) {
      x <- tolower(x)
    }
    
    return(x)
  }
  
  writeSourceCSV <- function(i) {
    # headerOffset <- 2 # current output has two header rows # VARIABLE
    
    # Define sheet and dimension name
    sheet <- sheetList[[i]]
    sheet <- sheet[!duplicated(sheet$Country),]
    sheet <- arrange(sheet, Country)
    dimension <- names(sheetList)[i]
    
    sheet_norm <- sheet[,c(which(colnames(sheet) == "Country"), which(colnames(sheet) %in% indicators_list$indicator_slug))]
    sheet_raw <- sheet[,c(which(colnames(sheet) == "Country"), which(colnames(sheet) %in% unlist(strsplit(indicators_list$indicator_raw_slug, ", "))))]
    colnames(sheet_raw) <- paste0(colnames(sheet_raw), "_raw")
    sheet <- left_join(sheet_norm, sheet_raw, by = c("Country" = "Country_raw"), suffix = c("", "_raw"))
    
    # write_csv(sheet_norm, paste0("Risk_sheets/indicators-normalised/", dimension, ".csv"))
    write_csv(sheet, paste0("output/crm-excel/", slugify(dimension), ".csv"))
    write_csv(sheet, paste0("output/crm-excel/archive/", Sys.Date(), "-", slugify(dimension), ".csv"))
    
    # Write csvs with readable variable names
    ## Order sheet columns to match indicators.csv
    inds <- indicators_list[which(indicators_list$Dimension == dimension), "indicator_slug"]
    sheet_norm <- sheet_norm[c("Country", inds)]
    
    ## Rename columns
    pairs <- subset(indicators_list,
                    Timeframe == "Emerging" & Dimension == dimension,
                    c(Indicator, indicator_slug))
    names(sheet_norm)[match(pairs[ ,"indicator_slug"], names(sheet_norm))] <- paste0("Emerging_", dimension, "_", pairs$Indicator)
    
    pairs <- subset(indicators_list,
                    Timeframe == "Underlying" & Dimension == dimension,
                    c(Indicator, indicator_slug))
    names(sheet_norm)[match(pairs[ ,"indicator_slug"], names(sheet_norm))] <- paste0("Underlying_", dimension, "_", pairs$Indicator)  
    
    # write_csv(sheet_norm, paste0("Risk_sheets/pretty-names/", dimension, ".csv") )
    # Join sheets together (<<- hoists variable to parent environment)
    used_indicators <<- left_join(used_indicators, sheet_norm, by = "Country")
    
    # Check if any sheets have empty columns
    empties <- empty_cols(sheet) %>% names()
    if (length(empties) > 0) {
      print(paste(dimension, " is empty on ", empties)) 
    } else {
      print(paste(dimension, " is filled")) 
    }
  }
  
  # —Run -----
  Map(writeSourceCSV, 1:length(sheetList))
  write_csv(used_indicators, "output/crm-indicators.csv")
}