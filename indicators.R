# Set method and date variables ----
as_of <- Sys.Date()
format <- "csv" # or "tbl" or "both"

#  CODE USED TO TO PRODUCE INDIVIDUAL INDICATOR DATASETS AND RISK COMPONENT SHEETS
runtime <- Sys.time()
#--------------------LOAD PACKAGES-----------------------------------------
packages <- c("curl", "dplyr", "EnvStats", "stats", "countrycode", "ggplot2", 
              "jsonlite","lubridate", "matrixStats", "readr", "readxl", "rvest",   
              "sjmisc", "stringr", "tidyr", "xml2", "zoo")
invisible(lapply(packages, require, quietly = TRUE, character.only = TRUE))

## Set up Spark
sc <- spark_connect(master = "local") # This is only for when running locally
# sc <- spark_connect(method = "databricks")
DBI::dbSendQuery(sc,"CREATE DATABASE IF NOT EXISTS crm")
sparklyr::tbl_change_db(sc, "crm")
# setwd("../../../dbfs/mnt/CompoundRiskMonitor")
# setwd("inputs-archive")
setwd("output")
#---------------------------------

## Direct Github location (data folder)
#---------------------------------
github <- "https://raw.githubusercontent.com/bennotkin/compoundriskdata/master/"
#---------------------------------

#--------------------FUNCTION TO CALCULATE NORMALISED SCORES-----------------
# Function to normalise with upper and lower bounds (when low score = high vulnerability)
normfuncneg <- function(df, upperrisk, lowerrisk, col1) {
  # Create new column col_name as sum of col1 and col2
  df[[paste0(col1, "_norm")]] <- ifelse(df[[col1]] <= upperrisk, 10,
                                        ifelse(df[[col1]] >= lowerrisk, 0,
                                               ifelse(df[[col1]] > upperrisk & df[[col1]] < lowerrisk, 10 - (upperrisk - df[[col1]]) / 
                                                        (upperrisk - lowerrisk) * 10, NA)))
  return(df)
}

# Function to normalise with upper and lower bounds (when high score = high vulnerability)
normfuncpos <- function(df, upperrisk, lowerrisk, col1) {
  # Create new column col_name as sum of col1 and col2
  df[[paste0(col1, "_norm")]] <- ifelse(df[[col1]] >= upperrisk, 10,
                                        ifelse(df[[col1]] <= lowerrisk, 0,
                                               ifelse(df[[col1]] < upperrisk & df[[col1]] > lowerrisk, 10 - (upperrisk - df[[col1]]) / 
                                                        (upperrisk - lowerrisk) * 10, NA)))
  return(df)
}

#--------------------FUNCTIONS TO LOAD INPUT DATA-----------------
loadInputs <- function(filename, group_by = "CountryCode", as_of = Sys.Date(), format = "csv", full = F){
  # The as_of argument let's you run the function from a given historical date. Update indicators.R
  # to use this feature -- turning indicators.R into a function? with desired date as an argument
  
  if(format == "csv") {
    # Read in CSV
    data <- suppressMessages(read_csv(paste0("inputs-archive/", filename, ".csv")))
  }
  if(format == "tbl") {
    # Read from Spark DataFrame
  }
  # Select only data from before the as_of date, for reconstructing historical indicators
  if(as_of < Sys.Date()) {
    data <- filter(data, access_date <= as_of)
  }
  # Select the most recent access_date for each group, unless group_by = F
  if(is.null(group_by)) {
    most_recent <- data
  } else {
    most_recent <- data %>%
      # .dots allows group_by to take a vector of character strings
      group_by(.dots = group_by) %>%
      slice_max(order_by = access_date) %>%
      ungroup()
  }
  if(!full) return(most_recent)
  return(data)
}

# Move external to functions file
try_log <- function(expr) {
  fun <- sub("\\(.*", "", deparse(substitute(expr)))
  tryCatch({
    expr
  }, error = function(e) {
    write(paste(Sys.time(), "Error on", fun, "\n", e), file = "input-errors.log", append = T)
  })
}

lap <- Sys.time() - runtime
print(paste("Setup:", lap, units(lap)))
lapStart <- Sys.time()
#
##
### ********************************************************************************************
####    CREATE ACAPS SHEET USING A RANGE OF SOURCE INDICATORS ----
### ********************************************************************************************
##
#

#--------------------—LOAD ACAPS realtime database-------------------------------------------
acaps_process <- function(as_of, format) {
  # SPLIT UP INTO INPUTS SECTION
  # Load website
  acaps <- read_html("https://www.acaps.org/countries")
  
  # Select relevant columns from the site all merged into a single string
  country <- acaps %>%
    html_nodes(".severity__country__label, .severity__country__crisis__label, .severity__country__crisis__value") %>%
    html_text()
  
  # Find country labels in the string (select 2nd lag behind a numberic variable if the given item is a character)
  countryisolate <- suppressWarnings(ifelse(!is.na(as.numeric(as.character(country))) & lag(is.na(as.numeric(as.character(country))), 2),
                                            lag(country, 2),
                                            NA
  ))
  
  # For all variables that have a country label, change to iso categories
  country[which(!is.na(countryisolate)) - 2] <- countrycode(country[which(!is.na(countryisolate)) - 2],
                                                            origin = "country.name",
                                                            destination = "iso3c",
                                                            nomatch = NULL
  )
  
  # Collect list of all world countries
  world <- map_data("world")
  world <- world %>%
    dplyr::rename(Country = region) %>%
    dplyr::mutate(Country = suppressWarnings(countrycode(Country,
                                                         origin = "country.name",
                                                         destination = "iso3c",
                                                         nomatch = NULL
    )))
  
  countrynam <- levels(as.factor(world$Country))
  
  # Find all countries in the list and replace with correct country name (then fill in remaining NAs)
  gap <- ifelse(country %in% countrynam, country, NA)
  gaplist <- na.locf(gap)
  
  # Create new dataframe with correct countrynames
  # FIX: column names are swapped. Country column lists event, not countryname
  acapslist <- cbind.data.frame(country, gaplist)
  acapslist <- acapslist[!acapslist$country %in% countrynam, ]
  acapslist <- acapslist %>%
    filter(country != "Countrylevel")
  
  # Create new column with the risk scores (and duplicate for missing rows up until the correct value)
  acapslist$risk <- suppressWarnings(ifelse(!is.na(as.numeric(as.character(acapslist$country))), as.numeric(as.character(acapslist$country)), NA))
  acapslist$risk <- c(na.locf(acapslist$risk), NA)
  
  # Remove duplicate rows and numeric rows
  acapslist <- acapslist %>%
    filter(is.na(as.numeric(as.character(country)))) %>%
    filter(country != "Country level") %>%
    filter(country != "Country Level") %>%
    filter(country != "")
  
  # Save csv with full acapslist
  # write.csv(acapslist, "Indicator_dataset/acaps.csv")
  
  # List of countries with specific hazards
  conflictnams <- acapslist %>%
    filter(str_detect(acapslist$country, c("conflict|Crisis|crisis|Conflict|Refugees|refugees|
                                       Migration|migration|violence|violence|Boko Haram"))) %>%
    filter(risk >= 4) %>%
    dplyr::select(gaplist)
  
  conflictnams <- unique(conflictnams)
  
  # Food security countries
  foodnams <- acapslist[str_detect(acapslist$country, c("Food|food|famine|famine")), ] %>%
    filter(risk >= 4) %>%
    dplyr::select(gaplist)
  
  foodnams <- unique(foodnams)
  
  # Natural hazard countries
  naturalnams <- acapslist[str_detect(acapslist$country, c("Floods|floods|Drought|drought|Cyclone|cyclone|
                                                        Flooding|flooding|Landslides|landslides|
                                                        Earthquake|earthquake")), ] %>%
    filter(risk >= 3) %>%
    dplyr::select(gaplist)
  
  naturalnams <- unique(naturalnams)
  
  # Epidemic countries
  healthnams <- acapslist[str_detect(acapslist$country, c("Epidemic|epidemic")), ] %>%
    filter(risk >= 3) %>%
    dplyr::select(gaplist)
  
  healthnams <- unique(healthnams)
  
  # Load countries in the CRM
  countrylist <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv"))
  
  acapssheet <- countrylist %>%
    dplyr::select(-X) %>%
    mutate(
      Fr_conflict_acaps = case_when(
        Country %in% unlist(as.list(conflictnams)) ~ 10,
        TRUE ~ 0
      ),
      H_health_acaps = case_when(
        Country %in% unlist(as.list(healthnams)) ~ 10,
        TRUE ~ 0
      ),
      NH_natural_acaps = case_when(
        Country %in% unlist(as.list(naturalnams)) ~ 10,
        TRUE ~ 0
      ),
      F_food_acaps = case_when(
        Country %in% unlist(as.list(foodnams)) ~ 10,
        TRUE ~ 0
      )
    )
  
  return(acapssheet)
}

acapssheet <- try_log(acaps_process(as_of = as_of, format = format))

lap <- Sys.time() - lapStart
print(paste("ACAPS sheet written", lap, units(lap)))
lapStart <- Sys.time()
#
##
### ********************************************************************************************
####    HEALTH: CREATE HEALTH SHEET USING A RANGE OF SOURCE INDICATORS ----
### ********************************************************************************************
##
#

#--------------------—HIS Score-----------------
ghsi_process <- function(as_of, format) {
  
  # OR instead of splitting, I could wrap everything above this (read.csv to archive) 
  # in an if statement, so you can run the script without this section if you're
  # trying to recreate data
  ghsi <- loadInputs("ghsi", group_by = "Country", as_of = as_of, format = format, as_of = as_of, format = format)
  
  # Normalise scores
  # Rename HIS to ghsi *everywhere*
  HIS <- normfuncneg(ghsi, 20, 70, "H_HIS_Score")
  return(HIS)
}
HIS <- try_log(ghsi_process(as_of = as_of, format = format))

#-----------------------—Oxford rollback Score-----------------
# RENAME Oxrollback to oxford_openness_risk
oxford_openness_process <- function(as_of, format) {
  OXrollback <- loadInputs("oxford_openness_risk", group_by = c("CountryCode", "Date"), as_of = as_of, format = format)
  
  # Remove NAs and select columns
  # Risk of Openness is a time series; select most recent
  OXrollback <- OXrollback[!is.na(OXrollback$openness_risk),c("CountryCode", "Date", "openness_risk")] %>%
    # mutate(Date = as.Date(Date)) %>%
    arrange(desc(Date)) %>%
    { .[!duplicated(.$CountryCode),] } %>%
    dplyr::select(-Date)
  
  colnames(OXrollback) <- paste0("H_", colnames(OXrollback))
  
  OXrollback <- OXrollback %>%
    rename(
      H_Oxrollback_score = H_openness_risk,
      Country = H_CountryCode
    ) #%>%
  # mutate(
  #   Country = countrycode(Countryname,
  #   origin = "country.name",
  #   destination = "iso3c",
  #   nomatch = NULL
  # ))
  
  upperrisk <- quantile(OXrollback$H_Oxrollback_score, probs = c(0.9), na.rm = T)
  lowerrisk <- quantile(OXrollback$H_Oxrollback_score, probs = c(0.1), na.rm = T)
  
  OXrollback <- normfuncpos(OXrollback, upperrisk, lowerrisk, "H_Oxrollback_score")
  return(OXrollback)
}
OXrollback <- try_log(oxford_openness_process(as_of = as_of, format = format))

#------------------------—COVID deaths and cases--------------------------
owid_covid_process <- function(as_of, format) {
  # Switching to `read_csv()` may save ~2 seconds of Health's ~40 seconds; 6 → 4 secs
  # See warning
  covidweb <- read_csv("https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv",
                       col_types = "cccDdddddddddddddddccccccddddddddcdddcdddddcdddddddddddddddddd")
  
  # Super slow to find changed data in this 30MB file. For now, not important because
  # it includes its own historical data
  # # DELETE for first time only
  # owid_covid <- mutate(covidweb, access_date = Sys.Date() - 1)
  # write.csv(owid_covid, "output/inputs-archive/owid_covid.csv", row.names = F)
  # 
  # archiveInputs(owid_covid, group_by = c("iso_code", "date"))
  # # SPLIT: move above to inputs.R
  # covidweb <- loadInputs("owid_covid", group_by = c("iso_code", "date"), as_of = as_of, format = format)
  
  covid <- covidweb %>%
    mutate(date = as.Date(date)) %>%
    filter(date > Sys.Date() - 28)
  
  # bi-weekly growth rate for covid deaths and cases
  covidgrowth <- covid %>%
    mutate(
      previous2week = case_when(
        date >= Sys.Date() - 13 ~ "twoweek",
        TRUE ~ "lasttwoweek"
      )) %>%
    group_by(iso_code, previous2week) %>%
    summarise(
      meandeaths = mean(new_deaths_per_million, na.rm = T),
      meancase = mean(new_cases_per_million, na.rm = T),
    )
  
  covidgrowth <- covidgrowth %>%
    group_by(iso_code) %>%
    filter(!is.na(meandeaths) & !is.na(meancase)) 
  
  # remove countries without two weeks
  # Slow (~4 seconds)
  covidgrowth <- covidgrowth %>%
    mutate(remove = iso_code %in% 
             as.data.frame(covidgrowth %>% 
                             dplyr::count(iso_code) %>% 
                             filter(n == 2) %>% 
                             dplyr::select(iso_code))$iso_code)
  
  # Calculate variables of interest
  covidgrowth <- covidgrowth %>%
    filter(remove == TRUE) %>%
    mutate(
      growthdeath = meandeaths[previous2week == "twoweek"] - meandeaths,
      growthratedeaths = case_when(
        meandeaths[previous2week == "lasttwoweek"] == 0 ~ 0.01,
        meandeaths > 0 ~ growthdeath / meandeaths[previous2week == "lasttwoweek"] * 100,
        TRUE ~ NA_real_
      ),
      growthcase = meancase[previous2week == "twoweek"] - meancase,
      growthratecases = case_when(
        meandeaths[previous2week == "lasttwoweek"] == 0 ~ 0.01,
        meancase > 0 ~ growthcase / meancase[previous2week == "lasttwoweek"] * 100,
        TRUE ~ NA_real_
      )
    ) %>%
    dplyr::filter(previous2week != "twoweek") %>%
    dplyr::select(-previous2week, -growthcase, -growthdeath, -meandeaths, -meancase, -remove)
  
  # Normalised scores for deaths
  covidgrowth <- normfuncpos(covidgrowth, 150, 0, "growthratedeaths")
  covidgrowth <- normfuncpos(covidgrowth, 150, 0, "growthratecases")
  
  # Rename columns
  colnames(covidgrowth) <- c(
    "Country", "H_Covidgrowth_biweeklydeaths", "H_Covidgrowth_biweeklycases",
    "H_Covidgrowth_deathsnorm", "H_Covidgrowth_casesnorm"
  )
  
  # Varibles on number of cases
  covidcurrent <- covid %>% 
    group_by(iso_code) %>%
    top_n(n = 1, date) %>%
    # filter(date == Sys.Date() - 1) %>%
    # filter(date == max(date)) %>% # This does not select the most recent date for each country
    dplyr::select(iso_code, new_cases_smoothed_per_million, new_deaths_smoothed_per_million) %>%
    rename(Country = iso_code)
  
  covidcurrent <- normfuncpos(covidcurrent, 250, 0, "new_cases_smoothed_per_million")
  covidcurrent <- normfuncpos(covidcurrent, 5, 0, "new_deaths_smoothed_per_million")
  
  # Rename columns
  colnames(covidcurrent) <- c(
    "Country", "H_new_cases_smoothed_per_million", "H_new_deaths_smoothed_per_million",
    "H_new_cases_smoothed_per_million_norm", "H_new_deaths_smoothed_per_million_norm"
  )
  
  # #—(Alternative COVID deaths)
  # # Load COVID data
  # cov <- read.csv("https://raw.githubusercontent.com/scc-usc/ReCOVER-COVID-19/master/results/forecasts/global_deaths_current_0.csv")
  # cov_current <- read.csv("https://raw.githubusercontent.com/scc-usc/ReCOVER-COVID-19/master/results/forecasts/global_deaths.csv")
  # 
  # # Summarise country totals (forecast)
  # cov_dat <- cov %>%
  #   dplyr::select(Country, colnames(cov)[10], colnames(cov)[9]) %>%
  #   rename(
  #     w8forecast = colnames(cov)[10], 
  #     w7forecast = colnames(cov)[9]
  #     ) %>%
  #   mutate(Country = suppressWarnings(countrycode(Country, 
  #                                                 origin = "country.name",
  #                                                 destination = "iso3c"
  #                                                 )
  #          )) %>%
  #   drop_na(Country)
  # 
  # # Summarise country totals (current)
  # cov_cur <- cov_current %>%
  #   dplyr::select(Country, last(colnames(cov_current))) %>%
  #   rename(
  #     current = last(colnames(cov_current)),
  #     ) %>%
  #   mutate(
  #     Country = suppressWarnings(countrycode(Country, 
  #                                                 origin = "country.name",
  #                                                 destination = "iso3c"
  #                                            )
  #       )) %>%
  #   drop_na(Country)
  # 
  # # Add population
  # pop <- wpp.by.year(wpp.indicator("tpop"), 2020)
  # 
  # pop$charcode <- suppressWarnings(countrycode(pop$charcode, 
  #                                              origin = "iso2c", 
  #                                              destination = "iso3c"
  #                                              )
  #                                  )
  # 
  # colnames(pop) <- c("Country", "Population")
  # 
  # # Join datasets
  # cov_forcast_alt <- left_join(cov_dat, pop, by = "Country", keep = F) %>%
  #   left_join(., cov_cur) %>%
  #   drop_na(Country) %>%
  #   mutate(
  #     week_increase = w8forecast - w7forecast,
  #     new_death_per_m = week_increase / (Population / 1000),
  #     add_death_prec_current = ((w8forecast / current) * 100) - 100
  #     ) %>%
  #   rename_with(.fn = ~ paste0("H_", .), 
  #               .cols = colnames(.)[-1]
  #               )
  # 
  # # Normalise
  # cov_forcast_alt <- normfuncpos(cov_forcast_alt, 100, 0, "H_add_death_prec_current")
  return(list(
    covidcurrent,
    covidgrowth
  ))
}

owid_covid <- try_log(owid_covid_process(as_of = as_of, format = format))
covidcurrent <- owid_covid[1]
covidgrowth <- owid_covid[2]

#--------------------------—Oxford Response Tracker----------------------------
Oxres_process <- function(as_of, format) {
  # SLOW: 10 seconds with w/ `read.csv`, 5 with `read_csv`
  # see warning
  Oxres <- read_csv("https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv",
                    col_types = "cccccdddddddddddddddddddddddddddddddddcdddddddddddd")
  
  Oxres$Date <- as.Date(as.character(Oxres$Date), "%Y%m%d")
  
  # Not including inputsArchive because dataset is large and includes historical data.
  # Still SPLIT?
  
  #Select latest data
  Ox_cov_resp <- Oxres %>%
    group_by(CountryCode) %>%
    filter(Date == max(Date)) %>%
    dplyr::select(
      CountryCode, Date, GovernmentResponseIndex, GovernmentResponseIndexForDisplay,
      EconomicSupportIndex, EconomicSupportIndexForDisplay, ContainmentHealthIndex,
      ContainmentHealthIndexForDisplay, `E1_Income support`, E1_Flag
    )
  
  colnames(Ox_cov_resp) <- c("Country", paste0("H_", colnames(Ox_cov_resp[,-1])))
  
  #Create normalised scores
  Ox_cov_resp <- normfuncneg(Ox_cov_resp, 15, 80, "H_GovernmentResponseIndexForDisplay")
  Ox_cov_resp <- normfuncneg(Ox_cov_resp, 0, 100, "H_EconomicSupportIndexForDisplay")
  return(Ox_cov_resp)
}
Ox_cov_resp <- try_log(Oxres_process(as_of = as_of, format = format))

#------------------------------—INFORM COVID------------------------------------------------------
inform_covid_process <- function(as_of, format) {
  inform_covid_warning <- loadInputs("inform_covid", group_by = c("Country"), as_of = as_of, format = format)
  inform_covid_warning <- normfuncpos(inform_covid_warning, 6, 2, "H_INFORM_rating.Value")
  return(inform_covid_warning)
}
inform_covid_warning <- try_log(inform_covid_process(as_of = as_of, format = format))

#----------------------------------—WHO DONS--------------------------------------------------------------
# REPLACE all WMO with WHO
who_process <- function(as_of, format) {
  wmo_don_full <- loadInputs("who_don", group_by = NULL, as_of = as_of, format = format)
  
  countrylist <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv"))
  wmo_don <- countrylist %>%
    dplyr::select(-X) %>%
    # Should we specify which dates we care about?
    # Also a problem where *end* of outbreaks are also reported
    mutate(wmo_don_alert = case_when(Country %in% wmo_don_full$wmo_country_alert ~ 10,
                                     TRUE ~ 0))  %>%
    rename(H_wmo_don_alert = wmo_don_alert) %>%
    dplyr::select(-Countryname)
  return(wmo_don)
}
wmo_don <- try_log(who_process(as_of = as_of, format = format))

#---------------------------------—Health ACAPS---------------------------------
acaps_health <- acapssheet[,c("Country", "H_health_acaps")]

#----------------------------------—Create combined Health Sheet-------------------------------------------
collate_health <- function(format) {
  countrylist <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv"))
  countrylist <- countrylist %>%
    dplyr::select(-X)
  
  health_sheet <- left_join(countrylist, HIS, by = "Country") %>%
    left_join(., OXrollback, by = "Country") %>%
    left_join(., covidgrowth, by = "Country") %>%
    left_join(., covidcurrent, by = "Country") %>%
    left_join(., Ox_cov_resp, by = "Country") %>%
    # left_join(., cov_forcast_alt, by = "Country") %>% # not current
    left_join(., inform_covid_warning, by = "Country", "Countryname") %>%
    left_join(., wmo_don, by = "Country") %>%
    left_join(., acaps_health, by = "Country") %>%
    arrange(Country)
  
  if(format == "csv" | format == "both") {
    write.csv(health_sheet, "Risk_sheets/healthsheet.csv")
  }
  if(format == "tbl" | format == "both") {
    # Write Spark DataFrame
  }
  
}

collate_health(format)

lap <- Sys.time() - lapStart
print(paste("Health sheet written", lap, units(lap)))
lapStart <- Sys.time()
#
##
### ********************************************************************************************
####    FOOD SECURITY: CREATE FOOD SECURITY SHEET USING A RANGE OF SOURCE INDICATORS ----
### ********************************************************************************************
##
#

#---------------------------------—LOAD FOOD SECURITY DATA---------------------------
# -------------------------------— Proteus Index -------------------------------
proteus_process <- function(as_of, format) {
  
  proteus <- loadInputs("proteus", group_by = c("Country"), as_of = as_of, format = format)
  
  upperrisk <- quantile(proteus$F_Proteus_Score, probs = c(0.90), na.rm = T)
  lowerrisk <- quantile(proteus$F_Proteus_Score, probs = c(0.10), na.rm = T)
  proteus <- normfuncpos(proteus, upperrisk, lowerrisk, "F_Proteus_Score")
  return(proteus)
}
proteus <- try_log(proteus_process(as_of = as_of, format = format))

#------------------—FEWSNET (with CRW threshold)---
fews_process <- function(as_of, format) {
  fewswb <- loadInputs("fewsnet", group_by = c("country", "year_month"), as_of = as_of, format = format)
  
  #Calculate country totals
  fewsg <- fewswb %>%
    #  dplyr::select(-X) %>%
    group_by(country, year_month) %>%
    mutate(countrypop = sum(pop)) %>%
    ungroup()
  
  #Calculate proportion and number of people in IPC class 
  fewspop <- fewsg %>%
    group_by(country, year_month) %>%
    mutate(
      countryproportion = (pop / countrypop) * 100,
      ipc3plusabsfor = case_when(fews_proj_med_adjusted >=3 ~ pop,
                                 TRUE ~ NA_real_),
      ipc3pluspercfor = case_when(fews_proj_med_adjusted >=3 ~ countryproportion,
                                  TRUE ~ NA_real_),
      ipc4plusabsfor = case_when(fews_proj_med_adjusted >= 4 ~ pop,
                                 TRUE ~ NA_real_),
      ipc4pluspercfor = case_when(fews_proj_med_adjusted >= 4 ~ countryproportion,
                                  TRUE ~ NA_real_),
      ipc3plusabsnow = case_when(fews_ipc_adjusted >=3 ~ pop,
                                 TRUE ~ NA_real_),
      ipc3pluspercnow = case_when(fews_ipc_adjusted >=3 ~ countryproportion,
                                  TRUE ~ NA_real_),
      ipc4plusabsnow = case_when(fews_ipc_adjusted >= 4 ~ pop,
                                 TRUE ~ NA_real_),
      ipc4pluspercnow = case_when(fews_ipc_adjusted >= 4 ~ countryproportion,
                                  TRUE ~ NA_real_)
    )
  
  #Functions to calculate absolute and geometric growth rates
  pctabs <- function(x) x- lag(x)
  pctperc <- function(x) x - lag(x) / lag(x)
  
  #Summarise country totals per in last round of FEWS
  fewssum <- fewspop %>%
    filter(year_month == "2021_02" | year_month == "2020_10") %>%
    group_by(country, year_month) %>%
    mutate(totalipc3plusabsfor = sum(ipc3plusabsfor, na.rm=T),
           totalipc3pluspercfor = sum(ipc3pluspercfor, na.rm=T),
           totalipc4plusabsfor = sum(ipc4plusabsfor, na.rm=T),
           totalipc4pluspercfor = sum(ipc4pluspercfor, na.rm=T),
           totalipc3plusabsnow = sum(ipc3plusabsnow, na.rm=T),
           totalipc3pluspercnow = sum(ipc3pluspercnow, na.rm=T),
           totalipc4plusabsnow = sum(ipc4plusabsnow, na.rm=T),
           totalipc4pluspercnow = sum(ipc4pluspercnow, na.rm=T)) %>%
    distinct(country, year_month, .keep_all = TRUE) %>%
    dplyr::select(-ipc3plusabsfor, -ipc3pluspercfor, -ipc4plusabsfor, -ipc4pluspercfor, 
                  -ipc3plusabsnow, -ipc3pluspercnow, -ipc4plusabsnow, -ipc4pluspercnow,
                  -admin_name, -pop) %>%
    group_by(country) %>%
    mutate(pctchangeipc3for = pctabs(totalipc3pluspercfor),
           pctchangeipc4for = pctperc(totalipc4pluspercfor),
           pctchangeipc3now = pctabs(totalipc3pluspercnow),
           pctchangeipc4now = pctperc(totalipc4pluspercnow),
           diffactfor = totalipc3pluspercfor - totalipc3pluspercnow,
           fshighrisk = case_when((totalipc3plusabsfor >= 5000000 | totalipc3pluspercfor >= 20) & pctchangeipc3for >= 5  ~ "High risk",
                                  (totalipc3plusabsnow >= 5000000 | totalipc3pluspercnow >= 20) & pctchangeipc3now >= 5  ~ "High risk",
                                  totalipc4pluspercfor >= 2.5  & pctchangeipc4for >= 10  ~ "High risk",
                                  totalipc4pluspercnow >= 2.5  & pctchangeipc4now >= 10  ~ "High risk",
                                  TRUE ~ "Not high risk")) %>%
    dplyr::select(-fews_ipc, -fews_ha, -fews_proj_near, -fews_proj_near_ha, -fews_proj_med, 
                  -fews_proj_med_ha, -fews_ipc_adjusted, -fews_proj_med_adjusted, -countryproportion) %>%
    filter(year_month == "2021_02")
  
  # Find max ipc for any region in the country
  fews_summary <- fewsg %>%
    group_by(country, year_month) %>%
    summarise(max_ipc = max(fews_proj_med_adjusted, na.rm = T)) %>%
    mutate(
      year_month = str_replace(year_month, "_", "-"),
      year_month = as.Date(as.yearmon(year_month)),
      year_month = as.Date(year_month)) %>%
    filter(!is.infinite(max_ipc)) %>%
    filter(year_month == max(year_month, na.rm = T))
  
  # Join the two datasets
  fews_dataset <- left_join(fewssum, fews_summary, by = "country") %>%
    mutate(
      fews_crm_norm = case_when(
        fshighrisk == "High risk" ~ 10,
        fshighrisk != "High risk" & max_ipc == 5 ~ 9,
        fshighrisk != "High risk" & max_ipc == 4 ~ 8,
        fshighrisk != "High risk" & max_ipc == 3 ~ 7,
        fshighrisk != "High risk" & max_ipc == 2 ~ 5,
        fshighrisk != "High risk" & max_ipc == 1 ~ 3,
        TRUE ~ NA_real_
      ),
      Country = countrycode(
        country,
        origin = "country.name",
        destination = "iso3c",
        nomatch = NULL
      )) %>%
    dplyr::select(-country) %>%
    rename_with(
      .fn = ~ paste0("F_", .),
      .cols = colnames(.)[!colnames(.) %in% c("Country", "country")]
    )
  
  colnames(fews_dataset[-1]) <- paste0("F_", colnames(fews_dataset[-1])) 
  return(fews_dataset)
}

fews_dataset <- try_log(fews_process(as_of = as_of, format = format))

#------------------------—WBG FOOD PRICE MONITOR------------------------------------
fpi_process <- function(as_of, format) {
  ag_ob_data <- read.csv(paste0(github, "Indicator_dataset/food-inflation.csv"))
  # FIX: Not yet recording historical data because data is structured messily, with 
  # dates as columns. Fortunately, dataset includes historical data
  ag_ob_data <- ag_ob_data %>%
    mutate_at(
      vars(contains("19"), contains("20"), contains("21")),
      ~ as.numeric(as.character(gsub(",", ".", .)))
    )
  
  ag_ob <- ag_ob_data %>%
    filter(X == "Food Change Yoy") %>%
    dplyr::select(-Income.Level, -Color.Bin, -X) %>%
    group_by(Country) %>%
    summarise(
      Sep = Sep.20[which(!is.na(Sep.20))[1]],
      Oct = Oct.20[which(!is.na(Oct.20))[1]],
      Nov = Nov.20[which(!is.na(Nov.20))[1]],
      Dec = Dec.20[which(!is.na(Dec.20))[1]],
      Jan = Jan.21[which(!is.na(Jan.21))[1]],
      Feb = Feb.21[which(!is.na(Feb.21))[1]],
      Mar = Mar.20[which(!is.na(Mar.21))[1]],
      Apr = Apr.21[which(!is.na(Apr.21))[1]],
      May = May.21[which(!is.na(May.21))[1]],
      Jun = Jun.21[which(!is.na(Jun.21))[1]],
      Jul = Jul.21[which(!is.na(Jul.21))[1]],
      Aug = Aug.21[which(!is.na(Aug.21))[1]]
    ) %>%
    mutate(fpv = case_when(
      !is.na(Aug) ~ Aug,
      is.na(Aug) & !is.na(Jul) ~ Jul,
      is.na(Aug) & is.na(Jul) & !is.na(Jun) ~ Jun,
      TRUE ~ NA_real_
    ),
    fpv_rating = case_when(
      fpv <= 0.02 ~ 1,
      fpv > 0.02 & fpv <= 0.05 ~ 3,
      fpv > 0.05 & fpv <= 0.30 ~ 5,
      fpv >= 0.30 ~ 7,
      TRUE ~ NA_real_
    ),
    Country = countrycode(Country,
                          origin = "country.name",
                          destination = "iso3c",
                          nomatch = NULL
    )) %>%
    rename_with(   
      .fn = ~ paste0("F_", .),
      .cols = colnames(.)[!colnames(.) %in% c("Country")]
    )
  return(ag_ob)
}
ag_ob <- try_log(fpi_process(as_of = as_of, format = format))


#-------------------------—FAO/WFP HOTSPOTS----------------------------
fao_wfp_process <- function(as_of, format) {
  # Kind of unnecessary
  fao_wfp <- loadInputs("fao_wfp", group_by = c("Country"), as_of = as_of, format = format)
}

collate_food <- function() {
  #------------------------—Create combined Food Security sheet--------------------
  countrylist <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv"))
  countrylist <- countrylist %>%
    dplyr::select(-X)
  
  food_sheet <- left_join(countrylist, proteus, by = "Country") %>%
    left_join(., fews_dataset, by = "Country") %>%
    # left_join(., fpv, by = "Country") %>%  # Reintroduce if FAO price site comes back online
    # left_join(., fpv_alt, by = "Country") %>% # not current
    # left_join(., artemis, by = "Country") %>% # not current
    left_join(., ag_ob, by = "Country") %>%
    left_join(., fao_wfp, by = "Country") %>%
    arrange(Country)
  
  if(format == "csv" | format == "both") {
    write.csv(food_sheet, "Risk_sheets/foodsecuritysheet.csv")
  }
  if(format == "tbl" | format == "both") {
    # Write Spark DataFrame
  }
}

collate_food(format)

lap <- Sys.time() - lapStart
print(paste("Food sheet written", lap, units(lap)))
lapStart <- Sys.time()
#
##
### ********************************************************************************************
####    MACRO: CREATE MACRO  SHEET USING A RANGE OF SOURCE INDICATORS ----
### ********************************************************************************************
##
#

#---------------------------—Economist Intelligence Unit---------------------------------
eiu_process <- function(as_of, format) {
  eiu_data <- loadInputs("eiu", group_by = c("`SERIES NAME`", "MONTH")) %>%
    select(-access_date)
  
  country_nam <- colnames(eiu_data) 
  country_nam <- country_nam[4:length(country_nam)]
  
  eiu_latest_month <- eiu_data %>%
    filter(MONTH == max(MONTH)) %>% 
    dplyr::select(-MONTH, -`SERIES CODE`) %>%
    #Pivot the database so countries are rows
    pivot_longer(
      !`SERIES NAME`,
      names_to = "Country",
      values_to = "Values"
    ) %>%
    pivot_wider(
      names_from = `SERIES NAME`,
      values_from = Values
    ) %>%
    rename(Macroeconomic_risk = `Macroeconomic risk`) %>%
    mutate(Macroeconomic_risk = (`Financial risk` + Macroeconomic_risk + `Foreign trade & payments risk`) / 3)
  
  eiu_one_year <- eiu_data %>%
    filter(MONTH %in% unique(eiu_data$MONTH)[-1]) %>%
    group_by(`SERIES NAME`) %>%
    summarise_at(country_nam, mean, na.rm = T) %>%
    ungroup %>%
    distinct(`SERIES NAME`, .keep_all = T) %>% 
    #Pivot the database so countries are rows
    pivot_longer(
      !`SERIES NAME`,
      names_to = "Country",
      values_to = "Values"
    ) %>%
    pivot_wider(
      names_from = `SERIES NAME`,
      values_from = Values
    ) %>%
    rename_with(
      .col = c(contains("risk"), contains("Overall")),
      .fn  = ~ paste0(., "_12")
    ) %>%
    rename(Macroeconomic_risk_12 = `Macroeconomic risk_12`) %>%
    mutate(Macroeconomic_risk_12 = (`Financial risk_12` + Macroeconomic_risk_12 + `Foreign trade & payments risk_12`) / 3)
  
  eiu_three_month <- eiu_data %>%
    filter(MONTH %in% head(unique(MONTH)[-1], 3)) %>%
    group_by(MONTH, `SERIES NAME`) %>%
    summarise_at(country_nam, mean, na.rm = T) %>%
    ungroup %>%
    dplyr::select(-MONTH) %>%
    distinct(`SERIES NAME`, .keep_all = T) %>% 
    #Pivot the database so countries are rows
    pivot_longer(
      !`SERIES NAME`,
      names_to = "Country",
      values_to = "Values"
    ) %>%
    pivot_wider(
      names_from = `SERIES NAME`,
      values_from = Values
    ) %>%
    rename_with(
      .col = c(contains("risk"), contains("Overall")),
      .fn  = ~ paste0(., "_3")
    ) %>%
    rename(Macroeconomic_risk_3 = `Macroeconomic risk_3`) %>%
    mutate(Macroeconomic_risk_3 = (`Financial risk_3` + Macroeconomic_risk_3 + `Foreign trade & payments risk_3`) / 3)
  
  # Join datasets
  eiu_joint <- left_join(eiu_latest_month, eiu_three_month, by = "Country") %>%
    left_join(., eiu_one_year, by = "Country") %>%
    mutate(
      EIU_3m_change = Macroeconomic_risk - Macroeconomic_risk_3,
      EIU_12m_change = Macroeconomic_risk - Macroeconomic_risk_12
    ) %>%
    dplyr::select(contains("Country"), contains("Macro"), contains("EIU")) %>%
    rename_with(
      .col = c(contains("Macro"), contains("EIU")),
      .fn = ~ paste0("M_", .)
    ) %>%
    rename(M_EIU_Score = `M_Macroeconomic_risk`,
           M_EIU_Score_12m = `M_Macroeconomic_risk_12`) %>%
    # Add Country name
    mutate(
      Country = suppressWarnings(countrycode(Country,
                                             origin = "country.name",
                                             destination = "iso3c",
                                             nomatch = NULL))
    )
  
  eiu_joint <- normfuncpos(eiu_joint, quantile(eiu_joint$M_EIU_Score, 0.90), quantile(eiu_joint$M_EIU_Score, 0.10), "M_EIU_Score")
  eiu_joint <- normfuncpos(eiu_joint, quantile(eiu_joint$M_EIU_12m_change, 0.90), quantile(eiu_joint$M_EIU_12m_change, 0.10), "M_EIU_12m_change")
  eiu_joint <- normfuncpos(eiu_joint, quantile(eiu_joint$M_EIU_Score_12m, 0.90), quantile(eiu_joint$M_EIU_Score_12m, 0.10), "M_EIU_Score_12m")
  return(eiu_joint)
}
eiu_joint <- try_log(eiu_process(as_of = as_of, format = format))

collate_macro <- function() {
  #-----------------------------—Create Combined Macro sheet-----------------------------------------
  countrylist <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv"))
  countrylist <- countrylist %>%
    dplyr::select(-X)
  
  macro_sheet <- #left_join(countrylist, macro, by = "Country") %>% # not current
    # left_join(., gdp, by = "Country") %>% # not current
    # left_join(., macrofin, by = "Country") %>% # not current
    left_join(countrylist, eiu_joint, by = "Country") %>%
    # left_join(., cvi, by = "Country") %>% # not current
    arrange(Country) 
  
  if(format == "csv" | format == "both") {
    write.csv(macro_sheet, "Risk_sheets/macrosheet.csv")
  }
  if(format == "tbl" | format == "both") {
    # Write Spark DataFrame
  }
}

collate_macro(format)

lap <- Sys.time() - lapStart
print(paste("Macro sheet written", lap, units(lap)))
lapStart <- Sys.time()
#
##
### ********************************************************************************************
####    SOCIO: CREATE SOCIO-ECONOMIC SHEET USING A RANGE OF SOURCE INDICATORS ----
### ********************************************************************************************
##
#

#---------------------------—Alternative socio-economic data (based on INFORM) - INFORM Income Support
# CAN I DELETE THIS? APPARENTLY NOT USED
# inform_risk <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/INFORM_Risk.csv"), col_types = cols()))

# # # DELETE for first time only
# # inform_risk <- mutate(inform_risk, access_date = Sys.Date())
# # write.csv(inform_risk, "output/inputs-archive/inform_risk.csv", row.names = F)

# archiveInputs(inform_risk, group_by = c("Country"))
# # SPLIT: move above to inputs.R
# inform_risk <- loadInputs("inform_risk", group_by = c("Country"), as_of = as_of, format = format)

# inform_data <- inform_risk %>%
#   dplyr::select(Country, "Socio-Economic Vulnerability") %>%
#   rename(S_INFORM_vul = "Socio-Economic Vulnerability")

# inform_data <- normfuncpos(inform_data, 7, 0, "S_INFORM_vul")
# inform_data <- normfuncpos(inform_data, 7, 0, "S_INFORM_vul")

income_support_process <- function(as_of, format) {
  #------------------------—Forward-looking socio-economic variables from INFORM---------------------------
  inform_covid_warning <- loadInputs("inform_covid", group_by = c("Country"), as_of = as_of, format = format)
  socio_forward <- inform_covid_warning %>%
    dplyr::select(
      Country, H_gdp_change.Value,H_gdp_change.Rating, H_unemployment.Value,
      H_unemployment.Rating, H_income_support.Value, H_income_support.Rating
    ) %>%
    rename_with(
      .fn = ~ str_replace(., "H_", "S_"),
      .cols = colnames(.)[-1]
    ) %>%
    mutate_at(
      vars(S_gdp_change.Rating, S_unemployment.Rating),
      funs(norm = case_when(
        . == "High" ~ 10,
        . == "Medium" ~ 7,
        . == "Low" ~ 0,
        TRUE ~ NA_real_
      ))
    ) %>%
    mutate(
      S_income_support.Rating_crm_norm = case_when(
        S_income_support.Value == "No income support" ~ 7,
        S_income_support.Value == "Government is replacing more than 50% of lost salary (or if a flat sum, it  ..." ~ 3,
        S_income_support.Value == "Government is replacing less than 50% of lost salary (or if a flat sum, it  ..." ~ 0,
        TRUE ~ NA_real_
      ))
  return(socio_forward)
}
socio_forward <- try_log(income_support_process(as_of = as_of, format = format))

#--------------------------—MPO: Poverty projections----------------------------------------------------
mpo_process <- function(as_of, format) {
  mpo <- loadInputs("mpo", group_by = c("Country"), as_of = as_of, format = format)
}
mpo <- try_log(mpo_process(as_of = as_of, format = format))

#-----------------------------—HOUSEHOLD HEATMAP FROM MACROFIN-------------------------------------
macrofin_process <- function(as_of, format) {
  macrofin <- loadInputs("macrofin", group_by = c("ISO3"), as_of = as_of, format = format)
  macrofin <- macrofin %>%
    mutate_at(
      vars(`Monetary.and.financial.conditions`, contains("risk")),
      funs(case_when(
        . == "Low" ~ 0,
        . == "Medium" ~ 0.5,
        . == "High" ~ 1,
        TRUE ~ NA_real_
      ))) %>%
    mutate(macrofin_risk = dplyr::select(., `Spillover.risks.from.the.external.environment.outside.the.region`:`Household.risks`) %>% rowSums(na.rm=T)) %>%
    rename_with(
      .fn = ~ paste0("M_", .),
      .cols = colnames(.)[!colnames(.) %in% c("Country.Name","ISO3")]
    ) %>%
    rename(Country = ISO3) %>%
    dplyr::select(-`Country.Name`)
  
  macrofin <- normfuncpos(macrofin, 2.1, 0, "M_macrofin_risk")
  
  household_risk <- macrofin %>%
    dplyr::select(Country, M_Household.risks) %>%
    mutate(M_Household.risks_raw = M_Household.risks,
           M_Household.risks = case_when(
             M_Household.risks == 0.5 ~ 7,
             M_Household.risks == 1 ~ 10,
             TRUE ~ M_Household.risks
           )) %>%
    rename(S_Household.risks = M_Household.risks,
           S_Household.risks_raw = M_Household.risks_raw)
  return(household_risk)
}
household_risk <- try_log(macrofin_process(as_of = as_of, format = format))

#----------------------------—WB PHONE SURVEYS-----------------------------------------------------
phone_process <- function(as_of, format) {
  wb_phone  <- loadInputs("wb_phone", group_by = c("Country"), as_of = as_of, format = format)
}
wb_phone <- try_log(phone_process(as_of = as_of, format = format))
#------------------------------—IMF FORECASTED UNEMPLOYMENT-----------------------------------------
imf_process <- function(as_of, format) {
  imf_unemployment  <- loadInputs("imf_unemployment", group_by = c("Country"), as_of = as_of, format = format)
  # FIX
  
  imf_un <- imf_unemployment %>%
    mutate_at(
      vars(starts_with("20")),
      ~ as.numeric(as.character(.))
    ) %>%
    mutate(change_unemp_21 = `2021` - `2020`,
           change_unemp_20 = `2020` - `2019`) %>%
    rename(
      Countryname = Country,
      Country = ISO3
    ) %>%
    rename_with(
      .fn = ~ paste0("S_", .),
      .cols = -contains("Country")
    ) %>%
    rename_with(.fn = ~ gsub("\\s", ".", .)) %>%
    dplyr::select(-Countryname) %>%
    filter(S_Subject.Descriptor == "Unemployment rate")
  
  # Normalise values
  imf_un <- normfuncpos(imf_un, 1, 0, "S_change_unemp_21")
  imf_un <- normfuncpos(imf_un, 3, 0, "S_change_unemp_20")
  
  # Max values for index
  imf_un <- imf_un %>%
    mutate(
      S_change_unemp_norm = rowMaxs(as.matrix(dplyr::select(.,
                                                            S_change_unemp_21_norm,
                                                            S_change_unemp_20_norm)),
                                    na.rm = T),
      S_change_unemp_norm = case_when(is.infinite(S_change_unemp_norm) ~ NA_real_,
                                      TRUE ~ S_change_unemp_norm)
    )
  
  return(imf_un)
}
# FIX WARNINGS
imf_un <- try_log(imf_process(as_of = as_of, format = format))

collate_socioeconomic <- function() {
  #--------------------------—Create Socioeconomic sheet -------------------------------------------
  socioeconomic_sheet <- #left_join(countrylist, ocha, by = "Country") %>% # not current
    #dplyr::select(-Countryname) %>%
    left_join(countrylist, inform_data, by = "Country") %>%
    left_join(., socio_forward, by = "Country") %>%
    left_join(., mpo, by = "Country") %>%
    left_join(., imf_un, by = "Country") %>%
    left_join(., household_risk, by = "Country") %>%
    left_join(., wb_phone, by = "Country") %>%
    arrange(Country)
  
  if(format == "csv" | format == "both") {
    write.csv(socioeconomic_sheet, "Risk_sheets/Socioeconomic_sheet.csv")
  }
  if(format == "tbl" | format == "both") {
    # Write Spark DataFrame
  }
}

collate_socioeconomic(format)

lap <- Sys.time() - lapStart
print(paste("Socioeconomic sheet written", lap, units(lap)))
lapStart <- Sys.time()
#
##
### ********************************************************************************************
####    NATURAL HAZARD: CREATE NATURAL HAZARDS SHEET USING A RANGE OF SOURCE INDICATORS -----
### ********************************************************************************************
##
#

#-------------------------------—NATURAL HAZARDS SHEET------------------------------------------------

#------------------------------—Load GDACS database--------------------------------------------------
# SPLIT
gdacs_process <- function(as_of, format) {
  if(format == "csv") {
    # Read in CSV
    gdacs <- suppressMessages(read_csv("inputs-archive/gdacs.csv")) %>%
      mutate(access_date = as.Date(access_date)) %>%
      filter(access_date <= as_of) %>%
      filter(access_date == max(access_date))
  }
  if(format == "tbl") {
    # Read from Spark DataFrame
  }
  
  gdaclist <- gdacs
  
  # change times
  gdaclist$date <- ifelse(gdaclist$hazard != c("drought") & gdaclist$status == "active", paste(as.Date(parse_date_time(gdaclist$date, orders = c("dm HM")))),
                          ifelse(gdaclist$hazard == c("drought"), paste(gdaclist$date),
                                 paste(as.Date(parse_date_time(gdaclist$date, orders = c("dmy"))))
                          )
  )
  
  # Remove duplicate countries for drought
  gdaclist$names <- as.character(gdaclist$names) # does this do anything?
  add <- gdaclist[which(gdaclist$hazard == "drought" & grepl("-", gdaclist$names)), ]
  gdaclist[which(gdaclist$hazard == "drought" & grepl("-", gdaclist$names)), ]$names <- sub("-.*", "", gdaclist[which(gdaclist$hazard == "drought" & grepl("-", gdaclist$names)), ]$names)
  add$names <- sub(".*-", "", add$names)
  gdaclist <- rbind(gdaclist, add)
  
  # Drought orange
  # UPDATE? Does this need to now be 2021?
  # All droughts on GDAC are current ... 
  gdaclist$status <- ifelse(gdaclist$hazard == "drought" & gdaclist$date == "2020", "active", gdaclist$status)
  gdaclist$status <- ifelse(gdaclist$hazard == "drought" & gdaclist$date == "2021", "active", gdaclist$status)
  
  # Country names
  gdaclist$namesiso <- suppressWarnings(countrycode(gdaclist$names, origin = "country.name", destination = "iso3c"))
  gdaclist$namesfull <- suppressWarnings(countrycode(gdaclist$names, origin = "country.name", destination = "iso3c", nomatch = NULL))
  
  # Create subset
  gdac <- gdaclist %>%
    dplyr::select(date, status, haz, hazard, namesiso)
  
  colnames(gdac) <- c("NH_GDAC_Date", "NH_GDAC_Hazard_Status", "NH_GDAC_Hazard_Severity", "NH_GDAC_Hazard_Type", "Country")
  
  gdac <- gdac %>%
    mutate(NH_GDAC_Hazard_Score_Norm = case_when(
      NH_GDAC_Hazard_Status == "active" & NH_GDAC_Hazard_Severity == "orange" ~ 10,
      TRUE ~ 0
    ),
    NH_GDAC_Hazard_Score = paste(NH_GDAC_Hazard_Status, NH_GDAC_Hazard_Severity, sep = " - ")
    ) %>%
    drop_na(Country)
  return(gdac)
}
gdac <- try_log(gdacs_process(as_of = as_of, format = format))

#----------------------—INFORM Natural Hazard and Exposure rating--------------------------
inform_nathaz_process <- function(as_of, format) {
  inform_risk <- loadInputs("inform_risk", group_by = c("Country"), as_of = as_of, format = format)
  # Rename country
  informnathaz <- inform_risk %>%
    dplyr::select(Country, Natural) %>%
    rename(NH_Hazard_Score = Natural) %>%
    drop_na(Country, NH_Hazard_Score)
  
  # Normalise scores
  informnathaz <- normfuncpos(informnathaz, 7, 1, "NH_Hazard_Score")
  return(informnathaz)
}
informnathaz <- try_log(inform_nathaz_process(as_of = as_of, format = format))

#---------------------------------- —IRI Seasonal Forecast ------------------------------------------
iri_process <- function(as_of, format) {
  iri_forecast <- loadInputs("iri_forecast", group_by = c("Country"), as_of = as_of, format = format)
  return(iri_forecast)
}
seasonl_risk <- try_log(iri_process(as_of = as_of, format = format) # Go through and actually name iri_forecast)

#-------------------------------------—Locust outbreaks----------------------------------------------
locust_process <- function(as_of, format) {
  fao_locust <- loadInputs("fao_locust", group_by = c("Country"), as_of = as_of, format = format)
  return(fao_locust)
}
locust_risk <- try_log(locust_process(as_of = as_of, format = format))

#---------------------------------—Natural Hazard ACAPS---------------------------------
acaps_nh <- acapssheet[,c("Country", "NH_natural_acaps")]

collate_nathaz <- function() {
  #-------------------------------------------—Create combined Natural Hazard sheet------------------------------
  countrylist <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv"))
  countrylist <- countrylist %>%
    dplyr::select(-X)
  
  nathazard_sheet <- left_join(countrylist, gdac, by = "Country") %>%
    left_join(., informnathaz, by = "Country") %>%
    left_join(., seasonl_risk, by = "Country") %>%
    left_join(., locust_risk, by = "Country") %>%
    left_join(., acaps_nh, by = "Country") %>%
    distinct(Country, .keep_all = TRUE) %>%
    drop_na(Country) %>%
    arrange(Country)
  
  if(format == "csv" | format == "both") {
    write.csv(nathazard_sheet, "Risk_sheets/Naturalhazards.csv")
  }
  if(format == "tbl" | format == "both") {
    # Write Spark DataFrame
  }
}

collate_nathaz(format)

lap <- Sys.time() - lapStart
print(paste("Natural hazard sheet written", lap, units(lap)))
lapStart <- Sys.time()
#
##
### ********************************************************************************************
####    FRAGILITY: CREATE FRAGILITY  SHEET USING A RANGE OF SOURCE INDICATORS ----
### ********************************************************************************************
##
#

# SLOW: Takes 50 seconds.

#-------------------------—FCS---------------------------------------------
fcs_process <- function(as_of, format) {
  fcs <- loadInputs("fcs", group_by = c("Country"), as_of = as_of, format = format)
  return(fcs)
}
fcv <- try_log(fcs_process(as_of = as_of, format = format))

#-----------------------------—IDPs--------------------------------------------------------
un_idp_process <- function(as_of, format) {
  un_idp <- loadInputs("un_idp", group_by = c("`Country of origin (ISO)`", "`Country of asylum (ISO)`", "`Year`"), as_of = as_of, format = format)
  # Calculate metrics
  idp <- un_idp %>%
    group_by(`Country of origin (ISO)`, Year) %>%
    summarise(
      refugees = sum(`Refugees under UNHCR mandate`, na.rm = T),
      idps = sum(`IDPs of concern to UNHCR`, na.rm = T)
    ) %>%
    group_by(`Country of origin (ISO)`) %>%
    mutate(
      sd_refugees = sd(refugees, na.rm = T),
      mean_refugees = mean(refugees, na.rm = T),
      z_refugees = (refugees - mean_refugees) / sd(refugees),
      refugees_fragile = case_when(
        z_refugees > 1 ~ "Fragile",
        z_refugees < 1 ~ "Not Fragile",
        z_refugees == NaN ~ "Not Fragile",
        TRUE ~ NA_character_
      ),
      mean_idps = mean(idps, na.rm = T),
      z_idps = case_when(
        sd(idps) != 0 ~ (idps - mean_idps) / sd(idps),
        sd(idps) == 0 ~ 0,
        TRUE ~ NA_real_
      ),
      idps_fragile = case_when(
        z_idps > 1 ~ "Fragile",
        z_idps < 1 ~ "Not Fragile",
        TRUE ~ NA_character_
      )
    ) %>%
    filter(Year == 2020) %>%
    dplyr::select(`Country of origin (ISO)`, refugees, z_refugees, refugees_fragile, idps, z_idps, idps_fragile)
  
  # Normalise scores
  idp <- normfuncpos(idp, 1, 0, "z_refugees")
  idp <- normfuncpos(idp, 1, 0, "z_idps")
  
  # Correct for countries with 0
  idp <- idp %>%
    mutate(
      z_refugees_norm = case_when(
        z_refugees == NaN ~ 0,
        TRUE ~ z_refugees_norm
      ),
      z_idps_norm = case_when(
        z_idps == NaN ~ 0,
        TRUE ~ z_idps_norm
      ),
      Country = countrycode(`Country of origin (ISO)`,
                            origin = "country.name",
                            destination = "iso3c",
                            nomatch = NULL
      )
    ) %>%
    dplyr::select(-`Country of origin (ISO)`)
  return(idp)
}
idp <- try_log(un_idp_process(as_of = as_of, format = format))

#-------------------------—ACLED data---------------------------------------------
acled_process <- function(as_of, format) {
  acled <- loadInputs("acled", group_by = NULL) #158274
  
  # Progress conflict data
  acled <- acled %>%
    mutate(
      fatalities = as.numeric(as.character(fatalities)),
      date = as.Date(event_date),
      month_yr = as.yearmon(date)
    ) %>%
    # Remove dates for the latest month (or month that falls under the prior 6 weeks)
    # Is there a way to still acknowledge countries with high fatalities in past 6 weeks?
    filter(as.Date(as.yearmon(date)) <= as.Date(as.yearmon(Sys.Date() - 45))) %>% 
    group_by(iso3, month_yr) %>%
    summarise(fatal_month = sum(fatalities, na.rm = T),
              fatal_month_log = log(fatal_month + 1)) %>%
    mutate(fatal_3_month = fatal_month + lag(fatal_month, na.rm= T) + lag(fatal_month, 2, na.rm= T),
           fatal_3_month_log = log(fatal_3_month + 1)) %>%
    group_by(iso3) %>%
    mutate(
      fatal_z = (fatal_3_month_log - mean(fatal_3_month_log, na.rm = T)) / sd(fatal_3_month_log, na.rm = T),
      sd = sd(fatal_3_month_log, na.rm = T),
      mean = mean(fatal_3_month_log, na.rm = T)
    ) %>%
    #Calculate month year based on present month (minus 6 weeks)
    filter(month_yr == paste(month.abb[month(format(Sys.Date() - 45))], year(format(Sys.Date() - 45)))) 
  
  # Normalise scores
  acled <- normfuncpos(acled, 1, -1, "fatal_z")
  
  # Correct for countries with 0
  acled <- acled %>%
    mutate(
      fatal_z_norm = case_when(
        is.nan(fatal_z) ~ 0,
        TRUE ~ fatal_z_norm
      ),
      Country = countrycode(
        iso3,
        origin = "country.name",
        destination = "iso3c",
        nomatch = NULL
      ),
      fatal_z_norm = case_when(
        fatal_3_month_log == 0 ~ 0,
        (fatal_3_month_log <= log(5 + 1)) ~ 0,
        TRUE ~ fatal_z_norm
      )
    ) %>%
    ungroup() %>%
    dplyr::select(-iso3)
  return(acled)
}
acled <- try_log(acled_process(as_of = as_of, format = format))

#--------------------------—REIGN--------------------------------------------
# Note that the file-loaded data set is one fewer than the downloaded dataset because Maia Sandu is duplicated.
# Going forward, I need to fix this so that back-and-forths in leadership are acknowledged
reign_process <- function(as_of, format) {
  reign <- loadInputs("reign", group_by = c("country", "leader", "year", "month"), as_of = as_of, format = format)
  
  reign_start <- reign %>%
    filter(year == max(year, na.rm= T)) %>%
    group_by(country) %>%
    slice(which.max(month)) %>%
    dplyr::select(country, month, pt_suc, pt_attempt, delayed, irreg_lead_ant, anticipation) %>%
    mutate(
      country = countrycode(country,
                            origin = "country.name",
                            destination = "iso3c",
                            nomatch = NULL
      )) %>%
    rename(Country = country)
  
  # Add FSI/BRD threshold (BRD is battle-related deaths)
  reign <- left_join(reign_start, fcv %>% dplyr::select(Country, FCV_normalised), by = "Country") %>%
    mutate(
      irreg_lead_ant = case_when(
        FCV_normalised == 10 ~ irreg_lead_ant,
        TRUE ~ 0
      ),
      delayed_adj = case_when(
        FCV_normalised == 10 ~ delayed,
        TRUE ~ 0
      ),
      anticipation_adj = case_when(
        FCV_normalised == 10 ~ anticipation,
        TRUE ~ 0
      ),
      pol_trigger = case_when(
        pt_suc + pt_attempt + delayed_adj + irreg_lead_ant + anticipation_adj >= 1 ~ "Fragile",
        TRUE ~ "Not Fragile"
      ),
      pol_trigger_norm = case_when(
        pt_suc + pt_attempt + delayed_adj + irreg_lead_ant + anticipation_adj >= 1 ~ 10,
        TRUE ~ 0
      )
    ) %>%
    dplyr::select(-FCV_normalised)
  return(reign)
}
reign <- try_log(reign_process(as_of = as_of, format = format))

collate_conflict <- function() {
  #-----------------—Join all dataset-----------------------------------
  conflict_dataset_raw <- left_join(fcv, reign, by = "Country") %>%
    left_join(., idp, by = "Country") %>%
    left_join(., acled, by = "Country") #%>%
  #dplyr::select(Countryname, FCV_normalised, pol_trigger_norm, z_idps_norm, fatal_z_norm) 
  
  conflict_dataset <- conflict_dataset_raw %>%
    # mutate(
    #   flag_count = as.numeric(unlist(row_count(
    #     .,
    #     pol_trigger_norm:fatal_z_norm,
    #     count = 10,
    #     append = F
    #   ))),
    #   fragile_1_flag = case_when(
    #     flag_count >= 1 ~ 10,
    #     TRUE ~ suppressWarnings(apply(conflict_dataset_raw %>% dplyr::select(pol_trigger_norm:fatal_z_norm), 
    #                  1,
  #                  FUN = max,
  #                  na.rm = T)
  #   )),
  #   fragile_1_flag = case_when(
  #     fragile_1_flag == -Inf ~ NA_real_,
  #     TRUE ~ fragile_1_flag
  #   )) %>%
  rename(FCS_Normalised = FCV_normalised, REIGN_Normalised = pol_trigger_norm,
         Displaced_UNHCR_Normalised = z_idps_norm, BRD_Normalised = fatal_z_norm#,
         # Number_of_High_Risk_Flags = flag_count, Overall_Conflict_Risk_Score = fragile_1_flag
  ) 
  
  #-------------------------------------—Create Fragility sheet--------------------------------------
  # Compile joint database
  countrylist <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv"))
  
  fragility_sheet <- left_join(countrylist, conflict_dataset, by = "Country") %>%
    dplyr::select(-X) %>%
    # dplyr::select(-X, -Number_of_High_Risk_Flags) %>%
    rename_with(
      .fn = ~ paste0("Fr_", .), 
      .cols = colnames(.)[!colnames(.) %in% c("Country", "Countryname") ]
    )
  
  if(format == "csv" | format == "both") {
    write.csv(fragility_sheet, "Risk_sheets/fragilitysheet.csv")
  }
  if(format == "tbl" | format == "both") {
    # Write Spark DataFrame
  }
}

collate_conflict(format = format)

lap <- Sys.time() - lapStart
print(paste("Fragility sheet written", lap, units(lap)))
total_time <- Sys.time() - runtime
print(paste("Total time", total_time, units(total_time)))
