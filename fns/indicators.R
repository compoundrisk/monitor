#  FUNCTIONS USED TO COLLECT INPUTS & PRODUCE INDIVIDUAL INDICATOR DATASETS AND RISK COMPONENT SHEETS


# ## Set up Spark
# sc <- spark_connect(master = "local") # This is only for when running locally
# # sc <- spark_connect(method = "databricks")
# # DBI::dbSendQuery(sc,"CREATE DATABASE IF NOT EXISTS crm")
# sparklyr::tbl_change_db(sc, "crm")
# # setwd("../../../dbfs/mnt/CompoundRiskMonitor")
# if(!dir.exists("input-archives")) dir.create("input-archives")
# #---------------------------------

#---------------------REUSED FUNCTIONS-------------------------------

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

## FUNCTION TO ARCHIVE AND LOAD ALL INPUT DATA `archiveInputs()` 
# _Edit this to use Spark_
# - Should I store input archives as a separate CSV file for each date? E.g. `who_dons_20211001` which includes all of the *new* data from October 10?
# - Provide schemas so `read_csv()` (or `spark_read_csv()`) doesn't have to guess columns (would be a separate file/table, e.g. `who_dons_schema`) -- or just one table for _all_ the schemata. (low priority)
# - When bringing in input archives, in order to select most recent, might make sense to use `memory = FALSE` in `spark_read_csv()`.

#---------------------------------
archiveInputs <- function(data,
                          path = paste0("output/inputs-archive/", deparse(substitute(data)), ".csv"), 
                          newFile = F,
                          # group_by defines the groups for which most recent data should be taken
                          group_by = "CountryCode",
                          today = Sys.Date(),
                          return = F
                          # large = F) 
                          ){
  # Read in the existing file for the input, which will be appended with new data
  prev <- suppressMessages(read_csv(path)) %>%
    mutate(access_date = as.Date(access_date))
  
  # Select the most recently added data for each unless group_by is set to false
  if(is.null(group_by)) {
    most_recent <- prev
  } else {
    most_recent <- prev %>%
      # .dots allows group_by to take a vector of character strings
      group_by(.dots = group_by) %>%
      slice_max(order_by = access_date)
  }
  
  # Add access_date for new data
  data <- mutate(data, access_date = today)
  
  # Row bind `most_recent` and `data`, in order to make comparison (probably a better way)
  # Could quicken a bit by only looking at columns that matter (ie. don't compare
  # CountryCode and CountryName both). Also, for historical datasets, don't need to compare
  # new dates. Those automatically get added. 
  # if(!large) {
    bound <- rbind(most_recent, data)
    data_fresh <- distinct(bound, across(-c(access_date)), .keep_all = T) %>%
      filter(access_date == today) %>% 
      distinct()
  # } else {
  #   # (Other way) Paste all columns together in order to compare via %in%, and then select the
  #   # data rows that aren't in 
  #   # This way was ~2x slower for a 200 row table, but faster (4.7 min compared to 6) for 80,000 rows
  #   data_paste <- do.call(paste0, select(data, -access_date))
  #   most_recent_paste <- do.call(paste0, select(most_recent, -access_date))
  #   data_fresh <- data[which(!sapply(1:length(data_paste), function(x) data_paste[x] %in% most_recent_paste)),]
  # }
  # Append new data to CSV
  combined <- rbind(prev, data_fresh) %>% distinct()
  write.csv(combined, path, row.names = F)
  if(return == T) return(combined)
}

#--------------------FUNCTIONS TO LOAD INPUT DATA-----------------
loadInputs <- function(filename, group_by = "CountryCode", as_of = Sys.Date(), format = "csv", full = F){
  # The as_of argument let's you run the function from a given historical date. Update indicators.R
  # to use this feature -- turning indicators.R into a function? with desired date as an argument
  
  if (format == "csv") {
    # Read in CSV
    data <- suppressMessages(read_csv(paste0("output/inputs-archive/", filename, ".csv")))
  }
  if (format == "spark") {
    # Read from Spark DataFrame
  }
  # Select only data from before the as_of date, for reconstructing historical indicators
  if (as_of < Sys.Date()) {
    data <- filter(data, access_date <= as_of)
  }
  # Select the most recent access_date for each group, unless group_by = F
  if (is.null(group_by)) {
    most_recent <- data
  } else {
    most_recent <- data %>%
      # .dots allows group_by to take a vector of character strings
      group_by(.dots = group_by) %>%
      slice_max(order_by = access_date) %>%
      ungroup()
  }
  if (!full) return(most_recent)
  return(data)
}

# Move external to functions file (is this still relevant?)
try_log <- function(expr) {
  fun <- sub("\\(.*", "", deparse(substitute(expr)))
  tryCatch({
    expr
  }, error = function(e) {
    write(paste(Sys.time(), "Error on", fun, "\n", e), file = "output/errors.log", append = T)
  })
}

#--------------------—INDICATOR SPECIFIC FUNCTIONS-----------------------------

#--------------------—LOAD ACAPS realtime database-----------------------------
acaps_collect <- function() {
  h <- new_handle()
  handle_setopt(h, ssl_verifyhost = 0, ssl_verifypeer = 0)
  curl_download(url = "https://www.acaps.org/countries",
                "output/inputs-archive/acaps.html",
                handle = h)
  # acaps <- read_html("acaps.html")
  # unlink("acaps.html")
}

## Add in *_collect() function for ACAPS
acaps_process <- function(as_of, format) {
  # SPLIT UP INTO INPUTS SECTION
  # Load website
  h <- new_handle()
  handle_setopt(h, ssl_verifyhost = 0, ssl_verifypeer=0)
  curl_download(url="https://www.acaps.org/countries", "acaps.html", handle = h)
  acaps <- read_html("acaps.html")
  unlink("acaps.html")
  
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
  # countrylist <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv"))
  
  acapssheet <- countrylist %>%
    # dplyr::select(-X) %>%
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

##### HEALTH

#--------------------—GHSI Score-----------------
ghsi_collect <- function() {
  ghsi <- read.csv(paste0(github, "Indicator_dataset/HIS.csv"))
  ghsi <- ghsi %>%
    rename(Country = H_Country) %>%
    dplyr::select(-X)
  archiveInputs(ghsi, group_by = "Country")
}

ghsi_process <- function(as_of, format) {
  
  # OR instead of splitting, I could wrap everything above this (read.csv to archive) 
  # in an if statement, so you can run the script without this section if you're
  # trying to recreate data
  ghsi <- loadInputs("ghsi", group_by = "Country", as_of = as_of, format = format)
  
  # Normalise scores
  # Rename HIS to ghsi *everywhere*
  HIS <- normfuncneg(ghsi, 20, 70, "H_HIS_Score")
  return(HIS)
}

## Oxford Openness
oxford_openness_collect <- function() {
  # Risk of Openness is the reviewed, and updated, version of Oxford Rollback. RENAME
  oxford_openness_risk <- read.csv("https://raw.githubusercontent.com/OxCGRT/covid-policy-scratchpad/master/risk_of_openness_index/data/riskindex_timeseries_latest.csv") %>%
    mutate(Date = as.Date(Date))
  
  archiveInputs(oxford_openness_risk, group_by = c("CountryCode", "Date"))
}

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

#------------------------—OWID COVID deaths and cases--------------------------

# _Add in *_collect() function_
owid_collect <- function() {
  covidweb <-
    read_csv(
      "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv",
      col_types = cols_only(
        iso_code = 'c',
        continent = 'c',
        location = 'c',
        date = 'D',
        new_cases_per_million = 'd',
        new_cases_smoothed_per_million = 'd',
        new_deaths_per_million = 'd',
        new_deaths_smoothed_per_million = 'd'))
  write.csv(covidweb, "output/inputs-archive/owid_covid.csv", row.names = F)
}

owid_covid_process <- function(as_of, format) {
  # Switching to `read_csv()` may save ~2 seconds of Health's ~40 seconds; 6 → 4 secs
  # See warning
  # covidweb <- read_csv("https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv",
  #                      col_types = "cccD-------dd-dd-------------------------------------------------")
  
  covidweb <- read_csv("output/inputs-archive/owid_covid.csv",
                       col_types = cols_only(
                         iso_code = 'c',
                         continent = 'c',
                         location = 'c',
                         date = 'D',
                         new_cases_per_million = 'd',
                         new_cases_smoothed_per_million = 'd',
                         new_deaths_per_million = 'd',
                         new_deaths_smoothed_per_million = 'd'))
  
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
    filter(date > as_of - 28)
  
  # bi-weekly growth rate for covid deaths and cases
  covidgrowth <- covid %>%
    mutate(
      previous2week = case_when(
        date >= as_of - 13 ~ "twoweek",
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
  owid <- left_join(covidcurrent, covidgrowth)
  owid <- subset(owid, Country %in% countrylist$Country)
  return(owid)
  # return(list(
  #   covidcurrent,
  #   covidgrowth
  # ))
}

#--------------------------—Oxford Response Tracker----------------------------
# _Add in *_collect() function_
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

#------------------------------—INFORM COVID------------------------------------------------------
# Also used for INFORM Income Support (Socio-economic vulnerability)
#---------------------------------
# SLOW
inform_covid_collect <- function() {
  inform_cov <- read_html("https://drmkc.jrc.ec.europa.eu/inform-index/INFORM-Covid-19/INFORM-Covid-19-Warning-beta-version")
  
  all_dat <- lapply(2:24, function(tt) {
    see <- lapply(c("data-country", "data-value", "style"), function(xx) {
      inform_cov %>% 
        html_nodes(paste0("td:nth_child(", paste(tt), ")")) %>%
        html_attr(xx)
    })
    do.call(rbind, Map(data.frame, cname = see[1], Value = see[2], Rating = see[3]))
  })
  
  inform_covid_warning_raw <- do.call(rbind, Map(data.frame, INFORM_rating=all_dat[1], covid_case_rate=all_dat[2], legal_stringency=all_dat[3],
                                                 international_travel=all_dat[4], internal_movement=all_dat[5], stay_home=all_dat[6],
                                                 income_support=all_dat[7], debt_relief=all_dat[8], gdp_change=all_dat[9],
                                                 unemployment=all_dat[10], inflation=all_dat[11], school_close=all_dat[12],
                                                 ipc_3_plus=all_dat[13], growth_events=all_dat[14], public_info=all_dat[15],
                                                 testing_policy=all_dat[16], contact_trace=all_dat[17], growth_conflict=all_dat[18],
                                                 seasonal_flood=all_dat[19], seasonal_cyclone=all_dat[20], seasonal_exposure=all_dat[21],
                                                 ASAP_hotspot=all_dat[22]))
  
  severity <- as.data.frame(all_dat[23]) %>%
    rename(INFORM_rating.cname = cname,
           INFORM_severity.Value = Value,
           INFORM_severity.Rating = Rating)
  
  inform_covid_warning_raw <- left_join(inform_covid_warning_raw, severity, by = "INFORM_rating.cname")
  
  inform_covid_warning <-  inform_covid_warning_raw %>%
    rename(
      Countryname = INFORM_rating.cname,
      hold_one = INFORM_severity.Rating
    ) %>%
    dplyr::select(-contains(".cname")) %>%
    mutate_at(
      vars(contains(".Rating")),
      ~ case_when(
        . == "background:#FF0000;" ~ "High",
        . == "background:#FFD800;" ~ "Medium",
        . == "background:#00FF00;" ~ "Low",
        TRUE ~ NA_character_
      )
    ) %>%
    mutate(
      hold_one = case_when(
        hold_one == "background:#FF0000;" ~ "High",
        hold_one == "background:#FFD800;" ~ "Medium",
        is.na(hold_one) ~ "Low",
        TRUE ~ NA_character_
      )) %>%
    rename(INFORM_severity.Rating = hold_one) %>%
    mutate(
      INFORM_rating.Value = as.numeric(as.character(INFORM_rating.Value)),
      Country = countrycode(Countryname, origin = "country.name", destination = "iso3c", nomatch = NULL
      )) %>%
    dplyr::select(-Countryname) %>%
    rename_with(
      .fn = ~ paste0("H_", .), 
      .cols = colnames(.)[!colnames(.) %in% c("Country", "Countryname")]
    )
  
  # FIX renaming
  inform_covid <- suppressMessages(type_convert(inform_covid_warning))
  archiveInputs(inform_covid, group_by = c("Country"))
}

inform_covid_process <- function(as_of, format) {
  inform_covid_warning <- loadInputs("inform_covid", group_by = c("Country"), as_of = as_of, format = format)
  inform_covid_warning <- normfuncpos(inform_covid_warning, 6, 2, "H_INFORM_rating.Value")
  return(inform_covid_warning)
}

#----------------------------------—WHO DONS--------------------------------------------------------------
dons_collect <- function() {
  dons_raw <- read_html("https://www.who.int/emergencies/disease-outbreak-news")
  
  dons_select <- dons_raw %>%
    html_nodes(".sf-list-vertical") %>%
    html_nodes("h4") #%>%
  #html_text()
  
  dons_date <- dons_select %>%
    html_nodes("span:nth-child(2)") %>%
    html_text()
  
  dons_text <- dons_select %>%
    html_nodes(".trimmed") %>%
    html_text()
  
  wmo_don_full <- bind_cols(dons_text, dons_date) %>%
    rename(text = "...1" ,
           date = "...2") %>%
    mutate(disease = trimws(sub("\\s[-——ｰ].*", "", text)),
           country = trimws(sub(".*[-——ｰ]", "", text)),
           country = trimws(sub(".*-", "", country)),
           date = dmy(date)) %>%
    separate_rows(country, sep = ",") %>%
    mutate(wmo_country_alert = countrycode(country,
                                           origin = "country.name",
                                           destination = "iso3c",
                                           nomatch = NULL
    ))
  
  who_don <- wmo_don_full
  
  archiveInputs(who_don, group_by = NULL)
}

#----------------------------------—WHO DONS--------------------------------------------------------------

# REPLACE all WMO with WHO
who_process <- function(as_of, format) {
  wmo_don_full <- loadInputs("who_don", group_by = NULL, as_of = as_of, format = format)
  
  # countrylist <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv"))
  wmo_don <- countrylist %>%
    # dplyr::select(-X) %>%
    # Should we specify which dates we care about?
    # Also a problem where *end* of outbreaks are also reported
    mutate(wmo_don_alert = case_when(Country %in% wmo_don_full$wmo_country_alert ~ 10,
                                     TRUE ~ 0))  %>%
    rename(H_wmo_don_alert = wmo_don_alert) %>%
    dplyr::select(-Countryname)
  return(wmo_don)
}

#---------------------------------—Health ACAPS---------------------------------
# acaps_health <- acapssheet[,c("Country", "H_health_acaps")]

#----------------------------------—Create combined Health Sheet-------------------------------------------
# collate_health <- function(format) {
#   countrylist <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv"))
#   countrylist <- countrylist %>%
#     dplyr::select(-X)

#   health_sheet <- left_join(countrylist, HIS, by = "Country") %>%
#     left_join(., OXrollback, by = "Country") %>%
#     left_join(., covidgrowth, by = "Country") %>%
#     left_join(., covidcurrent, by = "Country") %>%
#     left_join(., Ox_cov_resp, by = "Country") %>%
#     # left_join(., cov_forcast_alt, by = "Country") %>% # not current
#     left_join(., inform_covid_warning, by = "Country", "Countryname") %>%
#     left_join(., wmo_don, by = "Country") %>%
#     left_join(., acaps_health, by = "Country") %>%
#     arrange(Country)

#   if (format == "csv" | format == "both") {
#     write.csv(health_sheet, "Risk_sheets/healthsheet.csv")
#   }
#   if (format == "spark" | format == "both") {
#     # Write Spark DataFrame
#   }

# }

#### FOOD SECURITY
# -------------------------------— Proteus Index -------------------------------
proteus_collect <- function() {
  proteus <- read.csv(paste0(github, "Indicator_dataset/proteus.csv"))
  
  proteus <- proteus %>%
    rename(F_Proteus_Score = Proteus.index) %>%
    dplyr::select(-X) %>%
    mutate(
      Country = countrycode(Country,
                            origin = "country.name",
                            destination = "iso3c",
                            nomatch = NULL
      ))
  
  archiveInputs(proteus, group_by = c("Country"))
}

proteus_process <- function(as_of, format) {
  
  proteus <- loadInputs("proteus", group_by = c("Country"), as_of = as_of, format = format)
  
  upperrisk <- quantile(proteus$F_Proteus_Score, probs = c(0.90), na.rm = T)
  lowerrisk <- quantile(proteus$F_Proteus_Score, probs = c(0.10), na.rm = T)
  proteus <- normfuncpos(proteus, upperrisk, lowerrisk, "F_Proteus_Score")
  return(proteus)
}

#------------------—FEWSNET (with CRW threshold)---

#Load database
fews_collect <- function() {
  fewsnet <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/fews.csv"), col_types = cols()))
  archiveInputs(fewsnet, group_by = c("country", "year_month"))
}

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

#------------------------—WBG FOOD PRICE MONITOR------------------------------------
# _Add in *_collect() function_
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
    mutate(Country = countrycode(Country,
                          origin = "country.name",
                          destination = "iso3c",
                          nomatch = NULL
    )) %>%
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
    )) %>%
    rename_with(   
      .fn = ~ paste0("F_", .),
      .cols = colnames(.)[!colnames(.) %in% c("Country")]
    )
  return(ag_ob)
}


#-------------------------—FAO/WFP HOTSPOTS----------------------------
fao_wfp_collect <- function() {
  fao_wfp <- suppressWarnings(read_csv(paste0(github, "Indicator_dataset/WFP%3AFAO_food.csv"), col_types = cols()) %>%
                                dplyr::select(-X2))
  
  fao_wfp <- fao_wfp %>%
    mutate(Country = countrycode(Country,
                                 origin = "country.name",
                                 destination = "iso3c",
                                 nomatch = NULL
    ))
  
  # fao_wfp$F_fao_wfp_warning <- 10
  
  fao_all <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv")) %>%
    mutate(F_fao_wfp_warning = NA) %>%
    dplyr::select(-X)
  
  # fao_all <- subset(countrylist, Country %in% fao_wfp$Country) %>%
  # mutate(F_fao_wfp_warning = 10)
  
  fao_all[fao_all$Country %in% fao_wfp$Country,"F_fao_wfp_warning"] <- 10
  
  fao_wfp <- fao_all
  
  archiveInputs(fao_wfp, group_by = c("Country"))
}

fao_wfp_process <- function(as_of, format) {
  # Kind of unnecessary
  fao_wfp <- loadInputs("fao_wfp", group_by = c("Country"), as_of = as_of, format = format) %>%
    select(-X1, -Countryname)
  return(fao_wfp)
}



#### MACRO

#---------------------------—Economist Intelligence Unit---------------------------------
eiu_collect <- function() {
  url <- "https://github.com/bennotkin/compoundriskdata/blob/master/Indicator_dataset/RBTracker.xls?raw=true"
  destfile <- "RBTracker.xls"
  curl::curl_download(url, destfile)
  eiu <- read_excel(destfile, sheet = "Data Values", skip = 3)
  file.remove("RBTracker.xls")
  
  archiveInputs(eiu, group_by = c("`SERIES NAME`", "MONTH"))
}

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

#### SOCIO-ECONOMIC
#---------------------------—Alternative socio-economic data (based on INFORM) - INFORM Income Support
inform_socio_process <- function(as_of, format = format) {
  inform_risk <- loadInputs("inform_risk", group_by = c("Country"), as_of = as_of, format = format)

  inform_data <- inform_risk %>%
    dplyr::select(Country, "Socio-Economic Vulnerability") %>%
    rename(S_INFORM_vul = "Socio-Economic Vulnerability")

  inform_data <- normfuncpos(inform_data, 7, 0, "S_INFORM_vul")
  inform_data <- normfuncpos(inform_data, 7, 0, "S_INFORM_vul")
  return(inform_data)
}

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

#--------------------------—MPO: Poverty projections----------------------------------------------------
mpo_collect <- function() {
  # mpo <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/mpo.csv")))
  # archiveInputs(mpo, group_by = c("Country"))


  # FIX: Ideally most of this would be in the mpo_process() function, rather than the collect
  # function, but doing so would conflict with the current mpo archive structure
  mpo <- read_xlsx("restricted-data/mpo_global_AM21.xlsx")

  # Add population
  pop <- wpp.by.year(wpp.indicator("tpop"), 2020)

  pop$charcode <- suppressWarnings(countrycode(pop$charcode,
                                              origin = "iso2c",
                                              destination = "iso3c"
                                              )
                                  )

  colnames(pop) <- c("Country", "Population")

  mpo_data <- mpo %>%
    rename(Country = Code) %>%
    left_join(., pop, by= "Country")  %>%
    mutate_at(
      vars(contains("y20")),
      ~ as.numeric(as.character(.))
    ) %>%
    mutate(
      pov_prop_23_22 = y2023 - y2022,
      pov_prop_22_21 = y2022 - y2021,
      # pov_prop_21_20 = y2021 - y2020,
      # pov_prop_20_19 = y2020 - y2019,
      ) %>%
    filter(Label == "International poverty rate ($1.9 in 2011 PPP)") %>%
    rename_with(
      .fn = ~ paste0("S_", .),
      .cols = colnames(.)[!colnames(.) %in% c("Country")]
    ) 

  # Normalise based on percentiles
  mpo_data <- normfuncpos(mpo_data,
                          quantile(mpo_data$S_pov_prop_23_22, 0.95,  na.rm = T),
                          quantile(mpo_data$S_pov_prop_23_22, 0.05,  na.rm = T),
                          "S_pov_prop_23_22")
  mpo_data <- normfuncpos(mpo_data,
                          quantile(mpo_data$S_pov_prop_22_21, 0.95,  na.rm = T),
                          quantile(mpo_data$S_pov_prop_22_21, 0.05,  na.rm = T),
                          "S_pov_prop_22_21")
  # mpo_data <- normfuncpos(mpo_data,
  #                         quantile(mpo_data$S_pov_prop_21_20, 0.95,  na.rm = T),
  #                         quantile(mpo_data$S_pov_prop_21_20, 0.05,  na.rm = T),
  #                         "S_pov_prop_21_20")

  mpo_data <- mpo_data %>%
    mutate(
      S_pov_comb_norm = rowMaxs(as.matrix(dplyr::select(.,
                                                        S_pov_prop_23_22_norm,
                                                        S_pov_prop_22_21_norm
                                                        # S_pov_prop_21_20_norm
                                                        )),
                                na.rm = T)) %>%
          dplyr::select(Country,
                        S_pov_comb_norm, 
                        S_pov_prop_23_22_norm,
                        S_pov_prop_22_21_norm,
                        # S_pov_prop_21_20_norm, 
                        S_pov_prop_23_22,
                        S_pov_prop_22_21
                        # S_pov_prop_21_20
                        )

  # write_csv(mpo_data, "Indicator_dataset/mpo.csv")
  mpo <- mpo_data
  archiveInputs(mpo, group_by = c("Country"))
}


mpo_arch <- read_csv('output/inputs-archive/mpo.csv')
names(mpo_arch)
names(mpo_data)
names(mpo_data)[which(names(mpo_data) %ni% names(mpo_arch))]
names(mpo_arch)[which(names(mpo_arch) %ni% names(mpo_data))]


mpo_process <- function(as_of, format) {
  mpo <- loadInputs("mpo", group_by = c("Country"), as_of = as_of, format = format)
}

## MACROFIN / EFI Macro Financial Review Household Level Risk
mfr_collect <- function() {
  # If EFI Macro Financial Review is re-included above, we can reuse that. For clarity, moving data read here because it's not being used by macrosheet
  macrofin <- read.csv(paste0(github, "Indicator_dataset/macrofin.csv"))
  archiveInputs(macrofin, group_by = c("ISO3"))
}

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

#----------------------------—WB PHONE SURVEYS-----------------------------------------------------

## WB COVID PHONE SURVEYS
#Incorporate phone.R
#---------------------------------
phone_collect <- function() {
  wb_phone <- read_csv(paste0(github, "Indicator_dataset/phone.csv"))[,-1]
  archiveInputs(wb_phone , group_by = c("Country"))
}

# phone_collect <- function() { 
#   curl_download("https://datacatalogfiles.worldbank.org/ddh-published/0037769/DR0045662/data-coviddash-latest.xlsx",
#                 "covid-phone.xlsx"
#   )
#   phone_data <- read_excel("covid-phone.xlsx",
#                           sheet = "2. Harmonized Indicators",
#                           col_types = c(
#                             "numeric",
#                             rep("text", 2),
#                             "numeric",
#                             rep("text", 2),
#                             "numeric",
#                             rep("text", 3),
#                             rep("numeric", 3),
#                             rep("text", 11),
#                             rep("numeric", 4),
#                             rep("text", 4),
#                             "text",
#                             "numeric",
#                             "text")
#                           )
#   unlink("covid-phone.xlsx")

#   # phone_data <- read_excel("/Users/bennotkin/Downloads/data-coviddash-latest-8.xlsx",
#   #                          sheet = "2. Harmonized Indicators")
    
#   phone_compile <- phone_data %>%
#     filter(level_data == "Gender=All, Urb_rur=National. sector=All") %>%
#     mutate(survey_no = as.numeric(as.character(str_replace(wave, "WAVE", "")))) %>%
#     group_by(code) %>%
#     mutate(last_survey = max(survey_no, na.rm=T)) %>%
#     ungroup() %>%
#     filter(last_survey == survey_no) %>% 
#     # Two values for "% able to access [staple food item] in the past 7 days when needed? - any staple food"
#     # Drop "Able to access any staple food in the past 7 days - first 3 staple food items (% of HHs)"
#     filter(indicator_display != "Able to access any staple food in the past 7 days - first 3 staple food items (% of HHs)")

#   phone_unique <- phone_compile %>% 
#     distinct(code, indicator_description, .keep_all = T)

#   phone_wide <- phone_unique %>%
#     dplyr::select(code, indicator_description, indicator_val) %>%
#     pivot_wider(names_from = indicator_description, values_from = indicator_val  )

#   phone_index <-phone_wide %>%
#     dplyr::select(
#       "code", #"% of respondents currently employed/working",  
#       "% of respondents who have stopped working since COVID-19 outbreak", 
#       "% able to access [staple food item] in the past 7 days when needed? - any staple food" ,
#       # "% of HHs that saw reduced their remittances" , "% of HHs not able to perform normal farming activities (crop, livestock, fishing)" ,
#       # "% of HHs able to pay rent for the next month",
#       # "% of respondents who were not able to work as usual last week","Experienced decrease in wage income (% HHs with wage income as a source of livelihood in the past 12 months)",
#       # "% of HHs that experienced change in total income - decrease"  ,
#       "% of HHs used money saved for emergencies to cover basic living expenses" ,
#       "% of respondents received government assistance when experiencing labor income/job loss" ,   
#       # "% of HHs sold assets such as property during the pandemic in order to pay for basic living expenses" ,
#       "In the last 30 days, you skipped a meal because there was not enough money or other resources for food?(%)"   ,
#       # "In the last 30 days, your household worried about running out of food because of a lack of money or other resources?(%)" ,
#     )

#   # Normalised values
#   #phone_index <- normfuncpos(phone_index, 70, 0, "% of respondents currently employed/working")
#   phone_index <- normfuncpos(phone_index, 50, 0, "% of respondents who have stopped working since COVID-19 outbreak" )
#   phone_index <- normfuncneg(phone_index, 80, 100, "% able to access [staple food item] in the past 7 days when needed? - any staple food" )
#   #phone_index <- normfuncpos(phone_index, 70, 0, "% of HHs that saw reduced their remittances" )
#   #phone_index <- normfuncpos(phone_index, 25, 0, "% of HHs not able to perform normal farming activities (crop, livestock, fishing)")
#   #phone_index <- normfuncneg(phone_index, 50, 100, "% of HHs able to pay rent for the next month")
#   #phone_index <- normfuncpos(phone_index, 25, 0,  "% of respondents who were not able to work as usual last week")
#   #phone_index <- normfuncpos(phone_index, 50, 0,  "Experienced decrease in wage income (% HHs with wage income as a source of livelihood in the past 12 months)")
#   #phone_index <- normfuncpos(phone_index, 50, 0,  "% of HHs that experienced change in total income - decrease")
#   phone_index <- normfuncpos(phone_index, 25, 0,  "% of HHs used money saved for emergencies to cover basic living expenses" )
#   phone_index <- normfuncneg(phone_index, 5, 80,  "% of respondents received government assistance when experiencing labor income/job loss")
#   #phone_index <- normfuncpos(phone_index, 20, 0,  "% of HHs sold assets such as property during the pandemic in order to pay for basic living expenses"  )
#   phone_index <- normfuncpos(phone_index, 50, 0,  "In the last 30 days, you skipped a meal because there was not enough money or other resources for food?(%)"  )
#   #phone_index <- normfuncpos(phone_index, 50, 0,  "In the last 30 days, your household worried about running out of food because of a lack of money or other resources?(%)")
            
#   # Calculate index
#   phone_index_data <- phone_index %>%
#     mutate(
#       phone_average_index = dplyr::select(., contains("_norm")) %>% rowMeans(na.rm=T)  ) %>%
#     rename(Country = code) %>%
#     rename_with(
#       .fn = ~ paste0("S_", .), 
#       .cols = -contains("Country")
#     )

#   wb_phone <- normfuncpos(phone_index_data, 7, 0, "S_phone_average_index")
#   archiveInputs(wb_phone, group_by = c("Country"))
# }
phone_process <- function(as_of, format) {
  wb_phone  <- loadInputs("wb_phone", group_by = c("Country"), as_of = as_of, format = format)
}
#------------------------------—IMF FORECASTED UNEMPLOYMENT-----------------------------------------
imf_collect <- function() {
  imf_unemployment <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/imf_unemployment.csv")))
  archiveInputs(imf_unemployment, group_by = c("Country"))
}

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

#---------------------------------

#### NATURAL HAZARDS

#------------------------------—GDACS-----------------------------------------
gdacs_collect <- function() {
  gdacweb <- "https://www.gdacs.org/"
  gdac <- read_html(gdacweb)
  
  names <- c(
    ".alert_EQ_Green", ".alert_EQ_PAST_Green", ".alert_EQ_Orange", ".alert_EQ_PAST_Orange",
    ".alert_TC_Green", ".alert_TC_PAST_Green", ".alert_TC_Orange", ".alert_TC_PAST_Orange",
    ".alert_FL_Green", ".alert_FL_PAST_Green", ".alert_FL_Orange", ".alert_FL_PAST_Orange",
    ".alert_VO_Green", ".alert_VO_PAST_Green", ".alert_VO_Orange", ".alert_VO_PAST_Orange",
    ".alert_DR_Green", ".alert_DR_PAST_Green", ".alert_DR_Orange", ".alert_DR_PAST_Orange"
  )
  
  # Function to create database with hazard specific information
  haz <- lapply(names, function(i) {
    names <- gdac %>%
      html_nodes(i) %>%
      html_nodes(".alert_item_name, .alert_item_name_past") %>%
      html_text()
    
    mag <- gdac %>%
      html_nodes(i) %>%
      html_nodes(".magnitude, .magnitude_past") %>%
      html_text() %>%
      str_trim()
    
    date <- gdac %>%
      html_nodes(i) %>%
      html_nodes(".alert_date, .alert_date_past") %>%
      html_text() %>%
      str_trim()
    date <- gsub(c("-  "), "", date)
    date <- gsub(c("\r\n       "), "", date)
    
    cbind.data.frame(names, mag, date)
  })
  
  # Labels
  try(haz[[1]]$status <- paste("active"), silent = T)
  try(haz[[2]]$status <- paste("past"), silent = T)
  try(haz[[3]]$status <- paste("active"), silent = T)
  try(haz[[4]]$status <- paste("past"), silent = T)
  try(haz[[5]]$status <- paste("active"), silent = T)
  try(haz[[6]]$status <- paste("past"), silent = T)
  try(haz[[7]]$status <- paste("active"), silent = T)
  try(haz[[8]]$status <- paste("past"), silent = T)
  try(haz[[9]]$status <- paste("active"), silent = T)
  try(haz[[10]]$status <- paste("past"), silent = T)
  try(haz[[11]]$status <- paste("active"), silent = T)
  try(haz[[12]]$status <- paste("past"), silent = T)
  try(haz[[13]]$status <- paste("active"), silent = T)
  try(haz[[14]]$status <- paste("past"), silent = T)
  try(haz[[15]]$status <- paste("active"), silent = T)
  try(haz[[16]]$status <- paste("past"), silent = T)
  try(haz[[17]]$status <- paste("active"), silent = T)
  try(haz[[18]]$status <- paste("past"), silent = T)
  try(haz[[19]]$status <- paste("active"), silent = T)
  try(haz[[20]]$status <- paste("past"), silent = T)
  
  # Earthquake
  eq1 <- try(rbind(haz[[1]], haz[[2]]), silent = T)
  try(eq1$haz <- paste("green"), silent = T)
  eq2 <- try(rbind(haz[[3]], haz[[4]]), silent = T)
  try(eq2$haz <- paste("orange"), silent = T)
  eq <- try(rbind(eq1, eq2), silent = T)
  eq$hazard <- "earthquake"
  
  # Cyclone
  cy1 <- try(rbind(haz[[5]], haz[[6]]), silent = T)
  try(cy1$haz <- paste("green"), silent = T)
  cy2 <- try(rbind(haz[[7]], haz[[8]]), silent = T)
  try(cy2$haz <- paste("orange"), silent = T)
  cy <- try(rbind(cy1, cy2), silent = T)
  cy$hazard <- "cyclone"
  
  # Flood
  fl1 <- try(rbind(haz[[9]], haz[[10]]), silent = T)
  try(fl1$haz <- paste("green"), silent = T)
  fl2 <- try(rbind(haz[[11]], haz[[12]]), silent = T)
  try(fl2$haz <- paste("orange"), silent = T)
  fl <- try(rbind(fl1, fl2), silent = T)
  fl$hazard <- "flood"
  
  # Volcano
  vo1 <- try(rbind(haz[[13]], haz[[14]]), silent = T)
  try(vo1$haz <- paste("green"), silent = T)
  vo2 <- try(rbind(haz[[15]], haz[[16]]), silent = T)
  try(vo2$haz <- paste("orange"), silent = T)
  vo <- try(rbind(vo1, vo2), silent = T)
  vo$hazard <- "volcano"
  vo$names <- sub(".*in ", "", vo$names)
  
  # Drought
  dr1 <- try(rbind(haz[[17]], haz[[18]]), silent = T)
  try(dr1$haz <- paste("green"), silent = T)
  dr2 <- try(rbind(haz[[19]], haz[[20]]), silent = T)
  try(dr2$haz <- paste("orange"), silent = T)
  dr <- try(rbind(dr1, dr2), silent = T)
  dr$hazard <- "drought"
  dr$date <- try(str_sub(dr$names, start = -4), silent = T)
  dr$names <- try(gsub(".{5}$", "", dr$names), silent = T)
  
  # Combine into one dataframe
  gdaclist <- rbind.data.frame(eq, cy, fl, vo, dr)
  gdaclist[,1] <- gsub(c("\r\n\\s*"), "", gdaclist[,1])
  gdaclist[,2] <- gsub(c("\r\n\\s*"), "", gdaclist[,2])
  
  gdacs <- mutate(gdaclist,
                  access_date = Sys.Date(),
                  mag = na_if(mag, ""),
                  names = trimws(names)
                  # , current = TRUE
  )
  
  # Add all currently online events to gdacs file unless most recent access_date and
  # current data are fully identical
  gdacs_prev <- suppressMessages(read_csv("output/inputs-archive/gdacs.csv"))
  gdacs_prev_recent <- filter(gdacs_prev, access_date == max(access_date)) %>% distinct()
  if(!identical(select(gdacs_prev_recent, -access_date), select(gdacs, -access_date))) {
    gdacs <- rbind(gdacs_prev, gdacs) %>% distinct()
    write.csv(gdacs, "output/inputs-archive/gdacs.csv", row.names = F)
  }
  # # There may be a more efficient approach that gives all currently online events a TRUE `current` variable, and
  # # when an event is no longer current, it receives a FALSE for its next entry.
  # gdacs_prev <- read.csv("inputs-archive/gdacs.csv")
  # # Select only the most recent entries for each event, which were current at least until "today"
  # gdacs_prev_current <- filter(gdacs_prev, current == TRUE) %>%
  #   group_by(names, mag, date, status, haz, hazard) %>%
  #   slice_max(order_by = access_date)
  # # I need to select the rows that appear in gdacs_prev_current but not gdacs
  # bound <- rbind(gdacs, gdacs_prev_current)
  # gdacs_changed <- distinct(bound, across(-c(access_date)), .keep_all = T) %>%
  #   filter(access_date != today)
  # gdacs_changed <- gdacs_changed %>% mutate(current = FALSE, access_date = Sys.Date())
  # gdacs <- rbind(gdacs, gdacs_changed, gdacs_prev) %>% distinct()
}

gdacs_process <- function(as_of, format) {
  if (format == "csv") {
    # Read in CSV
    gdacs <- suppressMessages(read_csv("output/inputs-archive/gdacs.csv")) %>%
      mutate(access_date = as.Date(access_date)) %>%
      filter(access_date <= as_of) %>%
      filter(access_date == max(access_date))
  }
  if (format == "spark") {
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

#----------------------—INFORM Natural Hazard and Exposure rating--------------------------
inform_risk_collect <- function() {
  inform_risk <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/INFORM_Risk.csv"), col_types = cols()))
  archiveInputs(inform_risk, group_by = c("Country"))
}

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

#----------------------------------—IRI Seasonal Forecast ------------------------------------------
iri_collect <- function() {
  # Load from Github
  seasonl_risk <- suppressWarnings(read_csv(paste0(github, "Indicator_dataset/seasonal_risk_list"), col_types = cols()))
  seasonl_risk <- seasonl_risk %>%
    dplyr::select(-X1) %>%
    rename(
      Country = "ISO3",
      NH_seasonal_risk_norm = risklevel
    )
  
  iri_forecast <- seasonl_risk #Go through and reduce renamings
  archiveInputs(iri_forecast, group_by = c("Country"))
}

iri_process <- function(as_of, format) {
  iri_forecast <- loadInputs("iri_forecast", group_by = c("Country"), as_of = as_of, format = format)
  return(iri_forecast)
}

#-------------------------------------—Locust outbreaks----------------------------------------------
# List of countries and risk factors associated with locusts (FAO), see:http://www.fao.org/ag/locusts/en/info/info/index.html
locust_collect <- function() {
  locust_risk <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/locust_risk.csv"), col_types = cols()))
  locust_risk <- locust_risk %>%
    dplyr::select(-X1)
  
  fao_locust <- locust_risk
  
  archiveInputs(fao_locust, group_by = c("Country"))
}

locust_process <- function(as_of, format) {
  fao_locust <- loadInputs("fao_locust", group_by = c("Country"), as_of = as_of, format = format)
  return(fao_locust)
}

#---------------------------------—Natural Hazard ACAPS---------------------------------
# acaps_nh <- acapssheet[,c("Country", "NH_natural_acaps")]






####    FRAGILITY
#-------------------------—FCS---------------------------------------------

fcs_collect <- function() {
  fcv <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/Country_classification.csv"))) %>%
    dplyr::select(-X1, Countryname, -`IDA-status`) %>%
    mutate(FCV_status = tolower(FCV_status)) %>%
    mutate(
      FCS_normalised = case_when(
        FCV_status == tolower("High-Intensity conflict") ~ 10,
        FCV_status == tolower("Medium-Intensity conflict") ~ 10,
        FCV_status == tolower("High-Institutional and Social Fragility") ~ 10,
        TRUE ~ 0
      )
    )
  
  fcs <- fcv
  archiveInputs(fcs, group_by = c("Country"))
}

fcs_process <- function(as_of, format) {
  fcs <- loadInputs("fcs", group_by = c("Country"), as_of = as_of, format = format)
  return(fcs)
}

#-----------------------------—IDPs--------------------------------------------------------
idp_collect <- function() {
  idp_data <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/population.csv"),
                                        col_types = cols(
                                          `IDPs of concern to UNHCR` = col_number(),
                                          `Refugees under UNHCR mandate` = col_number(),
                                          Year = col_number()
                                        ), skip = 14
  ))
  
  un_idp <- idp_data
  archiveInputs(un_idp, group_by = c("`Country of origin (ISO)`", "`Country of asylum (ISO)`", "`Year`"))
}

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
    dplyr::select(-`Country of origin (ISO)`) %>% 
    rename(Displaced_UNHCR_Normalised = z_idps_norm)
  return(idp)
}

#-------------------------—ACLED data---------------------------------------------
acled_collect <- function() {
  # Select date as three years plus two month (date to retrieve ACLED data)
  three_year <- as.Date(as.yearmon(Sys.Date() - 45) - 3.2)
  
  # Get ACLED API URL
  acled_url <- paste0("https://api.acleddata.com/acled/read/?key=buJ7jaXjo71EBBB!!PmJ&email=bnotkin@worldbank.org&event_date=",
                      three_year,
                      "&event_date_where=>&fields=event_id_cnty|iso3|fatalities|event_date&limit=0")
  
  # acled_url2 <- paste0("https://api.acleddata.com/acled/read/?key=*9t-89Rn*bDb4qFXBAmO&email=ljones12@worldbank.org&event_date=",
  #                     three_year,
  #                     "&event_date_where=>&fields=iso3|fatalities|event_date|event_type|actor1|&limit=0")
  #
  # #Get ACLED API URL
  # acled_url <- paste0("https://api.acleddata.com/acled/read/?key=buJ7jaXjo71EBBB!!PmJ&email=bnotkin@worldbank.org&event_date=",
  #                     three_year,
  #                     "&event_date_where=>&fields=iso3|fatalities|event_date&limit=0")
  
  # Retrieve information
  acled_data <- fromJSON(acled_url)
  
  acled <- acled_data$data
  
  # # DELETE for first time only
  # acled <- mutate(acled, access_date = Sys.Date())
  # write.csv(acled, "inputs-archive/acled.csv", row.names = F)
  
  archiveInputs(acled, group_by = NULL)
}

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
    filter(as.Date(as.yearmon(date)) <= as.Date(as.yearmon(as_of - 45))) %>% 
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
    filter(month_yr == paste(month.abb[month(format(as_of - 45))], year(format(as_of - 45)))) 
  
  # Normalise scores
  acled <- normfuncpos(acled, 1, 0, "fatal_z")
  
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
    dplyr::select(-iso3) %>% 
    rename(BRD_Normalised = fatal_z_norm)
  return(acled)
}

#--------------------------—REIGN--------------------------------------------
reign_collect <- function() {
  # reign_data <- suppressMessages(read_csv("https://cdn.rawgit.com/OEFDataScience/REIGN.github.io/gh-pages/data_sets/REIGN_2021_5.csv", col_types = cols()))
  
  month <- as.numeric(format(Sys.Date(),"%m"))
  year <- as.numeric(format(Sys.Date(),"%Y"))
  
  l <- F
  i <- 0
  while(l == F & i < 20) {
    tryCatch(
      {
        reign_data <- suppressMessages(read_csv(paste0("https://raw.githubusercontent.com/OEFDataScience/REIGN.github.io/gh-pages/data_sets/REIGN_", year, "_", month, ".csv"),
                                                col_types = cols()))
        l <- T
        print(paste0("Found REIGN csv at ", year, "_", month))
      }, error = function(e) {
        print(paste0("No REIGN csv for ", year, "_", month))
      }, warning = function(w) {
      }, finally = {
      }
    )
    
    if(month > 1) {
      month <- month - 1
    } else {
      month <- 12
      year <- year - 1
    }
    i <- i + 1
  }
  
  reign <- reign_data
  # could speed up by only filtering reign for last two years, assumption being that they're
  # aren't many backfilled entries
  reign <- filter(reign, year > (format.Date(Sys.Date(), "%Y") %>% as.numeric() - 3))
  # thoughtfully develop a naming convention. perhaps the inputs-archive does append
  # _data (or just _inputs?). What is called reign and what is called reign_data, etc.
  # reign_raw reign_archive reign_data reign_inputs. What will all the tables be in Spark,
  # for each dataset?
  # Also, update code to use Spark
  # Also, run profiler on code
  
  # # DELETE for first time only
  # reign <- mutate(reign, access_date = Sys.Date(), precip = round(precip, 10)) # truncating precip so that it's easier to tell whether data matches
  # # write.csv(reign, "inputs-archive/reign.csv", row.names = F) #1.361MB
  
  archiveInputs(reign, group_by = c("country", "leader", "year", "month"))
}

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
  reign <- left_join(reign_start, fcs %>% dplyr::select(Country, FCS_normalised), by = "Country") %>%
    mutate(
      irreg_lead_ant = case_when(
        FCS_normalised == 10 ~ irreg_lead_ant,
        TRUE ~ 0
      ),
      delayed_adj = case_when(
        FCS_normalised == 10 ~ delayed,
        TRUE ~ 0
      ),
      anticipation_adj = case_when(
        FCS_normalised == 10 ~ anticipation,
        TRUE ~ 0
      ),
      pol_trigger = case_when(
        pt_suc + pt_attempt + delayed_adj + irreg_lead_ant + anticipation_adj >= 1 ~ "Fragile",
        TRUE ~ "Not Fragile"
      ),
      # pol_trigger_norm = case_when(
      REIGN_Normalised = case_when(
        pt_suc + pt_attempt + delayed_adj + irreg_lead_ant + anticipation_adj >= 1 ~ 10,
        TRUE ~ 0
      )
    ) %>%
    dplyr::select(-FCS_normalised)
  return(reign)
}