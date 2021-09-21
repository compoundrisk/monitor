####
#
#  CODE USED TO TO PRODUCE INDIVIDUAL INDICATOR DATASETS AND RISK COMPONENT SHEETS
#
####
runtime <- Sys.time()
#--------------------LOAD PACKAGES-----------------------------------------
# packagesAll <- c("cowplot", "lubridate", "rvest", "viridis", "countrycode", 
#                  "clipr", "awalker89", "openxlsx", "dplyr", "readxl", "gsheet",
#                  "zoo", "wppExplorer", "haven", "EnvStats", "jsonlite", 
#                  "matrixStats", "ggalt", "raster", "sf", "mapview", "maptools", 
#                  "ggthemes", "tidyverse", "sjmsc", "googledrive", "rgdal")

packages <- c("curl", "dplyr", "EnvStats", "stats", "countrycode", "ggplot2", 
              "jsonlite","lubridate", "matrixStats", "readr", "readxl", "rvest",   
              "sjmisc", "stringr", "tidyr", "xml2", "zoo")
invisible(lapply(packages, require, quietly = TRUE, character.only = TRUE))

# github <- "https://raw.githubusercontent.com/ljonestz/compoundriskdata/master/"
github <- "https://raw.githubusercontent.com/bennotkin/compoundriskdata/master/"

#--------------------FUNCTION TO CALCULATE NORMALISED SCORES-----------------
# Function to normalise with upper and lower bounds (when low score = high vulnerability)
normfuncneg <- function(df, upperrisk, lowerrisk, col1) {
  # Create new column col_name as sum of col1 and col2
  df[[paste0(col1, "_norm")]] <- ifelse(df[[col1]] <= upperrisk, 10,
                                        ifelse(df[[col1]] >= lowerrisk, 0,
                                               ifelse(df[[col1]] > upperrisk & df[[col1]] < lowerrisk, 10 - (upperrisk - df[[col1]]) / 
                                                        (upperrisk - lowerrisk) * 10, NA)
                                        )
  )
  df
}

# Function to normalise with upper and lower bounds (when high score = high vulnerability)
normfuncpos <- function(df, upperrisk, lowerrisk, col1) {
  # Create new column col_name as sum of col1 and col2
  df[[paste0(col1, "_norm")]] <- ifelse(df[[col1]] >= upperrisk, 10,
                                        ifelse(df[[col1]] <= lowerrisk, 0,
                                               ifelse(df[[col1]] < upperrisk & df[[col1]] > lowerrisk, 10 - (upperrisk - df[[col1]]) / 
                                                        (upperrisk - lowerrisk) * 10, NA)
                                        )
  )
  df
}

#--------------------FUNCTIONS TO ARCHIVE ALL INPUT DATA-----------------
archiveInputs <- function(data,
                          path = paste0("output/inputs-archive/", deparse(substitute(data)), ".csv"), 
                          newFile = F,
                          # group_by defines the groups for which most recent data should be taken
                          group_by = "CountryCode",
                          today = Sys.Date(),
                          return = F,
                          large = F) {
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
  if(!large) {
    bound <- rbind(most_recent, data)
    data_fresh <- distinct(bound, across(-c(access_date)), .keep_all = T) %>%
      filter(access_date == today) %>% 
      distinct()
  } else {
    # (Other way) Paste all columns together in order to compare via %in%, and then select the
    # data rows that aren't in 
    # This way was ~2x slower for a 200 row table, but faster (4.7 min compared to 6) for 80,000 rows
    data_paste <- do.call(paste0, select(data, -access_date))
    most_recent_paste <- do.call(paste0, select(most_recent, -access_date))
    data_fresh <- data[which(!sapply(1:length(data_paste), function(x) data_paste[x] %in% most_recent_paste)),]
  }
  # Append new data to CSV
  combined <- rbind(prev, data_fresh) %>% distinct()
  write.csv(combined, path, row.names = F)
  if(return == T) return(combined)
}

loadInputs <- function(filename, group_by = "CountryCode", as_of = Sys.Date(), full = F){
  # The as_of argument let's you run the function from a given historical date. Update indicators.R
  # to use this feature -- turning indicators.R into a function? with desired date as an argument
  # Read in CSV
  data <- suppressMessages(read_csv(paste0("output/inputs-archive/", filename, ".csv")))
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
write.csv(acapslist, "Indicator_dataset/acaps.csv")

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

# Write ACAPS sheet
write.csv(acapssheet, "Risk_sheets/acapssheet.csv")

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
ghsi <- read.csv(paste0(github, "Indicator_dataset/HIS.csv"))

ghsi <- ghsi %>%
  rename(Country = H_Country) %>%
  dplyr::select(-X)

# DELETE for first time only
# ghsi <- mutate(ghsi, access_date = Sys.Date() - 1)
# write.csv(ghsi, "output/inputs-archive/ghsi.csv", row.names = F)

archiveInputs(ghsi, group_by = "Country")
# SPLIT: move above to inputs.R
# OR instead of splitting, I could wrap everything above this (read.csv to archive) 
# in an if statement, so you can run the script without this section if you're
# trying to recreate data
ghsi <- loadInputs("ghsi", group_by = "Country")

# Normalise scores
# Rename HIS to ghsi *everywhere*
HIS <- normfuncneg(ghsi, 20, 70, "H_HIS_Score")

#-----------------------—Oxford rollback Score-----------------
#OXrollback <- read.csv("https://raw.githubusercontent.com/OxCGRT/covid-policy-scratchpad/master/rollback_checklist/rollback_checklist.csv")

# Risk of Openness is the reviewed, and updated, version of Oxford Rollback. RENAME
oxford_openness_risk <- read.csv("https://raw.githubusercontent.com/OxCGRT/covid-policy-scratchpad/master/risk_of_openness_index/data/riskindex_timeseries_latest.csv") %>%
  mutate(Date = as.Date(Date))

# # DELETE for first time only
# oxford_openness_risk <- mutate(oxford_openness_risk, access_date = Sys.Date() - 1)
# write.csv(oxford_openness_risk, "output/inputs-archive/oxford_openness_risk.csv", row.names = F)

archiveInputs(oxford_openness_risk, group_by = c("CountryCode", "Date"))
# SPLIT: move above to inputs.R
# RENAME Oxrollback to oxford_openness_risk
OXrollback <- loadInputs("oxford_openness_risk", group_by = c("CountryCode", "Date"))

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

#------------------------—COVID deaths and cases--------------------------
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
# covidweb <- loadInputs("owid_covid", group_by = c("iso_code", "date"))

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

#--------------------------—Oxford Response Tracker----------------------------
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

#------------------------------—INFORM COVID------------------------------------------------------
# SLOW
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

# # DELETE for first time only
# inform_covid <- mutate(inform_covid, access_date = Sys.Date()) %>% 
#   type_convert()
# write.csv(inform_covid, "output/inputs-archive/inform_covid.csv", row.names = F)

archiveInputs(inform_covid, group_by = c("Country"))
# SPLIT: move above to inputs.R
inform_covid_warning <- loadInputs("inform_covid", group_by = c("Country"))

inform_covid_warning <- normfuncpos(inform_covid_warning, 6, 2, "H_INFORM_rating.Value")

write.csv(inform_covid_warning, "Indicator_dataset/inform_covid_warning.csv")

#----------------------------------—WMO DONS--------------------------------------------------------------
# REPLACE all WMO with WHO
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

# # DELETE for first time only
# who_don <- mutate(wmo_don_full, access_date = Sys.Date())
# write.csv(who_don, "output/inputs-archive/who_don.csv", row.names = F)

archiveInputs(who_don, group_by = NULL)
# SPLIT: move above to inputs.R
wmo_don_full <- loadInputs("who_don", group_by = NULL)

countrylist <- read.csv(paste0(github, "Indicator_dataset/countrylist.csv"))
wmo_don <- countrylist %>%
  dplyr::select(-X) %>%
  # Should we specify which dates we care about?
  # Also a problem where *end* of outbreaks are also reported
  mutate(wmo_don_alert = case_when(Country %in% wmo_don_full$wmo_country_alert ~ 10,
                                   TRUE ~ 0))  %>%
  rename(H_wmo_don_alert = wmo_don_alert) %>%
  dplyr::select(-Countryname)

#---------------------------------—Health ACAPS---------------------------------
acaps_health <- acapssheet[,c("Country", "H_health_acaps")]

#----------------------------------—Create combined Health Sheet-------------------------------------------
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

write.csv(health_sheet, "Risk_sheets/healthsheet.csv")

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

# # DELETE for first time only
# proteus <- mutate(proteus, access_date = Sys.Date())
# write.csv(proteus, "output/inputs-archive/proteus.csv", row.names = F)

archiveInputs(proteus, group_by = c("Country"))
# SPLIT: move above to inputs.R
proteus <- loadInputs("proteus", group_by = c("Country"))

upperrisk <- quantile(proteus$F_Proteus_Score, probs = c(0.90), na.rm = T)
lowerrisk <- quantile(proteus$F_Proteus_Score, probs = c(0.10), na.rm = T)
proteus <- normfuncpos(proteus, upperrisk, lowerrisk, "F_Proteus_Score")

#------------------—FEWSNET (with CRW threshold)---

#Load database
fewswb <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/fews.csv"), col_types = cols()))

# # DELETE for first time only
# fewswb <- mutate(fewswb, access_date = Sys.Date())
# write.csv(fewswb, "output/inputs-archive/fewsnet.csv", row.names = F)

archiveInputs(fewswb, path = "output/inputs-archive/fewsnet.csv", group_by = c("country", "year_month"))
# SPLIT: move above to inputs.R
fewswb <- loadInputs("fewsnet", group_by = c("country", "year_month"))

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

#------------------------—WBG FOOD PRICE MONITOR------------------------------------
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
  Mar = Mar.20[which(!is.na(Mar.20))[1]],
  Apr = Apr.20[which(!is.na(Apr.20))[1]],
  May = May.20[which(!is.na(May.20))[1]],
  June = Jun.20[which(!is.na(Jun.20))[1]],
  Jul = Jul.20[which(!is.na(Jul.20))[1]],
  Aug = Aug.20[which(!is.na(Aug.20))[1]],
  Sep = Sep.20[which(!is.na(Sep.20))[1]],
  Oct = Oct.20[which(!is.na(Oct.20))[1]],
  Nov = Nov.20[which(!is.na(Nov.20))[1]],
  Dec = Dec.20[which(!is.na(Dec.20))[1]],
  Jan = Jan.21[which(!is.na(Jan.21))[1]],
  Feb = Feb.21[which(!is.na(Feb.21))[1]]
    ) %>%
mutate(fpv = case_when(
  !is.na(Feb) ~ Feb,
  is.na(Feb) & !is.na(Jan) ~ Jan,
  is.na(Feb) & is.na(Jan) & !is.na(Dec) ~ Nov,
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

#-------------------------—FAO/WFP HOTSPOTS----------------------------
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
  mutate(F_fao_wfp_warning = 0) %>%
  dplyr::select(-X)

# fao_all <- subset(countrylist, Country %in% fao_wfp$Country) %>%
  # mutate(F_fao_wfp_warning = 10)

fao_all[fao_all$Country %in% fao_wfp$Country,"F_fao_wfp_warning"] <- 10

fao_wfp <- fao_all

# # DELETE for first time only
# fao_wfp <- mutate(fao_wfp, access_date = Sys.Date())
# write.csv(fao_wfp, "output/inputs-archive/fao_wfp.csv", row.names = F)

archiveInputs(fao_wfp, group_by = c("Country"))
# SPLIT: move above to inputs.R
fao_wfp <- loadInputs("fao_wfp", group_by = c("Country"))

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

write.csv(food_sheet, "Risk_sheets/foodsecuritysheet.csv")

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
url <- "https://github.com/bennotkin/compoundriskdata/blob/master/Indicator_dataset/RBTracker.xls?raw=true"
destfile <- "RBTracker.xls"
curl::curl_download(url, destfile)
eiu <- read_excel(destfile, sheet = "Data Values", skip = 3)

# # DELETE for first time only
# eiu <- mutate(eiu, access_date = Sys.Date() - 1)
# write.csv(eiu, "output/inputs-archive/eiu.csv", row.names = F)

archiveInputs(eiu, group_by = c("`SERIES NAME`", "MONTH"))
# SPLIT: move above to inputs.R
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

write.csv(macro_sheet, "Risk_sheets/macrosheet.csv")

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

#---------------------------—Alternative socio-economic data (based on INFORM)
inform_risk <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/INFORM_Risk.csv"), col_types = cols()))

# # DELETE for first time only
# inform_risk <- mutate(inform_risk, access_date = Sys.Date())
# write.csv(inform_risk, "output/inputs-archive/inform_risk.csv", row.names = F)

archiveInputs(inform_risk, group_by = c("Country"))
# SPLIT: move above to inputs.R
inform_risk <- loadInputs("inform_risk", group_by = c("Country"))

inform_data <- inform_risk %>%
  dplyr::select(Country, "Socio-Economic Vulnerability") %>%
  rename(S_INFORM_vul = "Socio-Economic Vulnerability")

inform_data <- normfuncpos(inform_data, 7, 0, "S_INFORM_vul")
inform_data <- normfuncpos(inform_data, 7, 0, "S_INFORM_vul")

#------------------------—Forward-looking socio-economic variables from INFORM---------------------------
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

#--------------------------—MPO: Poverty projections----------------------------------------------------
mpo <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/mpo.csv")))

# # DELETE for first time only
# mpo <- mutate(mpo, access_date = Sys.Date())
# write.csv(mpo, "output/inputs-archive/mpo.csv", row.names = F)

archiveInputs(mpo, group_by = c("Country"))
# SPLIT: move above to inputs.R
mpo <- loadInputs("mpo", group_by = c("Country"))

#-----------------------------—HOUSEHOLD HEATMAP FROM MACROFIN-------------------------------------
# If EFI Macro Financial Review is re-included above, we can reuse that. For clarity, moving data read here because it's not being used by macrosheet
macrofin <- read.csv(paste0(github, "Indicator_dataset/macrofin.csv"))

# # DELETE for first time only
# macrofin <- mutate(macrofin, access_date = Sys.Date())
# write.csv(macrofin, "output/inputs-archive/macrofin.csv", row.names = F)

archiveInputs(macrofin, group_by = c("ISO3"))
# SPLIT: move above to inputs.R
macrofin <- loadInputs("macrofin", group_by = c("ISO3"))

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

#----------------------------—WB PHONE SURVEYS-----------------------------------------------------
wb_phone <- read.csv(paste0(github, "Indicator_dataset/phone.csv"))

# # DELETE for first time only
# wb_phone <- mutate(wb_phone , access_date = Sys.Date())
# write.csv(wb_phone , "output/inputs-archive/wb_phone.csv", row.names = F)

archiveInputs(wb_phone , group_by = c("Country"))
# SPLIT: move above to inputs.R
wb_phone  <- loadInputs("wb_phone", group_by = c("Country"))

#------------------------------—IMF FORECASTED UNEMPLOYMENT-----------------------------------------
imf_unemployment <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/imf_unemployment.csv")))

# # DELETE for first time only
# imf_unemployment  <- mutate(imf_unemployment , access_date = Sys.Date())
# write.csv(imf_unemployment , "output/inputs-archive/imf_unemployment.csv", row.names = F)

archiveInputs(imf_unemployment , group_by = c("Country"))
# SPLIT: move above to inputs.R
imf_unemployment  <- loadInputs("imf_unemployment", group_by = c("Country"))
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

write.csv(socioeconomic_sheet, "Risk_sheets/Socioeconomic_sheet.csv")

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
dr1$haz <- try(paste("green"), silent = T)
dr2 <- try(rbind(haz[[19]], haz[[20]]), silent = T)
dr2$haz <- try(paste("orange"), silent = T)
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
# DELETE for first time only
# write.csv(gdacs, "output/inputs-archive/gdacs.csv", row.names = F)

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
# gdacs_prev <- read.csv("output/inputs-archive/gdacs.csv")
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

# SPLIT
gdacs <- suppressMessages(read_csv("output/inputs-archive/gdacs.csv")) %>%
  mutate(access_date = as.Date(access_date)) %>%
  filter(access_date == max(access_date))

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

write.csv(gdac, "Indicator_dataset/gdaclistnormalised.csv")

#----------------------—INFORM Natural Hazard and Exposure rating--------------------------

# inform_risk <- read.csv(paste0(github, "Indicator_dataset/INFORM_Risk.csv"))

# SPLIT: move above to inputs.R
inform_risk <- loadInputs("inform_risk", group_by = c("Country"))

# Rename country
informnathaz <- inform_risk %>%
  dplyr::select(Country, Natural) %>%
  rename(NH_Hazard_Score = Natural) %>%
  drop_na(Country, NH_Hazard_Score)

# Normalise scores
informnathaz <- normfuncpos(informnathaz, 7, 1, "NH_Hazard_Score")

#---------------------------------- —IRI Seasonal Forecast ------------------------------------------
# Load from Github
seasonl_risk <- suppressWarnings(read_csv(paste0(github, "Indicator_dataset/seasonal_risk_list"), col_types = cols()))
seasonl_risk <- seasonl_risk %>%
  dplyr::select(-X1) %>%
  rename(
    Country = "ISO3",
    NH_seasonal_risk_norm = risklevel
  )

iri_forecast <- seasonl_risk #Go through and reduce renamings

# # DELETE for first time only
# iri_forecast <- mutate(iri_forecast, access_date = Sys.Date())
# write.csv(iri_forecast, "output/inputs-archive/iri_forecast.csv", row.names = F)

archiveInputs(iri_forecast, group_by = c("Country"))
# SPLIT: move above to inputs.R
iri_forecast <- loadInputs("iri_forecast", group_by = c("Country"))

seasonl_risk <- iri_forecast # Go through and actually name iri_forecast
#-------------------------------------—Locust outbreaks----------------------------------------------
# List of countries and risk factors associated with locusts (FAO), see:http://www.fao.org/ag/locusts/en/info/info/index.html

locust_risk <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/locust_risk.csv"), col_types = cols()))
locust_risk <- locust_risk %>%
  dplyr::select(-X1)

fao_locust <- locust_risk
# # DELETE for first time only
# fao_locust <- mutate(fao_locust, access_date = Sys.Date())
# write.csv(fao_locust, "output/inputs-archive/fao_locust.csv", row.names = F)

archiveInputs(fao_locust, group_by = c("Country"))
# SPLIT: move above to inputs.R
fao_locust <- loadInputs("fao_locust", group_by = c("Country"))

locust_risk <- fao_locust

#---------------------------------—Natural Hazard ACAPS---------------------------------
acaps_nh <- acapssheet[,c("Country", "NH_natural_acaps")]

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

write.csv(nathazard_sheet, "Risk_sheets/Naturalhazards.csv")

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

fcv <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/Country_classification.csv"))) %>%
  dplyr::select(-X1, Countryname, -`IDA-status`) %>%
  mutate(FCV_status = tolower(FCV_status)) %>%
  mutate(
    FCV_normalised = case_when(
      FCV_status == tolower("High-Intensity conflict") ~ 10,
      FCV_status == tolower("Medium-Intensity conflict") ~ 10,
      FCV_status == tolower("High-Institutional and Social Fragility") ~ 10,
      TRUE ~ 0
    )
  )

fcs <- fcv

# # DELETE for first time only
# fcs <- mutate(fcs, access_date = Sys.Date())
# write.csv(fcs, "output/inputs-archive/fcs.csv", row.names = F)

archiveInputs(fcs, group_by = c("Country"))
# SPLIT: move above to inputs.R
fcs <- loadInputs("fcs", group_by = c("Country"))

fcv <- fcs

#-----------------------------—IDPs--------------------------------------------------------
idp_data <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/population.csv"),
                    col_types = cols(
                      `IDPs of concern to UNHCR` = col_number(),
                      `Refugees under UNHCR mandate` = col_number(),
                      Year = col_number()
                    ), skip = 14
))

un_idp <- idp_data

# # DELETE for first time only
# un_idp <- mutate(un_idp, access_date = Sys.Date())
# write.csv(un_idp, "output/inputs-archive/un_idp.csv", row.names = F)

archiveInputs(un_idp, group_by = c("`Country of origin (ISO)`", "`Country of asylum (ISO)`", "`Year`"))
# SPLIT: move above to inputs.R
un_idp <- loadInputs("un_idp", group_by = c("`Country of origin (ISO)`", "`Country of asylum (ISO)`", "`Year`"))

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

#-------------------------—ACLED data---------------------------------------------
# Select date as three years plus two month (date to retrieve ACLED data)
three_year <- as.Date(as.yearmon(Sys.Date() - 45) - 3.2)

# Get ACLED API URL
acled_url <- paste0("https://api.acleddata.com/acled/read/?key=*9t-89Rn*bDb4qFXBAmO&email=ljones12@worldbank.org&event_date=",
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
# write.csv(acled, "output/inputs-archive/acled.csv", row.names = F)

archiveInputs(acled, group_by = NULL)
# SPLIT: move above to inputs.R
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
      (fatal_3_month_log <= log(5 + 1) & fatal_3_month_log != 0 ) & fatal_z <= 1 ~ 0,
      (fatal_3_month_log <= log(5 + 1) & fatal_3_month_log != 0 ) & fatal_z >= 1 ~ 5,
      TRUE ~ fatal_z_norm
    )
  ) %>%
  ungroup() %>%
  dplyr::select(-iso3)

#--------------------------—REIGN--------------------------------------------
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
# # write.csv(reign, "output/inputs-archive/reign.csv", row.names = F) #1.361MB

archiveInputs(reign, group_by = c("country", "leader", "year", "month"))
# SPLIT: move above to inputs.R
# Note that the file-loaded data set is one fewer than the downloaded dataset because Maia Sandu is duplicated.
# Going forward, I need to fix this so that back-and-forths in leadership are acknowledged
reign <- loadInputs("reign", group_by = c("country", "leader", "year", "month"))

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

write.csv(fragility_sheet, "Risk_sheets/fragilitysheet.csv")

lap <- Sys.time() - lapStart
print(paste("Fragility sheet written", lap, units(lap)))
