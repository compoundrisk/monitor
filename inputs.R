# ## Set up Spark
# sc <- spark_connect(master = "local") # This is only for when running locally
# # sc <- spark_connect(method = "databricks")
# # DBI::dbSendQuery(sc,"CREATE DATABASE IF NOT EXISTS crm")
# sparklyr::tbl_change_db(sc, "crm")
# # setwd("../../../dbfs/mnt/CompoundRiskMonitor")
# if(!dir.exists("input-archives")) dir.create("input-archives")
# #---------------------------------


## Direct Github location (data folder)
#---------------------------------
# github <- "https://raw.githubusercontent.com/bennotkin/compoundriskdata/master/"
#---------------------------------

## FUNCTION TO ARCHIVE ALL INPUT DATA `archiveInputs()` 
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

try_log <- function(expr) {
  fun <- sub("\\(.*", "", deparse(substitute(expr)))
  tryCatch({
    expr
  }, error = function(e) {
    write(paste(Sys.time(), "Error on", fun, "\n", e), file = "output/errors.log", append = T)
  })
}

#---------------------------------

## Add in ACAPS

##### HEALTH

## GHSI
#---------------------------------
ghsi_collect <- function() {
  ghsi <- read.csv(paste0(github, "Indicator_dataset/HIS.csv"))
  ghsi <- ghsi %>%
    rename(Country = H_Country) %>%
    dplyr::select(-X)
  archiveInputs(ghsi, group_by = "Country")
}
#---------------------------------

## Oxford Openness
#---------------------------------
oxford_openness_collect <- function() {
  # Risk of Openness is the reviewed, and updated, version of Oxford Rollback. RENAME
  oxford_openness_risk <- read.csv("https://raw.githubusercontent.com/OxCGRT/covid-policy-scratchpad/master/risk_of_openness_index/data/riskindex_timeseries_latest.csv") %>%
    mutate(Date = as.Date(Date))
  
  archiveInputs(oxford_openness_risk, group_by = c("CountryCode", "Date"))
}
#---------------------------------

## OWID Covid
# _Add in_

## Oxford Response Tracker
# _Add in_

## INFORM Covid
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
#---------------------------------

## WHO DONS
#---------------------------------
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
#---------------------------------

#### FOOD SECURITY
## Proteus Index
#---------------------------------
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
#---------------------------------

## FEWSNET
#---------------------------------

#Load database
fews_collect <- function() {
  fewsnet <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/fews.csv"), col_types = cols()))
  archiveInputs(fewsnet, group_by = c("country", "year_month"))
}
#---------------------------------

## WBG FOOD PRICE MONITOR
# _Add in_

## FAO/WFP HOTSPOTS
#---------------------------------
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
    mutate(F_fao_wfp_warning = 0) %>%
    dplyr::select(-X)
  
  # fao_all <- subset(countrylist, Country %in% fao_wfp$Country) %>%
  # mutate(F_fao_wfp_warning = 10)
  
  fao_all[fao_all$Country %in% fao_wfp$Country,"F_fao_wfp_warning"] <- 10
  
  fao_wfp <- fao_all
  
  archiveInputs(fao_wfp, group_by = c("Country"))
}
#---------------------------------

#### MACRO

## Economist Intelligence Unit
#---------------------------------
eiu_collect <- function() {
  url <- "https://github.com/bennotkin/compoundriskdata/blob/master/Indicator_dataset/RBTracker.xls?raw=true"
  destfile <- "RBTracker.xls"
  curl::curl_download(url, destfile)
  eiu <- read_excel(destfile, sheet = "Data Values", skip = 3)
  file.remove("RBTracker.xls")
  
  archiveInputs(eiu, group_by = c("`SERIES NAME`", "MONTH"))
}
#---------------------------------

#### SOCIO-ECONOMIC

## MPO: Poverty projections
#---------------------------------
mpo_collect <- function() {
  mpo <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/mpo.csv")))
  archiveInputs(mpo, group_by = c("Country"))
}
#---------------------------------

## MACROFIN / EFI Macro Financial Review Household Level Risk
#---------------------------------
mfr_collect <- function() {
  # If EFI Macro Financial Review is re-included above, we can reuse that. For clarity, moving data read here because it's not being used by macrosheet
  macrofin <- read.csv(paste0(github, "Indicator_dataset/macrofin.csv"))
  archiveInputs(macrofin, group_by = c("ISO3"))
}
#---------------------------------

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
#---------------------------------

## IMF FORECASTED UNEMPLOYMENT
#---------------------------------
imf_collect <- function() {
  imf_unemployment <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/imf_unemployment.csv")))
  archiveInputs(imf_unemployment, group_by = c("Country"))
}
#---------------------------------

#### NATURAL HAZARDS

## GDACS
#---------------------------------
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
#---------------------------------

## INFORM Natural Hazard and Exposure Rating
#---------------------------------
inform_risk_collect <- function() {
  inform_risk <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/INFORM_Risk.csv"), col_types = cols()))
  archiveInputs(inform_risk, group_by = c("Country"))
}
#---------------------------------

## IRI Seasonal Forecast
#---------------------------------
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
#---------------------------------

## Locust outbreaks
#---------------------------------
# List of countries and risk factors associated with locusts (FAO), see:http://www.fao.org/ag/locusts/en/info/info/index.html
locust_collect <- function() {
  locust_risk <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/locust_risk.csv"), col_types = cols()))
  locust_risk <- locust_risk %>%
    dplyr::select(-X1)
  
  fao_locust <- locust_risk
  
  archiveInputs(fao_locust, group_by = c("Country"))
}
#---------------------------------

# FRAGILITY

## FCS
#---------------------------------
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
#---------------------------------

## IDPs
#---------------------------------
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
#---------------------------------

## ACLED
#---------------------------------
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
#---------------------------------

## REIGN
#---------------------------------
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
#---------------------------------