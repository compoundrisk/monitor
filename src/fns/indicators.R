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
normfuncneg <- compiler::cmpfun(function(df, upperrisk, lowerrisk, col1) {
  # Create new column col_name as sum of col1 and col2
  df[[paste0(col1, "_norm")]] <- ifelse(df[[col1]] <= upperrisk, 10,
                                        ifelse(df[[col1]] >= lowerrisk, 0,
                                               ifelse(df[[col1]] > upperrisk & df[[col1]] < lowerrisk, 10 - (upperrisk - df[[col1]]) / 
                                                        (upperrisk - lowerrisk) * 10, NA)))
  return(df)
})

# Function to normalise with upper and lower bounds (when high score = high vulnerability)
normfuncpos <- compiler::cmpfun(function(df, upperrisk, lowerrisk, col1) {
  # Create new column col_name as sum of col1 and col2
  df[[paste0(col1, "_norm")]] <- ifelse(df[[col1]] >= upperrisk, 10,
                                        ifelse(df[[col1]] <= lowerrisk, 0,
                                               ifelse(df[[col1]] < upperrisk & df[[col1]] > lowerrisk, 10 - (upperrisk - df[[col1]]) / 
                                                        (upperrisk - lowerrisk) * 10, NA)))
  return(df)
})

## FUNCTION TO ARCHIVE AND LOAD ALL INPUT DATA `archiveInputs()` 
# _Edit this to use Spark_
# - Should I store input archives as a separate CSV file for each date? E.g. `who_dons_20211001` which includes all of the *new* data from October 10?
# - Provide schemas so `read_csv()` (or `spark_read_csv()`) doesn't have to guess columns (would be a separate file/table, e.g. `who_dons_schema`) -- or just one table for _all_ the schemata. (low priority)
# - When bringing in input archives, in order to select most recent, might make sense to use `memory = FALSE` in `spark_read_csv()`.

#---------------------------------
archiveInputs <- compiler::cmpfun(function(data,
                          path = paste0(inputs_archive_path, deparse(substitute(data)), ".csv"), 
                          newFile = F,
                          # group_by defines the groups for which most recent data should be taken
                          group_by = "CountryCode",
                          today = Sys.Date(),
                          return = F,
                          col_types = NULL
                          # large = F) 
){
  if (newFile) {
    path <- path # Need to use path argument before we change `data` – otherwise path is changed
    data <- data %>% mutate(access_date = today) #%>%
    # group_by(across(all_of(group_by))) # not needed for first save
    write.csv(data, path, row.names = F)
    if(return == T) return(data)
  } else {
    
    # Read in the existing file for the input, which will be appended with new data
    prev <- suppressMessages(read_csv(path, col_types = col_types)) %>%
      mutate(access_date = as.Date(access_date))
    
    # Select the most recently added data for each unless group_by is set to false
    if(is.null(group_by)) {
      most_recent <- prev
    } else {
      most_recent <- prev %>%
        group_by(across(all_of(group_by))) %>%
        slice_max(order_by = access_date)
    }
    
    # Round all numeric columns to an excessive 10 places so that rows aren't counted as different
    # just because of digit cropping (such as with MPO)
    most_recent <- mutate(most_recent, across(.cols = where(is.numeric), .fns = ~ round(.x, digits = 8)))
    data <- mutate(data, across(.cols = where(is.numeric), .fns = ~ round(.x, digits = 8)))
    
    # Add access_date for new data
    data <- mutate(data, access_date = today)
    
    # Row bind `most_recent` and `data`, in order to make comparison (probably a better way)
    # Could quicken a bit by only looking at columns that matter (ie. don't compare
    # CountryCode and CountryName both). Also, for historical datasets, don't need to compare
    # new dates. Those automatically get added. 
    # if(!large) {
    bound <- bind_rows(most_recent, data)
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
    combined <- bind_rows(prev, data_fresh) %>% distinct()
    write.csv(combined, path, row.names = F)
    if(return == T) return(combined)
  }
})

#--------------------FUNCTIONS TO LOAD INPUT DATA-----------------
loadInputs <- compiler::cmpfun(function(
  filename,
  group_by = "CountryCode",
  as_of = Sys.Date(),
  format = "csv",
  full = F,
  col_types = NULL) {
  
  if (format == "csv") {
    # Read in CSV
    data <- read_csv(paste0(inputs_archive_path, filename, ".csv"), col_types = col_types)
  }
  if (format == "spark") {
    # Read from Spark DataFrame
  }
  # Select only data from before the as_of date, for reconstructing historical indicators
  if (as_of < Sys.Date()) {
    data <- filter(data, access_date <= as_of)
  }
  if (full) return(data)
  
  # Select the most recent access_date for each group, unless group_by = F
  if (is.null(group_by)) {
    most_recent <- data
  } else {
    most_recent <- data %>%
      group_by(across(all_of(group_by))) %>%
      slice_max(order_by = access_date) %>%
      ungroup()
  }
  return(most_recent)
})

add_dimension_prefix <- function(df, prefix) {
  df <- df %>% rename_with(.cols = -Country, .fn = ~ paste0(prefix, .x))
  return(df)
}

# Function for adding new columns to an inputs-archive file
add_new_input_cols <- function(df1, df2) {
  column_differences(df1, df2)
  combined <- bind_rows(df1, df2[0,]) %>%
    relocate(access_date, .after = ncol(.))
  return(combined)
}
# # Move external to functions file (is this still relevant?)
# try_log <- function(expr) {
#   fun <- sub("\\(.*", "", deparse(substitute(expr)))
#   tryCatch({
#     expr
#   }, error = function(e) {
#     write(paste(Sys.time(), "Error on", fun, "\n", e), file = "output/errors.log", append = T)
#   })
# }

#--------------------—INDICATOR SPECIFIC FUNCTIONS-----------------------------

#--------------------—LOAD ACAPS realtime database-----------------------------
acaps_collect <- function() {
  h <- new_handle()
  handle_setopt(h, ssl_verifyhost = 0, ssl_verifypeer = 0)
  file_path <- paste_path(inputs_archive_path, "acaps", paste0("acaps-", Sys.Date(), ".html"))
  curl_download(url = "https://www.acaps.org/countries",
                file_path,
                handle = h)
  # Remove if text in ".severities" <div> of html is identical in previous run
  new <- read_html(file_path) %>%
    html_nodes(".severities") %>% html_text()
  previous <- read_most_recent(paste_path(inputs_archive_path, "acaps"), FUN = read_html, as_of = Sys.Date() - 1) %>%
    html_nodes(".severities") %>% html_text()
  if (identical(new, previous)) {
    unlink(file_path)
  }
}

## Add in *_collect() function for ACAPS
acaps_process <- function(as_of) {
  acaps <- read_most_recent(paste_path(inputs_archive_path, "acaps"), FUN = read_html, as_of = as_of)
  
  # Scrape ACAPS website
  parent_nodes <- acaps %>% 
    html_nodes(".severity__country")
  
  # Scrape crisis data for each listed country
  acaps_list <- lapply(parent_nodes, function(node) {
    country <- node %>%
      html_node(".severity__country__label") %>%
      html_text()
    country_level <- node %>%
      html_node(".severity__country__value") %>%
      html_text() %>%
      as.numeric()
    crises <- node %>%
      html_nodes(".severity__country__crisis__label") %>%
      html_text()
    values <- node %>%
      html_nodes(".severity__country__crisis__value") %>%
      html_text() %>% 
      as.numeric()
    if (length(crises) != length(values)) {
      stop(paste("Node lengths for labels and values do not match for", country))
    }
    
    df <- data.frame(
      Countryname = country,
      crisis = crises,
      value = values)
    return(df)
  }) %>%
    bind_rows() %>%
    subset(!str_detect(tolower(crisis), "country level")) %>%
    mutate(
      Countryname = case_when(
        Countryname == "CAR" ~ "Central African Republic",
        TRUE ~ Countryname),
      Country = name2iso(Countryname),
      .before = 1)
  
  select_acaps_countries <- function(data, string, minimum, category) {
    selected <- data %>%
      filter(str_detect(tolower(crisis), string)) %>%
      filter(value >= minimum) %>%
      group_by(Country)
    if (nrow(selected > 0)) {
     selected <- selected %>%
        summarise(
        crisis = paste(paste0(crisis, " (", value, ")"), collapse = "; "),
        value = max(value, na.rm = T)) %>%
      mutate(category = category, .after = Country)
    }
    return(selected)
  }
  
  # Conflict countries
  conflict_list <- select_acaps_countries(
    acaps_list,
    string = "conflict|Crisis|crisis|Conflict|Refugees|refugees|Migration|migration|violence|violence|Boko Haram",
    minimum = 4,
    category = "conflict")
  
  # Food security countries
  food_list <- select_acaps_countries(
    acaps_list,
    string = "Food|food|famine|famine",
    minimum = 4,
    category = "food")
  
  # Natural hazard countries
  natural_list <- select_acaps_countries(
    acaps_list,
    string = "flood|drought|cyclone|landslide|earthquake|volcan",
    minimum = 3,
    category = "natural")
  
  # Epidemic countries
  health_list <- select_acaps_countries(
    acaps_list,
    string = "epidemic",
    minimum = 3,
    category = "health")
  
  acaps_sheet <- bind_rows(conflict_list, food_list, natural_list, health_list) %>%
    mutate(inform_severity = case_when(
      !is.na(value) ~ 10,
      TRUE ~ 0)) %>%
    rename(inform_severity_text = crisis)
  
  return(acaps_sheet)
}

acaps_category_process <- function(as_of, category, prefix) {
  acaps_sheet <- acaps_process(as_of = as_of)
  
  output <- acaps_sheet[which(acaps_sheet$category == category),] %>%
    right_join(countrylist, by = "Country") %>%
    mutate(inform_severity = case_when(
      is.na(inform_severity) ~ 0,
      TRUE ~ inform_severity)) %>%
    dplyr::select(Country, inform_severity, inform_severity_text) %>%
    add_dimension_prefix(prefix)
    
  return(output)
}

#--------------------—ACAPS Risk List -----------------------------

acaps_risk_list_collect <- function() {
    ## Post credentials to get an authentication token
    credentials <- read.csv(paste_path(mounted_path, ".access/acaps-credentials.csv"))
    credentials <- list(username=credentials$username, password=credentials$password)
    auth_token_response <- httr::POST("https://api.acaps.org/api/v1/token-auth/", body=credentials)
    auth_token <- httr::content(auth_token_response)$token

    ## Pull data from ACAPS API and loop through the pages
    df <- data.frame()
    request_url <- "https://api.acaps.org/api/v1/risk-list/" # Replace with the URL of the dataset you want to access
    last_request_time <- Sys.time()
    while (TRUE) {

        ## Wait to avoid throttling
        while (as.numeric(Sys.time()-last_request_time) < 1) {
            Sys.sleep(0.1)
        }

        ## Make the request
        response <- httr::GET(request_url, httr::add_headers(Authorization=paste("Token", auth_token, sep=" ")))
        last_request_time <- Sys.time()

        ## Extract the data and convert any list-type columns to strings
        json <- httr::content(response, "text", encoding = "UTF-8")
        df_results <- jsonlite::fromJSON(json)$results
        # df_results <- df_results %>%
        #   mutate(country = sapply(country, toString)) %>%
        #   mutate(iso3 = sapply(iso3, toString)) %>%
        #   mutate(regions = sapply(regions, toString))

        ## Append to the main dataframe
        df <- rbind(df, df_results)

        ## Loop to the next page; if we are on the last page, break the loop
        if (("next" %in% names(httr::content(response))) && (typeof(httr::content(response)[["next"]]) != "NULL")) {
            request_url <- httr::content(response)[["next"]]
        }
        else {
            break
        }
    }

  # df[1:5,c(3,5)] %>% mutate(crisis_id2 = case_when(
  #   lengths(iso3) > lengths(crisis_id) ~ 
  #     data.frame(iso3 = dflist$iso3[[1]]) %>% 
  #   left_join(
  #     data.frame(iso3 = substr(dflist$crisis_id[[1]], 1, 3),
  #               crisis_id = substr(dflist$crisis_id[[1]], 4, 6))
  #     ) %>%
  #     mutate(crisis_id = paste0(iso3, crisis_id)) %>%
  #     .$crisis_id %>% list(),
  #   T ~ "SHORT"

  # ))

# # The length of the crisis_id column doesn't always match the length of the iso3 column
# # This function assigns a crisis_id of "ISONA" to all countries in multi-country events 
# # that don't have a crisis_id. Otherwise, unnest() would fail because of differing lengths 
# # (Try by running unnest(df, country, iso3, crisis_id))
# dfsave <-df
# df$country <- apply(df, 1, function(r) r$country <- paste(r$country, collapse = ", "))

# # If there are fewer ISOs than crisis IDs it might be because there are multiple crisis IDs
# # for a single country; duplicate the ISO
# df$iso3 <- apply(df, 1, function(r) {
#    if (length(r$iso3) >= length(r$crisis_id)) {
#     return(r$iso3)
#   } else if (length(r$iso3) < length(r$crisis_id)) {
#     crisis_isos <- str_extract_all(r$crisis_id, "[A-Z]{3}", simplify = T)
#     not_included_iso3 <- which_not(crisis_isos, r$iso3, swap = T)
#     not_included_crisis_id <- which_not(crisis_isos, r$iso3)
#     not_included <- c(not_included_iso3, not_included_crisis_id)
#     if (length(not_included) == 0) {
#       return((crisis_isos))
#     } else {
#       warning(paste("crisis_id and iso3 dont match on", r$risk_id))
#       return(r$iso3)
#     }
#   }
# })

# df$crisis_id <- apply(df, 1, function(r) {
#   if (length(r$iso3) == length(r$crisis_id) | length(r$iso3) == 1) {
#     return((r$crisis_id))
#   } else if (length(r$iso3) < length(r$crisis_id)) {
#     # If there are fewer iso3 items than crisis_id items it might be because a
#     # region is included in the crisis IDs. Check if this is the case (and, if
#     # so, use the country-related crisis IDs and the region ID)
#     reg_id <- r$crisis_id %>% subset(str_detect(., "REG")) 
#     r$crisis_id <- r$crisis_id %>% subset(!str_detect(., "REG"))
#     if (length(r$iso3) == length(r$crisis_id) | length(r$iso3) == 1) {
#       return((paste(r$crisis_id, reg_id)))
#     } else if (length(r$iso3) < length(r$crisis_id)) {
#       # Or, do they not match because there are multiple crisis IDs for the same
#       # country?
#       crisis_isos <- str_extract_all(r$crisis_id, "[A-Z]{3}", simplify = T) %>%
#         unique()
#       if (length(crisis_isos) == length(r$iso3)) {
#         return(r$crisis_id)
#       } else {
#         # Print an error if the region fix still doesn't work
#         print(r$risk_id)
#         stop("Fewer countries in iso3 column than crises in crisis_id column")
#       }
#     }
#   } else if (length(r$iso3) > length(r$crisis_id)) {
#   new_row <- data.frame(iso3 = r$iso3) %>%#[[1]]) %>%
#     left_join(
#       data.frame(iso3 = substr(r$crisis_id, 1, 3),
#                  crisis_id = substr(r$crisis_id, 4, 6)),
#                  by = "iso3") %>%
#       mutate(crisis_id = paste0(iso3, crisis_id))
#   # new_row <- list(new_row$iso3, new_row$crisis_id)
#   return(new_row$crisis_id)
# }})

df$country <- df$country %>% sapply(function(c) return(paste(c, collapse = ", ")))
df$crisis_id <- df$crisis_id %>% sapply(function(c) return(paste(c, collapse = ", ")))
df$iso3 <- df$iso3 %>% sapply(function(c) return(paste(c, collapse = ", ")))
df <- df %>% separate_rows(iso3, sep = ", ")

# test[1]

# df[1:5,c(3,5)]
#     dflist <- df[lengths(df$iso3) > 1 & lengths(df$iso3) != lengths(df$crisis_id),c(3,5)]

#     data.frame(iso3 = dflist$iso3[[1]]) %>% 
#     left_join(
#       data.frame(iso3 = substr(dflist$crisis_id[[1]], 1, 3),
#                 crisis_id = substr(dflist$crisis_id[[1]], 4, 6))
#       ) %>%
#       mutate(crisis_id = paste0(iso3, crisis_id)) %>%
#       .$crisis_id %>% list()
    
    acaps_risklist <- df %>% 
      mutate(intensity = str_replace_all(intensity, "\n", "")) %>%
    type_convert("cccccccDTDccccdcdcccD")

    # Delete this after one run on Databricks
    if (!file.exists(paste_path(inputs_archive_path, "acaps_risklist.csv"))) {
      print("No file acaps_risklist.csv, being created.")
      archiveInputs(acaps_risklist, group_by = "risk_id", newFile = T)
    } else {
      archiveInputs(acaps_risklist, group_by = "risk_id")
    }
}

acaps_risk_list_process <- function(as_of, dim, prefix, after = as.Date("2000-01-01")) {

    df <- loadInputs("acaps_risklist", group_by = "risk_id", as_of = as_of, col_types = "cccccccDTDccccdcdcccD")

    natural_words <- paste(c(
        "flood", "drought", "natural", " rain", "monsoon", "dry", "fire",
        "earthquak", "tropic", "cyclon", "volcan", "scarcity", "weather",
        "temperat", "landslid"),
         collapse = "|")

    conflict_words <- paste(c("violen", "armed", "clash", "gang", "attack",
        "fight", "offensive", "conflict", "fragil"),
        collapse = "|")

    socio_words <- paste(c(
        "livelihood", "cost", "inflation", "socioeconom", "price", "unemploy", "povert"),
         collapse = "|")

    keywords <- c(
      "Natural Hazard" = natural_words,
      "Conflict and Fragility" = conflict_words,
      "Socioeconomic" = socio_words)

    crisis_words <- keywords[dim]

    crisis_events <- as_tibble(df) %>% 
        subset(
            (str_detect(tolower(risk_title), crisis_words) |
            str_detect(tolower(rationale), crisis_words) |
            str_detect(tolower(vulnerability), crisis_words)) & 
            status != "Not materialised" & last_risk_update >= as_of - 60) %>%
        select(iso3, risk_level, risk_title, rationale, vulnerability, date_entered, last_risk_update, status) %>%
        filter(last_risk_update >= as.Date(after)) %>%
        group_by(risk_title, iso3) %>%
        mutate(
          last_risk_update = as.Date(last_risk_update),
          risk_text_full = paste(last_risk_update, risk_title, rationale, vulnerability, sep = "\\n"),
          # Takes excerpts from vulnerability and rationale columns
          across(
            .cols = c(rationale, vulnerability),
            .fns = ~ str_extract_all(.x, paste0("[^\\S\\r\\n\t]*[^\\.^\\n\\t]{0,30}(",crisis_words,")[^\\.\\n]{0,30}[^\\S$\\.\\n]{0,1}"), simplify = T) %>% 
              trimws() %>% paste(collapse = "//")),
          risk_text = paste(substr(risk_title, 1, 40), "// Rationale:", rationale, "// Vulnerability:", vulnerability)) %>%
        mutate(
            Countryname = iso2name(iso3),
            risk_level = case_when(
            risk_level == "High" ~ 10,
            risk_level == "Medium" ~ 7,
            risk_level == "Low" ~ 3,
            T ~ NA_real_)
            # risk_level_auto = risk_level,
            # Review = NA
          ) %>%
            ungroup() %>%
        select(Countryname, iso3, risk_level, risk_text, risk_text_full, last_risk_update)
        # group_by(iso3) %>%
        # slice_max(risk_level, n = 1, with_ties = T) %>%
        # group_by(iso3) %>%
        # summarize(
        #     acaps_risk_level = max(risk_level),
        #     acaps_risk_text = str_replace_all(paste(risk_text, collapse = " | "), "\\n", " "),
        #     acaps_risk_text_full = str_replace_all(paste(risk_text_full, collapse = " | "), "\\n", " "),
        #     acaps_risk_last_updated = as.Date(max(last_risk_update))
        # ) %>%
        # right_join(countrylist, by = c("iso3" = "Country")) %>%
        # # select(-Countryname) %>%
        # arrange(iso3) %>%
        # rename(Country = iso3) %>%
        # add_dimension_prefix(prefix)
  if (as_of >= as.Date("2023-04-19")) {
  path <- paste_path("hosted-data/acaps-risk-list-reviewed", dim)
  most_recent <- read_most_recent(path, as_of = as_of, return_date = T) 
  previous_review <- most_recent$data
  crisis_events_old <- filter(crisis_events, last_risk_update <= most_recent$date)
  crisis_events_new <- filter(crisis_events, last_risk_update > most_recent$date) %>% 
    mutate(risk_level_auto = risk_level)

  crisis_events_old <- left_join(crisis_events_old, select(previous_review, iso3, risk_text_full, risk_level, risk_level_auto, Review), by = c("iso3" = "iso3", "risk_text_full" = "risk_text_full", "risk_level" = "risk_level"))
  crisis_events_combined <- bind_rows(crisis_events_old, crisis_events_new)
  crisis_events_combined <- crisis_events_combined %>% select(Countryname, iso3, risk_level, risk_level_auto, risk_text, risk_text_full, last_risk_update, Review)

  return(crisis_events_combined) 
  } else {
    return(crisis_events)
  }
}

acaps_risk_list_reviewed_process <- function(dim, prefix, as_of) {
  path <- paste_path("hosted-data/acaps-risk-list-reviewed", dim)
  read_most_recent(path, as_of = as_of) %>%
    filter(Review) %>%
    group_by(iso3) %>%
    # slice_max(risk_level, n = 1, with_ties = T) %>%
    group_by(iso3) %>%
    summarize(
        acaps_risk_level = max(risk_level),
        acaps_risk_text = str_replace_all(paste(risk_text, collapse = " | "), "\\n", " "),
        acaps_risk_text_full = str_replace_all(paste(risk_text_full, collapse = " | "), "\\n", " "),
    ) %>%
    right_join(countrylist, by = c("iso3" = "Country")) %>%
    select(-Countryname) %>%
    arrange(iso3) %>%
    rename(Country = iso3) %>%
    add_dimension_prefix(prefix)
}

##### HEALTH

#--------------------—GHSI Score-----------------
ghsi_collect <- compiler::cmpfun(function() {
  ghsi <- read.csv("hosted-data/ghsi/ghsi.csv")
  ghsi <- ghsi %>%
    rename(Country = H_Country) %>%
    dplyr::select(-X)
  archiveInputs(ghsi, group_by = "Country")
})

ghsi_process <- function(as_of) {
  
  # OR instead of splitting, I could wrap everything above this (read.csv to archive) 
  # in an if statement, so you can run the script without this section if you're
  # trying to recreate data
  ghsi <- loadInputs("ghsi", group_by = "Country", as_of = as_of, format = "csv", col_types = "cdD")
  
  # Normalise scores
  upperrisk <- quantile(ghsi$H_HIS_Score, probs = c(0.05), na.rm = T)
  lowerrisk <- quantile(ghsi$H_HIS_Score, probs = c(0.85), na.rm = T)
  ghsi <- normfuncneg(ghsi, upperrisk, lowerrisk, "H_HIS_Score")
  return(ghsi)
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
  
  dons_urls <- dons_raw %>%
    html_nodes(".sf-list-vertical") %>%
    html_nodes("a") %>%
    html_attr("href")
  
  # Check if notification is announcing *end* of outbreak
  first_sentences <- sapply(dons_urls, function(url) {
    p <- read_html(url) %>% 
      html_nodes("article")  %>%
      html_node("div:first-child") %>%
      html_node("p:first-child") %>%
      html_text()
    sentence <- sub("(.*?\\.) [A-Z].*", "\\1", p)
    return(sentence)
  })
  declared_over <- grepl("declared the end of|declared over", first_sentences, ignore.case = T)
  
  who_dons <- data.frame(
    text = dons_text,
    date = dons_date,
    url = dons_urls,
    declared_over
  ) %>%
    mutate(disease = trimws(sub("\\s[-——ｰ–].*", "", text)),
           country = trimws(sub(".*[-——ｰ–]", "", text)),
           country = trimws(sub(".*-", "", country)),
           date = dmy(date)) %>%
    separate_rows(country, sep = ",") %>%
    mutate(who_country_alert = name2iso(country))
  
  archiveInputs(who_dons, group_by = NULL)
}

dons_process <- function(as_of) {
  who_dons <- loadInputs("who_dons", group_by = NULL, as_of = as_of, format = "csv") #, col_types = "cDcccclD") # column order is oddly different on Databricks version of inputs archive
  # Only include DONs alerts from the past 3 months and not declared over
  # (more robust version would filter out outbreaks if they were later declared over)
  who_dons_current <- who_dons %>%
    subset(date >= as_of - 92 & date <= as_of) %>% # change back to 92
    mutate(who_dons_text = paste(date, "–", text)) %>%
    rename(Country = who_country_alert) %>%
    select(Country, date, disease, who_dons_text, declared_over, url, access_date)
  
  # Remove diseases that have been declared over
  over <- subset(who_dons_current, declared_over)  
  
  for (i in seq_len(nrow(over))) {
    o <- over[i,]
    who_dons_current <- who_dons_current %>% mutate(
      declared_over = case_when(
        Country == o$Country & date < o$date & disease == o$disease ~ T,
        TRUE ~ declared_over
      )
    )
  }
  
  who_dons <- who_dons_current %>%
    subset(!declared_over) %>%
    group_by(Country) %>%
    summarize(
      who_dons_text = paste(who_dons_text, collapse = "; "),
      events = n())
  
  who_dons <- left_join(countrylist, who_dons, by = c("Country" = "Country")) %>%
    mutate(who_dons_alert = case_when(
      !is.na(who_dons_text) ~ 7,
      TRUE ~ 0)) %>%
    select(Country, who_dons_alert, who_dons_text) %>%
    add_dimension_prefix("H_")
  
  return(who_dons)
}

#---------------------------------—IFRC Epidemics---------------------------------

ifrc_collect <- function() {
url <- "https://goadmin.ifrc.org/api/v2/event"
queryString <- list(
  limit = "1000",
  ordering = "-disaster_start_date",
  disaster_start_date__gte = "2022-09-27T00:00:00.000Z",
  dtype = "1",
  offset = "0")
payload <- ""
encode <- "raw"
response <- httr::VERB("GET", url, body = payload,
                 add_headers(Origin = 'https://go.ifrc.org', Accept_Encoding = 'gzip, deflate, br',
                 Host = 'goadmin.ifrc.org',
                #  User_Agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15', 
                 Accept_Language = 'en', Referer = 'https://go.ifrc.org/', Connection = 'keep-alive', ''), query = queryString, content_type("application/octet-stream"), accept("*/*"), encode = encode)

json <- content(response, "text")
df <- jsonlite::fromJSON(json)$results 
df$dtype <- df$dtype$name
df$countries <- lapply(df$countries, function(i) {return(i$iso3)}) %>% unlist()
ifrc_epidemics <- df %>% mutate(
  disaster_start_date = as.Date(disaster_start_date),
  created_at = as.Date(created_at),
  updated_at = as.Date(updated_at),
  num_affected = as.numeric(num_affected)) %>%
  select(-field_reports, -appeals)

    # Delete this after one run on Databricks
    if (!file.exists(paste_path(inputs_archive_path, "ifrc_epidemics.csv"))) {
      print("No file ifrc_epidemics.csv, being created.")
      archiveInputs(ifrc_epidemics, group_by = "id", newFile = T, col_types = "ccdiccDDlllDilcllllcc")
    } else {
      archiveInputs(ifrc_epidemics, group_by = "id", col_types = "ccdiccDDlllDilcllllcc")
    }
}

ifrc_process <- function(as_of) {
df <- loadInputs("ifrc_epidemics", group_by = "id", as_of = as_of, col_types = "ccdiccDDlllDilcllllcc") %>% 
  select(Country = countries,
    severity_level = ifrc_severity_level, severity_level_color = ifrc_severity_level_display,
    disaster_start_date, updated_at, created_at, id, summary, name) %>%
    mutate(ifrc_epidemics = 10, ifrc_epidemics_text = name) %>%
    add_dimension_prefix("H_")

epidemics <- left_join(countrylist, df, by = "Country") %>% as_tibble() %>% arrange(H_ifrc_epidemics)
return(epidemics)
}


#---------------------------------—Health ACAPS---------------------------------
# acaps_health <- acapssheet[,c("Country", "H_health_acaps")]

#### FOOD SECURITY
# -------------------------------— EIU Global Food Security Index --------------
gfsi_collect <- function() {
    most_recent <- read_most_recent('hosted-data/gfsi', FUN = read_csv, col_types = "cdd", as_of = Sys.Date(), return_date = T)
    gfsi <- most_recent$data
    file_date <- most_recent$date
    
    # Delete this after one run on Databricks
    if (!file.exists(paste_path(inputs_archive_path, "gfsi.csv"))) {
      print("No file gfsi.csv, being created.")
    archiveInputs(gfsi, group_by = "Country", col_types = "cdd", newFile = T)
    } else {
    archiveInputs(gfsi, group_by = "Country", col_types = "cdd", today = file_date)
    }   
}

gfsi_process <- function(as_of) {
  gfsi <- loadInputs("gfsi", group_by = "Country", col_types = "cddD") %>%
       mutate(Country = name2iso(Country), F_GFSI = Score, .keep = "none")

  lowerrisk <- quantile(gfsi$F_GFSI, .9)
  upperrisk <- quantile(gfsi$F_GFSI, .1)
  gfsi <- gfsi %>% normfuncneg(upperrisk, lowerrisk, "F_GFSI")

  return(gfsi)
}

# -------------------------------— Proteus Index -------------------------------
proteus_collect <- function() {
  proteus <- read.csv("hosted-data/proteus/proteus.csv")
  
  proteus <- proteus %>%
    rename(F_Proteus_Score = Proteus.index) %>%
    dplyr::select(-X) %>%
    mutate(Country = name2iso(Country))
  
  archiveInputs(proteus, group_by = c("Country"))
}

proteus_process <- function(as_of) {
  
  proteus <- loadInputs("proteus", group_by = c("Country"),
    as_of = as_of, format = "csv", col_types = "cdD")
  
  upperrisk <- quantile(proteus$F_Proteus_Score, probs = c(0.90), na.rm = T)
  lowerrisk <- quantile(proteus$F_Proteus_Score, probs = c(0.10), na.rm = T)
  proteus <- normfuncpos(proteus, upperrisk, lowerrisk, "F_Proteus_Score")
  return(proteus)
}

#------------------—FEWSNET (with CRW threshold)---

#Load database
fews_collect <- function(as_of = Sys.Date()) {
  most_recent <- read_most_recent("hosted-data/fews", 
    FUN = read_csv, col_types = cols(), as_of = as_of, return_date = T)
  file_date <- most_recent[[2]]
  fewsnet <- most_recent$data
  archiveInputs(fewsnet,  group_by = c("admin_code", "year_month"), today = file_date)
}

fews_process <- function(as_of) {
  fewswb <- loadInputs("fewsnet", group_by = c("admin_code", "year_month"), 
    as_of = as_of, format = "csv", col_types = "cdcdddddddddcddcD") %>%
    mutate(year_month = as.yearmon(year_month, "%Y_%m")) %>%
    subset(as.Date(year_month) > as_of - 2 * 365) # Might be able to replace this with a shorter timespan
  
  #Calculate country totals
  fewsg <- fewswb %>%
    #  dplyr::select(-X) %>%
    group_by(country, year_month) %>%
    mutate(countrypop = sum(pop)) %>%
    ungroup()
  
  # Calculate proportion and number of people in IPC class 
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
    ) %>%
    ungroup()
  
  #Functions to calculate absolute and geometric growth rates
  pctabs <- function(x) x - lag(x)
  pctperc <- function(x) (x - lag(x)) / lag(x)
  
  #Summarise country totals per in last round of FEWS
  fewssum <- fewspop %>%
    subset(!is.na(fews_ipc) & as.Date(year_month) > as_of - 365 & as.Date(year_month) <= as_of) %>%
    group_by(admin_code) %>%
    slice_max(year_month, n = 2) %>% 
    ungroup() %>%
    group_by(country, year_month) %>%
    # Calculate total country-wide counts and proportions for total IPC3+ and 4+ classes
    summarize(across(
      .cols = matches("ipc.plus.*"),
      .fns = ~ sum(.x, na.rm = T),
      .names = "total{.col}"),
      .groups = "drop_last") %>%
    # Calculate percentage change for proportions in IPC3+ or IPC4+
    mutate(pctchangeipc3for = pctabs(totalipc3pluspercfor), # Latest ipc3+ forecast proportion - previous proportion
           pctchangeipc4for = pctperc(totalipc4pluspercfor), # Percentage growth in ipc4+ forecast proportion from previous proportion
           pctchangeipc3now = pctabs(totalipc3pluspercnow), # Latest ipc3+ current proportion - previous proportion
           pctchangeipc4now = pctperc(totalipc4pluspercnow), # Percentage growth in ipc4+ current proportion from previous proportion
           diffactfor = totalipc3pluspercfor - totalipc3pluspercnow,
           fshighrisk = case_when((totalipc3plusabsfor >= 5000000 | totalipc3pluspercfor >= 20) & pctchangeipc3for >= 5  ~ "High risk",
                                  (totalipc3plusabsnow >= 5000000 | totalipc3pluspercnow >= 20) & pctchangeipc3now >= 5  ~ "High risk",
                                  totalipc4pluspercfor >= 2.5  & pctchangeipc4for >= 10  ~ "High risk",
                                  totalipc4pluspercnow >= 2.5  & pctchangeipc4now >= 10  ~ "High risk",
                                  TRUE ~ "Not high risk")) %>%
    # dplyr::select(-fews_ipc, -fews_ha, -fews_proj_near, -fews_proj_near_ha, -fews_proj_med, 
    #               -fews_proj_med_ha, -fews_ipc_adjusted, -fews_proj_med_adjusted, -countryproportion) %>%
    group_by(country) %>%
    slice_max(year_month) %>%
    ungroup()
  # filter(year_month == "2021_10")
  
  # Find max ipc for any region in the country
  fews_summary <- fewsg %>%
    group_by(country, year_month) %>%
    subset(!is.na(fews_proj_med_adjusted)) %>%
    # Fixed with subset above: Yields warning of infinite values, but we filter these out below; not a concern
    summarise(max_ipc = max(fews_proj_med_adjusted, na.rm = T)) %>%
    # mutate(
    #   year_month = str_replace(year_month, "_", "-"),
    #   year_month = as.Date(as.yearmon(year_month)),
    #   year_month = as.Date(year_month)) %>%
    filter(!is.infinite(max_ipc)) %>%
    # filter(year_month == max(year_month, na.rm = T))
    group_by(country) %>% slice_max(year_month) %>% ungroup()
  
  # Join the two datasets
  fews_dataset <- left_join(fewssum, fews_summary, by = c("country", "year_month")) %>%
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
      fews_crm = paste(fshighrisk, "and max IPC of", max_ipc, "in", year_month),
      Country = name2iso(country)) %>%
    dplyr::select(-country) %>% dplyr::select(Country, everything()) %>%
    rename_with(
      .fn = ~ paste0("F_", .),
      .cols = colnames(.)[!colnames(.) %in% c("Country", "country")]
    )
  
  colnames(fews_dataset[-1]) <- paste0("F_", colnames(fews_dataset[-1])) 
  # if (nrow(fews_dataset) == 0) stop("FEWS dataset is empty (likely not refreshed in a year)")
  return(fews_dataset)
}

#------------------------—WBG FOOD PRICE MONITOR------------------------------------
# Taken from https://microdatalib.worldbank.org/index.php/catalog/12421/
# Behind intranet
fpi_collect <- function(as_of = Sys.Date()) {
  most_recent <- read_most_recent("hosted-data/food-price-inflation", FUN = read_csv, col_types = "dddddccD", as_of = as_of, return_date = T)
  file_date <- most_recent[[2]]
  
  wb_fpi <- most_recent[[1]] %>%
    subset(date > as_of - 365)
  archiveInputs(wb_fpi, group_by = c("ISO3", "date"), today = file_date)
}

fpi_collect_many <- function(as_of = Sys.Date()) {
  oldest_date <- loadInputs("wb_fpi", group_by = c("ISO3", "date"), 
    as_of = Sys.Date(), format = "csv", col_type = "dddddccDD") %>% 
    .$access_date %>% max()
  new_dates <- read_most_recent("hosted-data/food-price-inflation", 
    n = "all", FUN = paste, as_of = Sys.Date(), return_date = T)$date %>%
    .[. > oldest_date]
  lapply(new_dates, fpi_collect)
}

# dates <- list.files("hosted-data/food-price-inflation") %>%
#   str_extract("2022.*[^.csv]") %>%
#   as.Date()

# as_of <- dates[[3]]
# path = "output/inputs-archive/wb_fpi.csv"
# data = wb_fpi

# dates[-c(1:3)]

# lapply(dates[-c(1:2)], function (as_of) {
#   most_recent <- read_most_recent("hosted-data/food-price-inflation", FUN = read.csv, as_of = as_of, return_date = T)
#   file_date <- most_recent[[2]]

#   wb_fpi <- most_recent[[1]] %>%
#     mutate(date = as.Date(date)) %>%
#     subset(date > Sys.Date() - 365)
#   archiveInputs(wb_fpi, group_by = c("ISO3", "date"), today = file_date)
# })

fpi_process <- function (as_of) {
  fpi <- loadInputs("wb_fpi", group_by = c("ISO3", "date"), 
    as_of = as_of, format = "csv", col_types = "dddddccDD") %>%
    group_by(ISO3) %>%
    slice_max(order_by = date) %>%
    # in case any country's newest data is old, don't use
    subset(date > as_of - 92 & date <= as_of) %>%
    select(Country = ISO3, Inflation)
  
  fpi <- mutate(fpi,
                fpv_rating = case_when(
                  Inflation <= 2 ~ 1,
                  Inflation > 2 & Inflation <= 5 ~ 3,
                  Inflation > 5 & Inflation <= 30 ~ 5,
                  Inflation >= 30 ~ 7,
                  TRUE ~ NA_real_
                )) %>%
    add_dimension_prefix("F_")
  return(fpi) 
}

#-------------------------—FAO/WFP HOTSPOTS----------------------------
fao_wfp_collect <- function() {
  fao_wfp <- read_csv("hosted-data/fao-wfp/fao-wfp.csv", col_types = cols())
  fao_wfp <- fao_wfp %>%
    mutate(Country = name2iso(Country))
  
  fao_all <- countrylist
  fao_all[fao_all$Country %in% fao_wfp$Country,"F_fao_wfp_warning"] <- 10
  fao_all$Forecast_End <- max(fao_wfp$Forecast_End, na.rm = T)
  
  fao_wfp <- fao_all
  
  start_date <- min(
    (as.yearmon(fao_all$Forecast_End[1]) - 3/12) %>% as.Date(),
    Sys.Date())
  
  archiveInputs(fao_wfp, group_by = c("Country"), today = start_date)
}

fao_wfp_process <- function(as_of) {
  # Kind of unnecessary
  fao_wfp <- loadInputs("fao_wfp", group_by = c("Country"), 
    as_of = as_of, format = "csv", col_types = "ccdDD") %>%
    select(-Countryname) %>%
    filter(Forecast_End >= as_of)
  return(fao_wfp)
}

fao_wfp_web_collect <- function() {

  prev <- read_most_recent(paste_path(inputs_archive_path, "fao-wfp-web/"), FUN = read_file, as_of = Sys.Date(), return_date = T, return_name = T)
  prev_text <- prev$data
  prev_outlook <- prev$name %>% str_match("hunger-hotspots-(\\d\\d)") %>% .[[2]] %>% as.numeric
  outlook <- prev_outlook + 1

  no_data <- T
  while (no_data == T & outlook >= prev_outlook) {
    print(outlook)
    url <- "https://www.hungerhotspots.org//Components/ajaxengine.cfc"
    payload <- ""
    encode <- "raw"
    queryString <- list(
      method = "controllerNEW",
      nIdOutlook = outlook,
      nConcern = "0",
      init = "1",
      object = "comMap",
      action = "getData",
      timeout = "600",
      cClientSession = "F2C6472F-E99C-4E2D-86CE102C4C94B29C")

    response <- VERB("GET", url, body = payload, query = queryString,
        content_type("application/octet-stream"),
        set_cookies(
            `cfid` = "cf5be69a-98d3-496f-aed4-deef0d844775",
            `cftoken` = "0", `CF_CLIENT_AC3F2F44A9F80153B3F52E57CCD20EEB_LV` = "1669922581475", 
            `CF_CLIENT_AC3F2F44A9F80153B3F52E57CCD20EEB_TC` = "1669922581475", 
            `CF_CLIENT_AC3F2F44A9F80153B3F52E57CCD20EEB_HC` = "2"),
        encode = encode)

    full_text <- content(response, "text")
    if (full_text %>% str_detect('TOCOLOR\\\\+":1')) {
        no_data <- F
      } else {
        outlook <- outlook - 1
    }
  }
  
  if (!identical(full_text, prev_text) & !no_data) {
    file_name <- paste_path(inputs_archive_path, paste0("fao-wfp-web/hunger-hotspots-", outlook, "-", Sys.Date(), ".txt"))
    cat(full_text, file = file_name)
  }
}

fao_wfp_web_process <- function(as_of) {
  full_text <- read_most_recent(paste_path(inputs_archive_path, "fao-wfp-web/"), FUN = read_file, as_of = as_of, return_date = T, return_name = T)

  divs <- full_text %>% 
      str_extract_all("<div style.*?NSTARRED....") %>%
      unlist() %>%
      str_replace_all("\\\\+t", "") %>%
      str_replace_all("\\\\+r", "") %>%
      str_replace_all("\\\\+n", "") %>%
      str_replace_all("\\\\\\+", "")

  hotspots <- lapply(divs, function(d) {
      country <- str_extract(d, 'fas fa-square\\\\*"></i>.*?<div') %>%
          str_extract("(?<=</span>).*?(?=</div>)")
      color <- str_match(d, " color: rgb.?\\(([0-9, ]+)\\)")[2]
      red <- str_extract(color, "^[0-9]+") %>% as.numeric()
      concern <- if (red == 11) "highest" else if (red == 16) "very high" else if (red == 0) "high" else warning("No class assigned to color")
      # ipc <- str_extract_all(d, "IPC/CH.*?(?=<div style)") %>%
      #     lapply(function(s) {
      #         phase <- str_match(s, "IPC/CH PHASE (.)")[2]
      #         numbers <- str_match(s, "number.*?(\\d+\\.\\d).*?<div>.*?([A-Za-z]+)")
      #         pop <- paste(numbers[,2], numbers[,3])
      #         return(c("IPC" = phase, "pop" = pop))
      #     })
      return(c('Country' = country, 'Color' = color, 'F_FAO_WFP_Concern' = concern))
  }) %>% bind_rows() %>%
  mutate(Countryname = Country, Country = name2iso(Countryname), .before = 1) %>%
  mutate(F_FAO_WFP_Hunger_Hotspot = case_when(
    F_FAO_WFP_Concern == "highest" ~ 10,
    F_FAO_WFP_Concern == "very high" ~  7,
    F_FAO_WFP_Concern == "high" ~  5
  ))

  return(hotspots)
  food <- full_join(fews, hotspots, by = "Country") %>%
    select(Country, F_fews_crm_norm, F_FAO_WFP_Hunger_Hotspot, F_FAO_WFP_Concern) %>% 
    mutate(F_FAO_WFP_Concern = factor(F_FAO_WFP_Concern, levels = c("high", "very high", "highest")))

  ggplot(food) + ggrepel::geom_text_repel(aes(x = F_fews_crm_norm, y = F_FAO_WFP_Hunger_Hotspot, label = iso2name(Country))) +
    labs(x = "FEWS, normalised", y = "Hunger Hotspots, normalised", title = "Lowest Hunger Hotspot class corresponds with upper FEWS classes") +
    theme_bw() +
    scale_y_continuous(breaks = c("high" = 8, "very high" = 9, "highest" = 10)) +
   ggh4x::force_panelsizes(rows = unit(6, "in"), cols = unit(6, "in"))
  # ggplot(food) + geom_text(aes(x = jitter(F_fews_crm_norm), y = F_FAO_WFP_Concern, label = iso2name(Country))) +
  #   labs(x = "FEWS, normalised", y = "Hunger Hotspots, normalised", title = "Lowest Hunger Hotspot class corresponds with upper FEWS classes") +
  #   theme_bw() +
  #  ggh4x::force_panelsizes(rows = unit(6, "in"), cols = unit(6, "in"))

}

#### MACRO

mfr_watchlist_collect <- function() {
  most_recent <- read_most_recent("hosted-data/mfr-watchlist", FUN = read_csv, 
    na = "-", col_types = "cdcccccc", as_of = Sys.Date(), return_date = T) 
  mfr_watchlist <- most_recent$data %>% mutate(iso = name2iso(Country))
  file_date <- most_recent$date

  # Delete this after one run on Databricks
  if (!file.exists(paste_path(inputs_archive_path, "mfr_watchlist.csv"))) {
    print("No file mfr_watchlist.csv, being created.")
    archiveInputs(mfr_watchlist, group_by = "iso", newFile = T, today = file_date)
  } else {
    archiveInputs(mfr_watchlist, group_by = "iso", today = file_date)
  }
}

mfr_watchlist_process <- function(as_of = as_of) {
  watchlist <- loadInputs("mfr_watchlist", group_by = "iso", as_of = as_of) %>%
    mutate(
      Country = iso,
      M_MFR_Watchlist = case_when(
        `MFR risk rating` == "High" ~ 10,
        `MFR risk rating` == "Medium" ~ 7,
        `MFR risk rating` == "Moderate" ~ 0
      ),
      M_DSA_Rating =  case_when(
        `DSA rating` %in% c("High", "In distress") ~ 10,
        `DSA rating` == "Medium" ~ 7,
        `DSA rating` == "Moderate" ~ 0
      )
    ) %>% 
    select(Country, M_MFR_Risk_Rating_text = `MFR risk rating`, M_MFR_Watchlist, M_DSA_Rating_text = `DSA rating`, M_DSA_Rating)
  return(watchlist)  
}

mfr_process <- function(as_of = as_of) {
  watchlist <- mfr_watchlist_process(as_of)
  review <- macrofin_process(as_of)

  mfr <- full_join(watchlist, review, by = "Country")
  mfr %>% mutate(
    M_MFR = case_when(
      !is.na(M_MFR_Watchlist) ~ M_MFR_Watchlist,
      !is.na(M_DSA_Rating) ~ M_DSA_Rating,
      !is.na(M_Macro_Financial_Risk) ~ M_Macro_Financial_Risk),
    M_MFR_raw = case_when(
      !is.na(M_MFR_Risk_Rating_text) ~ paste("Watchlist:", M_MFR_Risk_Rating_text),
      !is.na(M_DSA_Rating_text) ~ paste("DSA:", M_DSA_Rating_text),
      !is.na(M_Macro_Financial_Risk_raw) ~ paste("MFR Financial Risk:", M_Macro_Financial_Risk_raw)))
}

#---------------------------—Economist Intelligence Unit---------------------------------
eiu_collect <- function() {
  # url <- "https://github.com/bennotkin/compoundriskdata/blob/master/Indicator_dataset/RBTracker.xls?raw=true"
  # destfile <- "RBTracker.xls"
  # curl::curl_download(url, destfile)
  # eiu <- read_excel(destfile, sheet = "Data Values", skip = 3)
  # file.remove("RBTracker.xls")
  
  # first_of_month <- str_replace(Sys.Date(), "\\d\\d$", "01") %>% as.Date()
  
  # eiu <- read_xls("hosted-data/RBTracker.xls", sheet = "Data Values", skip = 3)
  most_recent <- read_most_recent("hosted-data/eiu", FUN = read_excel, as_of = Sys.Date(), return_date = T, sheet = "Data Values", skip = 3)
  eiu <- most_recent$data
  file_date <- most_recent$date
  
  archiveInputs(eiu, group_by = c("SERIES NAME", "MONTH"), today = file_date)
}

eiu_process <- function(as_of) {
  eiu_data <- loadInputs("eiu", group_by = c("SERIES NAME", "MONTH"), 
    as_of = as_of, col_types = "ccDddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddD") %>%
    select(-access_date) %>%
    subset(MONTH <= as_of)
  
  eiu_data_long <- eiu_data %>%
    select(-`SERIES CODE`) %>%
    subset(`SERIES NAME` %in% c("Macroeconomic risk", "Financial risk", "Foreign trade & payments risk")) %>%
    pivot_longer(-c(`SERIES NAME`, MONTH), names_to = "Country", values_to = "Values") %>%
    pivot_wider(names_from = `SERIES NAME`, values_from = Values) %>%
    mutate(Macroeconomic_risk = (`Financial risk` + `Macroeconomic risk` + `Foreign trade & payments risk`) / 3)
  
  eiu_latest_month <- eiu_data_long %>%
    group_by(Country) %>%
    slice_max(MONTH, n = 1) %>%
    # summarize(Macroeconomic_risk = mean(Macroeconomic_risk))
    summarize(across(-MONTH, ~ mean(.x, na.rm = T)))
  eiu_one_year <- eiu_data_long %>%
    group_by(Country) %>%
    slice_max(MONTH, n = 13) %>%
    slice_min(MONTH, n = 12) %>%
    summarize(across(-MONTH, ~ mean(.x, na.rm = T), .names = "{.col}_12"))
  # eiu_three_month <- eiu_data_long %>%
  #     group_by(Country) %>%
  #     slice_max(MONTH, n = 4) %>%
  #     slice_min(MONTH, n = 3) %>%
  #     summarize(Macroeconomic_risk_3 = mean(Macroeconomic_risk))
  
  eiu_joint <-
    reduce(list(eiu_latest_month, eiu_one_year),#, eiu_three_month),
           full_join, by = "Country") %>%
    mutate(
      EIU_12m_change = Macroeconomic_risk - Macroeconomic_risk_12
      #   EIU_3m_change = Macroeconomic_risk - Macroeconomic_risk_3
    ) %>%
    rename(EIU_Score = `Macroeconomic_risk`,
           EIU_Score_12m = `Macroeconomic_risk_12`) %>%
    rename_with(.col = -Country, .fn = ~ paste0("M_", .)) %>%
    mutate(Country = name2iso(Country)) %>%
    { .[,sort(names(.))] }
  
  #   eiu_joint <- normfuncpos(eiu_joint, quantile(eiu_joint$M_EIU_Score, 0.95), quantile(eiu_joint$M_EIU_Score, 0.10), "M_EIU_Score")
  eiu_joint <- normfuncpos(eiu_joint, quantile(eiu_joint$M_EIU_12m_change, 0.95), quantile(eiu_joint$M_EIU_12m_change, 0.10), "M_EIU_12m_change")
  eiu_joint <- normfuncpos(eiu_joint, quantile(eiu_joint$M_EIU_Score_12m, 0.95), quantile(eiu_joint$M_EIU_Score_12m, 0.10), "M_EIU_Score_12m")

  # Add a cap so countries with low absolute risk are not flagged
  eiu_joint <- eiu_joint %>% 
    mutate(M_EIU_12m_change_norm = case_when(
      M_EIU_12m_change_norm >= 7 & M_EIU_Score < 50 ~ 7,
      T ~ M_EIU_12m_change_norm
    ))

  return(eiu_joint)
}

#### SOCIO-ECONOMIC
#---------------------------—Alternative socio-economic data (based on INFORM)
inform_socio_process <- function(as_of) {
  inform_risk <- loadInputs("inform_risk", group_by = c("Country"), 
    as_of = as_of, format = "csv", col_types = "cddddddddddddddddddddddddddddddddcDddddcd") %>%
    distinct()
  
  inform_data <- inform_risk %>%
    dplyr::select(Country, "Socio-Economic Vulnerability") %>%
    rename(S_INFORM_vul = "Socio-Economic Vulnerability")
  
  inform_data <- normfuncpos(inform_data, 7, 0, "S_INFORM_vul")
  inform_data <- normfuncpos(inform_data, 7, 0, "S_INFORM_vul")
  return(inform_data)
}

#--------------------------—MPO: Poverty projections----------------------------------------------------
mpo_collect <- function() {
  most_recent <- read_most_recent("hosted-data/mpo", FUN = read_xlsx, as_of = Sys.Date(), return_date = T)
  file_date <- most_recent[[2]]
  # mpo <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/mpo.csv")))
  # archiveInputs(mpo, group_by = c("Country"))
  
  
  # FIX: Ideally most of this would be in the mpo_process() function, rather than the collect
  # function, but doing so would conflict with the current mpo archive structure
  mpo <- most_recent[[1]]
  
  # Add population
  pop <- wpp.by.year(wpp.indicator("tpop"), 2020)
  
  pop$charcode <- suppressWarnings(countrycode(pop$charcode,
                                               origin = "iso2c",
                                               destination = "iso3c"))
  
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
    # filter(Label == "International poverty rate ($1.9 in 2011 PPP)") %>%
    filter(substr(Indicator, 4, 10) == "POV1") %>%
    rename_with(
      .fn = ~ paste0("S_", .),
      .cols = colnames(.)[!colnames(.) %in% c("Country")]
    )
  
  # Normalise based on percentiles
  mpo_data <- normfuncpos(mpo_data, .5, 0,
                          # quantile(mpo_data$S_pov_prop_23_22, 0.98,  na.rm = T),
                          # quantile(mpo_data$S_pov_prop_23_22, 0.05,  na.rm = T),
                          "S_pov_prop_23_22")
  mpo_data <- normfuncpos(mpo_data, .5, 0,
                          # quantile(mpo_data$S_pov_prop_22_21, 0.98,  na.rm = T),
                          # quantile(mpo_data$S_pov_prop_22_21, 0.05,  na.rm = T),
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
  archiveInputs(mpo, group_by = c("Country"), today = file_date)
  write.csv(mpo_data, paste_path(inputs_archive_path, "mpo-alt.csv"), row.names = F)
}

mpo_process <- function(as_of) {
  # Specify the expected types for each column; I should do this everywhere.
  # col_types <- cols(
  #   Country = "c",
  #   S_pov_comb_norm = "d",
  #   S_pov_prop_22_21_norm = "d",
  #   S_pov_prop_21_20_norm = "d",
  #   S_pov_prop_20_19_norm = "d",
  #   S_pov_prop_22_21 = "d",
  #   S_pov_prop_21_20 = "d",
  #   S_pov_prop_20_19 = "d",
  #   S_pov_prop_23_22_norm = "d",
  #   S_pov_prop_23_22 = "d",
  #   access_date = "D")
  # mpo <- loadInputs("mpo", group_by = c("Country"), as_of = as_of, format = "csv", col_types = col_types)

mpo <- read_csv(paste_path(inputs_archive_path, "mpo-alt.csv"), col_types = "cddddd")

return(mpo)
}

## MACROFIN / EFI Macro Financial Review Household Level Risk
mfr_collect <- function() {
  macrofin <- read.csv("hosted-data/efi-mfr/macrofin.csv")
  archiveInputs(macrofin, group_by = c("ISO3"))
}

macrofin_process <- function(as_of) {
  macrofin <- loadInputs("macrofin", group_by = c("ISO3"), 
    as_of = as_of, format = "csv", col_types = "cccccccccccDc")
  
  macrofin <- macrofin %>%
    # dplyr::select(Country = ISO3, Household.risks, Macroeconomic.risks, Monetary.and.financial.conditions, Risk.appetite) %>%
    # We are switching to using the 
    dplyr::select(Country = ISO3, Household.risks, Macro.Financial.Risk) %>%
    mutate(S_Household.risks_raw = Household.risks,
           S_Household.risks = case_when(
             Household.risks == "Low" ~ 0,
             Household.risks == "Medium" ~ 7,
             Household.risks == "High" ~ 10,
             TRUE ~ NA_real_),
          #  M_MFR_raw = paste0("Macro: ", Macroeconomic.risks,
          #                     "; Monetary and Financial: ", Monetary.and.financial.conditions,
          #                     "; Risk Appetite: ",  Risk.appetite),
           M_Macro_Financial_Risk_raw = Macro.Financial.Risk,
           M_Macro_Financial_Risk = case_when(
            # Macroeconomic.risks == "High" | Monetary.and.financial.conditions == "High" | Risk.appetite == "High" ~ 10,
            # Macroeconomic.risks == "Medium" | Monetary.and.financial.conditions == "Medium" | Risk.appetite == "Medium" ~ 7,
            # Macroeconomic.risks == "Low" | Monetary.and.financial.conditions == "Low" | Risk.appetite == "Low" ~ 0,
            Macro.Financial.Risk == "High" ~ 10,
            Macro.Financial.Risk == "Medium" ~ 7,
            Macro.Financial.Risk == "Low" ~ 0,
             TRUE ~ NA_real_)) %>%
             select(Country, starts_with("M_"), starts_with("S_"))
  return(macrofin)
}

#------------------------------—IMF FORECASTED UNEMPLOYMENT-----------------------------------------
imf_collect <- function() {
  most_recent <- read_most_recent("hosted-data/imf-unemployment", FUN = read_csv,
    as_of = Sys.Date(),
    col_types = "cccclcccccccccd",
    na = c("NA", "n/a", ""), 
    return_date = T)

  imf_unemployment <- most_recent$data
  file_date <- most_recent$date
  
  # imf_archive <- read_csv("output/inputs-archive/imf_unemployment.csv") %>%
  #   mutate(`2027` = NA, .after = `2026`)
  # write.csv(imf_archive, "output/inputs-archive/imf_unemployment.csv")
  
  archiveInputs(imf_unemployment, group_by = c("Country"), today = file_date)
}

imf_process <- function(as_of) {
  imf_unemployment  <- loadInputs("imf_unemployment", group_by = c("Country"), as_of = as_of, format = "csv")# , col_types = "ccccclccccccccccccdD") #Databricks has different columns
  # FIX
  
  imf_un <- imf_unemployment %>%
    select(c(Country, ISO3, "2020", "2021", "2022", `Subject Descriptor`)) %>%
    mutate_at(
      vars(starts_with("20")),
      ~ as.numeric(as.character(.))
    ) %>%
    # Warnings. Introduces NAs … because there are NAs.
    mutate(
      change_unemp_22 = `2022` - `2021`,
      change_unemp_21 = `2021` - `2020`,
      #  change_unemp_20 = `2020` - `2019`
    ) %>%
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
  imf_un <- normfuncpos(imf_un, 1, 0, "S_change_unemp_22")
  imf_un <- normfuncpos(imf_un, 1, 0, "S_change_unemp_21")
  # imf_un <- normfuncpos(imf_un, 3, 0, "S_change_unemp_20")
  
  # Max values for index
  imf_un <- imf_un %>%
    mutate(
      S_change_unemp_norm = rowMaxs(as.matrix(dplyr::select(.,
                                                            S_change_unemp_22_norm,
                                                            S_change_unemp_21_norm)),
                                    na.rm = T),
      S_change_unemp_norm = case_when(is.infinite(S_change_unemp_norm) ~ NA_real_,
                                      TRUE ~ S_change_unemp_norm)
    )
  
  return(imf_un)
}

#---------------------------------

eiu_security_process <- function(as_of) {
  eiu_data <- loadInputs("eiu", group_by = c("SERIES NAME", "MONTH"), 
    as_of = as_of, col_types = "ccDddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddD") %>%
        select(-access_date) %>%
        subset(MONTH <= as_of)

    series_names <- c("1. Armed conflict", "2. Terrorism", "3. Violent demonstrations",
        "5. Violent crime", "6. Organised crime",
        "7. Kidnapping/extortion", "8. Cyber security, likelihood of attacks")

    eiu_long <- eiu_data %>%
        select(-`SERIES CODE`) %>%
        subset(`SERIES NAME` %in% series_names) %>%
        pivot_longer(-c(`SERIES NAME`, MONTH), names_to = "Country", values_to = "Values") %>%
        rename(Series = `SERIES NAME`) %>%
        group_by(Country, Series)

    eiu_1_month <- eiu_long %>%
        slice_max(MONTH, n = 1) %>%
        select(Country, Series, one_month = Values)
    eiu_1_year <- eiu_long %>%
        slice_max(MONTH, n = 13) %>%
        slice_min(MONTH, n = 12) %>%
        summarize(one_year_mean = mean(Values))

    eiu_aggregate <- 
        # reduce(list(eiu_1_month, eiu_3_month, eiu_1_year, eiu_3_year),#, eiu_three_month),
        reduce(list(eiu_1_month, eiu_1_year),#, eiu_three_month),
            full_join, by = c("Country", "Series")) %>%
            ungroup() %>% group_by(Country) %>%
            summarize(across(contains("_"), ~ mean(.x))) %>%
        mutate(
            security_1_month = one_month,
            security_1_year = one_year_mean,
            security_change_12_1 = one_month - one_year_mean,
            .keep = "unused"
            ) %>%
        # full_join(sec_risk) %>% 
        arrange(desc(security_change_12_1))

    eiu_security_risk <- 
        normfuncpos(eiu_aggregate, 0.2, 0, "security_change_12_1") %>%
        # normfuncpos(quantile(eiu_aggregate$security, 0.95), quantile(eiu_aggregate$security, 0.05), "security") %>%
        # select(Country, security, security_norm, change_12_1, change_12_1_norm) %>%
        select(Country, security_1_month, security_change_12_1, security_change_12_1_norm) %>%
        mutate(security_change_12_1_norm = case_when(
            security_1_month <= 2 & security_change_12_1_norm > 7 ~ 7,
            T ~ security_change_12_1_norm)) %>% 
            arrange(desc(security_change_12_1_norm)) %>%
    mutate(Country = name2iso(Country)) %>%
    rename(eiu_security_change_12_1_norm = security_change_12_1_norm,
           eiu_security_change_12_1 = security_change_12_1) %>%
           add_dimension_prefix("Fr_")

    return(eiu_security_risk)
}

#### NATURAL HAZARDS

#------------------------------—GDACS-----------------------------------------
gdacs_collect <- function() {
  gdacs_url <- "https://www.gdacs.org/"
  gdacs_web <- read_html(gdacs_url) %>%
    html_node("#containerAlertList")

  alert_classes <- c(
    ".alert_EQ_Green", ".alert_EQ_PAST_Green", ".alert_EQ_Orange", ".alert_EQ_PAST_Orange", ".alert_EQ_Red", ".alert_EQ_PAST_Red",
    ".alert_TC_Green", ".alert_TC_PAST_Green", ".alert_TC_Orange", ".alert_TC_PAST_Orange", ".alert_TC_Red", ".alert_TC_PAST_Red",
    ".alert_FL_Green", ".alert_FL_PAST_Green", ".alert_FL_Orange", ".alert_FL_PAST_Orange", ".alert_FL_Red", ".alert_FL_PAST_Red",
    ".alert_VO_Green", ".alert_VO_PAST_Green", ".alert_VO_Orange", ".alert_VO_PAST_Orange", ".alert_VO_Red", ".alert_VO_PAST_Red",
    ".alert_DR_Green", ".alert_DR_PAST_Green", ".alert_DR_Orange", ".alert_DR_PAST_Orange", ".alert_DR_Red", ".alert_DR_PAST_Red"
  )
  
  # Function to create database with hazard specific information
  gdacs <- lapply(alert_classes, function(i) {
    names <- gdacs_web %>%
      html_nodes(i) %>%
      html_nodes(".alert_item_name, .alert_item_name_past") %>%
      html_text()
    
    mag <- gdacs_web %>%
      html_nodes(i) %>%
      html_nodes(".magnitude, .magnitude_past") %>%
      html_text() %>%
      str_trim()
    
    date <- gdacs_web %>%
      html_nodes(i) %>%
      html_nodes(".alert_date, .alert_date_past") %>%
      html_text() %>%
      str_trim()
    date <- gsub(c("-  "), "", date)
    date <- gsub(c("\r\n       "), "", date)

    color <- str_extract(tolower(i), "red|orange|green")
    hazard = str_extract(i, "EQ|TC|FL|VO|DR") %>%
      str_replace_all(c(
        "EQ" = "earthquake",
        "TC" = "cyclone",
        "FL" = "flood",
        "VO" = "volcano",
        "DR" = "drought"))
    status <- if (hazard != "drought" & str_detect(tolower(i), "past")) "past" else "active"
    len <- length(names)
    
    if (hazard == "drought") date <- as.character(Sys.Date() - 7 * as.numeric(str_extract(date, "\\d+")))

    return(cbind.data.frame(names, mag, date, 
      status = rep(status, len),
      haz = rep(color, len),
      hazard = rep(hazard, len)))
  }) %>% 
    bind_rows() %>% #separate_rows(names, sep = "-|, ") %>% 
    mutate(names = trimws(names),
      country = name2iso(names, multiple_matches = T)) %>%
      separate_rows(country, sep = ", ")

# Add countries for cyclones
cyclone_urls <- gdacs_web %>% html_node("#gdacs_eventtype_TC") %>%
  html_nodes(".alert_item") %>%
  html_nodes("a") %>%
  html_attr("href")

cyclone_countries <- cyclone_urls %>%
  lapply(function(url) {
    table <- read_html(url) %>%
      html_node(".summary") %>%
      html_table()
    info <- table$X2 %>% setNames(table$X1)
    name <- info["Name"]
    countries <- info["Exposed countries"]
  return(c(name, countries))
  }) %>%
  bind_rows()

gdacs[match(cyclone_countries$Name, gdacs$names),"country"] <- cyclone_countries$`Exposed countries` %>% name2iso()

  # Clean up irregular characters
  gdacs <- mutate(gdacs,
                  access_date = Sys.Date(),
                  # mag = na_if(mag, "") %>% str_replace_all("//s", " "),
                  mag = na_if(mag, "") %>% {gsub(c("\r\n\\s*"), "", .)},
                  mag = mag %>% str_replace_all("\\s", " "),
                  # names = trimws(names) %>% str_replace_all("//s", " "),
                  names = trimws(names) %>% {gsub(c("\r\n\\s*"), "", .)} %>% str_replace_all("//s", " "),
                  date = date %>% str_replace_all("\\s", " ")
                  # , current = TRUE
  )
  
  # Add all currently online events to gdacs file unless most recent access_date and
  # current data are fully identical
  gdacs_prev <- read_csv(paste_path(inputs_archive_path, "gdacs.csv"), col_types = "ccccccDc")
  gdacs_prev_recent <- filter(gdacs_prev, access_date == max(access_date)) %>% distinct()
  if(!identical(select(gdacs_prev_recent, -access_date), select(gdacs, -access_date))) {
    gdacs <- bind_rows(gdacs_prev, gdacs) %>% distinct()
    write.csv(gdacs, paste_path(inputs_archive_path, "gdacs.csv"), row.names = F)
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

gdacs_process <- function(as_of) {
  # if (format == "csv") {
    # Read in CSV
    gdacs <- read_csv(paste_path(inputs_archive_path, "gdacs.csv"), col_types = "ccccccDc") %>%
      mutate(access_date = as.Date(access_date)) %>%
      filter(access_date <= as_of) %>%
      filter(access_date == max(access_date))
  # }
  # if (format == "spark") {
  #   # Read from Spark DataFrame
  # }
  
  # gdaclist <- gdacs %>% 
  #   mutate(
  #     parsed_date = case_when(
  #       str_detect(date, "\\d{4}-\\d{2}-\\d{2}") ~ date,
  #       str_detect(date, "\\d{2} [a-zA-Z]{3} \\d{2}") ~ paste(parse_date_time(date, orders = c("dm Y"))),
  #       T ~ paste(parse_date_time(date, orders = c("dm H:M"))))) %>%

  # This whole mess is because GDACS doesn't always include a year in the date
  # So in January events from December are given the current year instead of last year's year
  gdaclist <- gdacs %>% mutate(
    date_original = date,
    date = case_when(
      str_detect(date, "\\d{2} [:alpha:]{3} \\d{4}") ~ 
        paste(parse_date_time(paste(date), orders = "d b Y")),
      str_detect(date, "\\d{4}-\\d{2}-\\d{2}") ~ 
        paste(parse_date_time(paste(date), orders = "Y m d")),
      str_detect(date, "\\d{2} [:alpha:]{3}\\s*\\d{2}:\\d{2}") ~
        paste(parse_date_time(paste(date), orders = "d b HM")),
      T ~ NA_character_),
    date = case_when(
      year(date) != 0 ~ date,
      year(date) == 0 & month(date) <= month(access_date) ~ 
        paste(year(access_date), month(date), day(date)),
      year(date) == 0 & month(date) > month(access_date) ~ 
        paste(year(access_date) - 1, month(date), day(date),
      T ~ NA_character_)),
    date = parse_date_time(date, orders = "Y m d") %>% as.Date()) %>%
    # Suppress warnings because the above is *guaranteed* to give warnings because
    # case_when does not prevent functions from running on full vectors, and so 
    # multipe date formats are being given to each parse_date_time call
    # Instead, create own warning
    suppressWarnings()
    if (any(is.na(gdaclist$date))) {
      failed_dates <- filter(gdaclist, is.na(date))
      warning(paste0("Date(s) failed to parse in GDAC:\n", paste0(capture.output(failed_dates), collapse = "\n")))
    }

  # Removed because I now handle event names with multiple country names with names2iso()
  # # Remove duplicate countries for drought
  # # Once I started processing country names in gdacs_collect() I also separated droughts in gdacs_collect()
  # # I don't need to do this if country values are not NA. Some country values may still be NA if they don't match a country
  # if (!any(!is.na(gdaclist$country))) {
  #   # Select drought events with a dash in the name
  #   add <- gdaclist[which(gdaclist$hazard == "drought" & grepl("-", gdaclist$names)), ]
  #   gdaclist[which(gdaclist$hazard == "drought" & grepl("-", gdaclist$names)), ]$names <- sub("-.*", "", gdaclist[which(gdaclist$hazard == "drought" & grepl("-", gdaclist$names)), ]$names)
  #   add$names <- sub(".*-", "", add$names)
  #   gdaclist <- rbind(gdaclist, add)
  # }
  
  # Drought orange
  # UPDATE? Does this need to now be 2021?
  # All droughts on GDAC are current ... 
  # Can I just set all droughts to active? 
  # gdaclist$status <- ifelse(gdaclist$hazard == "drought" & gdaclist$date == "2020", "active", gdaclist$status)
  # gdaclist$status <- ifelse(gdaclist$hazard == "drought" & gdaclist$date == "2021", "active", gdaclist$status)
  # gdaclist$status <- ifelse(gdaclist$hazard == "drought" & gdaclist$date == "2022", "active", gdaclist$status)
  
  # # Country names
  # if (!any(!is.na(gdaclist$country))) {
  #   gdaclist$country <- suppressWarnings(countrycode(gdaclist$names, origin = "country.name", destination = "iso3c"))
  #   gdaclist$namesfull <- suppressWarnings(countrycode(gdaclist$names, origin = "country.name", destination = "iso3c", nomatch = NULL))
  # }

  # Create subset
  gdacs <- gdaclist %>%
    dplyr::select(Country = country,
                  NH_GDAC_Date = date, 
                  NH_GDAC_Hazard_Status = status, 
                  NH_GDAC_Hazard_Magnitude = mag, 
                  NH_GDAC_Hazard_Severity = haz,
                  NH_GDAC_Hazard_Type = hazard) %>%
    separate_rows(Country, sep = ", ")
    
  gdacs <- gdacs %>%
    mutate(NH_GDAC_Hazard_Score_Norm = case_when(
      NH_GDAC_Hazard_Status == "active" & NH_GDAC_Hazard_Severity == "red" ~ 10,
      NH_GDAC_Hazard_Status == "active" & NH_GDAC_Hazard_Severity == "orange" ~ 10,
      TRUE ~ 0
    ),
    NH_GDAC_Hazard_Score = paste(NH_GDAC_Date, NH_GDAC_Hazard_Type, NH_GDAC_Hazard_Severity, NH_GDAC_Hazard_Magnitude, sep = " - ")
    ) %>%
    drop_na(Country)
  # Limit one entry per country
  gdacs_summary <- gdacs %>% group_by(Country) %>%
    arrange(desc(NH_GDAC_Hazard_Score_Norm)) %>%
    summarize(
      NH_GDAC_Hazard_Score_Norm = max(NH_GDAC_Hazard_Score_Norm, na.rm = T), 
      NH_GDAC_Hazard_Score = paste(NH_GDAC_Hazard_Score, collapse = "; "))

  return(gdacs_summary)
}

#----------------------—INFORM Natural Hazard and Exposure rating--------------------------
inform_risk_collect <- function() {
  # # Delete after it run on Github once
  # read_csv("output/inputs-archive/inform_risk.csv", na = "x") %>%
  # write.csv("output/inputs-archive/inform_risk.csv", row.names = F)

  most_recent <- read_most_recent("hosted-data/inform-risk", FUN = read_csv, as_of = Sys.Date(), col_types = "cdcdddddddddddddddddddddddddddddddddddcd", return_date = T)
  file_date <- most_recent[[2]]

  inform_risk <- most_recent$data %>%
    mutate(`Countries in HVC` = str_replace(`Countries in HVC`, "^$", NA_character_))

  archiveInputs(inform_risk, today = file_date, group_by = c("Country"))
}

inform_nathaz_process <- function(as_of) {
  inform_risk <- loadInputs("inform_risk", group_by = c("Country"),
    as_of = as_of, format = "csv", col_types = "cddddddddddddddddddddddddddddddddcDddddcd") %>%
    distinct()
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
iri_collect <- function(as_of = Sys.Date()) {
  
  # library(raster)
  # library(rgdal)
  # library(sf)
  # # library(dplyr)
  # # library(RColorBrewer)
  # library(exactextractr)
  
  # is_diff_month <- function(s) {
  #   m <- str_replace(s, ".*20..-(..)-...tif.*", "\\1")
  #   return(m != format(Sys.Date(), "%m"))
  # }
  # expect_new <- 
  #   read_most_recent(paste_path(inputs_archive_path, "iri/forecast"), FUN = is_diff_month, as_of = as_of) &
  #   as.numeric(format(as_of, "%d")) >= 15
  
  # if (expect_new) {
    
  iri_urls <- list(c('forecast', 'https://iridl.ldeo.columbia.edu/SOURCES/.IRI/.FD/.NMME_Seasonal_Forecast/.Precipitation_ELR/Y/-85/85/RANGE/X/-180/180/RANGEEDGES/a:/.dominant/:a:/.target_date/:a/X/Y/fig-/colors/plotlabel/black/thin/coasts_gaz/thin/countries_gaz/-fig/(L)cvn/1.0/plotvalue/(F)cvn/last/plotvalue/(antialias)cvn/true/psdef/(framelabel)cvn/(%=[target_date]%20IRI%20Seasonal%20Precipitation%20Forecast%20issued%20%=[F])psdef/(dominant)cvn/-100.0/100.0/plotrange/(plotaxislength)cvn/590/psdef/(plotborderbottom)cvn/40/psdef/(plotbordertop)cvn/40/psdef/selecteddata/band3spatialgrids/data.tiff'),
                    c('continuity', 'https://iridl.ldeo.columbia.edu/SOURCES/.IRI/.MD/.IFRC/.IRI/.Seasonal_Forecast/a:/.pic3mo_same/:a:/.forecasttime/L/first/VALUE/:a:/.observationtime/:a/X/Y/fig-/colors/plotlabel/plotlabel/black/thin/countries_gaz/-fig/F/last/plotvalue/X/-180/180/plotrange/Y/-66.25/76.25/plotrange/(antialias)cvn/true/psdef/(plotaxislength)cvn/550/psdef/(XOVY)cvn/null/psdef/(framelabel)cvn/(%=[forecasttime]%20Forecast%20Precipitation%20Tendency%20same%20as%20Observed%20%=[observationtime],%20issued%20%=[F])psdef/(plotbordertop)cvn/60/psdef/(plotborderbottom)cvn/40/psdef/selecteddata/band3spatialgrids/data.tiff'))
  
  curl_iri <- function(x) {
    iri_date <- if (format(as_of, "%d") >= 15) paste0('?F=', format(as_of, "%b%%20%Y")) else paste0('?F=', format(as_of - 30, "%b%%20%Y"))
    dest_file <- x[[1]]
    iri_curl <- paste0('curl -g -k -b "', paste_path(mounted_path, '.access/iri-access.txt'), '" "', x[[2]], iri_date, '" > tmp-', dest_file, '.tiff')
    system(iri_curl)
    iri_exists <- !grepl(404, suppressWarnings(readLines(paste0('tmp-', dest_file, ".tiff"), 1)))
    return(T)
  }
  
  lapply(iri_urls, curl_iri)
  
  # It would be faster to just place and name the file correctly the first time, and then remove it if duplicate
  continuity_new <- raster("tmp-continuity.tiff")
  continuity_old <- read_most_recent(paste_path(inputs_archive_path, "iri/continuity"), FUN = raster, as_of = as_of)
  if(!identical(values(continuity_new), values(continuity_old))) {
    file.rename("tmp-continuity.tiff", paste0(inputs_archive_path, "iri/continuity/iri-continuity-", as_of, ".tiff"))
  }
  file.remove("tmp-continuity.tiff")
  forecast_new <- raster("tmp-forecast.tiff")
  forecast_old <- read_most_recent(paste_path(inputs_archive_path, "iri/forecast"), FUN = raster, as_of = as_of)
  if(!identical(values(forecast_new), values(forecast_old))) {
    file.rename("tmp-forecast.tiff", paste0(inputs_archive_path, "iri/forecast/iri-forecast-", as_of, ".tiff"))
  }
  file.remove("tmp-forecast.tiff")
  # }
}

iri_process <- function(
  sp_path = read_most_recent(paste_path(inputs_archive_path, "iri/forecast"), FUN = paste, as_of = as_of) ,
  continuity_path = read_most_recent(paste_path(inputs_archive_path, "iri/continuity"), FUN = paste, as_of = as_of) ,
  include_area = F,
  drop_geometry = F,
  country_list = F,
  probability_threshold = 50,
  full_output = F,
  pop_threshold = 25,
  agri_threshold = 0.35,
  as_of,
  format) {
  
  classify_country_size <- function(areas) {
    classes <- sapply(areas, function(area) {
      if (area <= quantile(areas, 0.2)) {
        class <- 1
      } else {
        if (area <= quantile(areas, 0.4)) {
          class <- 2
        } else {
          if (area <= quantile(areas, 0.6)) {
            class <- 3
          } else {
            if (area <= quantile(areas, 0.8)) {
              class <- 4
            } else {
              if (area <= quantile(areas, 1)) {
                class <- 5
              } else {
                class <- NA
              }}}}}
      return(class)
    })
    return(classes)
  }
  
  proportion_thresholds <- data.frame(class = 1:5, proportion_threshold = c(1, 0.666, 0.5, 0.333, 0.333))
  
  s <- stack(list(
    # Selected precipitation forecasts issued
    sp = crop(raster(sp_path), extent(-180.5, 180.5, -65.5, 75.5)),
    # Continuity wet/dry condition data
    continuity = crop(raster(continuity_path), extent(-180.5, 180.5, -65.5, 75.5)),
    # Population density (gwp 2020 resampled to same grid as forecast)
    pop_density = raster(paste_path(inputs_archive_path, "iri/pop-density.tiff")),
    # Proportino crop+pasture, resampled to same grid as forecast
    agri_density = raster(paste_path(inputs_archive_path, "iri/crop-pasture-density.tiff"))))
  
  countries <- st_read(paste_path(inputs_archive_path, "world-borders/TM_WORLD_BORDERS-0.3.shp")) %>%
    dplyr::select(-fips, -iso2, -un, -area, -pop2005, -lon, -lat, -Pixelcount)
  st_crs(countries) <- st_crs(s)
  
  # Filter sp layer for population density and crop + pasture density
  s$sp[(s$pop_density < 25 | is.na(s$pop_density)) & (s$agri_density < 0.35 | is.na(s$agri_density))] <- NA
  # IRI does not do this, but not doing so means a country can have more dry pixels than total pixels
  s$continuity[(s$pop_density < 25 | is.na(s$pop_density)) & (s$agri_density < 0.35 | is.na(s$agri_density))] <- NA
  
  s$wet <- ifelse(values(s$sp) > probability_threshold, 1, NA)
  # Continuity of 2 means 40–505 probability of below normal precipation and a dry past 3 months
  s$dry <- ifelse(values(s$sp) < -probability_threshold | values(s$continuity) == 2, 1, NA)
  
  country_extract <- function(x, include_area) {
    output <- exact_extract(
      x = x,
      y = countries,
      include_area = include_area) %>%
      sapply(function(x) {
        x <- as_tibble(x) %>%
          tidyr::drop_na()
        if(include_area) {
          x <- mutate(x, area_scaled = area / 1e+10)
          return(sum(x$area_scaled))
        }
        return(nrow(x))
      })
    return(output)
  }
  
  if (country_list) {
    countries <- subset(countries, iso3 %in% countrylist$Country)
  }
  
  if (include_area) {
    # Separate out countries with 0 pixels because you can't `include_area` for features with no pixels
    countries$pixels <- country_extract(x = s$sp, include_area = F)
    zeros <- subset(countries, pixels == 0) %>% 
      mutate(wet = NA, dry = NA)
    countries <- subset(countries, pixels > 0)
  }
  
  countries$pixels <- country_extract(x = s$sp, include_area = include_area)
  countries$wet <- country_extract(x = s$wet, include_area = include_area)
  countries$dry <- country_extract(x = s$dry, include_area = include_area)
  
  if (include_area) {
    countries <- rbind(zeros, countries)
  }

  if(drop_geometry) {
    countries <- countries %>% st_drop_geometry()
  }
  
  countries <- mutate(countries,
                      size_class = classify_country_size(pixels))
  countries <- left_join(countries, proportion_thresholds, by = c("size_class" = "class"))
  
  countries <- mutate(countries,
                      anomalous = wet + dry,
                      proportion_wet = wet/pixels,
                      proportion_dry = dry/pixels,
                      proportion_anomalous = anomalous/pixels,
                      wet_flag = case_when(
                        proportion_wet >= proportion_threshold ~ T,
                        is.na(proportion_wet) ~ NA,
                        TRUE ~ F),
                      dry_flag = case_when(
                        proportion_dry >= proportion_threshold ~ T,
                        is.na(proportion_dry) ~ NA,
                        TRUE ~ F),
                      anomalous_flag = case_when(
                        proportion_anomalous >= proportion_threshold ~ T,
                        is.na(proportion_anomalous) ~ NA,        
                        TRUE ~ F),
                      flag = wet_flag | dry_flag) %>% 
    rename(Country = iso3)
  
  if(full_output) {
    return(countries)
  }
  
  output <- countries %>%
    mutate(
      NH_seasonal_risk_norm = case_when(
        anomalous_flag ~ 10,
        is.na(proportion_anomalous) ~ NA_real_,
        T ~ 0),
      NH_proportion_dry = proportion_dry,
      NH_proportion_wet = proportion_wet,
      NH_seasonal_proportion_anomalous = proportion_anomalous) %>%
    dplyr::select(Country, contains("NH"))
  
  return(output)
}

# # For temporarily generating file locally,
# iri <- iri_process(drop_geometry = T, as_of = as_of)
# write.csv(iri, "~/Documents/world-bank/crm/compoundriskdata/Indicator_dataset/iri-precipitation-temp.csv")

iri_process_temp <- function() {
  # Not currently used but helpful if IRI script breaks
  iri <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/iri-precipitation-temp.csv"), col_types = cols())) 
  return(iri)
}

#-------------------------------------—Locust outbreaks----------------------------------------------
# List of countries and risk factors associated with locusts (FAO), see:http://www.fao.org/ag/locusts/en/info/info/index.html
locust_collect <- function() {
  locust_risk <- read_csv("hosted-data/fao-locust/fao-locust.csv", col_types = cols())
  locust_risk <- locust_risk %>%
    dplyr::select(Country, NH_locust_norm)
  
  fao_locust <- locust_risk
  
  archiveInputs(fao_locust, group_by = c("Country"))
}

locust_process <- function(as_of) {
  fao_locust <- loadInputs("fao_locust", group_by = c("Country"), 
    as_of = as_of, format = "csv", col_types = "cdD")
  return(fao_locust)
}

#---------------------------------—Natural Hazard ACAPS---------------------------------
# acaps_nh <- acapssheet[,c("Country", "NH_natural_acaps")]

####    FRAGILITY

#-------------------------—FCS---------------------------------------------

fcs_collect <- function() {
  most_recent <- read_most_recent("hosted-data/fcs", FUN = read_csv, 
    as_of = Sys.Date(), return_date = T, col_types = "c")
  file_date <- most_recent[[2]]

  fcs <- most_recent$data %>%
    select(-`IDA-status`)

  # archiveInputs(fcs, group_by = "Country", col_types = "cccc")


  # fcv <- suppressMessages(read_csv(paste0(github, "Indicator_dataset/Country_classification.csv"))) %>%
  #   dplyr::select(-`IDA-status`) %>%
  #   mutate(FCV_status = tolower(FCV_status)) %>%
  #   mutate(
  #     FCS_normalised = case_when(
  #       FCV_status == tolower("High-Intensity conflict") ~ 10,
  #       FCV_status == tolower("Medium-Intensity conflict") ~ 10,
  #       FCV_status == tolower("High-Institutional and Social Fragility") ~ 10,
  #       TRUE ~ 0
  #     )
  #   )
  
  # fcs <- fcv
  archiveInputs(fcs, group_by = c("Country"), col_types = "cccc", today = file_date)
}

fcs_process <- function(as_of) {
  fcs <- loadInputs("fcs", group_by = c("Country"), col_types = "cccc", as_of = as_of, format = "csv") %>%
    mutate(FCS_normalised = case_when(
      !is.na(FCV_status) ~ 10,
      T ~ 0))

  return(fcs)
}

#-------------------------—FSI---------------------------------------------

fsi_collect <- function() {
    most_recent <- read_most_recent('hosted-data/fsi', FUN = read_xlsx, as_of = Sys.Date(), return_date = T)
    fsi <- most_recent$data
    file_date <- most_recent$date
    
    archiveInputs(fsi, group_by = "Country", col_types = "cTcddddddddddddd", today = file_date)
}

fsi_process <- function(as_of) {
  fsi <- loadInputs("fsi", group_by = "Country", as_of = as_of, col_types = "cTcddddddddddddd") %>%
        mutate(Country = name2iso(Country), FSI = Total, .keep = "none") %>%
        normfuncpos(quantile(.$FSI, .98), quantile(.$FSI, .4), "FSI")
  return(fsi)
}

#-----------------------------—IDPs--------------------------------------------------------
idp_collect <- function() {
  un_idp <- read_csv("hosted-data/unhcr-idp/unhcr-idp.csv",
                                        col_types = cols(
                                          `IDPs of concern to UNHCR` = col_number(),
                                          `Refugees under UNHCR's mandate` = col_number(),
                                          Year = col_number()),
                                        skip = 14) %>%
              select(Year,
                      `Country of origin (ISO)`,
                      `Country of asylum (ISO)`,
                      `Refugees under UNHCR mandate` = `Refugees under UNHCR's mandate`,
                      `IDPs of concern to UNHCR`)
  
  update_date <- readLines("hosted-data/unhcr-idp/unhcr-idp.csv", 3)[3] %>%
    str_extract("\\d+ [[:alpha:]]+ 20\\d\\d") %>%
    as.Date(format = "%d %B %Y")
  
  archiveInputs(un_idp, group_by = c("Country of origin (ISO)", "Country of asylum (ISO)", "Year"), today = update_date)
}

un_idp_process <- function(as_of) {
  un_idp <- loadInputs("un_idp", group_by = c("Country of origin (ISO)", "Country of asylum (ISO)", "Year"), 
    as_of = as_of, format = "csv", col_types = "dccccddD")
  recent_year <- max(un_idp$Year)
  
  # Calculate metrics
  idp <- un_idp %>%
    subset(Year > recent_year - 5) %>%
    group_by(`Country of origin (ISO)`, Year) %>%
    summarise(
      refugees = sum(`Refugees under UNHCR mandate`, na.rm = T),
      idps = sum(`IDPs of concern to UNHCR`, na.rm = T)
    ) %>%
    group_by(`Country of origin (ISO)`) %>%
    mutate(
      # We aren't using refugee data, only IDPs
      # sd_refugees = sd(refugees, na.rm = T),
      # mean_refugees = mean(refugees, na.rm = T),
      # z_refugees = (refugees - mean_refugees) / sd(refugees),
      # refugees_fragile = case_when(
      #   z_refugees > 1 ~ "Fragile",
      #   z_refugees < 1 ~ "Not Fragile",
      #   z_refugees == NaN ~ "Not Fragile",
      #   TRUE ~ NA_character_
      # ),
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
    filter(Year == recent_year) %>%
    # dplyr::select(`Country of origin (ISO)`, refugees, z_refugees, refugees_fragile, idps, z_idps, idps_fragile)
    dplyr::select(`Country of origin (ISO)`, idps, z_idps, idps_fragile)
  
  
  # Normalise scores
  # idp <- normfuncpos(idp, 1, 0, "z_refugees")
  idp <- normfuncpos(idp, 1, 0, "z_idps")
  
  # Correct for countries with 0
  idp <- idp %>%
    mutate(
      # z_refugees_norm = case_when(
      #   z_refugees == NaN ~ 0,
      #   TRUE ~ z_refugees_norm),
      z_idps_norm = case_when(
        z_idps == NaN ~ 0,
        TRUE ~ z_idps_norm)) %>%
    # dplyr::select(-`Country of origin (ISO)`) %>% 
    rename(
      Country = `Country of origin (ISO)`,
      Displaced_UNHCR_Normalised = z_idps_norm)
  
  return(idp)
}

#-------------------------—ACLED data---------------------------------------------
acled_collect <- function() {
  # Select date as three years plus two month (date to retrieve ACLED data)
  three_year <- as.Date(as.yearmon(Sys.Date() - 45) - 3.2)
  
  # Get ACLED API URL
  acled_url <- paste0("https://api.acleddata.com/acled/read/?key=buJ7jaXjo71EBBB!!PmJ&email=bnotkin@worldbank.org&event_date=",
                      three_year,
                      "&event_date_where=>&fields=event_id_cnty|iso|fatalities|event_type|event_date&limit=0")
  
  # Retrieve information
  acled_data <- fromJSON(acled_url)
  
  acled <- acled_data$data %>%
    mutate(iso = as.numeric(iso),
        iso3 = countrycode(iso, origin = "iso3n", destination = "iso3c"),
        iso3b = substr(event_id_cnty, 1, 3),
        iso3 = case_when(is.na(iso3) ~ iso3b, T ~ iso3),
        fatalities = as.numeric(fatalities),
        event_date = as.Date(event_date)) %>%
    select(-iso3b) %>%
    subset(fatalities > 0)
  
  # # DELETE for first time only
  # Actually no:
  # ACLED includes historical data, it takes forever to archive, and I don't
  # understand why the dataset differs each day, so for now I'm just writing 
  # it fresh each time (like OWID COVID)
  acled <- mutate(acled, access_date = Sys.Date())
  write.csv(acled, paste_path(inputs_archive_path, "acled.csv"), row.names = F)
  
  # # If I want to reduce file size, zipping takes ~10 seconds (unzipping: <1s)
  # # and reduces size from 40 MB to 4 MB
  # unzip("output/inputs-archive/acled.zip", exdir = "output/inputs-archive", junkpaths = T)
  # archiveInputs(acled, group_by = "event_id_cnty", col_types = "cddDcD")
  # zip("output/inputs-archive/acled.zip", "output/inputs-archive/acled.R") 
  # file.remove("output/inputs-archive/acled.R")
}

acled_process <- function(as_of) {
  # Because we have no ACLED data from the past few months, and because ACLED data is also historic,
  # use June 2022 as access_date for all earlier retroactive runs
  # effective_access_date <- if (as_of < as.Date("2022-06-06")) as.Date("2022-06-06") else as_of
  
  # unzip("output/inputs-archive/acled.zip", exdir = "output/inputs-archive", junkpaths = T)
  # acled <- loadInputs("acled", group_by = "event_id_cnty", as_of = effective_access_date, col_types = "cddDc") #158274
  # file.remove("output/inputs-archive/acled.R")

  acled <- read_csv(paste_path(inputs_archive_path, "acled.csv"), col_types = "cddcDcD")

  # Select date as three years plus two month (date to retrieve ACLED data)
  three_year <- as.yearmon(as_of - 45) - 3.2
  
  # Progress conflict data
  acled <- acled %>%
    mutate(
      #   fatalities = as.numeric(as.character(fatalities)),
      date = as.Date(event_date),
      month_yr = as.yearmon(date)
    ) %>%
    filter(month_yr >= three_year) %>%
    filter(event_type != "Strategic developments") %>%
    # Remove dates for the latest month (or month that falls under the prior 6 weeks)
    # Is there a way to still acknowledge countries with high fatalities in past 6 weeks?
    filter(month_yr <= as.yearmon(as_of - 45)) %>% 
    group_by(iso3, month_yr) %>%
    summarise(fatal_month = sum(fatalities, na.rm = T),
              fatal_month_log = log(fatal_month + 1),
              .groups = "drop_last") %>%
    mutate(fatal_3_month = fatal_month + lag(fatal_month) + lag(fatal_month, 2),
           fatal_3_month_log = log(fatal_3_month + 1)) %>%
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
      Country = iso3,
      fatal_z_norm = case_when(
        fatal_3_month_log == 0 ~ 0,
        (fatal_3_month_log <= log(25 + 1)) ~ 0,
        TRUE ~ fatal_z_norm
      )
    ) %>%
    ungroup() %>%
    dplyr::select(-iso3) %>% 
    rename(BRD_Normalised = fatal_z_norm) %>%
    relocate(Country, .before = 1)
  return(acled)
}

acled_events_process <- function(as_of) {
    # Compares the average weekly event count from the past month of ACLED data to
    # average weekly count from the past year

    # Normalize to high 0.25 and low 0, with a cap at 7 when monthly count is less than 5

    # Data is updated on Monday or Tuesday, current to the most recent Friday
    # For Sunday thru Tuesday, need to subtract an additional week
    friday <- if (wday(as_of) > 3) {
        as_of - wday(as_of) - 1
    } else {
        as_of - wday(as_of) - 8
    }

    acled <- read_csv(paste_path(inputs_archive_path, "acled.csv"), col_types = "cddcDcD") %>%
        subset(event_date > (friday - 360) & event_date <= friday)

    acled_year <- acled %>%
        # subset(event_type != "Violence against civilians") %>%
        group_by(iso3) %>%
        summarize(events_month_avg = n() / 12)

    acled_month <- acled %>%
        subset(event_date >= friday - 30) %>%
      # subset(event_type != "Violence against civilians") %>%
        group_by(iso3) %>%
        summarize(events_1_month = n())

    acled_conflict_change <- full_join(acled_year, acled_month, by = "iso3") %>%
        mutate(events_1_month = case_when(is.na(events_1_month) ~ as.integer(0), T ~ events_1_month)) %>%
        mutate(acled_events_increase = (events_1_month - events_month_avg) / events_month_avg) %>%
        normfuncpos(0.25, 0, "acled_events_increase") %>%
        mutate(acled_events_increase_norm = case_when(
                acled_events_increase_norm > 7 & events_1_month <= 5 ~ 7,
                T ~ acled_events_increase_norm)) %>%
        mutate(Country = iso3,
              acled_events_increase_pct = 100*acled_events_increase,
              .keep = "unused")

    return(acled_conflict_change)
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
        reign_data <- read_csv(paste0("https://raw.githubusercontent.com/OEFDataScience/REIGN.github.io/gh-pages/data_sets/REIGN_", year, "_", month, ".csv"),
                                                col_types = cols())
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
reign_process <- function(as_of) {
  reign <- loadInputs("reign", group_by = c("country", "leader", "year", "month"),
    as_of = as_of,
    format = "csv",
    col_types = "dccdddddddcddddddddddddddddddddddddddddddD")
  
  reign_start <- reign %>%
    filter(year == max(year, na.rm= T)) %>%
    group_by(country) %>%
    slice(which.max(month)) %>%
    dplyr::select(Country = country, month, pt_suc, pt_attempt, delayed, irreg_lead_ant, anticipation) %>%
    mutate(country = name2iso(Country))
  
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

#--------------------------GIC Global Instances of Coups-----------------------
gic_collect <- function() {
  gic <- read_tsv("http://www.uky.edu/~clthyn2/coup_data/powell_thyne_coups_final.txt",
                  col_types = "cdddddddc") %>%
    subset(year > 2020)
  
  version_date <- gic$version[1] %>%
    str_replace_all(c("\\." = "-", "V" = "")) %>%
    as.Date()
  
  # The May 2022 update started including `ccode_gw` and `ccode_polity` columns; the latter
  # seems to relate to "correlates of war"
  # https://correlatesofwar.org/data-sets/downloadable-files/cow-country-codes/cow-country-codes/view
  # Run the below one time on Databricks
  # prev <- suppressMessages(read_csv("output/inputs-archive/gic.csv")) %>%
  #   mutate(access_date = as.Date(access_date)) %>%
  #   mutate(ccode_gw = NA_real_, ccode_polity = NA_real_, .after = country)
  # write.csv(prev, "output/inputs-archive/gic.csv", row.names = F)
  
  archiveInputs(gic, group_by = NULL, today = version_date)
}

gic_process <- function(as_of) {
  # coups_raw <- loadInputs("gic", group_by = NULL, as_of = as_of, format = "csv")
  # Not using loadInputs because I want to be able to include results from before an "access_date"
  # (This is less because I may have retreived the dataset much later than the version, but because 
  # the event occured before the version date; that though is true of literally every event in this monitor,
  # and, in fact, true of all datasets that record events.
  coups_raw <- read_csv(paste_path(inputs_archive_path, "gic.csv"), col_types = "cdddddddc")
  
  coups <- coups_raw %>%
    mutate(
      date = as.Date(paste(year, month, day, sep = "-")),
      version = as.Date(str_replace_all(version, c("\\." = "-", "V" = "")))) %>%
    subset(date <= as_of & date >= as_of - 365) %>%
    rename(Countryname = country) %>%
    mutate(Country = name2iso(Countryname)) %>% 
    group_by(Country, date) %>%
    slice_max(version) %>%
    select(Country, Countryname, date, coup)
  
  coups_recent <- coups %>%
    # subset(date >= as_of - 365) %>%
    group_by(Country) %>%
    mutate(coup_text = case_when(
      coup == 1 ~ "failed",
      coup == 2 ~ "successful"
    )) %>%
    summarize(
      coup_text = paste(paste(date, coup_text), collapse = "; "),
      coup = max(coup))
  
  return(coups_recent)
}

#--------------------------IFES Inter. Foundation for Electoral Systems -------
ifes_collect <- function() {
  # string <- read_html("https://www.electionguide.org/ajax/election/?sEcho=1&iColumns=5&sColumns=&iDisplayStart=0&iDisplayLength=2000&iSortCol_0=3&sSortDir_0=desc&iSortingCols=1") %>%
  #   html_text()
  
  # ifes <- string %>%     
  #   str_replace_all(c(
  #     ',\\s\\["https.*?",' = "\n",
  #     '\\[' = '',
  #     '\\]' = '',
  #     '\\}' = ''
  #   )) %>%
  #   str_replace(".*?\\n", "") %>%
  #   str_replace_all(', "', ',"') %>%
  #   str_replace_all('\\n "', '\n"') %>%
  #   str_replace(',"iTotal.*', '') %>%
  #   read_csv(
  #     col_names = c(
  #       "Countryname",
  #       "country_slug",
  #       "office",
  #       "election_slug",
  #       "date",
  #       "status",
  #       "election_id",
  #       "text",
  #       "election_type",
  #       "country_id"),
  #     col_types = cols_only(
  #       Countryname = 'c',
  #       country_slug = 'c',
  #       office = 'c',
  #       election_slug = "c",
  #       date = 'D',
  #       status = 'c',
  #       election_id = 'd',
  #       text = 'c',
  #       election_type = 'c',
  #       country_id = 'd'))
  
  ifes_upcoming <- read_html("https://www.electionguide.org/elections/type/upcoming/") %>%
    html_nodes("table") %>%
    html_table() %>% .[[1]]

  ifes_past <- read_html("https://www.electionguide.org/elections/type/past/") %>%
    html_nodes("table") %>%
    html_table() %>% .[[1]]

ifes <- bind_rows(ifes_upcoming, ifes_past) %>% 
    mutate(Countryname = Country,
            office = `Election for`,
            date = str_extract(`Date`, ".*\\d\\d\\d\\d"),
            date = as.Date(date, format = "%b %d, %Y"),
            status = Status,
            election_type = NULL,
            # Last_Held = as.Date(`Last Held*`, format = "%b %d, %Y"),
            .keep = "none")

  archiveInputs(ifes, group_by = NULL)

  #   # Using the API
  #   # I've abandoned this for now because it doesn't appear there is any useful additional data from the API
  # # 2022-08-10 Learned that while the url is elections_demo it is in fact the current data
  # ifes_data <- system(paste0("curl -X GET https://electionguide.org/api/v1/elections_demo/ -H 'Authorization: Token ", readLines(".access/ifes-authorization.txt", warn = F),"'"),
  #  intern = T) %>%
  #  fromJSON()

  # str(ifes_data, max.levels = 1)


  # ifes_data %>% str()
  # ifes_meta <- system("curl -X GET https://electionguide.org/api/v1/metadata/ -H 'Authorization: Token 6233e188716a27eb8d26668fea3a068d4c067b85'",
  #  intern = T) %>%
  #  fromJSON()

  # ifes_data2 <- system("curl -X GET https://electionguide.org/api/v1/api_elections/ -H 'Authorization: Token 6233e188716a27eb8d26668fea3a068d4c067b85'",
  #  intern = T) %>%
  #  fromJSON()
}

ifes_process <- function(as_of) {
  # elections_all <- loadInputs("ifes", group_by = NULL, as_of = as_of, format = "csv")
  elections_all <- read_csv(paste_path(inputs_archive_path, "ifes.csv"), col_types = "ccccDcdccdD") %>% 
    subset(date >= as_of - 182 & date <= as_of + 182)
  
  elections <- elections_all %>% 
    mutate(
      election_type = case_when(
        # election_type == "null" & grepl("president", tolower(text)) ~ "Head of Government (coded as null)",
        election_type == "null" & str_detect(text, " pre|^pre|Pre") ~ "Head of Government (coded as null)",
        election_type == "null" & str_detect(office, " pre|^pre|Pre") ~ "Head of Government (coded as null)",
        TRUE ~ election_type)) %>%
    subset(
      str_detect(election_type, "Head of")) %>%
    mutate(
      Country = name2iso(Countryname),
      .before = Countryname)
  
  filter_for_fcs <- function(data, country_column) {
    on_fcs <- fcs_process(as_of = as_of) %>%
      subset(FCS_normalised == 10, select = Country)
    
    filtered <- subset(data, get(country_column) %in% on_fcs$Country)
    # filtered <- data[which(data[, country_column] %in% on_fcs[, "Country"]),]
    return(filtered)
  }
  
  # anticipation: Is there an election in the next 6 months?
  # elections_next_6_months <- 
  elections_next_6m <- elections %>%
    subset(date >= as_of) %>%
    group_by(Country) %>%
    summarize(
      election_6m_text = paste(paste(date, text), collapse = "; "),
      election_6m = 1) %>% 
    filter_for_fcs("Country")
  
  delayed <- elections %>%
    subset(
      # looking for delayed elections within 6 months in either direction
      # (date > as_of - 182 & date < as_of + 182) &
      (status == "Postponed" | status == "Cancelled")) %>%
    group_by(Country) %>%
    summarize(
      delayed_text = paste(paste(date, status), collapse = "; "),
      delayed = 1) %>%
    filter_for_fcs("Country")
  
  irregular <- elections %>%
    subset(
      # looking for delayed elections within 6 months in either direction
      # (date >= as_of & date < as_of + 182) &
      (str_detect(status, "Snap") | str_detect(status, "Moved"))) %>%
    group_by(Country) %>%
    summarize(
      irregular_text = paste(paste(date, status), collapse = "; "),
      irregular = 1) %>%
    filter_for_fcs("Country")
  
  ifes <- merge_indicators(elections_next_6m, delayed, irregular)
  
  return(ifes)
}

pseudo_reign_process <- function(as_of) {
  gic <- gic_process(as_of = as_of)
  ifes <- ifes_process(as_of = as_of)
  
  pseudo_reign <- merge_indicators(gic, ifes) %>%
    replace_NAs_0(c("delayed", "irregular", "election_6m", "coup")) %>%
    mutate(
      Fr_coup_election_count = rowSums(select(. , delayed, irregular, election_6m, coup)),
      Fr_pseudo_reign_norm = ifelse(Fr_coup_election_count > 0, 10, 0)) %>%
    as_tibble() %>%
    mutate(Fr_coup_election_text = case_when(
      Fr_coup_election_count > 0 ~ paste0(
        ifelse(!is.na(coup_text), paste0("coup: ", coup_text, "; "), ""),
        ifelse(Fr_coup_election_count - coup > 0,
               paste("election: ",
                     ifelse(!is.na(election_6m_text), election_6m_text, ""),
                     ifelse(!is.na(delayed_text), delayed_text, ""),
                     ifelse(!is.na(irregular_text), irregular_text, "")), ""))))
  return(pseudo_reign)
}
