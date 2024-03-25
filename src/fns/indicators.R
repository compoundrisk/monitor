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
archiveInputs <- compiler::cmpfun(function(new_data,
                          path = paste0(inputs_archive_path, deparse(substitute(new_data)), ".csv"), 
                          newFile = F,
                          # group_by defines the groups for which most recent data should be taken
                          group_by = "CountryCode",
                          today = Sys.Date(),
                          return = F,
                          col_types = cols(.default = "c")
                          # large = F) 
                          ) {
  if (newFile) {
    path <- path # Need to use path argument before we change `new_data` – otherwise path is changed (because `deparse(substitute())`)
    new_data <- new_data %>% mutate(access_date = today) #%>%
    # group_by(across(all_of(group_by))) # not needed for first save
    write.csv(new_data, path, row.names = F)
    if(return == T) return(new_data)
  } else {
    
    # Read in the existing file for the input, which will be appended with new data
    prev <- suppressMessages(read_csv(path, col_types = col_types))
    if (nrow(problems(prev)) > 0) stop("Column types do not match.")
    prev <- prev %>% mutate(access_date = as.Date(access_date)) %>%
      # In case new columns have been added, want access_date as last column for col_type purposes
      select(-access_date, access_date)
    
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
    # most_recent <- mutate(most_recent, across(.cols = where(is.numeric), .fns = ~ round(.x, digits = 8)))
    new_data <- mutate(new_data, across(.cols = where(is.numeric), .fns = ~ round(.x, digits = 8)))
    
    # Add access_date for new data
    new_data <- mutate(new_data, access_date = today)
    if (class(col_types) == "col_spec") {
      new_data <- mutate(new_data, across(-access_date, ~ as.character(.x)))
    }

    # Row bind `most_recent` and `new_data`, in order to make comparison (probably a better way)
    # Could quicken a bit by only looking at columns that matter (ie. don't compare
    # CountryCode and CountryName both). Also, for historical datasets, don't need to compare
    # new dates. Those automatically get added. 
    # if(!large) {
    bound <- bind_rows(most_recent, new_data)
    data_fresh <- distinct(bound, across(-c(access_date)), .keep_all = T) %>%
      filter(access_date == today) %>% 
      distinct()
    # } else {
    #   # (Other way) Paste all columns together in order to compare via %in%, and then select the
    #   # data rows that aren't in 
    #   # This way was ~2x slower for a 200 row table, but faster (4.7 min compared to 6) for 80,000 rows
    #   data_paste <- do.call(paste0, select(new_data, -access_date))
    #   most_recent_paste <- do.call(paste0, select(most_recent, -access_date))
    #   data_fresh <- new_data[which(!sapply(1:length(data_paste), function(x) data_paste[x] %in% most_recent_paste)),]
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
inform_severity_collect <- function() {
  # Method using INFORM Severity's own site; previous method used acaps.org
  inform_directory <- file.path(inputs_archive_path, "inform-severity")

  if (!dir.exists(inform_directory)) {
    dir.create(inform_directory)
  }
  
  existing_files <- list.files(inform_directory) %>%
    str_replace("^\\d{8}--", "")
  existing_files_misnamed <- list.files(inform_directory) %>%
    subset(!str_detect(., "^\\d{8}"))
  
  urls <- read_html("https://drmkc.jrc.ec.europa.eu/inform-index/INFORM-Severity/Results-and-data") %>%
    html_elements("a") %>%
    html_attr("href") %>%
    { .[str_detect(., "xlsx") & !is.na(.)] } %>%
    { data.frame(url = paste0("https://drmkc.jrc.ec.europa.eu", .))} %>%
    mutate(
      file_name = str_extract(url, "[^/]*?.xlsx"),
      url = str_replace_all(url, " ", "%20")) %>%
    filter(file_name %ni% existing_files | file_name %in% existing_files_misnamed) %>%
    .[nrow(.):1,] %>% # Reverses order
    filter(!is.na(url))
    
  if (nrow(urls) > 0) {
    urls %>% apply(1, function(url) {
      destfile <- file.path(inform_directory, url["file_name"])
      curl_download(url["url"], destfile = destfile)
      if (!str_detect(url["file_name"], "^20\\d{6}")) {
        # Rename file with YYYYMMDD prefix if it doesn't alreay have one
        # Using "--" to signal the prefix is not a part of the original file name
        write_date <- format(as.Date(pull(read_xlsx(destfile, range = "A3", col_names = "date")), format = "%d/%m/%Y"), "%Y%m%d--")
        file.rename(destfile, file.path(inform_directory, paste0(write_date, url["file_name"])))
      }
    })
  }
}

## Add in *_collect() function for ACAPS
inform_severity_process <- function(as_of, dimension, prefix) {
  if (dimension %ni% c("Natural Hazard", "Conflict and Fragility", "Food", "Health", "all")) {
    stop('Dimension name must be one of following: "Natural Hazard", "Conflict and Fragility", "Food", "Health" or "all"')
  }

  column_names <- read_most_recent(
    paste_path(inputs_archive_path, "inform-severity"),
    FUN = read_xlsx, as_of = as_of, date_format = "%Y%m%d",
    sheet = "INFORM Severity - all crises",
    range = "A2:Z2", col_names = letters) %>% unlist()
  all_crises <-
    read_most_recent(
      paste_path(inputs_archive_path, "inform-severity"),
      FUN = read_xlsx, as_of = as_of, date_format = "%Y%m%d",
      sheet = "INFORM Severity - all crises",
      skip = 1, range = "A5:Z500", col_names = column_names, na = "x") %>%
    filter(!is.na(COUNTRY)) %>%
    select(CRISIS, ISO3, any_of(c("DRIVERS", "TYPE OF CRISIS")), `INFORM Severity Index`, `Last updated`) %>%
    mutate(`Last updated` = as.Date(`Last updated`)) %>%
    rename(
      DRIVERS = any_of("TYPE OF CRISIS"),
      inform_severity = `INFORM Severity Index`,
      inform_severity_text = CRISIS)
  
  keywords <- c(
    "Natural Hazard" = "flood|drought|cyclone|landslide|earthquake|volcan",
    "Conflict and Fragility" = "conflict|crisis|refugees|migration|violence|boko haram",
    "Food" = "food|famine",
    "Health" = "epidemic",
    "all" = ".*")

  thresholds <- c(
    "Natural Hazard" = 3,
    "Conflict and Fragility" = 4,
    "Food" = 4,
    "Health" = 3,
    "all" = 4)

  crisis_words <- keywords[dimension]
  minimum <- thresholds[dimension]

  # Right now I'm using the country level data sheet from ACAPS, but maybe I should be doing it by crisis instead so that the index is forthe specific category
  select_crises <- all_crises %>%
    filter(str_detect(tolower(DRIVERS), crisis_words) & inform_severity >= minimum) %>%
    group_by(ISO3)
  if (nrow(select_crises > 0)) {
    select_crises <- select_crises %>%
      summarise(
        inform_severity_text = paste(paste0(inform_severity_text, " (", inform_severity, ")"), collapse = "; "),
        inform_severity = 10)
  }

  output <- left_join(countrylist, select_crises, by = c("Country" = "ISO3")) %>%
    replace_NAs_0("inform_severity") %>%
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
        "flood", "drought", "natural", " rain", "monsoon", "dry", "(?<!cease)fire(?!d |wood)",
        "earthquak", "tropic", "cyclon", "volcan", "scarcity", "weather",
        "temperat", "landslid"),
         collapse = "|")

    conflict_words <- paste(c("violen", "armed", "clash", "gang", "attack",
        "fight", "offensive", "conflict", "fragil", "protection concern"),
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
      mutate(last_risk_update = as.Date(last_risk_update)) %>%
        subset(
            (str_detect(tolower(risk_title), crisis_words) |
            str_detect(tolower(rationale), crisis_words) |
            str_detect(tolower(vulnerability), crisis_words)) & 
            status != "Not materialised" & last_risk_update >= as_of - 60) %>%
        select(iso3, all_countries = country, risk_level, risk_title, rationale, vulnerability, date_entered, last_risk_update, status) %>%
        filter(last_risk_update >= as.Date(after)) %>%
        group_by(risk_title, iso3) %>%
        mutate(
          risk_text_full = paste(last_risk_update, risk_title, paste("Rationale:", rationale), paste("Vulnerability:", vulnerability), sep = "\\n"),
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
        select(Countryname, iso3, all_countries, risk_level, risk_text, risk_text_full, last_risk_update)
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
    # Compare to the most recent reviewed ACAPS Risk List file
    path <- paste_path("hosted-data/acaps-risk-list-reviewed", dim)
    most_recent <- read_most_recent(path, as_of = as_of, return_date = T) 
    previous_review <- most_recent$data
    # Separate today's crisis_events file into events that were updated before the last manual review and after the last manual review
    crisis_events_old <- filter(crisis_events, last_risk_update <= most_recent$date)
    crisis_events_new <- filter(crisis_events, last_risk_update > most_recent$date) %>% 
      mutate(risk_level_auto = risk_level)
    
    # Previously I was including previously reviewed items in the output, but I don't
    # need to because acaps_risk_list_reviewed_process() reads all reviewed files
    # crisis_events_old <- left_join(crisis_events_old, select(previous_review, iso3, risk_text_full, risk_level, risk_level_auto, Review), by = c("iso3" = "iso3", "risk_text_full" = "risk_text_full", "risk_level" = "risk_level"))
    # crisis_events_combined <- bind_rows(crisis_events_old, crisis_events_new)
    # crisis_events_combined <- crisis_events_combined %>% select(Countryname, iso3, all_countries, risk_level, risk_level_auto, risk_text, risk_text_full, last_risk_update, Review)
    # return(crisis_events_combined) 

    crisis_events_new <- crisis_events_new %>% select(Countryname, iso3, all_countries, risk_level, risk_level_auto, risk_text, risk_text_full, last_risk_update) %>%
      mutate(Review = "NA")
    return(crisis_events_new)

  } else {
    return(crisis_events)
  }
}

acaps_risk_list_reviewed_process <- function(dim, prefix, as_of) {
  path <- paste_path("hosted-data/acaps-risk-list-reviewed", dim)
  output <- read_most_recent(path, as_of = Sys.Date(), n = "all") %>%
    bind_rows() %>%
    mutate(last_risk_update = as.Date(last_risk_update, format = "%m/%d/%y")) %>%
    dplyr::filter(Review & between(last_risk_update, as_of - 60, last_risk_update)) %>%
    group_by(iso3) %>%
    # slice_max(risk_level, n = 1, with_ties = T) %>%
    # group_by(iso3) %>%
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
  return(output)
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
  url <- "https://www.who.int/api/emergencies/"
  params <- list(
    "sf_provider" = "dynamicProvider372",
    "sf_culture" = "en",
    "$orderby" = "PublicationDateAndTime desc",
    "$format" = "json",
    "$select" = "DonId,Title,OverrideTitle,UseOverrideTitle,regionscountries,FormattedDate,Overview,UrlName",
    "$top" = "20")
  response <- request(url) %>%
    req_url_path_append("diseaseoutbreaknews") %>%
    req_url_query(!!!params) %>%
    req_perform() %>%
    resp_body_json()
  dons_news <- response[[2]] %>%
    lapply(unlist) %>% bind_rows()

  # Get country names and codes from regionscountries field
  country_filter <- paste0(
    "regionscountries/any(a:a eq ",
    paste(unique(dons_news$regionscountries), collapse = " or a eq "),
    ")")
  country_ids <- request(url) %>%
    req_url_path_append("countries") %>%
    req_url_query(
      "$filter" = country_filter,
      "$select" = "Code,WhoCode,Title,regionscountries") %>%
    req_perform() %>%
    resp_body_json() %>%
    .[[2]] %>%
    # Alternative to read many pages; should be unnecessary
    # req_perform_iterative(next_req = iterate_with_offset("$skip", offset = 50), max_reqs = 7) %>%
    #   resps_successes() %>% resps_data(\(resp) resp_body_json(resp)) %>%
    #   keep_at("value") %>% .[[1]] %>%
    lapply(unlist) %>% bind_rows() %>%
    rename(who_country_alert = Code, country = Title)

  who_dons <- left_join(dons_news, country_ids, by = "regionscountries") %>%
   # It would make sense to use `relationship = "many-to-one"` but `regionscountries`
   # has multiple countries per id (e.g. Kosovo and Greenland)
    mutate(
      text = case_when(as.logical(UseOverrideTitle) ~ OverrideTitle, T ~ Title),
      text_long = str_replace_all(Overview, "<.*?>", ""),
      date = dmy(FormattedDate),
      disease = trimws(sub("\\s[-——ｰ–].*", "", text)),
      country = country,
      who_country_alert = who_country_alert,
      url = file.path("https://www.who.int/emergencies/disease-outbreak-news/item", UrlName),
      first_sentence = str_extract(text_long, "(.*?\\.) [A-Z].*", group = T),
      declared_over = str_detect(tolower(first_sentence), "declared the end of|declared over"),
      .keep = "none") %>%
      select(-first_sentence)
  
  archiveInputs(who_dons, group_by = NULL)
}

dons_process <- function(as_of) {
  who_dons <- loadInputs("who_dons", group_by = NULL, as_of = as_of, format = "csv") #, col_types = "cDcccclD") # column order is oddly different on Databricks version of inputs archive
  # Only include DONs alerts from the past 3 months and not declared over
  # (more robust version would filter out outbreaks if they were later declared over)
  who_dons_current <- who_dons %>%
    subset(date >= as_of - 92 & date <= as_of) %>% # change back to 92
    # Only select most recent observation per URL, per country
    slice_max(by = c(who_country_alert, url, disease), order_by = access_date) %>%
    # Remove observations where country name doesn't appear in story
    filter(verify_country(text_long, who_country_alert, remove_negative_looks = T)) %>%
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

  json <- content(response, "text", encoding = "UTF-8")
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
    archiveInputs(ifrc_epidemics, group_by = "id", newFile = T)
  } else {
    archiveInputs(ifrc_epidemics, group_by = "id")
  }
}

# Code for cleaning up ifrc_epidemics.csv archive file
# df <- read_csv('output/inputs-archive/ifrc_epidemics.csv', col_types = cols(.default = "c"))
# df %>% select(-access_date) %>% filter(!duplicated(.))
# df %>% filter(id == '6200') %>% select(-access_date) %>% filter(!duplicated(.)) %>% .[22:23]
# repaired <- df %>% group_by(id) %>% mutate(translation_module_original_language = base::unique(translation_module_original_language) %>% subset(!is.na(.)))    
# # repaired <- repaired %>% group_by(id) %>% mutate(active_deployments = base::unique(active_deployments) %>% subset(!is.na(.)))    
# repaired <- repaired[!duplicated(select(repaired, -access_date)),]
# write.csv(repaired, 'output/inputs-archive/ifrc_epidemics.csv', row.names = F)

ifrc_process <- function(as_of) {
  df <- loadInputs("ifrc_epidemics", group_by = "id", as_of = as_of, col_types = cols(.default = "c")) %>% 
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
  most_recent <- read_most_recent("hosted-data/fews", FUN = read_csv, as_of = as_of,
    col_types = cols(.default = "c"), return_name = T)
  
  new_path <- paste_path(inputs_archive_path, "fews", most_recent$name)
  if (file.exists(new_path)) {
    message(paste(new_path, "already exists. File not rewritten."))
  } else {
    write_csv(most_recent$data, new_path)
  }
}

fews_collect_api <- function() {
  # Learn download URL from resource metadata
  url <- 'https://datacatalogapi.worldbank.org/ddhxext/ResourceView?resource_unique_id=DR0091743'
  queryString <- list('resource_unique_id' = "DR0091743")
  response <- VERB("GET", url, query = queryString)
  metadata <- fromJSON(content(response, "text"))
  version_date <- as.Date(str_extract(basename(metadata$distribution$url), "20\\d{2}-\\d{1,2}-\\d{1,2}"))
  local_most_recent <- read_most_recent(file.path(inputs_archive_path, "fews"), FUN = paste, as_of = Sys.Date(), return_date = T, return_name = T)
  
  if (version_date != local_most_recent$date) {
    filename <- file.path(inputs_archive_path, "fews", paste0("fews-", version_date, ".csv"))
    curl::curl_download(url = metadata$distribution$url, destfile = filename)
  }
}

fews_collect_many <- function(as_of = Sys.Date()) {
  if (!dir.exists(paste_path(inputs_archive_path, "fews"))) {
    dir.create(paste_path(inputs_archive_path, "fews"))
  }
  existing_files <- list.files(paste_path(inputs_archive_path, "fews"))
  new_dates <- read_most_recent("hosted-data/fews", FUN = paste, as_of = as_of,
    n = "all", return_date = T, return_name = T) %>%
    { .[["date"]][.$name %ni% existing_files] }
  lapply(new_dates, fews_collect) %>% invisible()
}

fews_process <- function(as_of) {
  fewswb <- read_most_recent(directory_path = file.path(inputs_archive_path, "fews"), 
    FUN = read_csv, col_types = cols(.default = "c"), as_of = as_of) %>%
    mutate(year_month = as.yearmon(year_month, "%Y_%m")) %>%
    subset(as.Date(year_month) > as_of - 2 * 365) %>% # Might be able to replace this with a shorter timespan
    mutate(across(.cols = c(admin_code, pop, contains("fews")), ~ as.numeric(.x)))
 
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
    # Try moving above to fews_collect()
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
    n = "all", FUN = paste, as_of = as_of, return_date = T)$date %>%
    .[. > oldest_date]
  lapply(new_dates, fpi_collect) %>% invisible()
}

fpi_collect_api <- function(as_of = Sys.Date()) {
  metadata_url <- "https://microdata.worldbank.org/index.php/api/tables/info/fcv/wld_2021_rtfp_v02_m"
  metadata <- fromJSON(metadata_url)$result$metadata
  metadata$date <- metadata$description %>%
    str_extract("\\d{4}-\\d{2}-\\d{2}") %>%
    as.Date()
  local_most_recent_date <- read_csv(
    file.path(inputs_archive_path, "wb_fpi.csv"),
    col_select = access_date, col_types = "D")$access_date %>%
    tail(n = 1)
  if (metadata$date != local_most_recent_date) {
    wb_fpi <- lapply((lubridate::year(Sys.Date()) -2):lubridate::year(Sys.Date()), function(year) {
      first_call <- fromJSON(paste0("https://microdata.worldbank.org/index.php/api/tables/data/fcv/wld_2021_rtfp_v02_m?limit=1000&offset=0&year=", year, "&fields=o_food_price_index,h_food_price_index,l_food_price_index,c_food_price_index,inflation_food_price_index,ISO3,DATES"))
      total_rows <- first_call$found
      offsets <- seq_len(floor(total_rows)/1000)*1000 
      fpi_1year <- offsets %>%
        lapply(function(offset) {
          url <- paste0("https://microdata.worldbank.org/index.php/api/tables/data/fcv/wld_2021_rtfp_v02_m?limit=1000&offset=", offset, "&year=", year, "&fields=o_food_price_index,h_food_price_index,l_food_price_index,c_food_price_index,inflation_food_price_index,ISO3,DATES")
          df <- fromJSON(url)
          return(df$data)
        }) %>% bind_rows()
      fpi_1year <- bind_rows(first_call$data, fpi_1year)
      return(fpi_1year)
    }) %>% bind_rows()
    wb_fpi <- wb_fpi %>%
      distinct() %>%
      select(
        Open = o_food_price_index,
        High = h_food_price_index,
        Low = l_food_price_index,
        Close = c_food_price_index,
        Inflation = inflation_food_price_index,
        ISO3,
        date = DATES)
    archiveInputs(wb_fpi, group_by = c("ISO3", "date"), today = metadata$date)
  }
}


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
# fao_wfp_collect <- function() {
#   fao_wfp <- read_csv("hosted-data/fao-wfp/fao-wfp.csv", col_types = cols())
#   fao_wfp <- fao_wfp %>%
#     mutate(Country = name2iso(Country))
  
#   fao_all <- countrylist
#   fao_all[fao_all$Country %in% fao_wfp$Country,"F_fao_wfp_warning"] <- 10
#   fao_all$Forecast_End <- max(fao_wfp$Forecast_End, na.rm = T)
  
#   fao_wfp <- fao_all
  
#   start_date <- min(
#     (as.yearmon(fao_all$Forecast_End[1]) - 3/12) %>% as.Date(),
#     Sys.Date())
  
#   archiveInputs(fao_wfp, group_by = c("Country"), today = start_date)
# }

# fao_wfp_process <- function(as_of) {
#   # Kind of unnecessary
#   fao_wfp <- loadInputs("fao_wfp", group_by = c("Country"), 
#     as_of = as_of, format = "csv", col_types = "ccdDD") %>%
#     select(-Countryname) %>%
#     filter(Forecast_End >= as_of)
#   return(fao_wfp)
# }

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
      cClientSession = "6216C788-462B-4F1B-88F33A01C97F15DC",
      `_` = "1699039352859")

    response <- VERB("GET", url, body = payload, query = queryString,
        content_type("application/octet-stream"),
        set_cookies(
          `cfid` = "91e70de2-19a1-4f7e-91a9-84a2d857a2f7",
          `cftoken` = "0",
          `CF_CLIENT_AC3F2F44A9F80153B3F52E57CCD20EEB_LV` = "1699039845381",
          `CF_CLIENT_AC3F2F44A9F80153B3F52E57CCD20EEB_TC` = "1699039845381",
          `CF_CLIENT_AC3F2F44A9F80153B3F52E57CCD20EEB_HC` = "2"),
        encode = encode)

    full_text <- content(response, "text")
    if (full_text %>% str_detect('TOCOLOR\\\\+":1')) {
        no_data <- F
        print(paste("Data found at outlook =", outlook))
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
  mutate(Countryname = Country, Country = name2iso(Countryname, region_names = T), .before = 1) %>%
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
# # EIU's review months are currently incorrect: the listed months are one month too early
# # While they fix, I need to replace each month with month+1: e.g. May 2023 -> June 2023
adjust_months <- function(in_file, out_file) {
  downloaded <- read.csv(in_file, colClasses = "character", header = F)
  names(downloaded) <- downloaded[1,]
  downloaded <- downloaded[-1,]
  downloaded[2,-c(1,2)] <- paste(as.yearmon(unlist(downloaded[2,-c(1,2)])) + 1/12)
  write.csv(downloaded, out_file, row.names = F)
}
# adjust_months(
#   in_file = "/Users/bennotkin/Downloads/EIU_OR_RiskTracker_ByGeography-9.csv",
#   out_file = "hosted-data/eiu/EIU_OR_RiskTracker_ByGeography-2024-01-01.csv")

eiu_collect <- function(as_of = Sys.Date(), print_date = F) {
  most_recent <- read_most_recent("hosted-data/eiu", FUN = read_csv, col_types = cols(.default = "c"),
    as_of = as_of, return_date = T)
  
  eiu <- most_recent$data[-1,] %>%
    # { setNames(., c("Series", "Code", name2iso(names(.)[3:ncol(.)]))) } %>% # Alt method, same speed
    rename(Series = Geography, Code = `...2`) %>%
    rename_with(.cols = c(-Series, -Code), ~ name2iso(.x)) %>% # Maybe better to do this in eiu_process()
    # mutate(across(c(-Series, -Code), ~ as.numeric(.x)))
    # I pivot longer and then wide to make countries observations and series variables;
    # Most series will have data for each country-month, but most countries won't have
    # for each series-month
    pivot_longer(cols = c(-1, -2), names_to = "Country", values_to = "Values") %>%
    mutate(across(c(Series, Code, Country), ~ as.factor(.x))) %>%
    { full_join(
        filter(., Series != "Review month"),
        filter(., Series == "Review month") %>% select(Country, Month = Values),
        by = "Country") } %>%
      select(-Series) %>%
      mutate(
        Month = as.factor(Month),
        Values = suppressWarnings(as.numeric(Values))) %>%
      filter(!is.na(Values)) %>%
      pivot_wider(names_from = Code, values_from = Values) %>%
    select(Country, Month, everything())

  if (!file.exists(paste_path(inputs_archive_path, "eiu.csv"))) {
    newFile <- T
    print("No file eiu.csv, being created.")
  } else newFile <- F

  archiveInputs(eiu, group_by = c("Country", "Month"), col_types = "ffddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
    today = most_recent$date, newFile = newFile)
  if (print_date) print(most_recent$date)
}

eiu_collect_many <- function(as_of = Sys.Date()) {
  if (!file.exists(paste_path(inputs_archive_path, "eiu.csv"))) {
    oldest_date <- read_most_recent("hosted-data/eiu", 
    n = "all", FUN = paste, as_of = as_of, return_date = T)$date %>% min()
    eiu_collect(as_of = oldest_date)
  }
  oldest_date <- loadInputs("eiu", group_by = c("Country", "Month"), 
    as_of = Sys.Date(), format = "csv", col_type = "ffdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddD") %>% 
    .$access_date %>% max()
  new_dates <- read_most_recent("hosted-data/eiu", 
    n = "all", FUN = paste, as_of = as_of, return_date = T)$date %>%
    .[. > oldest_date]
  lapply(new_dates, eiu_collect, print_date = T)
}

eiu_process <- function(as_of) {
  eiu_data <- loadInputs("eiu", group_by = c("Month", "Country"), 
    as_of = as_of, col_types = "ffdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddD") %>%
    mutate(Month = as.yearmon(Month)) %>%
    filter(Month > as.yearmon(as_of) - 1.5)
  eiu_data <- eiu_data %>% select(Country, Month, Financial = FR00, Trade_and_Payments = PR00, Macroeconomic = MR00) %>%
    mutate(EIU_Score = (Financial + Trade_and_Payments + Macroeconomic)/3)

  eiu_latest_quarter <- eiu_data %>%
    group_by(Country) %>%
    slice_max(Month, n = 1)
  eiu_one_year <- eiu_data %>%
    group_by(Country) %>%
    slice_max(Month, n = 5) %>%
    slice_min(Month, n = 4) %>%
    summarize(across(-Month, ~ mean(.x, na.rm = T), .names = "{.col}_12m"))
  # eiu_three_month <- eiu_data_long %>%
  #     group_by(Country) %>%
  #     slice_max(MONTH, n = 4) %>%
  #     slice_min(MONTH, n = 3) %>%
  #     summarize(Macroeconomic_risk_3 = mean(Macroeconomic_risk))
  
  eiu_joint <-
    reduce(list(eiu_latest_quarter, eiu_one_year),#, eiu_three_month),
           full_join, by = "Country") %>%
    mutate(
      EIU_12m_change = EIU_Score - EIU_Score_12m,
      #   EIU_3m_change = Macroeconomic_risk - Macroeconomic_risk_3
    ) %>%
    rename_with(.col = c(-Country, -Month), .fn = ~ paste0("M_", .)) %>%
    filter(!is.na(M_EIU_Score))

  
  #   eiu_joint <- normfuncpos(eiu_joint, quantile(eiu_joint$M_EIU_Score, 0.95), quantile(eiu_joint$M_EIU_Score, 0.10), "M_EIU_Score")
  eiu_joint <- normfuncpos(eiu_joint, quantile(eiu_joint$M_EIU_12m_change, 0.95), quantile(eiu_joint$M_EIU_12m_change, 0.10), "M_EIU_12m_change")
  eiu_joint <- normfuncpos(eiu_joint, quantile(eiu_joint$M_EIU_Score_12m, 0.95), quantile(eiu_joint$M_EIU_Score_12m, 0.10), "M_EIU_Score_12m")

  # Add a cap so countries with low absolute risk are not flagged
  eiu_joint <- eiu_joint %>% 
    mutate(M_EIU_12m_change_norm = case_when(
      M_EIU_12m_change_norm >= 7 & M_EIU_Score < 50 ~ 7,
      T ~ M_EIU_12m_change_norm)) %>%
    { .[,sort(names(.))] }

  return(eiu_joint)
}

#### SOCIO-ECONOMIC
#---------------------------—Alternative socio-economic data (based on INFORM)
inform_socio_process <- function(as_of) {
  inform_risk <- loadInputs("inform_risk", group_by = c("Country"), 
    as_of = as_of, format = "csv", col_types = cols(.default = "c")) %>%
    distinct()
  
  inform_data <- inform_risk %>%
    dplyr::select(Country, `Socio-Economic Vulnerability`) %>%
    mutate(S_INFORM_vul = as.numeric(`Socio-Economic Vulnerability`), .keep = "unused")
  
  inform_data <- normfuncpos(inform_data, 7, 0, "S_INFORM_vul")
  inform_data <- normfuncpos(inform_data, 7, 0, "S_INFORM_vul")
  return(inform_data)
}

#--------------------------—MPO: Poverty projections----------------------------------------------------
# Uses World Bank API, but it doesn't include projections
# yr <- year(Sys.Date())
# request_url <- paste0("http://api.worldbank.org/v2/country/all/indicator/SI.POV.DDAY?format=json&date=", yr-1, ":", yr)
# response <- httr::GET(request_url)
# df <- httr::content(response, "text", encoding = "UTF-8") %>% jsonlite::fromJSON()

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
    as_of = as_of, format = "csv", col_types = "ccccccccccccD")
  
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
  eiu_data <- loadInputs("eiu", group_by = c("Month", "Country"), 
    as_of = as_of, col_types = "ffdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddD") %>%
    mutate(Month = as.yearmon(Month)) %>%
    filter(Month > as.yearmon(as_of) - 1.5)

    series_names <- c(
      "Armed_Conflict" = "SR01",
      "Terrorism" = "SR02",
      "Violent_Demonstrations" = "SR03",
      "Violent_Crime" = "SR05",
      "Organised_Crime" = "SR06",
      "Kidnapping_Extortion" = "SR07",
      "Cyber_Security" = "SR08")

    eiu_data <- eiu_data %>% select(Country, Month, all_of(series_names))

    eiu_latest_quarter <- eiu_data %>%
      group_by(Country) %>%
      slice_max(Month, n = 1) %>%
      ungroup() %>%
      mutate(Security_1q = rowMeans(select(., c(-Country, -Month))))
    eiu_one_year <- eiu_data %>%
      group_by(Country) %>%
      slice_max(Month, n = 5) %>%
      slice_min(Month, n = 4) %>%
      summarize(across(-Month, ~ mean(.x, na.rm = T), .names = "{.col}_12m")) %>%
      mutate(Security_12m = rowMeans(select(., -Country)))

    eiu_aggregate <-
      reduce(list(eiu_latest_quarter, eiu_one_year),#, eiu_three_month),
            full_join, by = "Country") %>%
      mutate(Security_Change = Security_1q - Security_12m) %>%
      select(Country, -Month, Security_Change, Security_1q, Security_12m, everything())

    # eiu_long <- eiu_data %>%
    #   select(-`SERIES CODE`) %>%
    #   subset(`SERIES NAME` %in% series_names) %>%
    #   pivot_longer(-c(`SERIES NAME`, MONTH), names_to = "Country", values_to = "Values") %>%
    #   rename(Series = `SERIES NAME`) %>%
    #   group_by(Country, Series)

    # eiu_1_month <- eiu_long %>%
    #     slice_max(MONTH, n = 1) %>%
    #     select(Country, Series, one_month = Values)
    # eiu_1_year <- eiu_long %>%
    #     slice_max(MONTH, n = 13) %>%
    #     slice_min(MONTH, n = 12) %>%
    #     summarize(one_year_mean = mean(Values))

    # eiu_aggregate <- 
    #     # reduce(list(eiu_1_month, eiu_3_month, eiu_1_year, eiu_3_year),#, eiu_three_month),
    #     reduce(list(eiu_1_month, eiu_1_year),#, eiu_three_month),
    #         full_join, by = c("Country", "Series")) %>%
    #         ungroup() %>% group_by(Country) %>%
    #         summarize(across(contains("_"), ~ mean(.x))) %>%
    #     mutate(
    #         security_1_month = one_month,
    #         security_1_year = one_year_mean,
    #         security_change_12_1 = one_month - one_year_mean,
    #         .keep = "unused"
    #         ) %>%
    #     # full_join(sec_risk) %>% 
    #     arrange(desc(security_change_12_1))

    eiu_security_risk <- 
        normfuncpos(eiu_aggregate, 0.2, 0, "Security_Change") %>%
        # normfuncpos(quantile(eiu_aggregate$security, 0.95), quantile(eiu_aggregate$security, 0.05), "security") %>%
        # select(Country, security, security_norm, change_12_1, change_12_1_norm) %>%
        select(Country, Security_1q, Security_Change, Security_Change_norm) %>%
        mutate(Security_Change_norm = case_when(
            Security_1q <= 2 & Security_Change_norm > 7 ~ 7,
            T ~ Security_Change_norm)) %>% 
            arrange(desc(Security_Change_norm), desc(Security_Change)) %>%
    rename(EIU_Security_Change_norm = Security_Change_norm,
           EIU_Security_Change = Security_Change) %>%
           add_dimension_prefix("Fr_")

    return(eiu_security_risk)
}

#### NATURAL HAZARDS

#------------------------------—GDACS-----------------------------------------
gdacs_collect <- function() {
  gdacs_url <- "https://www.gdacs.org/"
  gdacs_web <- read_html(gdacs_url) %>%
    html_node("#containerAlertList")

  event_types <- c(
    "#gdacs_eventtype_EQ",
    "#gdacs_eventtype_TC",
    "#gdacs_eventtype_FL",
    "#gdacs_eventtype_VO",
    "#gdacs_eventtype_DR"
    # "#gdacs_eventtype_WF",
  )

  # Function to create database with hazard specific information
  gdacs <- lapply(event_types, function(type) {
    event_html <- gdacs_web %>%
        html_nodes(type)
    
    names <- event_html %>%
        html_nodes(".alert_item_name, .alert_item_name_past") %>%
        html_text()

    urls <- event_html %>%
      html_nodes(".alert_item") %>%
      html_nodes("a") %>%
      html_attr("href")

    mag <- event_html %>%
      html_nodes(".magnitude, .magnitude_past") %>%
      html_text() %>%
      str_trim()
    
    date <- event_html %>%
      html_nodes(".alert_date, .alert_date_past") %>%
      html_text() %>%
      str_trim()
    date <- gsub(c("-  "), "", date)
    date <- gsub(c("\r\n       "), "", date)

    hazard <- str_extract(type, "EQ|TC|FL|VO|DR") %>%
        str_replace_all(c(
          "EQ" = "earthquake",
          "TC" = "cyclone",
          "FL" = "flood",
          "VO" = "volcano",
          "DR" = "drought"))

    alert_class <- event_html %>%
      html_nodes("a > div:first-child") %>%
      html_attr("class")

    color <- str_extract(tolower(alert_class), "red|orange|green")

    status <- case_when(
      hazard != "drought" & str_detect(tolower(alert_class), "past") ~ "past", T ~ "active")

    events <- data.frame(names, mag, date, status, haz = color, hazard =rep(hazard, length(names)), urls)

    return(events)
  }) %>% 
    bind_rows() %>% #separate_rows(names, sep = "-|, ") %>% 
    mutate(names = trimws(names)) %>%
    filter(names != "Off shore")

  gdacs_no_cyclones <- filter(gdacs, hazard != "cyclone") %>%
    mutate(
      country = name2iso(names, multiple_matches = T)) %>%
      separate_rows(country, sep = ", ")

  # Add countries for cyclones and other countryless events
  gdacs_no_countries <- bind_rows(
    filter(gdacs, hazard == "cyclone"),
    filter(gdacs_no_cyclones, is.na(country))
  )

  gdacs_no_countries$country <- gdacs_no_countries$urls %>% lapply(function(url) {
    table <- read_html(url) %>%
      html_node(".summary") %>%
      html_table()
    info <- table$X2 %>% setNames(table$X1)
    names(info) <- str_replace_all(names(info), c("Countries:" = "Exposed countries", "Name:" = "Name"))
    # name <- info["Name"]
    countries <- info["Exposed countries"]
  return(countries)
  })
  gdacs_found_countries <- gdacs_no_countries %>% filter(country != "Off-shore") %>%
    mutate(country = name2iso(country)) %>%
    separate_rows(country, sep = ", ")

  gdacs <- bind_rows(
    gdacs_no_cyclones %>% filter(!is.na(country)),
    gdacs_found_countries
  )

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
  gdacs_prev <- read_csv(paste_path(inputs_archive_path, "gdacs.csv"), col_types = "c") %>%
    mutate(access_date = as.Date(access_date))
  gdacs_prev_recent <- filter(gdacs_prev, access_date == max(access_date)) %>% distinct()
  if(!identical(select(gdacs_prev_recent, -access_date), select(gdacs, -access_date))) {
    gdacs <- bind_rows(gdacs_prev, gdacs) %>% distinct() %>%
      select(-access_date, access_date)
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
    gdacs <- read_csv(paste_path(inputs_archive_path, "gdacs.csv"), col_types = "c") %>%
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
      str_detect(date, "Week") ~
        paste(as_of - 7*as.numeric(str_extract(date, '(\\d+) Week', group = T))),
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

  most_recent <- read_most_recent("hosted-data/inform-risk", FUN = read_csv, as_of = Sys.Date(),
  col_types = cols(.default = "c"), na = c("", "NA", "x"), return_date = T)
  file_date <- most_recent[[2]]

  inform_risk <- most_recent$data %>%
    mutate(across(-c(Country, `RISK CLASS`, `Countries in HVC`), ~ as.numeric(.x)))

  archiveInputs(inform_risk, today = file_date, group_by = c("Country"))
}

inform_nathaz_process <- function(as_of) {
  inform_risk <- loadInputs("inform_risk", group_by = c("Country"),
    as_of = as_of, format = "csv", col_types = cols(.default = "c")) %>%
    distinct()
  # Rename country
  informnathaz <- inform_risk %>%
    dplyr::select(Country, Natural) %>%
    mutate(NH_Hazard_Score = as.numeric(Natural), .keep = "unused") %>%
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
    file.copy("tmp-continuity.tiff", paste0(inputs_archive_path, "iri/continuity/iri-continuity-", as_of, ".tiff"))
  } else file.remove("tmp-continuity.tiff")
  forecast_new <- raster("tmp-forecast.tiff")
  forecast_old <- read_most_recent(paste_path(inputs_archive_path, "iri/forecast"), FUN = raster, as_of = as_of)
  if(!identical(values(forecast_new), values(forecast_old))) {
    file.copy("tmp-forecast.tiff", paste0(inputs_archive_path, "iri/forecast/iri-forecast-", as_of, ".tiff"))
  } else file.remove("tmp-forecast.tiff")
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
# List of countries and risk factors associated with locusts (FAO), see: http://www.fao.org/ag/locusts/en/info/info/index.html
fao_locust_pdf_collect <- function(bulletin_url) {
      locust_pdf <- pdf_text(bulletin_url)
      
      fao_locust <- locust_pdf %>%
        subset(str_detect(., "Desert Locust Bulletin")) %>%
        lapply(\(page) {
          bulletin_date <- page[[1]] %>%
            str_extract("No. \\d+\\s+ (\\d+ [A-Za-z]+ \\d{4})", group = T) %>%
            dmy()
          if (is.na(bulletin_date) || bulletin_date < as.Date("2018-01-01")) return(NULL) # Formatting changes before this date
          locust_lines <- page[1] %>%
            str_split_1("\\n") %>%
            str_sub(1, 68) %>%
            trimws()
          locust_lines[locust_lines == ""] <- "&&&"
          locust_vector <- locust_lines %>%
            paste(collapse = " ") %>%
            str_split_1("(&&&\\s)+") %>%    
            subset(str_detect(., "^[A-Z]*\\sREGION:.*"))
          concern_levels <- locust_vector %>%
            str_extract("[A-Z]*(?=\\sSITUATION)")
          NH_locust_norm <- case_when(
            concern_levels == "CALM" ~ 0,
            concern_levels == "CAUTION" ~ 3,
            concern_levels == "THREAT" ~ 7,
            concern_levels == "DANGER" ~ 10)
          countries <- name2iso(locust_vector)
          fao_locust <- tibble(
              Country = countries,
              NH_locust_norm = NH_locust_norm,
              NH_locust_level = concern_levels,
              bulletin_url = bulletin_url,
              bulletin_date = bulletin_date) %>%
            mutate(NH_locust_level = paste(NH_locust_level, "-", bulletin_date)) %>%
    full_join(select(countrylist, Country), by = "Country") %>%
            tidyr::replace_na(replace = list(
              NH_locust_norm = 0,
              NH_locust_level = "No reporting",
              bulletin_url = bulletin_url,
              bulletin_date = bulletin_date)) %>%
            separate_longer_delim(Country, delim = ", ") %>%
          arrange(Country)
          archiveInputs(fao_locust, group_by = "Country", today = bulletin_date)
          return(fao_locust)
        }) %>% bind_rows()
    return(fao_locust)
}

fao_locust_multi_collect <- function() {
  fao_locust_existing <- read_csv(file.path(inputs_archive_path, "fao_locust.csv"), col_types = "cdccDD")

  site_body <- read_html("https://www.fao.org/locust-watch/information/bulletin/en") %>%
    html_element("body")
  bulletin_cards <- site_body %>%
    html_elements(".card-body")
  bulletin_urls <- bulletin_cards %>%
    html_elements("a.title-link") %>%
    html_attr("href")
  archive_urls <- site_body %>%
    html_elements(".accordion") %>%
    html_elements(".accordion-body") %>%
    lapply(\(year_archive) {
      archive_url <- year_archive %>% html_elements("a") %>% html_attr("href")
      return(archive_url)
    }) %>%
    unlist() %>%
     # Remove archive urls with different document formats
    subset(!str_detect(., "8668|8669|8670|8671|8672|8673|8674|8675|8836|8837|8838|8839|8840|8841|8842|8843|8844"))
  bulletin_urls <- c(bulletin_urls, archive_urls) %>%
    which_not(fao_locust_existing$bulletin_url) %>%
    sort()
  if (length(bulletin_urls) > 0) {
  fao_locust <- bulletin_urls %>%
    lapply(fao_locust_pdf_collect) %>%
    bind_rows() %>%
    arrange(bulletin_date, Country)
  }
}

fao_locust_process <- function(as_of) {
  fao_locust <- loadInputs("fao_locust", group_by = c("Country"), 
    as_of = as_of, format = "csv", col_types = "cdccDD")
  return(fao_locust)
}

#---------------------------------—Natural Hazard ACAPS---------------------------------
# acaps_nh <- acapssheet[,c("Country", "NH_natural_acaps")]

####    FRAGILITY

#-------------------------—FCS---------------------------------------------
fcs_create_file <- function(list, date) {
  fcv_statuses <- lapply(seq_along(list), function(i, fcv_status) { 
    df <- data.frame(list[i])
    df <- df %>% mutate(
      FCV_status = names(df) %>% str_replace_all("\\.", " ")
      ) %>% 
    rename(Country = 1) %>%
    mutate(Country = name2iso(Country))
    return(df)
    }) %>%
    bind_rows()

  fcs <- country_groups %>% 
    mutate(
        Countryname = Economy,
        Country = Code,
        `IDA-status` = case_when(
            `Lending category` == "IDA" ~ "Yes",
            T ~ "NA"),
    .keep = "none") %>%
    subset(Country %in% countrylist$Country) %>%
    left_join(fcv_statuses, by = "Country")
  file_name <- paste0("hosted-data/fcs/fcs-list-", date, ".csv")

  write.csv(fcs, file_name, row.names = F)
}

# fcs_create_file(
#   list = list(
#     "conflict" = c("Afghanistan", "Burkina Faso", "Cameroon", "Central African Republic", "Congo, Democratic Republic of", "Ethiopia", "Iraq", "Mali", "Mozambique", "Myanmar", "Niger", "Nigeria", "Somalia", "South Sudan", "Sudan", "Syrian Arab Republic", "Ukraine", "West Bank and Gaza (territory)", "Yemen, Republic of"),
#     "institutional and social fragility" = c("Burundi", "Chad", "Comoros", "Congo, Republic of", "Eritrea", "Guinea-Bissau", "Haiti", "Kiribati", "Kosovo", "Lebanon", "Libya", "Marshall Islands", "Micronesia, Federated States of", "Papua New Guinea", "São Tomé and Príncipe", "Solomon Islands", "Timor-Leste", "Tuvalu", "Venezuela, RB", "Zimbabwe")),
#   date = "2023-07-10")

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
    
    archiveInputs(fsi, group_by = "Country", col_types = "cdcddddddddddddd", today = file_date)
}

fsi_process <- function(as_of) {
  fsi <- loadInputs("fsi", group_by = "Country", as_of = as_of, col_types = "cdcddddddddddddd") %>%
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
  credentials <- read_csv(paste_path(mounted_path, ".access/acled.csv"), col_types = "c")
  acled_url <- paste0("https://api.acleddata.com/acled/read/?key=", credentials$key, "&email=", credentials$username, "&event_date=",
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
  
  # Manually add in coups before they are added to dataset
  if (max(coups_raw$access_date) < as.Date("2023-9-05")) {
    coups_raw <- coups_raw %>% bind_rows(data.frame(country = "Gabon", year = 2023, month = 8, day = 30, coup = 2, version = "manual", access_date = as.Date("2023-09-05"), date = "2023-08-30"))
  }

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
