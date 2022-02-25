
######################################################################################################
#
#  FUNCTIONS USED TO CREATE THE GLOBAL DATABASET ON COMPOUND DISASTER RISK
#  AND DIMENSION SHEETS FOR EXCEL
#  (to be run after risk component sheets have been generated)
#
######################################################################################################

# LOAD PACKAGES ----
# source("libraries.R")

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

# Function for gathering all of a dimension's sources together
merge_indicators <- function(...) {
  sources <- list(...)
  sheet <- countrylist
  for(s in sources) {
    sheet <- left_join(sheet, s, by = "Country")
  }
  # sheet <- lapply(sources, function(s) {
  #   sheet <- left_join(sheet, s, by = "Country")
  #   return(sheet)
  # })
  sheet <- arrange(sheet, Country) %>%
    distinct(Country, .keep_all = TRUE) %>%
    drop_na(Country)
  if ('Countryname' %in% names(sheet)) {
    sheet <- select(sheet, -Countryname)
  }
  # if (format == "csv" | format == "both") {
  #   write.csv(sheet, paste0(directory, "/", dim, "-sheet.csv"))
  #   # print(paste0("risk-sheets/", dim, "-sheet.csv"))
  # }
  # if (format == "tbl" | format == "both") {
  #   # Write Spark DataFrame
  # }
  # if (return == T) {
  #   # write.csv(sheet, paste0("risk-sheets/", dim, "-sheet.csv"))
  #   # print(paste0("risk-sheets/", dim, "-sheet.csv"))
  return(sheet)
}

assign_ternary_labels <- function(x, high, medium, low) {
  label <- ifelse(x >= low & x < medium, "Low",
                  ifelse(x >= medium & x < high, "Medium",
                         ifelse(x >= high, "High", NA
                         )
                  )
  )
  return(label)
}

aggregate_dimension <- function(dim, ..., prefix = "") {
  
  if(!dim %in% indicators_list$Dimension) {
    stop(cat("Dimension name", dim, "not in indicators-list.csv"))
  }
  
  # # At some point this should be broken up into
  # make_dimension_sheet <- function(dim, ...) {
  #     specify_used_variables(dim)
  #     sheet <- merge_indicators() %>%
  #         select_used_variables() %>%
  #         calculate_dimension_values("underlying") %>%
  #         calculate_dimension_values("emerging") %>%
  #         calculate_dimension_values("overall") %>%
  #         rename_dimension_columns(dim)
  #     return(sheet)
  # }
  
  sheet <- merge_indicators(...)
  
  # Make sure variables have the prefix that will be used for selection (should always be there
  # except in case of Fragility); should probably happen here and not in indicators.R, but that 
  # it's already there
  if(dim == "Conflict and Fragility") {
    sheet <- sheet %>% rename_with(
      .fn = ~ paste0("Fr_", .),
      .cols = -contains("Country") & !starts_with(paste0("Fr_"))
    )
  }
  
  # Specify which variables to keep
  
  # specify_used_variables <- function(dim) {
  #     dimension_indicators <- filter(indicators_list, Dimension == dim)
  #     underlying_indicators <- filter(dimension_indicators, Timeframe == "Underlying")
  #     emerging_indicators <- filter(dimension_indicators, Timeframe == "Emerging")
  #     list()
  # }
  
  dimension_indicators <- filter(indicators_list, Dimension == dim)
  underlying_indicators <- filter(dimension_indicators, Timeframe == "Underlying")
  emerging_indicators <- filter(dimension_indicators, Timeframe == "Emerging")
  
  # Select only used variables (raw and normalised) and calculate dimension values
  raw_vars <- sheet %>%
    select(Country,
           # Select the raw variables 
           any_of(unlist(strsplit(underlying_indicators$indicator_raw_slug, ", "))), 
           any_of(unlist(strsplit(emerging_indicators$indicator_raw_slug, ", ")))) %>%
    # Add a 'raw' suffix to raw variables. Should this happen earlier, in input processing?
    rename_with(
      .fn = ~ paste0(., "_raw"),
      .cols = -contains("Country"))
  
  sheet <- sheet %>% 
    # Select only the normalised variables that are actually used in the monitor (as defined in `indicators-list.csv`)
    select(Country,
           any_of(underlying_indicators$indicator_slug),
           any_of(emerging_indicators$indicator_slug)) %>%
    left_join(., raw_vars, by = "Country")
  # Calculate underlying dimension value by taking max value of primary underlying indicators
  sheet <- sheet %>% mutate(
    underlying = rowMaxs(as.matrix(select(.,
                                          any_of(underlying_indicators$indicator_slug[which(underlying_indicators$secondary == F)]))), na.rm = T) %>% {
                                            case_when(
                                              is.infinite(.) ~ NA_real_,
                                              TRUE ~ .
                                            )
                                          },
  ) %>%
    # In case all primary variables are NA, take max of secondary indicators (currently only used for emerging food)
    mutate(
      underlying = if(length(which(underlying_indicators$secondary == T)) > 0) {
        case_when(
          is.na(underlying) ~ rowMaxs(as.matrix(select(.,
                                                       any_of(underlying_indicators$indicator_slug[which(underlying_indicators$secondary == T)])))),
          TRUE ~ underlying)
      } else { underlying },
      underlying_labels = assign_ternary_labels(underlying, high = 10, medium = 7, low = 0) %>% as.factor()
    ) %>%
    # Calculate emerging dimension value by taking max value of primary emerging indicators
    mutate(
      emerging = rowMaxs(as.matrix(select(.,
                                          any_of(emerging_indicators$indicator_slug[which(emerging_indicators$secondary == F)]))), na.rm = T) %>% {
                                            case_when(
                                              is.infinite(.) ~ NA_real_,
                                              TRUE ~ .
                                            )
                                          }) %>%
    # In case all primary variables are NA, take max of secondary indicators (only used for emerging food)
    mutate(
      emerging = if(length(which(emerging_indicators$secondary == T)) > 0) {
        case_when(
          is.na(emerging) ~ rowMaxs(as.matrix(select(.,
                                                     any_of(emerging_indicators$indicator_slug[which(emerging_indicators$secondary == T)])))),
          TRUE ~ emerging)
      } else { emerging },
      emerging_labels = assign_ternary_labels(emerging, high = 10, medium = 7, low = 0) %>% as.factor()
    ) %>%
    # Calculate overall dimension value by taking geometric mean of underlying and emerging
    mutate(
      overall = case_when(
        !is.na(underlying) ~ sqrt(underlying * emerging),
        TRUE ~ emerging),
      overall_labels = assign_ternary_labels(overall, high = 7, medium = 5, low = 0)  %>% as.factor()
    ) %>%
    # Calculate reliablity (this should maybe be distributed among the underlying and emerging functions above)
    mutate(
      underlying_reliability = rowSums(is.na(select(., any_of(underlying_indicators$indicator_slug))), na.rm = T) / nrow(underlying_indicators),
      emerging_reliability = rowSums(is.na(select(., any_of(emerging_indicators$indicator_slug))), na.rm = T) / nrow(emerging_indicators)
    ) %>%
    # We could assign dimension levels (high, medium, low) here, but don't think we need to
    # Rename underlying and emerging columns with dimension-specific names
    # !! and :0 are used to name variables programmatically, tells R to evaluate LHS
    rename(
      !!paste0("Underlying_", dim) := underlying,
      !!paste0("Emerging_", dim) := emerging,
      !!paste0("Overall_", dim) := overall,
      !!paste0("Underlying_", dim, "_Labels") := underlying_labels,
      !!paste0("Emerging_", dim, "_Labels") := emerging_labels,
      !!paste0("Overall_", dim, "_Labels") := overall_labels,
      !!paste0("Underlying_", dim, "_Reliability") := underlying_reliability,
      !!paste0("Emerging_", dim, "_Reliability") := emerging_reliability, 
    )
  
  # Check to see we have correct number of colummns
  raw_count <- unlist(strsplit(dimension_indicators$indicator_raw_slug, ", ")) %>% length()
  norm_count <- dimension_indicators$indicator_slug %>% length()  
  column_count <- raw_count + norm_count + 9 # 1 country cols, 3 aggregate cols, 3 label cols, 2 reliability cols
  
  if(ncol(sheet) != column_count) {
    warning(paste("Wrong number of columns in", dim, "aggregation:", ncol(sheet), "instead of", column_count))
  }
  
  # Check if any sheets have empty columns
  empties <- empty_cols(sheet) %>% names()
  if (length(empties) > 0) {
    warning(paste(dim, " is empty on ", empties)) 
    # } else {
    #   print(paste(dimension, " is filled")) 
  }

  return(sheet)
}



count_flags <- function(df, outlook, high, medium) {
  select_columns <- select(df, contains(outlook) & -contains("reliability") & -contains("labels"))
  df <- mutate(df, flags = rowSums(select_columns >= high, na.rm = T) + 
  rowSums(select_columns >= medium & select_columns < high, na.rm = T)/2)

df <- rename(
  df,
  !!paste0(tools::toTitleCase(outlook),"_Flag_Number of Flags") := flags,
)

  # if(tolower(outlook) == "emerging") {
  #   df <- rename(df, `Emerging_Number of Emerging Threat Flags` = flags)
  # }
  # if(tolower(outlook) == "underlying") {
  #   df <- rename(df, `Underlying_Number of Underlying Vulnerability Flags` = flags)
  # }
  # if(tolower(outlook) == "overall") {
  #   df <- rename(df, `Overall_Number of Overall Alert Flags` = flags)
  # }

  return(df)
}




write_excel_source_file <- function(sheet, filename, directory_path, archive_path = NULL) {
  sheet <- select(sheet,
                  -starts_with("Underlying_"),
                  -starts_with("Emerging"),
                  -starts_with("Overall_"))
  write_csv(sheet, paste_path(directory_path, filename))
  if(!is.null(archive_path)) {
    write_csv(sheet, paste_path(archive_path, filename))
  }
}

write_excel_source_files <- function(
  all_dimensions,
  health_sheet,
  food_sheet,
  macro_sheet,
  socio_sheet,
  natural_hazards_sheet,
  fragility_sheet,
  # reliability_sheet,
  directory_path,
  filepaths = F, # are the sheet arguments filepaths to CSVs (T) or are they dataframes (F)
  archive = F, # Set T to also save output in the crm-excel/archive/ folder
  countries = NULL
) {
  
  ensure_directory_exists(directory_path)
  if(archive) {
    ensure_directory_exists(directory_path, "archive")
    archive_path <- ensure_directory_exists(directory_path, "archive", Sys.Date(), new = T, return = T, suffix = "run_")
  } else {
    archive_path <- NULL
  }

  if(filepaths) {
    health_sheet <- read.csv(health_sheet)
    food_sheet <- read.csv(food_sheet)
    macro_sheet <- read.csv(macro_sheet)
    socio_sheet <- read.csv(socio_sheet)
    natural_hazards_sheet <- read.csv(natural_hazards_sheet)
    fragility_sheet <- read.csv(fragility_sheet)
    # reliability_sheet <- read.csv(reliability_sheet)
  }
  
if (!is.null(countries)) {
  all_dimensions <- subset(all_dimensions, Country %in% countries)
  health_sheet <- subset(health_sheet, Country %in% countries)
  food_sheet <- subset(food_sheet, Country %in% countries)
  macro_sheet <- subset(macro_sheet, Country %in% countries)
  socio_sheet <- subset(socio_sheet, Country %in% countries)
  natural_hazards_sheet <- subset(natural_hazards_sheet, Country %in% countries)
  fragility_sheet <- subset(fragility_sheet, Country %in% countries)
}

  write_excel_source_file(health_sheet, "health.csv", directory_path = directory_path, archive_path = archive_path)
  write_excel_source_file(food_sheet, "food-security.csv", directory_path = directory_path, archive_path = archive_path)
  write_excel_source_file(macro_sheet, "macro-fiscal.csv", directory_path = directory_path, archive_path = archive_path)
  write_excel_source_file(socio_sheet, "socioeconomic-vulnerability.csv", directory_path = directory_path, archive_path = archive_path)
  write_excel_source_file(natural_hazards_sheet, "natural-hazard.csv", directory_path = directory_path, archive_path = archive_path)
  write_excel_source_file(fragility_sheet, "conflict-and-fragility.csv", directory_path = directory_path, archive_path = archive_path)
  # write_excel_source_file(reliability_sheet, "reliability-sheet.csv", directory_path = directory_path, archive_path = archive_path)

  # Write reliability sheet
  # This is necessary for the Excel dashboard, so maybe should be lumped with `write_excel_source_files()`
  reliability <- select(all_dimensions, Country, contains("Reliability")) %>%
    mutate(
          Countryname = countrycode(Country, origin = "iso3c", destination = "country.name"),
          Underlying_Reliability = rowMeans(select(., starts_with("Underlying"))),
          Emerging_Reliability = rowMeans(select(., starts_with("Emerging"))), .after = Country) #%>%
    # Add columns that Excel file expects
    # mutate(id = row_number(),
    #        Countyname = countrycode(Country, origin = "iso3c", destination = "country.name"), .before = 1)
  # Previously I was rounding this to one decimal (`round(rowMeans(...), 1)`) but I'm not sure this is helpful
  multi_write.csv(reliability, "reliability-sheet.csv", c(directory_path, archive_path))
}

pretty_col_names <- function(data) {
  pretty <- data %>%
    rename_with(
      .fn = function(x) {
        i <- match(x, indicators_list$indicator_slug)
        names <- paste(indicators_list$Timeframe[i], indicators_list$Dimension[i], indicators_list$Indicator[i], sep = "_")
        # print(paste(x[i], "-", names[i]))
        return(names)
      },
      .cols = any_of(indicators_list$indicator_slug)) %>%
    rename_with(
      .cols = any_of(paste0(unlist(strsplit(indicators_list$indicator_raw_slug[c(1:5,28:6)], ", ")), "_raw")),
      .fn = function(x) {
        # Unnest multi-value cells (for indicators that use multiple raw variables)
        df <- indicators_list %>% 
          separate_rows(indicator_raw_slug, sep = ",") %>%
          mutate(indicator_raw_slug = trimws(indicator_raw_slug))
        # Create 
        df <- mutate(df, new_names = paste(
          Timeframe,
          Dimension,
          Indicator,
          sep = "_"))
        dups <- df$new_names %>%
          { c(which(duplicated(., fromLast = T)), which(duplicated(.))) } %>%
          unique() %>% sort()
        df[dups,"new_names"] <- paste0(df$new_names[dups], " (", df$indicator_raw_slug[dups], ")")
        df <- mutate(df,
          new_names = paste(new_names, "Raw"),
          old_names = paste0(indicator_raw_slug, "_raw"))
        i <- match(x, df$old_names)
        return(df$new_names[i])
      }
    )
  return(pretty)
}



# # This was an attempt to rename columns without unnesting the multi-value cells;
# # it worked until I needed to differentiate raw indicators with the same Indicator
# # Name (e.g. S_pov_prop_22_21 and S_pov_prop_21_20) 
#   pretty3 <- rename_with(
#     pretty[,31:1],
#     .fn = function(x) {
#       x <- sub("(.*)_raw$", "\\1", x)
#       i <- sapply(x, grep, indicators_list$indicator_raw_slug)
#       print(i)
#       names <- paste(indicators_list$Timeframe[i], indicators_list$Dimension[i], indicators_list$Indicator[i], "Raw", sep = "_")
#       df <- data.frame(raw_slug = x, name = names)
#       print(df)
#       print(paste(x, "-->", names))
#       # print(paste(indicators_list$indicator_raw_slug[i]))
#       return(names)
#     },
#     .cols = any_of(paste0(unlist(strsplit(indicators_list$indicator_raw_slug, ", ")), "_raw"))
#     )

# Move this basic function elsewhere, somewhere more basic (helpers.R?)
ensure_directory_exists <- function(..., return = F, new = F, suffix = "") {
  path <- paste_path(...)
  if(!dir.exists(path)) {
    already_exists <- F
    dir.create(path)
    if(dir.exists(path)) {
      message(paste("Directory at", path, "did not exist but was created"))
    }
  } else {
    already_exists <- T
  }
  if(already_exists & new) {
    count <- 1
    while(already_exists) {
      count <- count + 1
      suffixed_path <- paste0(path, "-", suffix, leading_zeros(count, 2))
      if(!dir.exists(suffixed_path)) {
        already_exists <- F
        dir.create(suffixed_path)
        path <- suffixed_path
      }
    }
  }
  if(return) return(path)
}

lengthen_data <- function(data) {
  long <- data %>% 
    select(-contains("_labels")) %>% # Drop labels because labels are not another observation, but describe other observations
    mutate(across(everything(), as.character)) %>%
    pivot_longer(., cols = -contains("country"), names_to = "Name", values_to = "Value_Char") %>%
    separate(Name, into = c("Outlook", "Dimension", "Key"), sep = "_", extra = "merge", fill = "right") %>%
    mutate(
    Value = as.numeric(case_when(
      !is_string_number(Value_Char) ~ Value_Char,
      TRUE ~ NA_character_)), .before = Value_Char)
  return(long)
}

add_secondary_columns <- function(data) {
  output <- data %>%
  # Fill in other columns (`add_data_level_column()`? `add_secondary_columns()`?) 
  mutate(
    `Data Level` = case_when(
      is.na(Key) ~ "Dimension Value",
      grepl("Flag", Dimension) ~ "Flag Count",
      grepl(" Raw$", Key) ~ "Raw Indicator Data",
      Key == "Reliability" ~ "Reliability",
      Key %in% indicators_list$Indicator ~ "Indicator",
      TRUE ~ NA_character_),
    # When Data Level is Dimension Value, set dimension as Key
    Key = case_when(
      `Data Level` == "Dimension Value" & is.na(Key) ~ Dimension,
      TRUE ~ Key),
    # Display Status tells the dashboard how prominent the data should be
    # I believe this is for whether it shows up in the table before expansion 
    `Display Status` = case_when(
      `Data Level` == "Dimension Value" ~ "Main",
      `Data Level` == "Flag Count" ~ "Main",
      `Data Level` == "Indicator" ~ "Secondary",
      grepl("Raw", `Data Level`) ~ "Tertiary",
      TRUE ~ NA_character_),
    # Does a data point contribute to the Overall outlook? / should it be 
    # shown when Overall is selected on the dashboard?
    `Overall Contribution` = case_when(
      Outlook == "Overall" ~ T,
      `Data Level` == "Indicator" ~ T,
      `Data Level` == "Raw Indicator Data" ~ T,
      TRUE ~ F),
    # Re-assign risk labels; for flag counts and indicators, risk label is
    # just the number, rounded down, as a character
    `Risk Label` = case_when(
      `Data Level` == "Dimension Value" & (Outlook == 'Emerging' | Outlook == 'Underlying') ~ assign_ternary_labels(Value, 10, 7, 0),
      `Data Level` == "Dimension Value" & Outlook == 'Overall' ~ assign_ternary_labels(Value, 7, 5, 0),
      `Data Level` == "Flag Count" ~ as.character(floor(Value)),
      `Data Level` == "Indicator" ~ as.character(floor(Value)),
      TRUE ~ NA_character_),
    # Add column for values in character form
    Value_Char = case_when(
      is.na(Value) ~ Value_Char,
      `Data Level` == "Flag Count" ~ as.character(Value),
      `Data Level` == "Dimension Value" ~ as.character(floor(Value)),
      # TRUE ~ as.character(floor(Value))
      TRUE ~ as.character(round(Value, 1))),
    Date = Sys.Date()) # Should the date column so system date, or should it show the access date (this would be a good bit harder to do)
  return(output)
}
round_value_col <- function(data) {
  output <- data %>% 
    mutate(
      # Round value column (maybe separate into new function?)
      Value = case_when(
        `Data Level` == "Flag Count" ~ Value,
        grepl(" Raw$", Key) ~ round(Value, 3),
        TRUE ~ round(Value, 3)))
  return(output)
}

factorize_columns <- function(data) {
  data <- data %>%
    mutate(
        Outlook = ordered(Outlook, levels = c("Overall", "Underlying", "Emerging")),
        Dimension = ordered(Dimension, c("Flag", "Food Security", "Conflict and Fragility", "Health", "Macro Fiscal",  "Natural Hazard", "Socioeconomic Vulnerability")),
        Country = ordered(Country, levels = sort(unique(Country))),
        Countryname = ordered(Countryname, levels = sort(unique(Countryname))),
        `Data Level` = ordered(`Data Level`, c("Flag Count", "Dimension Value", "Indicator", "Raw Indicator Data", "Reliability")),  
        `Display Status` = factor(`Display Status`))
        Indicator = 
  return(data)
}

order_columns_and_raws <- function(data) {
  data <- data %>%
    relocate(Value_Char, `Risk Label`, .after = Value) %>%
    relocate(`Data Level`, .before = Outlook) %>%
    arrange(Country, Outlook, Dimension, `Data Level`)
  return(data)
}

# Move this basic function elsewhere, somewhere more basic
leading_zeros <- function(data, length, filler = "0") {
  strings <- sapply(data, function(x) {
    if(is.na(x)) {
      x <- 0
      warning("NAs converted to 0. Likelihood of duplicate strings")
    }
    if(nchar(x) > length) {
      stop(paste("String is longer than desired length of", length, ":", x, "in", deparse(substitute(data))))
    }
    string <- paste0(paste0(rep(filler, length - nchar(x)), collapse = ""), x)
    return(string)
  })
  return(strings)
}

create_id <- function(data) {
  data <- mutate(data,
    Indicator_ID = case_when(
        `Data Level` == "Indicator" ~ indicators_list$indicator_id[match(Key, indicators_list$Indicator)],
        # TASK: / FIX: This results in multiple raw variables receiving the same 
        `Data Level` == "Raw Indicator Data" ~
          separate_rows(indicators_list, indicator_raw_slug, sep = ",")$indicator_id[match(sub("(.*) Raw", "\\1", Key), separate_rows(indicators_list, indicator_raw_slug, sep = ",")$Indicator)],
          # [match(sub("(.*) Raw", "\\1", Key), separate_rows(indicators_list, indicator_raw_slug, sep = ",")[,"Indicator"]]),
        TRUE ~ as.integer(0),
      ),
      Index = as.integer(paste0(
        leading_zeros(as.numeric(Country), 3),
        # Does it make more sense to order by Data Level before Dimension, Outlook, and even Country?
        leading_zeros(as.numeric(Outlook), 1),
        leading_zeros(as.numeric(Dimension), 1),
        leading_zeros(as.numeric(`Data Level`), 1),
        leading_zeros(Indicator_ID, 2)#,
        # ifelse(`Data Level` == "Raw Indicator Data", 1, 0)
        )),
      .before = 1) %>%
      select(-Indicator_ID)
  return(data)
}

append_if_exists <- function(data, path) {
  if(file.exists(path)) {
    already_exists <- T
    existing <- read_csv(path)
    data <- mutate(data, Run_ID = max(existing$Run_ID) + 1, .before = Index)
    prior_run <- subset(existing, Run_ID = max(Run_ID)) %>% factorize_columns()
    # TODO Check also if data is fresh. If all duplicates, don't append
    # Write a function that can be used for archiveInputs, countFlagChanges, and here
    # that more robustly checks difference; for now a sum comparison will do
    # if(sum(select(data, Index, Value) == select(prior_run, Index, Value)) > 0) {

    # }
  } else {
    already_exists <- F
    message(paste0("No file yet exists at ", path))
    data <- mutate(data, Run_ID = 1, .before = Index)
  }
  data <- data %>%
    mutate(
      Record_ID = as.numeric(paste0(Run_ID, leading_zeros(Index, 8))),
      .before = 1)
  if(!already_exists) existing <- data[0,]
  combined <- rbind(existing, data)
  return(combined)
}


# #
# ##
# ### ********************************************************************************************
# ####    WRITE MINIMAL DIMENSION SHEETS ----
# ### ********************************************************************************************
# ##
# #

# write_minimal_dim_sheets <- function() {
#   # Saves CSVs with only used indicators (normalized, not raw) to crm_excel folder
#   # Delete. Now included in `libraries.R`
#   # countrylist <- read.csv("https://raw.githubusercontent.com/ljonestz/compoundriskdata/update_socio_eco/Indicator_dataset/countrylist.csv")
#   # countrylist <- countrylist %>%
#   # dplyr::select(-X) %>% 
#   # arrange(Country)
  
#   # Load dimension / data dictionary
#   indicators_list <- as.data.frame(read.csv("indicators-list.csv"))
  
#   health_sheet <- read.csv("output/risk-sheets/health-sheet.csv")[,-1] # drops first column, X, which is row number
#   food_sheet <- read.csv("output/risk-sheets/food-sheet.csv")[,-1]
#   fragility_sheet <- read.csv("output/risk-sheets/fragility-sheet.csv")[,-1]
#   macro_sheet <- read.csv("output/risk-sheets/macro-sheet.csv")[,-1]
#   natural_hazards_sheet <- read.csv("output/risk-sheets/natural_hazards-sheet.csv")[,-1]
#   socio_sheet <- read.csv("output/risk-sheets/socio-sheet.csv")[,-1]
  
#   # Compile list of all dimension data for Map function
#   # List names should match dimension names in `indicator` data frame
#   unique(indicators_list$Dimension)
#   sheetList <- list("Health" = health_sheet,
#                     "Food Security" = food_sheet,
#                     "Conflict and Fragility" = fragility_sheet, # Technical note uses various names; what do we want to use? Fragility and Conflict, Conflict and Fragility, Conflict Fragility and Institutional Risk
#                     "Macro Fiscal" = macro_sheet,
#                     "Natural Hazard" = natural_hazards_sheet,
#                     "Socioeconomic Vulnerability" = socio_sheet
#   )
  
#   # Make directory from within script to enable volume sharing with docker
#   # (All contents are emptied when volume is shared)
#   # dir.create("external/crm-excel")
#   # dir.create("external/crm-excel/archive")
  
#   # —Write function to apply to each sheet ----
#   used_indicators <- countrylist
  
#   writeSourceCSV <- function(i) {
#     # headerOffset <- 2 # current output has two header rows # VARIABLE
    
#     # Define sheet and dimension name
#     sheet <- sheetList[[i]]
#     sheet <- sheet[!duplicated(sheet$Country),]
#     sheet <- arrange(sheet, Country)
#     dimension <- names(sheetList)[i]
    
#     sheet_norm <- sheet[,c(which(colnames(sheet) == "Country"), which(colnames(sheet) %in% indicators_list$indicator_slug))]
#     sheet_raw <- sheet[,c(which(colnames(sheet) == "Country"), which(colnames(sheet) %in% unlist(strsplit(indicators_list$indicator_raw_slug, ", "))))]
#     colnames(sheet_raw) <- paste0(colnames(sheet_raw), "_raw")
#     sheet <- left_join(sheet_norm, sheet_raw, by = c("Country" = "Country_raw"), suffix = c("", "_raw"))
    
#     # write_csv(sheet_norm, paste0("Risk_sheets/indicators-normalised/", dimension, ".csv"))
#     write_csv(sheet, paste0("output/crm-excel/", slugify(dimension, space_replace = "-"), ".csv"))
#     write_csv(sheet, paste0("output/crm-excel/archive/", Sys.Date(), "-", slugify(dimension, space_replace = "-"), ".csv"))
    
#     # Write csvs with readable variable names
#     ## Order sheet columns to match indicators.csv
#     inds <- indicators_list[which(indicators_list$Dimension == dimension), "indicator_slug"]
#     sheet_norm <- sheet_norm[c("Country", inds)]
    
#     ## Rename columns
#     pairs <- subset(indicators_list,
#                     Timeframe == "Emerging" & Dimension == dimension,
#                     c(Indicator, indicator_slug))
#     names(sheet_norm)[match(pairs[ ,"indicator_slug"], names(sheet_norm))] <- paste0("Emerging_", dimension, "_", pairs$Indicator)
    
#     pairs <- subset(indicators_list,
#                     Timeframe == "Underlying" & Dimension == dimension,
#                     c(Indicator, indicator_slug))
#     names(sheet_norm)[match(pairs[ ,"indicator_slug"], names(sheet_norm))] <- paste0("Underlying_", dimension, "_", pairs$Indicator)  
    
#     # write_csv(sheet_norm, paste0("Risk_sheets/pretty-names/", dimension, ".csv") )
#     # Join sheets together (<<- hoists variable to parent environment)
#     used_indicators <<- left_join(used_indicators, sheet_norm, by = "Country")
    
#     # Check if any sheets have empty columns
#     empties <- empty_cols(sheet) %>% names()
#     if (length(empties) > 0) {
#       print(paste(dimension, " is empty on ", empties)) 
#     } else {
#       print(paste(dimension, " is filled")) 
#     }
#   }
  
#   # —Run -----
#   Map(writeSourceCSV, 1:length(sheetList))
#   write_csv(used_indicators, "output/crm-indicators.csv")
# }

# #
# ##
# ### ********************************************************************************************
# ####    CREATE GLOBAL DATABASE WITH ALL RISK SHEETS ----
# ### ********************************************************************************************
# ##
# #

# join_risk_sheets <- function() {
#   # Ideally we'd calculate dimension values for each sheet individually
#   # # Load risk sheets
#   # Do these need to be saved, or can they be held in memory?
#   healthsheet <- read.csv("output/risk-sheets/health-sheet.csv")[,-1] # drops first column, X, which is row number
#   foodsecurity <- read.csv("output/risk-sheets/food-sheet.csv")[,-1]
#   fragilitysheet <- read.csv("output/risk-sheets/fragility-sheet.csv")[,-1]
#   macrosheet <- read.csv("output/risk-sheets/macro-sheet.csv")[,-1]
#   Naturalhazardsheet <- read.csv("output/risk-sheets/natural_hazards-sheet.csv")[,-1]
#   Socioeconomic_sheet <- read.csv("output/risk-sheets/socio-sheet.csv")[,-1]
#   # countrylist <- read.csv("https://raw.githubusercontent.com/ljonestz/compoundriskdata/master/Indicator_dataset/countrylist.csv")[,-1]
  
#   # Join datasets
#   df <- left_join(countrylist, healthsheet, by = c("Country")) %>%
#     left_join(., foodsecurity, by = c("Country")) %>%
#     left_join(., fragilitysheet, by = c("Country")) %>%
#     left_join(., macrosheet, by = c("Country")) %>%
#     left_join(., Naturalhazardsheet, by = c("Country")) %>%
#     left_join(., Socioeconomic_sheet, by = c("Country")) %>%
#     distinct(Country, .keep_all = TRUE) %>%
#     drop_na(Country)
  
#   return(df)
# }

# #
# ##
# ### ********************************************************************************************
# ####    CREATE A FLAG SUMMARY SHEET ----
# ### ********************************************************************************************
# ##
# #

# # Add underlying and emerging risk scores
# #—`riskflags` ----
# calculate_dims <- function(riskflags) {
#   # This should be applied to each risk sheet individually, so I don't need
#   # to specify all the indicators; function would be *generic*
#   riskflags <- riskflags %>%
#     mutate(
#       UNDERLYING_RISK_HEALTH = pmax(
#         H_HIS_Score_norm,
#         H_INFORM_rating.Value_norm
#       ),
#       UNDERLYING_RISK_FOOD_SECURITY = F_Proteus_Score_norm,
#       UNDERLYING_RISK_MACRO_FISCAL = M_EIU_Score_12m_norm,
#       UNDERLYING_RISK_SOCIOECONOMIC_VULNERABILITY = S_INFORM_vul_norm,
#       UNDERLYING_RISK_NATURAL_HAZARDS = pmax(
#         NH_Hazard_Score_norm,
#         na.rm=T
#       ),
#       UNDERLYING_RISK_FRAGILITY_INSTITUTIONS = Fr_FCS_normalised,
#       EMERGING_RISK_HEALTH = pmax(
#         H_Oxrollback_score_norm,
#         H_Covidgrowth_casesnorm,
#         H_Covidgrowth_deathsnorm,
#         H_new_cases_smoothed_per_million_norm,
#         H_new_deaths_smoothed_per_million_norm,
#         H_GovernmentResponseIndexForDisplay_norm,
#         H_health_acaps,
#         H_wmo_don_alert,
#         na.rm = T
#       ),
#       EMERGING_RISK_FOOD_SECURITY = case_when(
#         (!is.na(F_fews_crm_norm) | !is.na(F_fao_wfp_warning)) ~ as.numeric(pmax(
#           F_fews_crm_norm,
#           F_fao_wfp_warning,
#           na.rm = T
#         )),
#         ((is.na(F_fews_crm_norm) | is.na(F_fao_wfp_warning)) & !is.na(F_fpv_rating)) ~  as.numeric(F_fpv_rating),
#         TRUE ~ NA_real_
#       ),
#       EMERGING_RISK_MACRO_FISCAL = M_EIU_12m_change_norm,
#       EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY = pmax(
#         S_pov_comb_norm,
#         S_change_unemp_norm,
#         S_income_support.Rating_crm_norm,
#         S_Household.risks,
#         S_phone_average_index_norm,
#         na.rm = T
#       ),
#       EMERGING_RISK_NATURAL_HAZARDS = as.numeric(pmax(
#         NH_GDAC_Hazard_Score_Norm,
#         NH_natural_acaps,
#         NH_seasonal_risk_norm,
#         NH_locust_norm,
#         na.rm = T
#       )),
#       EMERGING_RISK_FRAGILITY_INSTITUTIONS =pmax(
#         Fr_REIGN_Normalised,
#         Fr_Displaced_UNHCR_Normalised,
#         Fr_BRD_Normalised,
#         na.rm = T
#       )
#     ) %>%
#     dplyr::select(
#       Countryname, Country, UNDERLYING_RISK_HEALTH, UNDERLYING_RISK_FOOD_SECURITY,
#       UNDERLYING_RISK_MACRO_FISCAL, UNDERLYING_RISK_SOCIOECONOMIC_VULNERABILITY,
#       UNDERLYING_RISK_NATURAL_HAZARDS, UNDERLYING_RISK_FRAGILITY_INSTITUTIONS,
#       EMERGING_RISK_HEALTH, EMERGING_RISK_FOOD_SECURITY, EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY, EMERGING_RISK_MACRO_FISCAL,
#       EMERGING_RISK_NATURAL_HAZARDS, EMERGING_RISK_FRAGILITY_INSTITUTIONS #, F_fews_crm_norm
#     )
  
#   return(riskflags)
# }

# vars <- c(
#   "UNDERLYING_RISK_HEALTH", "UNDERLYING_RISK_FOOD_SECURITY",
#   "UNDERLYING_RISK_MACRO_FISCAL", "UNDERLYING_RISK_SOCIOECONOMIC_VULNERABILITY",
#   "UNDERLYING_RISK_NATURAL_HAZARDS", "UNDERLYING_RISK_FRAGILITY_INSTITUTIONS",
#   "EMERGING_RISK_HEALTH", "EMERGING_RISK_FOOD_SECURITY",
#   "EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY",
#   "EMERGING_RISK_MACRO_FISCAL",
#   "EMERGING_RISK_NATURAL_HAZARDS", "EMERGING_RISK_FRAGILITY_INSTITUTIONS"
# )

# # —Create ternary risk flags----
# assign_dim_levels <- function(x) {
  
#   x[paste0(vars, "_RISKLEVEL")] <- lapply(x[vars], function(tt) {
#     ifelse(tt >= 0 & tt < 7, "Low risk",
#            ifelse(tt >= 7 & tt < 10, "Medium risk",
#                   ifelse(tt == 10, "High risk", NA
#                   )
#            )
#     )
#   })
#   return(x)
# }

# # —Calculate total compound risk scores ----
# # count_flags <- function(riskflags) {
# #   riskflags$UNDERLYING_FLAGS <- as.numeric(unlist(row_count(
# #     riskflags,
# #     UNDERLYING_RISK_HEALTH:UNDERLYING_RISK_FRAGILITY_INSTITUTIONS,
# #     count = 10,
# #     append = F
# #   )))
  
# #   riskflags$EMERGING_FLAGS <- as.numeric(unlist(row_count(
# #     riskflags, 
# #     EMERGING_RISK_HEALTH:EMERGING_RISK_FRAGILITY_INSTITUTIONS,
# #     count = 10,
# #     append = F
# #   )))
  
# #   riskflags$medium_risk_underlying <- as.numeric(unlist(row_count(riskflags,
# #                                                                   UNDERLYING_RISK_HEALTH_RISKLEVEL:UNDERLYING_RISK_FRAGILITY_INSTITUTIONS_RISKLEVEL,
# #                                                                   count = "Medium risk",
# #                                                                   append = F
# #   )))
  
# #   riskflags$medium_risk_emerging <- as.numeric(unlist(row_count(riskflags,
# #                                                                 EMERGING_RISK_HEALTH_RISKLEVEL:EMERGING_RISK_FRAGILITY_INSTITUTIONS_RISKLEVEL,
# #                                                                 count = "Medium risk",
# #                                                                 append = F
# #   )))
  
# #   riskflags$UNDERLYING_FLAGS_MED <- as.numeric(unlist(riskflags$UNDERLYING_FLAGS + (riskflags$medium_risk_underlying / 2)))
# #   riskflags$EMERGING_FLAGS_MED <- as.numeric(unlist(riskflags$EMERGING_FLAGS + (riskflags$medium_risk_emerging / 2)))
  
# #   # Drop ternary rates (may want to reinstate in the future)
# #   riskflags <- riskflags %>%
# #     dplyr::select(
# #       -medium_risk_emerging, -medium_risk_underlying, -all_of(paste0(vars, "_RISKLEVEL"))
# #     ) %>%
# #     distinct(Country, .keep_all = TRUE) %>%
# #     drop_na(Country)
  
# #   return(riskflags)
# # }
# # #
# # ##
# # ### ********************************************************************************************
# # ####    CREATE DATABASE OF ALTERNATIVE RISK SCORES ----
# # ### ********************************************************************************************
# # ##
# # #

# # # —Alternative combined risk scores ----
# # names <- c(
# #   "EMERGING_RISK_FRAGILITY_INSTITUTIONS", 
# #   "EMERGING_RISK_MACRO_FISCAL", "UNDERLYING_RISK_FRAGILITY_INSTITUTIONS"
# # )

# # riskflags[paste0(names, "_plus1")] <- lapply(riskflags[names], function(xx) {
# #   ifelse(xx == 0, xx + 1, xx)
# # })

# # # Geometric means
# # riskflags <- riskflags %>%
# #   rowwise() %>%
# #   mutate(
# #     EMERGING_RISK_FRAGILITY_INSTITUTIONS_MULTIDIMENSIONAL = geoMean(c(
# #       EMERGING_RISK_FRAGILITY_INSTITUTIONS_plus1,
# #       EMERGING_RISK_MACRO_FISCAL_plus1),
# #       na.rm = T
# #     ),
# #     EMERGING_RISK_FRAGILITY_INSTITUTIONS_MULTIDIMENSIONAL_SQ = geoMean(c(
# #       UNDERLYING_RISK_FRAGILITY_INSTITUTIONS_plus1,
# #       EMERGING_RISK_FRAGILITY_INSTITUTIONS_MULTIDIMENSIONAL),
# #       na.rm = T
# #     )
# #   )

# # # remove unnecessary variables
# # riskflags <- riskflags %>%
# #   dplyr::select(-contains("_plus1"))

# # # Alternative combined total scores
# # altflag <- globalrisk
# # names <- c(
# #   "S_INFORM_vul_norm", "H_Oxrollback_score_norm", "H_wmo_don_alert",
# #   "H_Covidgrowth_casesnorm", "H_Covidgrowth_deathsnorm", "H_HIS_Score_norm", "H_INFORM_rating.Value_norm",
# #   "H_new_cases_smoothed_per_million_norm", "H_new_deaths_smoothed_per_million_norm",
# #   "F_Proteus_Score_norm", "F_fews_crm_norm", "F_fao_wfp_warning", "F_fpv_rating", #"D_WB_external_debt_distress_norm",
# #   "M_EIU_12m_change_norm", "M_EIU_Score_12m_norm",
# #   "NH_GDAC_Hazard_Score_Norm",  "H_GovernmentResponseIndexForDisplay_norm",  "H_GovernmentResponseIndexForDisplay_norm", 
# #   "S_Household.risks", "S_phone_average_index_norm",  "NH_seasonal_risk_norm","NH_locust_norm", "NH_natural_acaps","Fr_FCS_Normalised", 
# #   "Fr_REIGN_Normalised", "Fr_Displaced_UNHCR_Normalised", "Fr_BRD_Normalised"
# # )

# # altflag[paste0(names, "_plus1")] <- lapply(altflag[names], function(xx) {
# #   ifelse(xx == 0, xx + 1, xx)
# # })

# # # Calculate alternative variables
# # altflag <- altflag %>%
# #   rowwise() %>%
# #   mutate(
# #     EMERGING_RISK_HEALTH_AV = geoMean(c(
# #       H_Oxrollback_score_norm_plus1,
# #       H_Covidgrowth_casesnorm_plus1,
# #       H_Covidgrowth_deathsnorm_plus1,
# #       H_new_cases_smoothed_per_million_norm_plus1,
# #       H_new_deaths_smoothed_per_million_norm_plus1,
# #       H_GovernmentResponseIndexForDisplay_norm_plus1,
# #       H_wmo_don_alert_plus1,
# #       na.rm = T
# #     )),
# #     EMERGING_RISK_MACRO_FISCAL_AV = M_EIU_12m_change_norm,
# #     EMERGING_RISK_FRAGILITY_INSTITUTIONS_AV = geoMean(c(
# #       Fr_REIGN_Normalised,
# #       Fr_Displaced_UNHCR_Normalised,
# #       Fr_BRD_Normalised,
# #       na.rm = T
# #     ))
# #     ,
# #     EMERGING_RISK_HEALTH_SQ_ALT = geoMean(c(
# #       H_Oxrollback_score_norm_plus1,
# #       max(altflag$H_Covidgrowth_casesnorm,
# #           altflag$H_Covidgrowth_deathsnorm,
# #           altflag$H_new_cases_smoothed_per_million_norm,
# #           altflag$H_new_deaths_smoothed_per_million_norm,
# #           altflag$H_add_death_prec_current,
# #           altflag$H_health_acaps,
# #           altflag$H_wmo_don_alert,
# #           na.rm = T
# #       )
# #     ),
# #     na.rm = T
# #     )
# #   )

# # #--------------------------------—Calculate Coefficient of Variation----------------------------------------------------------------
# # altflag <- altflag %>%
# #   rowwise() %>%
# #   mutate(
# #     H_coefvar = cv(c( # Should these match the selected indicators?
# #       H_Oxrollback_score_norm,
# #       H_Covidgrowth_casesnorm,
# #       H_Covidgrowth_deathsnorm,
# #       H_new_cases_smoothed_per_million_norm,
# #       H_new_deaths_smoothed_per_million_norm,
# #       # H_add_death_prec_current,
# #       H_wmo_don_alert),
# #       na.rm = T
# #     ),
# #     M_coefvar = cv(c(M_EIU_12m_change_norm),
# #                    na.rm = T
# #     ),
# #     Fr_coefvar = cv(c(
# #       Fr_REIGN_Normalised,
# #       Fr_Displaced_UNHCR_Normalised,
# #       Fr_BRD_Normalised),
# #       na.rm = T
# #     ),
# #     NH_coefvar = cv(c(
# #       NH_GDAC_Hazard_Score_Norm,
# #       NH_natural_acaps,
# #       NH_seasonal_risk_norm),
# #       na.rm = T
# #     ),
# #     F_coefvar = cv(c(
# #       F_fews_crm_norm,
# #       F_fao_wfp_warning,
# #       F_fpv_rating),
# #       na.rm = T
# #     ),
# #     S_coefvar = cv(c(
# #       S_pov_comb_norm,
# #       S_change_unemp_20_norm,
# #       S_income_support.Rating_crm_norm,
# #       na.rm = T
# #     ))
# #   )

# # # Merge datasets to include alt variables
# # riskflags <- inner_join(riskflags,
# #                         altflag,
# #                         by = c("Country", "Countryname"),
# #                         keep = F
# # )

# #-----------------------------—Calculate Overall Alert w/ Geometric Mean ----------------------------------------------
# # Geometric mean of underlying and emerging (and emerging when underlying is NA)
# # This is how we are now calculating overall alert, though here it's called emerging risk sq

# calculate_overall_dims <- function(riskflags) {
#   riskflags <- riskflags %>%
#     mutate(
#       OVERALL_HEALTH_GEO = case_when(
#         !is.na(UNDERLYING_RISK_HEALTH) ~ sqrt(UNDERLYING_RISK_HEALTH * EMERGING_RISK_HEALTH),
#         TRUE ~ EMERGING_RISK_HEALTH
#       ),
#       # OVERALL_HEALTH_GEO_ALT = case_when(
#       #   !is.na(UNDERLYING_RISK_HEALTH) ~ sqrt(UNDERLYING_RISK_HEALTH * EMERGING_RISK_HEALTH_SQ_ALT),
#       #   TRUE ~ EMERGING_RISK_HEALTH
#       # ),
#       OVERALL_FOOD_SECURITY_GEO = case_when(
#         !is.na(UNDERLYING_RISK_FOOD_SECURITY) ~ sqrt(UNDERLYING_RISK_FOOD_SECURITY * EMERGING_RISK_FOOD_SECURITY),
#         TRUE ~ EMERGING_RISK_FOOD_SECURITY
#       ),
#       OVERALL_MACRO_FISCAL_GEO = case_when(
#         !is.na(UNDERLYING_RISK_MACRO_FISCAL) ~ sqrt(UNDERLYING_RISK_MACRO_FISCAL * EMERGING_RISK_MACRO_FISCAL),
#         TRUE ~ EMERGING_RISK_MACRO_FISCAL
#       ),
#       OVERALL_SOCIOECONOMIC_VULNERABILITY_GEO = case_when(
#         !is.na(UNDERLYING_RISK_SOCIOECONOMIC_VULNERABILITY) ~ sqrt(UNDERLYING_RISK_SOCIOECONOMIC_VULNERABILITY * EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY),
#         TRUE ~ as.numeric(EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY)
#       ),
#       OVERALL_NATURAL_HAZARDS_GEO = case_when(
#         !is.na(UNDERLYING_RISK_NATURAL_HAZARDS) ~ sqrt(UNDERLYING_RISK_NATURAL_HAZARDS * EMERGING_RISK_NATURAL_HAZARDS),
#         TRUE ~ EMERGING_RISK_NATURAL_HAZARDS
#       ),
#       OVERALL_FRAGILITY_INSTITUTIONS_GEO = case_when(
#         !is.na(UNDERLYING_RISK_FRAGILITY_INSTITUTIONS) ~ sqrt(UNDERLYING_RISK_FRAGILITY_INSTITUTIONS * EMERGING_RISK_FRAGILITY_INSTITUTIONS),
#         TRUE ~ EMERGING_RISK_FRAGILITY_INSTITUTIONS
#       )
#     )
  
#   # Calculate total emerging risk scores for SQ
#   geonam <- c(
#     "OVERALL_HEALTH_GEO", "OVERALL_FOOD_SECURITY_GEO",
#     "OVERALL_MACRO_FISCAL_GEO","OVERALL_SOCIOECONOMIC_VULNERABILITY_GEO",
#     "OVERALL_NATURAL_HAZARDS_GEO",
#     "OVERALL_FRAGILITY_INSTITUTIONS_GEO"
#   )
  
#   # Emerging risk score as all high risk scores
#   riskflags$OVERALL_FLAGS_GEO <- rowSums(riskflags[geonam] >= 7, na.rm = T) 
  
#   # Emerging risk score as high + med
#   riskflags$OVERALL_FLAGS_GEO_MED <-  rowSums(riskflags[geonam] >= 7, na.rm = T) + (rowSums(riskflags[geonam] < 7 & riskflags[geonam] >= 5, na.rm = T) / 2)
  
  
#   #-----------------------------—Calculate Overall Alert w/ Arithmetic Mean ----------------------------------------------
#   riskflags <- riskflags %>%
#     mutate(
#       OVERALL_HEALTH_AV = case_when(
#         !is.na(UNDERLYING_RISK_HEALTH) ~ (UNDERLYING_RISK_HEALTH + EMERGING_RISK_HEALTH)/2,
#         TRUE ~ EMERGING_RISK_HEALTH
#       ),
#       OVERALL_FOOD_SECURITY_AV = case_when(
#         !is.na(UNDERLYING_RISK_FOOD_SECURITY) ~ (UNDERLYING_RISK_FOOD_SECURITY + EMERGING_RISK_FOOD_SECURITY)/2,
#         TRUE ~ EMERGING_RISK_FOOD_SECURITY
#       ),
#       OVERALL_MACRO_FISCAL_AV = case_when(
#         !is.na(UNDERLYING_RISK_MACRO_FISCAL) ~ (UNDERLYING_RISK_MACRO_FISCAL + EMERGING_RISK_MACRO_FISCAL)/2,
#         TRUE ~ EMERGING_RISK_MACRO_FISCAL
#       ),
#       OVERALL_SOCIOECONOMIC_VULNERABILITY_AV = case_when(
#         !is.na(UNDERLYING_RISK_SOCIOECONOMIC_VULNERABILITY) ~ (UNDERLYING_RISK_SOCIOECONOMIC_VULNERABILITY + EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY)/2,
#         TRUE ~ as.numeric(EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY)
#       ),
#       OVERALL_NATURAL_HAZARDS_AV = case_when(
#         !is.na(UNDERLYING_RISK_NATURAL_HAZARDS) ~ (UNDERLYING_RISK_NATURAL_HAZARDS + EMERGING_RISK_NATURAL_HAZARDS)/2,
#         TRUE ~ EMERGING_RISK_NATURAL_HAZARDS
#       ),
#       OVERALL_FRAGILITY_INSTITUTIONS_AV = case_when(
#         !is.na(UNDERLYING_RISK_FRAGILITY_INSTITUTIONS) ~ (UNDERLYING_RISK_FRAGILITY_INSTITUTIONS + EMERGING_RISK_FRAGILITY_INSTITUTIONS)/2,
#         TRUE ~ EMERGING_RISK_FRAGILITY_INSTITUTIONS
#       )
#     )
  
#   # Calculate total emerging risk scores for SQ
#   arithnam <- c(
#     "OVERALL_HEALTH_AV", "OVERALL_FOOD_SECURITY_AV",
#     "OVERALL_MACRO_FISCAL_AV","OVERALL_SOCIOECONOMIC_VULNERABILITY_AV",
#     "OVERALL_NATURAL_HAZARDS_AV",
#     "OVERALL_FRAGILITY_INSTITUTIONS_AV"
#   )
  
#   # Emerging risk score as all high risk scores
#   riskflags$OVERALL_FLAGS_AV <- rowSums(riskflags[arithnam] >= 7, na.rm = T) 
  
#   # Emerging risk score as high + med
#   riskflags$OVERALL_FLAGS_AV_MED <-  rowSums(riskflags[arithnam] >= 7, na.rm = T) + (rowSums(riskflags[arithnam] < 7 & riskflags[arithnam] >= 5, na.rm = T) / 2)
  
  
#   #-----------------------------—Calculate Overall Alert w/ Filter: Emerging & Underlying == 10 ----------------------------------------------
#   riskflags <- riskflags %>%
#     mutate(
#       OVERALL_HEALTH_FILTER_10 = case_when(
#         UNDERLYING_RISK_HEALTH == 10 & EMERGING_RISK_HEALTH == 10 ~ 10,
#         TRUE ~ 0
#       ),
#       OVERALL_FOOD_SECURITY_FILTER_10 = case_when(
#         UNDERLYING_RISK_FOOD_SECURITY == 10 & EMERGING_RISK_FOOD_SECURITY == 10 ~ 10,
#         TRUE ~ 0
#       ),
#       OVERALL_MACRO_FISCAL_FILTER_10 = case_when(
#         UNDERLYING_RISK_MACRO_FISCAL == 10 & EMERGING_RISK_MACRO_FISCAL == 10 ~ 10,
#         TRUE ~ 0
#       ),
#       OVERALL_SOCIOECONOMIC_VULNERABILITY_FILTER_10 = case_when(
#         UNDERLYING_RISK_SOCIOECONOMIC_VULNERABILITY == 10 & EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY == 10 ~ 10,
#         TRUE ~ 0
#       ),
#       OVERALL_NATURAL_HAZARDS_FILTER_10 = case_when(
#         UNDERLYING_RISK_NATURAL_HAZARDS == 10 & EMERGING_RISK_NATURAL_HAZARDS == 10 ~ 10,
#         TRUE ~ 0
#       ),
#       OVERALL_FRAGILITY_INSTITUTIONS_FILTER_10 = case_when(
#         UNDERLYING_RISK_FRAGILITY_INSTITUTIONS == 10 & EMERGING_RISK_FRAGILITY_INSTITUTIONS == 10 ~ 10,
#         TRUE ~ 0
#       )
#     )
  
#   # Calculate total emerging risk scores for SQ
#   filter10nam <- c(
#     "OVERALL_HEALTH_FILTER_10", "OVERALL_FOOD_SECURITY_FILTER_10",
#     "OVERALL_MACRO_FISCAL_FILTER_10","OVERALL_SOCIOECONOMIC_VULNERABILITY_FILTER_10",
#     "OVERALL_NATURAL_HAZARDS_FILTER_10",
#     "OVERALL_FRAGILITY_INSTITUTIONS_FILTER_10"
#   )
  
#   # Emerging risk score as all high risk scores
#   riskflags$OVERALL_FLAGS_FILTER_10 <- rowSums(riskflags[filter10nam] >= 7, na.rm = T) 
  
#   # Emerging risk score as high + med
#   riskflags$OVERALL_FLAGS_FILTER_10_MED <-  rowSums(riskflags[filter10nam] >= 7, na.rm = T) + (rowSums(riskflags[filter10nam] < 7 & riskflags[filter10nam] >= 5, na.rm = T) / 2)
  
#   #-----------------------------—Calculate Overall Alert w/ Filter: Emerging & Underlying >= 7 ----------------------------------------------
#   riskflags <- riskflags %>%
#     mutate(
#       OVERALL_HEALTH_FILTER_7 = case_when(
#         UNDERLYING_RISK_HEALTH >= 7 & EMERGING_RISK_HEALTH >= 7 ~ 10,
#         TRUE ~ 0
#       ),
#       OVERALL_FOOD_SECURITY_FILTER_7 = case_when(
#         UNDERLYING_RISK_FOOD_SECURITY >= 7 & EMERGING_RISK_FOOD_SECURITY >= 7 ~ 10,
#         TRUE ~ 0
#       ),
#       OVERALL_MACRO_FISCAL_FILTER_7 = case_when(
#         UNDERLYING_RISK_MACRO_FISCAL >= 7 & EMERGING_RISK_MACRO_FISCAL >= 7 ~ 10,
#         TRUE ~ 0
#       ),
#       OVERALL_SOCIOECONOMIC_VULNERABILITY_FILTER_7 = case_when(
#         UNDERLYING_RISK_SOCIOECONOMIC_VULNERABILITY >= 7 & EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY >= 7 ~ 10,
#         TRUE ~ 0
#       ),
#       OVERALL_NATURAL_HAZARDS_FILTER_7 = case_when(
#         UNDERLYING_RISK_NATURAL_HAZARDS >= 7 & EMERGING_RISK_NATURAL_HAZARDS >= 7 ~ 10,
#         TRUE ~ 0
#       ),
#       OVERALL_FRAGILITY_INSTITUTIONS_FILTER_7 = case_when(
#         UNDERLYING_RISK_FRAGILITY_INSTITUTIONS >= 7 & EMERGING_RISK_FRAGILITY_INSTITUTIONS >= 7 ~ 10,
#         TRUE ~ 0
#       )
#     )
  
#   # Calculate total emerging risk scores for SQ
#   filter7nam <- c(
#     "OVERALL_HEALTH_FILTER_7", "OVERALL_FOOD_SECURITY_FILTER_7",
#     "OVERALL_MACRO_FISCAL_FILTER_7","OVERALL_SOCIOECONOMIC_VULNERABILITY_FILTER_7",
#     "OVERALL_NATURAL_HAZARDS_FILTER_7",
#     "OVERALL_FRAGILITY_INSTITUTIONS_FILTER_7"
#   )
  
#   # Emerging risk score as all high risk scores
#   riskflags$OVERALL_FLAGS_FILTER_7 <- rowSums(riskflags[filter7nam] >= 7, na.rm = T) 
  
#   # Emerging risk score as high + med
#   riskflags$OVERALL_FLAGS_FILTER_7_MED <-  rowSums(riskflags[filter7nam] >= 7, na.rm = T) + (rowSums(riskflags[filter7nam] < 7 & riskflags[filter7nam] >= 5, na.rm = T) / 2)
  
#   #-----------------------------—Calculate Overall Alert w/ Filter: Emerging & Underlying >= 7, Medium if one is below 7 but >= 5 ----------------------------------------------
#   riskflags <- riskflags %>%
#     mutate(
#       OVERALL_HEALTH_FILTER_7_5 = case_when(
#         UNDERLYING_RISK_HEALTH >= 7 & EMERGING_RISK_HEALTH >= 7 ~ 10,
#         UNDERLYING_RISK_HEALTH >= 7 & EMERGING_RISK_HEALTH >= 5 ~ 5,
#         UNDERLYING_RISK_HEALTH >= 5 & EMERGING_RISK_HEALTH >= 7 ~ 5,
#         TRUE ~ 0
#       ),
#       OVERALL_FOOD_SECURITY_FILTER_7_5 = case_when(
#         UNDERLYING_RISK_FOOD_SECURITY >= 7 & EMERGING_RISK_FOOD_SECURITY >= 7 ~ 10,
#         UNDERLYING_RISK_FOOD_SECURITY >= 7 & EMERGING_RISK_FOOD_SECURITY >= 7 ~ 10,
#         UNDERLYING_RISK_FOOD_SECURITY >= 7 & EMERGING_RISK_FOOD_SECURITY >= 7 ~ 10,
#         TRUE ~ 0
#       ),
#       OVERALL_MACRO_FISCAL_FILTER_7_5 = case_when(
#         UNDERLYING_RISK_MACRO_FISCAL >= 7 & EMERGING_RISK_MACRO_FISCAL >= 7 ~ 10,
#         UNDERLYING_RISK_MACRO_FISCAL >= 7 & EMERGING_RISK_MACRO_FISCAL >= 7 ~ 10,
#         UNDERLYING_RISK_MACRO_FISCAL >= 7 & EMERGING_RISK_MACRO_FISCAL >= 7 ~ 10,
#         TRUE ~ 0
#       ),
#       OVERALL_SOCIOECONOMIC_VULNERABILITY_FILTER_7_5 = case_when(
#         UNDERLYING_RISK_SOCIOECONOMIC_VULNERABILITY >= 7 & EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY >= 7 ~ 10,
#         UNDERLYING_RISK_SOCIOECONOMIC_VULNERABILITY >= 7 & EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY >= 5 ~ 5,
#         UNDERLYING_RISK_SOCIOECONOMIC_VULNERABILITY >= 5 & EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY >= 7 ~ 5,
#         TRUE ~ 0
#       ),
#       OVERALL_NATURAL_HAZARDS_FILTER_7_5 = case_when(
#         UNDERLYING_RISK_NATURAL_HAZARDS >= 7 & EMERGING_RISK_NATURAL_HAZARDS >= 7 ~ 10,
#         UNDERLYING_RISK_NATURAL_HAZARDS >= 7 & EMERGING_RISK_NATURAL_HAZARDS >= 5 ~ 5,
#         UNDERLYING_RISK_NATURAL_HAZARDS >= 5 & EMERGING_RISK_NATURAL_HAZARDS >= 7 ~ 5,
#         TRUE ~ 0
#       ),
#       OVERALL_FRAGILITY_INSTITUTIONS_FILTER_7_5 = case_when(
#         UNDERLYING_RISK_FRAGILITY_INSTITUTIONS >= 7 & EMERGING_RISK_FRAGILITY_INSTITUTIONS >= 7 ~ 10,
#         UNDERLYING_RISK_FRAGILITY_INSTITUTIONS >= 7 & EMERGING_RISK_FRAGILITY_INSTITUTIONS >= 5 ~ 5,
#         UNDERLYING_RISK_FRAGILITY_INSTITUTIONS >= 5 & EMERGING_RISK_FRAGILITY_INSTITUTIONS >= 7 ~ 5,
#         TRUE ~ 0
#       )
#     )
  
#   # Calculate total emerging risk scores for SQ
#   filter75nam <- c(
#     "OVERALL_HEALTH_FILTER_7_5", "OVERALL_FOOD_SECURITY_FILTER_7_5",
#     "OVERALL_MACRO_FISCAL_FILTER_7_5","OVERALL_SOCIOECONOMIC_VULNERABILITY_FILTER_7_5",
#     "OVERALL_NATURAL_HAZARDS_FILTER_7_5",
#     "OVERALL_FRAGILITY_INSTITUTIONS_FILTER_7_5"
#   )
  
#   # Emerging risk score as all high risk scores
#   riskflags$OVERALL_FLAGS_FILTER_7_5 <- rowSums(riskflags[filter75nam] >= 7, na.rm = T) 
  
#   # Emerging risk score as high + med
#   riskflags$OVERALL_FLAGS_FILTER_7_5_MED <-  rowSums(riskflags[filter75nam] >= 7, na.rm = T) + (rowSums(riskflags[filter75nam] < 7 & riskflags[filter75nam] >= 5, na.rm = T) / 2)
#   return(riskflags)
# }
# #
# ##
# ### ********************************************************************************************
# ####    CREATE DATABASE OF RELIABILITY SCORES ----
# ### ********************************************************************************************
# ##
# #

# calculate_reliability <- function(globalrisk) {
#   # Calculate the number of missing values in each of the source indicators for the various risk components (as a proportion)
#   reliabilitysheet <- globalrisk %>%
#     mutate(
#       RELIABILITY_UNDERLYING_HEALTH = rowSums(is.na(globalrisk %>%
#                                                       dplyr::select(H_HIS_Score_norm, H_INFORM_rating.Value_norm)),
#                                               na.rm = T
#       ) / 2,
#       RELIABILITY_UNDERLYING_FOOD_SECURITY = case_when(
#         is.na(F_Proteus_Score_norm) ~ 1,
#         TRUE ~ 0
#       ),
#       RELIABILITY_UNDERLYING_MACRO_FISCAL = rowSums(is.na(globalrisk %>%
#                                                             dplyr::select(
#                                                               M_EIU_Score_12m_norm 
#                                                             )),
#                                                     na.rm = T
#       ) / 4,
#       RELIABILITY_UNDERLYING_SOCIOECONOMIC_VULNERABILITY = case_when(
#         is.na(S_INFORM_vul_norm) ~ 1,
#         TRUE ~ 0
#       ),
#       RELIABILITY_UNDERLYING_NATURAL_HAZARDS = case_when(
#         is.na(NH_Hazard_Score_norm) ~ 1,
#         TRUE ~ 0
#       ),
#       RELIABILITY_UNDERLYING_FRAGILITY_INSTITUTIONS = case_when(
#         is.na(Fr_FCS_normalised) ~ 1,
#         TRUE ~ 0
#       ),
#       RELIABILITY_EMERGING_HEALTH = rowSums(is.na(globalrisk %>% 
#                                                     dplyr::select(
#                                                       H_Oxrollback_score_norm,
#                                                       H_Covidgrowth_casesnorm,
#                                                       H_Covidgrowth_deathsnorm,
#                                                       H_new_cases_smoothed_per_million_norm,
#                                                       H_new_deaths_smoothed_per_million_norm,
#                                                       H_GovernmentResponseIndexForDisplay_norm
#                                                     )),
#                                             na.rm = T
#       ) / 6,
#       RELIABILITY_EMERGING_FOOD_SECURITY = rowSums(is.na(globalrisk %>%
#                                                            dplyr::select(
#                                                              F_fews_crm_norm,
#                                                              F_fao_wfp_warning,
#                                                              F_fpv_rating,
#                                                            )),
#                                                    na.rm = T
#       ) / 2,
#       EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY = rowSums(is.na(globalrisk %>%
#                                                                   dplyr::select(
#                                                                     S_pov_comb_norm,
#                                                                     S_change_unemp_norm,
#                                                                     S_income_support.Rating_crm_norm,
#                                                                     S_Household.risks,
#                                                                     S_phone_average_index_norm
#                                                                   )),
#                                                           na.rm = T
#       ) / 3,
#       RELIABILITY_EMERGING_MACRO_FISCAL = rowSums(is.na(globalrisk %>%
#                                                           dplyr::select(
#                                                             M_EIU_12m_change_norm
#                                                           )),
#                                                   na.rm = T
#       ) / 4,
#       RELIABILITY_EMERGING_NATURAL_HAZARDS = rowSums(is.na(globalrisk %>%
#                                                              dplyr::select(
#                                                                NH_GDAC_Hazard_Score_Norm,
#                                                                NH_Hazard_Score_norm
#                                                              )),
#                                                      na.rm = T
#       ) / 3,
#       RELIABILITY_EMERGING_FRAGILITY_INSTITUTIONS = rowSums(is.na(globalrisk %>%
#                                                                     dplyr::select(
#                                                                       Fr_REIGN_Normalised,
#                                                                       Fr_Displaced_UNHCR_Normalised,
#                                                                       Fr_BRD_Normalised,
#                                                                     )),
#                                                             na.rm = T
#       ) / 3
#     )
  
#   # Create total reliability variabiles
#   reliabilitysheet <- reliabilitysheet %>%
#     mutate(
#       RELIABILITY_SCORE_UNDERLYING_RISK = round(rowMeans(dplyr::select(., starts_with("RELIABILITY_UNDERLYING"))), 1),
#       RELIABILITY_SCORE_EMERGING_RISK = round(rowMeans(dplyr::select(., starts_with("RELIABILITY_EMERGING"))), 1)
#     ) %>%
#     dplyr::select(
#       Countryname, Country, RELIABILITY_SCORE_UNDERLYING_RISK, RELIABILITY_SCORE_EMERGING_RISK, RELIABILITY_UNDERLYING_HEALTH, RELIABILITY_UNDERLYING_FOOD_SECURITY,
#       RELIABILITY_UNDERLYING_MACRO_FISCAL, RELIABILITY_UNDERLYING_SOCIOECONOMIC_VULNERABILITY,
#       RELIABILITY_UNDERLYING_NATURAL_HAZARDS, RELIABILITY_UNDERLYING_FRAGILITY_INSTITUTIONS,
#       RELIABILITY_EMERGING_HEALTH, RELIABILITY_EMERGING_FOOD_SECURITY,
#       EMERGING_RISK_SOCIOECONOMIC_VULNERABILITY, RELIABILITY_EMERGING_MACRO_FISCAL,
#       RELIABILITY_EMERGING_NATURAL_HAZARDS, RELIABILITY_EMERGING_FRAGILITY_INSTITUTIONS
#     ) %>%
#     arrange(Country)
#   return(reliabilitysheet)
# }
# # Write as a csv file for the reliability sheet
# # write.csv(reliabilitysheet, "risk-sheets/reliability-sheet.csv")

# #------------------------------—Combine the reliability sheet with the global database------------------------------------
# # reliable <- reliabilitysheet %>%
# #   dplyr::select(Countryname, Country, RELIABILITY_SCORE_UNDERLYING_RISK, RELIABILITY_SCORE_EMERGING_RISK)

# # globalrisk <- left_join(globalrisk, reliable, by = c("Countryname", "Country"))

# # # Save database of all risk indicators (+ reliability scores)
# # write.csv(globalrisk, "Risk_sheets/Global_compound_risk_database.csv")
# # write.csv(globalrisk, "output/risk-sheets/Global_compound_risk_database.csv")
# # write.csv(globalrisk, paste0("output/risk-sheets/archive/", Sys.Date(),"-Global_compound_risk_database.csv"))

# # # #------------------------------—Combine the reliability sheet with the summary risk flag sheet-----------------------------
# join_risk_reliable <- function(risk, reliable) {
#   reliable <- reliable %>%
#     dplyr::select(Countryname, Country, RELIABILITY_SCORE_UNDERLYING_RISK, RELIABILITY_SCORE_EMERGING_RISK)
  
#   riskflags <- left_join(risk %>%
#                            dplyr::select(
#                              "Countryname", "Country",
#                              contains(c("_AV", "_SQ", "_ALT", "_FILTER", "_GEO", "UNDERLYING_", "EMERGING_", "coefvar"))
#                            ),
#                          reliable,
#                          by = c("Countryname", "Country")) %>%
#     dplyr::select(-contains("S_phone"))
#   return(riskflags)
# }
# # 
# # # Write csv file of all risk flags (+reliability scores)

# # write.csv(riskflags, "Risk_sheets/Compound_Risk_Flag_Sheets.csv")
# # write.csv(riskflags, "output/risk-sheets/Compound_Risk_Flag_Sheets.csv")
# # write.csv(riskflags, paste0("output/risk-sheets/archive/", Sys.Date(), "-Compound_Risk_Flag_Sheets.csv"))

# #
# ##
# ### ********************************************************************************************
# ####    LONG AND MINIMAL OUTPUT FILE ----
# ### ********************************************************************************************
# ##
# #

# # —Only use the relevant aggregated metrics; renamed for dashboard ----
# select_aggregates <- function(riskflags) {
#   names <- read_csv("riskflags-dashboard-names.csv")
#   riskflags_select <- riskflags[,c("Country", "Countryname", names$old_name)]
#   names(riskflags_select) <- c("Country", "Countryname", names$new_name)
#   return(riskflags_select)
# }

# # —Lengthen data ----
# lengthen_flags <- function(riskflags_select) {
#   flags_long <- pivot_longer(riskflags_select, !c("Country", "Countryname"), values_to = "Value") %>%
#     separate(name, into = c("Outlook", "Key"), sep = "_" ) %>%
#     mutate(
#       Dimension = case_when(
#         grepl("Flag", Key) ~ "Flag",
#         TRUE ~ Key),
#       Key = case_when(
#         Dimension == "Flag" ~ "Number of Flags",
#         TRUE ~ Key),
#       `Data Level` = case_when(
#         Dimension == "Flag" ~ "Flag Count",
#         TRUE ~ "Dimension Value"
# ),
#       `Display Status` = "Main"
#     )
#   return(flags_long)
# }

# lengthen_inds <- function(indicators_select) {
#   indicators_long <- pivot_longer(indicators_select, !c("Country", "Countryname"), values_to = "Value") %>% 
#     separate(name, into = c("Outlook", "Dimension", "Key"), sep = "_" ) %>%
#     mutate(`Data Level` = "Indicator",
#            `Display Status` = "Secondary")
#   return(indicators_long)
# }

# join_inds_flags <- function(indicators_long, flags_long) {
#   # indicators_select <- read_csv("crm-indicators.csv") # written in `alt-sheets.R` 
  
#   long <- rbind(indicators_long, flags_long) %>%
#     mutate(
#       `Overall Contribution` = case_when(
#         Outlook == "Overall" ~ T,
#         `Data Level` == "Indicator" ~ T,
#         TRUE ~ F),
#       `Risk Label` = case_when(
#         `Data Level` == 'Dimension Value' & (Outlook == 'Emerging' | Outlook == 'Underlying') & Value >= 10 ~ "High",
#         `Data Level` == 'Dimension Value' & (Outlook == 'Emerging' | Outlook == 'Underlying') & Value >= 7 ~ "Medium",
#         `Data Level` == 'Dimension Value' & (Outlook == 'Emerging' | Outlook == 'Underlying') & Value >= 0 ~ "Low",
#         `Data Level` == 'Dimension Value' & (Outlook == 'Overall') & Value >= 7 ~ "High",
#         `Data Level` == 'Dimension Value' & (Outlook == 'Overall') & Value >= 5 ~ "Medium",
#         `Data Level` == 'Dimension Value' & (Outlook == 'Overall') & Value >= 0 ~ "Low",
#         `Data Level` == 'Flag Count' ~ as.character(floor(Value)),
#         `Data Level` == 'Indicator' ~ as.character(floor(Value))
#       ),
#       Value = case_when(
#         `Data Level` == "Flag Count" ~ (Value),
#         TRUE ~ floor(Value)
#       ),
#       Value_Char = case_when(
#         `Data Level` == "Flag Count" ~ as.character(Value),
#         TRUE ~ as.character(floor(Value))
#       )
#     ) %>% mutate(
#       Outlook = factor(Outlook, levels = c("Overall", "Underlying", "Emerging")),
#       Dimension = factor(Dimension, c("Flag", "Food Security", "Conflict and Fragility", "Health", "Macro Fiscal",  "Natural Hazard", "Socioeconomic Vulnerability")),
#       Country = factor(Country, levels = sort(unique(Country))),
#       Countryname = factor(Countryname, levels = sort(unique(Countryname))),
#       `Data Level` = factor(`Data Level`, c("Flag Count", "Dimension Value", "Indicator")),  
#       `Display Status` = factor(`Display Status`)
#     ) %>%
#     relocate(Value_Char, `Risk Label`, .after = Value) %>%
#     relocate(`Data Level`, .before = Outlook) %>%
#     arrange(Country, Outlook, Dimension, `Data Level`) %>%
#     subset(Key != "Indicator Reliability") %>%
#     mutate(Index = row_number(), .before = 1)
#   return(long)
# }

# # Could replace with source updates, using input archives
# track_indicator_updates <- function(long) {
#   # prev should be more robustly defined – like filtering crm-output-all.csv
#   # for most recent before current/as_of date
#   prev <- read_csv("output/crm-output-latest.csv")
  
#   indicators_list <- as.data.frame(read.csv("indicators-list.csv"))
  
#   updateLog <- data.frame(
#     Indicator = indicators_list$Indicator,
#     Changed_Countries = sapply(indicators_list$Indicator, function(indicator)  {
#       long_ind <- subset(long, Key == indicator)
#       prev_ind <- subset(prev, Key == indicator)
      
#       # Make sure Countries line up in the two columns
#       wrong_rows <- which(subset(long_ind)[, 'Country'] != subset(prev_ind)[, 'Country'])
#       if (sum(wrong_rows) > 0) {
#         warning("Rows are not aligned. See following rows in the newer dataframe: ", long_ind$Index[wrong_rows,])
#       }
      
#       changes <- which(subset(long_ind)[, 'Value'] != subset(prev_ind)[, 'Value'])
      
#       return(length(changes))
#     })) %>%
#     mutate(
#       Update_Date = case_when(
#         Changed_Countries > 0 ~ Sys.Date()
#       ))
  
#   updateLog <- updateLog %>% .[!is.na(.$Update_Date),]    
#   updateLogPrevious <- read_csv("output/updates-log.csv")
#   updateLogCombined <- rbind(updateLog, updateLogPrevious) %>%
#     .[!is.na(.$Update_Date),]
#   return(updateLogCombined)
# }

# # Write long output file (crm-output-latest.csv) ----
# # outputPrevious <- read_csv("output/crm-output-latest.csv")
# # write_csv(long, "output/crm-output-latest.csv")

# append_output_all <- function(long) {
#   outputAll <- read_csv("output/crm-output-all.csv")
#   # colnames(outputAll) <- colnames(outputAll) %>% gsub("\\.", " ", .)
  
#   long <- long %>%
#     mutate(
#       Date = Sys.Date(),
#       Record_ID = if(!is.null(outputAll$Record_ID)) {
#         max(outputAll$Record_ID) + long$Index
#       } else {Index}
#     ) %>%
#     relocate(Record_ID, .before = 1)
  
#   outputAll <- rbind(outputAll, long)
#   write_csv(outputAll, "output/crm-output-all.csv")
# }
# # Country changes ----
# # List countries that changed 
# # flagChanges  <- merge(subset(long, Dimension == "Flag")[,c("Index", "Countryname", "Country", "Outlook", "Value")],
# #       subset(prev, Dimension == "Flag")[,c("Index", "Value")],
# #       by = "Index") %>%
# #        rename(Count = Value.x,
# #               `Previous Count` = Value.y) %>%
# #               # Risk = `Risk Label.x`,
# #               # `Previous Risk` = `Risk Label.y`) %>%
# #   mutate(`Flag Change` = Count - `Previous Count`
# #          # `Risk Change` = as.numeric(Risk) - as.numeric(`Previous Risk`)
# #          )

countFlagChanges <- function(data, early = Sys.Date() - 1 , late = Sys.Date()) {
  data <- data %>%
    select(Record_ID, Index, Countryname, Country, Outlook, Value, Date, `Data Level`) %>%
    subset(`Data Level` == 'Flag Count') %>%
    select(-`Data Level`) %>%
    arrange(Date) %>%
    group_by(Index)
  current <- subset(data, Date <= late) %>%
    slice_tail()
  prior <- subset(data, Date <= early) %>%
    slice_tail()
  if(sum(select(prior, -Value, -Date, -Record_ID) != select(current, -Value, -Date, -Record_ID)) > 0) warning("Rows do not match between comparison dates.")
  current <- current %>%
    rename(Count = Value)
  prior <- prior %>%
    rename(`Prior Count` = Value,
           `Prior Date` = Date,
           `Prior Record_ID` = Record_ID)
  combined <- merge(current, select(prior, -Countryname, -Country, -Outlook), by = "Index") %>%
    mutate(`Flag Change` = Count - `Prior Count`) %>% 
    relocate(`Prior Record_ID`, .after = Record_ID) %>%
    relocate(`Count`, .after = Date) %>%
    relocate(`Prior Date`, .after = Date)
  return(combined)
}

# # flagChanges <- countFlagChanges(outputAll)

# # rmarkdown::render('output-report.Rmd', output_format = c("html_document", "pdf_document"), output_file = paste0("reports/", Sys.Date(), "-report") %>% rep(2))
# # write.csv(flagChanges, "output/reports/flag-changes.csv")

# # rmarkdown::render('output-report.Rmd', output_format = "html_document", output_file = paste0("output/reports/", Sys.Date(), "-report"))


# Add in-crisis labels
label_crises <- function() {
  acaps_high_severity <- function() {
    # SPLIT UP INTO INPUTS SECTION
    # Load website
    h <- new_handle()
    handle_setopt(h, ssl_verifyhost = 0, ssl_verifypeer=0)
    curl_download(url="https://www.acaps.org/countries", "acaps.html", handle = h)
    acaps <- read_html("acaps.html")
    unlink("acaps.html")
    
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

      df <- data.frame(
              Countryname = country,
              value = country_level
              # crisis = crises,
              # value = values
      )
      return(df)
      }) %>%
      bind_rows() %>%
      mutate(
        Countryname = case_when(
          Countryname == "CAR" ~ "Central African Republic",
          TRUE ~ Countryname),
        Country = countrycode(Countryname, origin = "country.name", destination = "iso3c"),
        .before = 1)

  high_severity_countries <- pull(subset(acaps_list, value >= 4, select = Country))
    return(high_severity_countries)
  }

  in_crisis <- acaps_high_severity()
  # in_crisis <- acaps[which(rowSums(acaps[,3:6])>0),2]

  latest <- read.csv(paste_path(output_directory, "crm-dashboard-data.csv"))
  crisis_rows <- latest
  crisis_rows[,c(1,4:12)] <- NA
  # crisis_rows$`Risk Label` <- as.character(crisis_rows$`Risk Label`)
  crisis_rows <- distinct(crisis_rows)
  crisis_rows$Data.Level <- "Crisis Status"
  # crisis_rows$Key <- ""
  crisis_rows[,c("Value")] <- 0
  crisis_rows[which(crisis_rows$Country %in% in_crisis),c("Value")] <- 1
  crisis_rows <- dplyr::mutate(crisis_rows,
                        Risk.Label =
                          dplyr::case_when(
                            Value == 1 ~ paste0("*", Countryname),
                            Value == 0 ~ Countryname
                          ))
  crisis_rows$Overall.Contribution <- FALSE
  crisis_rows <- dplyr::mutate(crisis_rows, Index = dplyr::row_number() + max(latest$Index))
  comb <- rbind(latest, crisis_rows)
  # write_csv(comb, "output/scheduled/crm-dashboard-data-with-crisis.csv")
  return(comb)
}
