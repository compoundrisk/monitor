# Databricks notebook source
# For some reason `setwd(...)` loads rlang 0.4.12 to the namespace, so this needs to precede
# library(rlang, lib.loc = "/dbfs/mnt/CompoundRiskMonitor/lib")

# COMMAND ----------

as_of <- Sys.Date()
# format <- "csv" # or "spark" or "both"; format of how input archives are saved (in case I switch )

if (dir.exists("/dbfs")) {
  # The mounted path is the path to mounted storage on Databricks
  mounted_path <- "/dbfs/mnt/CompoundRiskMonitor"
  # mounted_output_directory <- paste_path(mounted_path, "output")
  working_path <- "/tmp/crm/monitor"
  setwd(working_path)
  } else {
    mounted_path <- ""
    working_path <- ""
  }

inputs_archive_path <- paste_path(mounted_path, "output/inputs-archive/")

# if run is a job, save files to "output/scheduled"; if manually, save to "output/manual"
run_type <- tryCatch(dbutils.widgets.get("run_type"), error = function(e) {return("manual")})
output_directory <- paste_path(mounted_path, "output/", run_type) #paste_path("output/", run_type)

mounted_output_directory <- paste_path(mounted_path, "output/", run_type)


# COMMAND ----------

error_delay <- tryCatch(dbutils.widgets.get("error_delay"), error = function(e) {return(F)})
error_delay <- if (error_delay) T else F

# COMMAND ----------

source("src/fns/prep.R")
source("src/fns/indicators.R")
source("src/fns/aggregation.R")

# Create a temporary folder to save everything in? Is this helpful
# dir.create("tmp")

# COMMAND ----------

# Reference table of all indicators. Used for selecting indicators by
# dimension and outlook. To change which variables are selected, edit
# this table.

# COMMAND ----------
ensure_directory_exists(output_directory)
ensure_directory_exists(mounted_output_directory)
ensure_directory_exists(mounted_output_directory, "archive")
archive_directory <- ensure_directory_exists(mounted_output_directory, "archive", as.character(as_of),
                                             new = T, suffix = "run_", return = T)
dim_path <- ensure_directory_exists(output_directory, "dimensions", return = T)
# ensure_directory_exists(output_directory, "dimensions/archive")
dim_archive_path <- ensure_directory_exists(archive_directory, "dimensions", return = T)
# ensure_directory_exists(output_directory, "aggregated-archive")
# aggregated_archive_path <- ensure_directory_exists(output_directory, "aggregated-archive", Sys.Date(), new = T, suffix = "run_", return = T)

# COMMAND ----------

# COMMAND ----------

# HEALTH
# Writes sheet of health variables to output/risk-sheets/health-sheet.csv
lap_start()
health_sheet <- aggregate_dimension(
  "Health", # Important for these dimension names to match the names to match what's in indicators-list.csv
  # acaps_category_process(as_of, category = "health", prefix = "H_") %>%
    # delay_error(return = NA, on = error_delay),
  ghsi_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  # oxford_openness_process(as_of = as_of),
  # owid_covid_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  # Oxres_process(as_of = as_of),
  # inform_covid_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  dons_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  ifrc_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay))
# Does it make sense to move all output writing to the end, in one spot?
# Write to a temporary directory, and then move everything to the intended spot?
# (/output/scheduled/ or /output/manual/run-date/)
# Do I even need to
multi_write.csv(health_sheet, "health-sheet.csv", c(dim_path, dim_archive_path))
lap_print("Health sheet is aggregated and saved.")

# COMMAND ----------

# FOOD
# Writes sheet of food variables to output/risk-sheets/food-sheet.csv
lap_start()
food_sheet <- aggregate_dimension(
  "Food Security",
  gfsi_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  proteus_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  fews_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  fpi_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  fao_wfp_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay))
multi_write.csv(food_sheet, "food-sheet.csv", c(dim_path, dim_archive_path))
lap_print("Food sheet is aggregated and saved.")

# COMMAND ----------

# MACRO FISCAL
# Writes sheet of macro fiscal variables to output/risk-sheets/macro-sheet.csv
lap_start()
macro_sheet <- aggregate_dimension(
  "Macro Fiscal",
  eiu_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  macrofin_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay))
multi_write.csv(macro_sheet, "macro-sheet.csv", c(dim_path, dim_archive_path))
lap_print("Macro sheet is aggregated and saved.")


# COMMAND ----------

# SOCIO-ECONOMIC
# Writes sheet of socio-economic variables to output/risk-sheets/socio-sheet.csv
lap_start()
socio_sheet <- aggregate_dimension(
  "Socioeconomic Vulnerability",
  inform_socio_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  # income_support_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  mpo_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  macrofin_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  # phone_process(as_of = as_of),
  # Fix warnings
  imf_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  acaps_risk_list_process(as_of, dim = "Socioeconomic", prefix = "S_") %>% delay_error(return = NA, on = error_delay))
multi_write.csv(socio_sheet, "socio-sheet.csv", c(dim_path, dim_archive_path))
lap_print("Socio sheet is aggregated and saved.")

# COMMAND ----------

# NATURAL HAZARDS
# Writes sheet of natural hazard variables to output/risk-sheets/natural_hazards-sheet.csv
lap_start()
natural_hazards_sheet <- aggregate_dimension(
  "Natural Hazard",
  gdacs_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  inform_nathaz_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  iri_process(drop_geometry = T, as_of = as_of) %>% delay_error(return = NA, on = error_delay), # Rename iri_forecast)
  locust_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  acaps_category_process(as_of, category = "natural", prefix = "NH_") %>% delay_error(return = NA, on = error_delay),
  acaps_risk_list_process(as_of, dim = "Natural Hazard", prefix = "NH_") %>% delay_error(return = NA, on = error_delay))
multi_write.csv(natural_hazards_sheet, "natural_hazards-sheet.csv", c(dim_path, dim_archive_path))
lap_print("Natural hazards sheet is aggregated and saved.")

# COMMAND ----------

# CONFLICT AND FRAGILITY
lap_start()
fragility_sheet <- aggregate_dimension(
  "Conflict and Fragility",
  # Unlike other dimensions. conflict uses arithmetic mean outlook to calculate overall
  overall_method = "geometric", 
  fsi_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  fcs_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  acled_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  acled_events_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  eiu_security_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay),
  acaps_risk_list_process(as_of, dim = "Conflict and Fragility", prefix = "Fr_") %>% delay_error(return = NA, on = error_delay),
  # reign_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay)),
  pseudo_reign_process(as_of = as_of) %>% delay_error(return = NA, on = error_delay))
multi_write.csv(fragility_sheet, "fragility-sheet.csv", c(dim_path, dim_archive_path))
lap_print("Fragility sheet is aggregated and saved.")

# For while we process a firewall request for ACAPS risk list and IFRC
write.csv(ifrc_process(as_of = as_of), "hosted-data/ifrc-epidemics-temp.csv", row.names = F)
write.csv(acaps_risk_list_process(as_of, dim = "Socioeconomic", prefix = "S_"), "hosted-data/acaps-socio-temp-auto.csv", row.names = F)
write.csv(acaps_risk_list_process(as_of, dim = "Natural Hazard", prefix = "NH_"), "hosted-data/acaps-natural-temp-auto.csv", row.names = F)
write.csv(acaps_risk_list_process(as_of, dim = "Conflict and Fragility", prefix = "Fr_"), "hosted-data/acaps-conflict-temp-auto.csv", row.names = F)

# COMMAND ----------

# Make sure to rename indicators with readable variable names
# (included in old `writeSourceCSV()`) – probably after joining dimension sheets

# COMMAND ----------

# Combine all dimension sheets (should this be a named function? wouldn't be much shorter)
all_dimensions <- list(
  health_sheet,
  food_sheet,
  macro_sheet,
  socio_sheet,
  natural_hazards_sheet,
  fragility_sheet) %>%
  # NB: reduce(., full_join, ...) is the equivalent of multiple sequential full_joins
  reduce(full_join, by = "Country") %>%
  count_flags(outlook = "emerging", high = 10, medium = 7) %>%
  count_flags(outlook = "underlying", high = 10, medium = 7) %>%
  count_flags(outlook = "overall", high = 7, medium = 5) %>%
  mutate(Countryname = iso2name(Country), .after = Country)

# write.csv(all_dimensions, paste0(output_directory, "crm-wide.csv"))
multi_write.csv(all_dimensions, "crm-wide.csv", c(output_directory, archive_directory))

# Rename with pretty names

long <- pretty_col_names(all_dimensions) %>%
  lengthen_data() %>%
  add_secondary_columns(as_of) %>%
  round_value_col() %>%
  factorize_columns() %>%
  order_columns_and_raws() %>%
  # mutate(Index = row_number(), .before = 1) # Do I need this? Even if not useful for matching, it is useful for sorting
  create_id()

# Make function: `write_dashboard_data(data)`? – or should it go after I've written the appended file,
# and it takes a date argument? I think yes
# dashboard_data <- subset(long, `Data Level` != "Reliability" & `Data Level` != "Raw Indicator Data") # %>%
dashboard_data <- subset(long, `Data Level` != "Reliability") %>%
  add_overall_indicators() # %>%
#  mutate(Index = row_number()) # Don't include because indices should match between long and dashboard
# write.csv(dashboard_data, paste0(output_directory, "/crm-dashboard-data.csv"))
multi_write.csv(dashboard_data, "crm-dashboard-data.csv", c(output_directory, archive_directory))
# write.csv(dashboard_data, paste0(output_directory, "/crm-runs/", Sys.Date(), "-crm-run.csv"))

# Fix so that this uses dashboard_data instead of reading the CSV
dashboard_crisis <- label_crises(dashboard_data)
multi_write.csv(dashboard_crisis, "crm-dashboard-data.csv", c(output_directory, archive_directory))
# write.csv(dashboard_crisis, paste_path(mounted_path, "staging/crm-dashboard-stg.csv"), row.names = F)
write.csv(dashboard_crisis, paste_path(mounted_path, "production/crm-dashboard-prod.csv"), row.names = F)

# track_indicator_updates()
# I've already written this. Do I still use it? My concern is that its reliance 
# on the previous version of the file is messy and error-prone.  Also, now that 
# I'm tracking inputs, I can track this directly.  But whether I use inputs or 
# output to track indicator changes, it shouldn't be order specific. Now, if I 
# run track_ind...() it depends on when I last ran it ...  which sort of makes 
# sense (can I envision an alternative cases?) but really there's no reason not 
# to just use the combined output file, and  compare against the previous date 
# (the way `countFlagChanges()` does? can I just abstract `flagChanges()`?)

all_runs <- append_if_exists(long, paste_path(output_directory, "crm-all-runs.csv"), col_types = 'dddccccccdccccD')

# all_runs <- subset(all_runs, !between(Date, as.Date("2022-11-15"), as.Date("2022-11-19")))
# Task: what if I run the monitor multiple times in a day? 
write_csv(all_runs, paste_path(output_directory, "crm-all-runs.csv"))
# multi_write.csv(all_runs, "crm-all-runs.csv", c(output_directory, archive_directory))

# test <- all_dimensions %>%
#   pivot_longer(., cols = -contains("country") & -contains("_labels") & -contains("_raw"), names_to = "Name", values_to = "Value") %>%
#   separate(Name, into = c("Outlook", "Key"), sep = "_", extra = "merge") #%>%
#   # pivot_longer(., cols = contains("_labels"), names)

# pivot wide data to long format (name `lengthen_data()`)

# COMMAND ----------

dimension_dates <- date_dimension_highs(all_runs)

# COMMAND ----------

# Edit to include reliability sheet output and to only take crm-wide.csv?
write_excel_source_files(
  all_dimensions = all_dimensions,
  health_sheet = join_dimension_dates(health_sheet, "Health"),
  food_sheet = join_dimension_dates(food_sheet, "Food Security"),
  macro_sheet = join_dimension_dates(macro_sheet, "Macro Fiscal"),
  socio_sheet = join_dimension_dates(socio_sheet, "Socioeconomic Vulnerability"),
  natural_hazards_sheet = join_dimension_dates(natural_hazards_sheet, "Natural Hazard"),
  fragility_sheet = join_dimension_dates(fragility_sheet, "Conflict and Fragility"),
  filepaths = F,
  archive = T,
  directory_path = paste_path(output_directory, "crm-excel/")) # Task: move this so it can use the `output_directory` variable at top of file

ind_list <- date_indicators(all_runs)
# write.csv(ind_list, "indicators-list-dated.csv", row.names = F, na = "")
write.csv(ind_list, paste_path(output_directory, "crm-excel", "indicators-list-dated.csv"), row.names = F, na = "")
# write.csv(ind_list, paste_path(archive_directory, "indicators-list-dated.csv"), row.names = F, na = "")

# Task: add code for actually updating `crm-dashboard.xlsx`
# (Probably a shell command?)

# COMMAND ----------

# Where does this best belong? After the *_process() functions? before all-runs?
release_delayed_errors()
