# Databricks notebook source
# For some reason `setwd(...)` loads rlang 0.4.12 to the namespace, so this needs to precede
# library(rlang, lib.loc = "/dbfs/mnt/CompoundRiskMonitor/lib")

# COMMAND ----------

as_of <- Sys.Date()
format <- "csv" # or "spark" or "both"; format of how input archives are saved (in case I switch )

# if run is a job, save files to "output/scheduled"; if manually, save to "output/manual"
run_type <- tryCatch(dbutils.widgets.get("run_type"), error = function(e) {return("manual")})
output_directory <- paste0("output/", run_type)

# COMMAND ----------

# setwd("../../../dbfs/mnt/CompoundRiskMonitor")
source("fns/prep.R")
source("fns/indicators.R")
source("fns/aggregation.R")

# Create a temporary folder to save everything in? Is this helpful
# dir.create("tmp")

# COMMAND ----------

# Reference table of all indicators. Used for selecting indicators by
# dimension and outlook. To change which variables are selected, edit
# this table.

# COMMAND ----------
ensure_directory_exists(output_directory)
ensure_directory_exists(output_directory, "archive")
archive_directory <- ensure_directory_exists(output_directory, "archive", Sys.Date(),
                                             new = T, suffix = "run_", return = T)
dim_path <- ensure_directory_exists(output_directory, "dimensions", return = T)
# ensure_directory_exists(output_directory, "dimensions/archive")
dim_archive_path <- ensure_directory_exists(archive_directory, "dimensions", return = T)
# ensure_directory_exists(output_directory, "aggregated-archive")
# aggregated_archive_path <- ensure_directory_exists(output_directory, "aggregated-archive", Sys.Date(), new = T, suffix = "run_", return = T)

# COMMAND ----------

# ACAPS
# acaps_sheet <- acaps_process(as_of = as_of, format = format)

# COMMAND ----------

# HEALTH
# Writes sheet of health variables to output/risk-sheets/health-sheet.csv
health_sheet <- aggregate_dimension(
  "Health", # Important for these dimension names to match the names to match what's in indicators-list.csv
  acaps_category_process(as_of, format, category = "health", prefix = "H_"),
  ghsi_process(as_of = as_of, format = format),
  # oxford_openness_process(as_of = as_of, format = format),
  owid_covid_process(as_of = as_of, format = format),
  # Oxres_process(as_of = as_of, format = format),
  inform_covid_process(as_of = as_of, format = format),
  dons_process(as_of = as_of, format = format))
# Does it make sense to move all output writing to the end, in one spot?
# Write to a temporary directory, and then move everything to the intended spot?
# (/output/scheduled/ or /output/manual/run-date/)
# Do I even need to
multi_write.csv(health_sheet, "health-sheet.csv", c(dim_path, dim_archive_path))

# COMMAND ----------

# FOOD
# Writes sheet of food variables to output/risk-sheets/food-sheet.csv
food_sheet <- aggregate_dimension(
  "Food Security",
  proteus_process(as_of = as_of, format = format),
  fews_process(as_of = as_of, format = format),
  fpi_process(as_of = as_of, format = format),
  fao_wfp_process(as_of = as_of, format = format))
multi_write.csv(food_sheet, "food-sheet.csv", c(dim_path, dim_archive_path))

# COMMAND ----------

# MACRO FISCAL
# Writes sheet of macro fiscal variables to output/risk-sheets/macro-sheet.csv
macro_sheet <- aggregate_dimension(
  "Macro Fiscal",
  eiu_process(as_of = as_of, format = format)
)
multi_write.csv(macro_sheet, "macro-sheet.csv", c(dim_path, dim_archive_path))

# COMMAND ----------

# SOCIO-ECONOMIC
# Writes sheet of socio-economic variables to output/risk-sheets/socio-sheet.csv
socio_sheet <- aggregate_dimension(
  "Socioeconomic Vulnerability",
  inform_socio_process(as_of = as_of, format = format),
  income_support_process(as_of = as_of, format = format),
  mpo_process(as_of = as_of, format = format),
  macrofin_process(as_of = as_of, format = format),
  # phone_process(as_of = as_of, format = format),
  # Fix warnings
  imf_process(as_of = as_of, format = format))
multi_write.csv(socio_sheet, "socio-sheet.csv", c(dim_path, dim_archive_path))

# COMMAND ----------

# NATURAL HAZARDS
# Writes sheet of natural hazard variables to output/risk-sheets/natural_hazards-sheet.csv
natural_hazards_sheet <- aggregate_dimension(
  "Natural Hazard",
  gdacs_process(as_of = as_of, format = format),
  inform_nathaz_process(as_of = as_of, format = format),
  #   iri_process(drop_geometry = T, as_of = as_of, format = format), # Rename iri_forecast)
  iri_process_temp(),
  locust_process(as_of = as_of, format = format),
  acaps_category_process(as_of, format, category = "natural", prefix = "NH_"))
multi_write.csv(natural_hazards_sheet, "natural_hazards-sheet.csv", c(dim_path, dim_archive_path))

# COMMAND ----------

# CONFLICT AND FRAGILITY
# Writes sheet of conflict and fragility variables to output/risk-sheets/fragility-sheet.csv
# REIGN uses fcs, so run it first to make it available
fcs <- fcs_process(as_of = as_of, format = "csv")

fragility_sheet <- aggregate_dimension(
  "Conflict and Fragility",
  fcs,
  un_idp_process(as_of = as_of, format = format),
  acled_hdx_process(as_of = as_of, format = format),
  # reign_process(as_of = as_of, format = format)),
  pseudo_reign_process(as_of = as_of, format = format))
# fragility_sheet <- fragility_sheet %>%
#   rename_with(
#     .fn = ~ paste0("Fr_", .),
#     .cols = colnames(.)[!colnames(.) %in% c("Country", "Countryname")]
#   )
multi_write.csv(fragility_sheet, "fragility-sheet.csv", c(dim_path, dim_archive_path))

# COMMAND ----------

# TOMORROW: Does it actually make more sense to join all the indicators at once, and then just
# lapply the selecting aggregating files? It might be better to keep them separate because that
# leaves more room for nuance – bundling everything up too tight means I can't treat Fragility
# different than food, e.g..

# write_sheet <- function(dim, sheet, format)   {
#   if (format == "csv" | format == "both") {
#     write.csv(sheet, paste0("output/risk-sheets/", dim, "-sheet.csv"))
#   }
#   if (format == "tbl" | format == "both") {
#     # Write Spark DataFrame
#   }
#   if (format == "return") {
#     return(sheet)
#   }
# }
# write_sheet("fragility", fragility_sheet, format = format)

# COMMAND ----------

# Make sure to rename indicators with readable variable names
# (included in old `writeSourceCSV()`) – probably after joining dimension sheets

# COMMAND ----------

# Combine all dimension sheets (should this be a named function? wouldn't be much shorter)
# reduce(., full_join, ...) is the equivalent of multiple sequential full_joins
all_dimensions <- list(
  health_sheet,
  food_sheet,
  macro_sheet,
  socio_sheet,
  natural_hazards_sheet,
  fragility_sheet) %>%
  reduce(full_join, by = "Country") %>%
  count_flags(outlook = "emerging", high = 10, medium = 7) %>%
  count_flags(outlook = "underlying", high = 10, medium = 7) %>%
  count_flags(outlook = "overall", high = 7, medium = 5) %>%
  mutate(Countryname = countrycode(
      Country,
      origin = "iso3c",
      destination = "country.name"),
    .after = Country)

# write.csv(all_dimensions, paste0(output_directory, "crm-wide.csv"))
multi_write.csv(all_dimensions, "crm-wide.csv", c(output_directory, archive_directory))

# Rename with pretty names

long <- pretty_col_names(all_dimensions) %>%
  lengthen_data() %>%
  add_secondary_columns() %>%
  round_value_col() %>%
  factorize_columns() %>%
  order_columns_and_raws() %>%
  # mutate(Index = row_number(), .before = 1) # Do I need this? Even if not useful for matching, it is useful for sorting
  create_id()

# Make function: `write_dashboard_data(data)`? – or should it go after I've written the appended file,
# and it takes a date argument? I think yes
# dashboard_data <- subset(long, `Data Level` != "Reliability" & `Data Level` != "Raw Indicator Data") # %>%
dashboard_data <- subset(long, `Data Level` != "Reliability") # %>%
#  mutate(Index = row_number()) # Don't include because indices should match between long and dashboard
# write.csv(dashboard_data, paste0(output_directory, "/crm-dashboard-data.csv"))
multi_write.csv(dashboard_data, "crm-dashboard-data.csv", c(output_directory, archive_directory))
# write.csv(dashboard_data, paste0(output_directory, "/crm-runs/", Sys.Date(), "-crm-run.csv"))

# Fix so that this uses dashboard_data instead of reading the CSV
dashboard_crisis <- label_crises()
multi_write.csv(dashboard_crisis, "crm-dashboard-data.csv", c(output_directory, archive_directory))
write.csv(dashboard_crisis, "production/crm-dashboard-prod.csv")

# track_indicator_updates()
# I've already written this. Do I still use it? My concern is that its reliance 
# on the previous version of the file is messy and error-prone.  Also, now that 
# I'm tracking inputs, I can track this directly.  But whether I use inputs or 
# output to track indicator changes, it shouldn't be order specific. Now, if I 
# run track_ind...() it depends on when I last ran it ...  which sort of makes 
# sense (can I envision an alternative cases?) but really there's no reason not 
# to just use the combined output file, and  compare against the previous date 
# (the way `countFlagChanges()` does? can I just abstract `flagChanges()`?)

all_runs <- append_if_exists(long, paste_path(output_directory, "crm-all-runs.csv"))
# Task: what if I run the monitor multiple times in a day? 
write_csv(all_runs, paste0(output_directory, "crm-all-runs.csv"))
# multi_write.csv(all_runs, "crm-all-runs.csv", c(output_directory, archive_directory))

# test <- all_dimensions %>%
#   pivot_longer(., cols = -contains("country") & -contains("_labels") & -contains("_raw"), names_to = "Name", values_to = "Value") %>%
#   separate(Name, into = c("Outlook", "Key"), sep = "_", extra = "merge") #%>%
#   # pivot_longer(., cols = contains("_labels"), names)

# pivot wide data to long format (name `lengthen_data()`)

# COMMAND ----------

# Edit to include reliability sheet output and to only take crm-wide.csv?
write_excel_source_files(
  all_dimensions = all_dimensions,
  health_sheet = health_sheet,
  food_sheet = food_sheet,
  macro_sheet = macro_sheet,
  socio_sheet = socio_sheet,
  natural_hazards_sheet = natural_hazards_sheet,
  fragility_sheet = fragility_sheet,
  filepaths = F,
  archive = T,
  directory_path = paste_path(output_directory, "crm-excel/")) # Task: move this so it can use the `output_directory` variable at top of file

# Task: add code for actually updating `crm-dashboard.xlsx`
# (Probably a shell command?)