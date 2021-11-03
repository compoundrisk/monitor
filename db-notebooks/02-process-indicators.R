# Databricks notebook source
as_of <- Sys.Date()
format <- "csv" # or "tbl" or "both"
directory <- "output/risk-sheets"

# COMMAND ----------

getwd()

# COMMAND ----------

# setwd("../../../dbfs/mnt/CompoundRiskMonitor")
source("fns/libraries.R")
source("fns/indicators.R")
source("fns/aggregation.R")

# COMMAND ----------

# ACAPS
acapssheet <- acaps_process(as_of = as_of, format = format)

# COMMAND ----------

# HEALTH
# Writes sheet of health variables to output/risk-sheets/health-sheet.csv
collate_sheets(
  "health",
  acapssheet[, c("Country", "H_health_acaps")],
  ghsi_process(as_of = as_of, format = format),
  oxford_openness_process(as_of = as_of, format = format),
  owid_covid_process(as_of = as_of, format = format),
  Oxres_process(as_of = as_of, format = format),
  inform_covid_process(as_of = as_of, format = format),
  who_process(as_of = as_of, format = format),
  format = format,
  directory = directory)

# COMMAND ----------

# FOOD
# Writes sheet of food variables to output/risk-sheets/food-sheet.csv
collate_sheets(
  "food",
  proteus_process(as_of = as_of, format = format),
  fews_process(as_of = as_of, format = format),
  fpi_process(as_of = as_of, format = format),
  fao_wfp_process(as_of = as_of, format = format),
  format = format,
  directory = directory)

# COMMAND ----------

# MACRO FISCAL
# Writes sheet of macro fiscal variables to output/risk-sheets/macro-sheet.csv
collate_sheets(
  "macro",
  eiu_process(as_of = as_of, format = format),
  format = format,
  directory = directory
)

# COMMAND ----------

# SOCIO-ECONOMIC
# Writes sheet of socio-economic variables to output/risk-sheets/socio-sheet.csv
collate_sheets(
  "socio",
  inform_socio_process(as_of = as_of, format = format),
  income_support_process(as_of = as_of, format = format),
  mpo_process(as_of = as_of, format = format),
  macrofin_process(as_of = as_of, format = format),
  phone_process(as_of = as_of, format = format),
  # Fix warnings
  imf_process(as_of = as_of, format = format),
  format = format,
  directory = directory)

# COMMAND ----------

# NATURAL HAZARDS
# Writes sheet of natural hazard variables to output/risk-sheets/natural_hazards-sheet.csv
collate_sheets(
  "natural_hazards",
  gdacs_process(as_of = as_of, format = format),
  inform_nathaz_process(as_of = as_of, format = format),
  iri_process(as_of = as_of, format = format), # Rename iri_forecast)
  locust_process(as_of = as_of, format = format),
  acapssheet[, c("Country", "NH_natural_acaps")],
  format = format,
  directory = directory)

# COMMAND ----------

# CONFLICT AND FRAGILITY
# Writes sheet of conflict and fragility variables to output/risk-sheets/fragility-sheet.csv
# REIGN uses fcs, so run it first to make it available
fcs <- fcs_process(as_of = as_of, format = "csv")

fragility_sheet <- collate_sheets(
  "fragility",
  fcs,
  un_idp_process(as_of = as_of, format = format),
  acled_process(as_of = as_of, format = format),
  reign_process(as_of = as_of, format = format),
  format = "",
  return = T
)
fragility_sheet <- fragility_sheet %>%
  rename_with(
    .fn = ~ paste0("Fr_", .),
    .cols = colnames(.)[!colnames(.) %in% c("Country", "Countryname")]
  )

write.csv(fragility_sheet, paste0(directory, "/fragility-sheet.csv")

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
