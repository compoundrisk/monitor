# Databricks notebook source
setwd("../../../dbfs/mnt/CompoundRiskMonitor")
source("libraries.R")
source("indicators.R")

# COMMAND ----------

as_of <- Sys.Date()
format <- "csv" # or "tbl" or "both"

# COMMAND ----------

# ACAPS
acapssheet <- acaps_process(as_of = as_of, format = format)

# COMMAND ----------

# HEALTH
collate_sheets(
  "health",
#   acapssheet[,c("Country", "H_health_acaps")],
  ghsi_process(as_of = as_of, format = format),
  oxford_openness_process(as_of = as_of, format = format),
  owid_covid_process(as_of = as_of, format = format),
  Oxres_process(as_of = as_of, format = format),
  inform_covid_process(as_of = as_of, format = format),
  who_process(as_of = as_of, format = format),
  format = "csv")

# COMMAND ----------

# FOOD
collate_sheets(
  "food",
  proteus_process(as_of = as_of, format = format),
  fews_process(as_of = as_of, format = format),
  fpi_process(as_of = as_of, format = format),
  format = format)

# COMMAND ----------

# MACRO FISCAL
collate_sheets(
  "macro",
  eiu_process(as_of = as_of, format = format),
  format = format
)

# COMMAND ----------

# SOCIO-ECONOMIC
collate_sheets(
  "socio",
  income_support_process(as_of = as_of, format = format),
  mpo_process(as_of = as_of, format = format),
  macrofin_process(as_of = as_of, format = format),
  phone_process(as_of = as_of, format = format),
  # Fix warnings
  imf_process(as_of = as_of, format = format),
  format = format)

# COMMAND ----------

# NATURAL HAZARDS
collate_sheets(
  "natural_hazards",
  gdacs_process(as_of = as_of, format = format),
  inform_nathaz_process(as_of = as_of, format = format),
  iri_process(as_of = as_of, format = format), # Go through and actually name iri_forecast)
  locust_process(as_of = as_of, format = format),
#   acapssheet[,c("Country", "NH_natural_acaps")],
  format = format)

# COMMAND ----------

# CONFLICT AND FRAGILITY
# REIGN uses fcs, so run it first to make it available
fcs <- fcs_process(as_of = as_of, format = format)

fragility_sheet <- collate_sheets(
  "fragility",
  fcs,
  un_idp_process(as_of = as_of, format = format),
  acled_process(as_of = as_of, format = format),
  reign_process(as_of = as_of, format = format),
  format = "return"
  )
fragility_sheet <- fragility_sheet %>% 
  rename_with(
  .fn = ~ paste0("Fr_", .), 
  .cols = colnames(.)[!colnames(.) %in% c("Country", "Countryname") ]
)
write_sheet <- function(dim, sheet, format)   {
  if(format == "csv" | format == "both") {
    write.csv(sheet, paste0("risk-sheets/", dim, "-sheet.csv"))
    # print(paste0("risk-sheets/", dim, "-sheet.csv"))
  }
  if(format == "tbl" | format == "both") {
    # Write Spark DataFrame
  }
  if(format == "return") {
    # write.csv(sheet, paste0("risk-sheets/", dim, "-sheet.csv"))
    # print(paste0("risk-sheets/", dim, "-sheet.csv"))
    return(sheet)
  }
}
write_sheet("fragility", fragility_sheet, format = format)

