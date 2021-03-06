# For some reason `setwd(...)` loads rlang 0.4.12 to the namespace, so this needs to precede
# library(rlang, lib.loc = "/dbfs/mnt/CompoundRiskMonitor/lib")

# COMMAND ----------

# Collecting data from external sources, and appending new data to inputs
# archive. Some sources require cleaning in order to be appended. The next
# notebook (02-process-indicators) reads from these archives to generate
# indicator values.

# Databricks notebook source
# Set working directory, load libraries and read-in functions
# setwd("../../../dbfs/mnt/CompoundRiskMonitor")

## COMMAND ----------

source("src/fns/prep.R")
source("src/fns/indicators.R")

# COMMAND ----------

error_delay <- tryCatch(dbutils.widgets.get("error_delay"), error = function(e) {return(F)})
error_delay <- if (error_delay) T else F

# COMMAND ----------

# source("libraries.R")

# COMMAND ----------

# Set up Spark connection
# sc <- spark_connect(method = "databricks")
# DBI::dbSendQuery(sc,"CREATE DATABASE IF NOT EXISTS crm")
# sparklyr::tbl_change_db(sc, "crm")

# COMMAND ----------

## Direct Github location (data folder)
# github <- "https://raw.githubusercontent.com/bennotkin/compoundriskdata/master/"

# COMMAND ----------

# Each *_collect() function gathers data from a foreign source (including
# from the bennotkin fork of the CRM Github repo), and archives the new data 
# into inputs-archive/<indicator-name>.csv

# COMMAND ----------

# HEALTH
lap_start()
acaps_collect() %>% delay_error(return = NA, on = error_delay)
# Add in OWID
owid_collect() %>% delay_error(return = NA, on = error_delay)
# Add in Oxford Response Tracker
ghsi_collect() %>% delay_error(return = NA, on = error_delay)
# oxford_openness_collect()
inform_covid_collect() %>% delay_error(return = NA, on = error_delay)
dons_collect() %>% delay_error(return = NA, on = error_delay)
lap_print("Health dimension finished collecting")

# COMMAND ----------

# FOOD
lap_start()
fpi_collect_many() %>% delay_error(return = NA, on = error_delay)
proteus_collect() %>% delay_error(return = NA, on = error_delay)
fews_collect() %>% delay_error(return = NA, on = error_delay)
fao_wfp_collect() %>% delay_error(return = NA, on = error_delay)
lap_print("Food dimension finished collecting")

# COMMAND ----------

# MACRO FISCAL
lap_start()
eiu_collect() %>% delay_error(return = NA, on = error_delay)
lap_print("Macro-fiscal dimension finished collecting")

# COMMAND ----------

# SOCIO-ECONOMIC
lap_start()
mpo_collect() %>% delay_error(return = NA, on = error_delay)
mfr_collect() %>% delay_error(return = NA, on = error_delay)
imf_collect() %>% delay_error(return = NA, on = error_delay)
lap_print("Socio-economic dimension finished collecting")

# COMMAND ----------

# NATURAL HAZARDS
lap_start()
gdacs_collect()  %>% delay_error(return = NA, on = error_delay)
inform_risk_collect()  %>% delay_error(return = NA, on = error_delay)
iri_collect() %>% delay_error(return = NA, on = error_delay)
locust_collect()  %>% delay_error(return = NA, on = error_delay)
lap_print("Natural hazards dimension finished collecting")

# COMMAND ----------

# FRAGILITY AND CONFLICT
lap_start()
fcs_collect() %>% delay_error(return = NA, on = error_delay)
idp_collect() %>% delay_error(return = NA, on = error_delay)
acled_collect() %>% delay_error(return = NA, on = error_delay)
# reign_collect()
gic_collect() %>% delay_error(return = NA, on = error_delay)
ifes_collect() %>% delay_error(return = NA, on = error_delay)
lap_print("Fragility dimension finished collecting")

# COMMAND ----------

release_delayed_errors()