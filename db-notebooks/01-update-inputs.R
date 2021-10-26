# Databricks notebook source
# Set working directory, load libraries and read-in functions
# setwd("../../../dbfs/mnt/CompoundRiskMonitor")
source("libraries.R")
source("inputs.R")

# COMMAND ----------

# source("libraries.R")

# COMMAND ----------

# Set up Spark connection
# sc <- spark_connect(method = "databricks")
# DBI::dbSendQuery(sc,"CREATE DATABASE IF NOT EXISTS crm")
# sparklyr::tbl_change_db(sc, "crm")

# COMMAND ----------

## Direct Github location (data folder)
github <- "https://raw.githubusercontent.com/bennotkin/compoundriskdata/master/"

# COMMAND ----------

# # HEALTH
# # Add in OWID
# # Add in Oxford Response Tracker
# try_log(ghsi_collect())
# try_log(oxford_openness_collect())
# try_log(inform_covid_collect())
# try_log(dons_collect())

# COMMAND ----------

# HEALTH
# Add in OWID
# Add in Oxford Response Tracker
ghsi_collect()
oxford_openness_collect()
inform_covid_collect()
dons_collect()

# COMMAND ----------

# FOOD
# Add in WBG Food Price Monitor
proteus_collect()
fews_collect()
fao_wfp_collect()

# COMMAND ----------

# MACRO FISCAL
eiu_collect()

# COMMAND ----------

# SOCIO-ECONOMIC
mpo_collect()
mfr_collect()
phone_collect()
imf_collect()

# COMMAND ----------

# NATURAL HAZARDS
gdacs_collect()
inform_risk_collect()
iri_collect()
locust_collect()

# COMMAND ----------

# FRAGILITY AND CONFLICT
fcs_collect()
idp_collect()
# acled_collect()
# reign_collect()

# COMMAND ----------

# crm_test <- data.frame(a = 1:4, b = rep(2))

# COMMAND ----------

# crm_test <- copy_to(
#   sc,
#   crm_test,
#   overwrite = TRUE
# )

# COMMAND ----------

# spark_write_table(
#   crm_test,
#   "crm_test_tbl"
# )

# COMMAND ----------

# dbGetQuery(sc, "SELECT count(*) FROM crm_test2")