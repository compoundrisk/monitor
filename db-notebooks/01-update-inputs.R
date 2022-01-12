# Collecting data from external sources, and appending new data to inputs
# archive. Some sources require cleaning in order to be appended. The next
# notebook (02-process-indicators) reads from these archives to generate
# indicator values.

# Databricks notebook source
# Set working directory, load libraries and read-in functions
# setwd("../../../dbfs/mnt/CompoundRiskMonitor")
source("fns/libraries.R")
source("fns/indicators.R")

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
acaps_collect()
# Add in OWID
owid_collect()
# Add in Oxford Response Tracker
ghsi_collect()
# oxford_openness_collect()
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
# phone_collect()
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
gic_collect()
ifes_collect()
