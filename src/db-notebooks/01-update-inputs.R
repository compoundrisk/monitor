# For some reason `setwd(...)` loads rlang 0.4.12 to the namespace, so this needs to precede
# library(rlang, lib.loc = "/dbfs/mnt/CompoundRiskMonitor/lib")

# COMMAND ----------

# Collecting data from external sources, and appending new data to inputs
# archive. Some sources require cleaning in order to be appended. The next
# notebook (02-process-indicators) reads from these archives to generate
# indicator values.

# Databricks notebook source
# Set working directory, load libraries and read-in functions

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
  
# COMMAND ----------

source("src/fns/prep.R")
source("src/fns/indicators.R")

# Until I also add the output repository, output folder resides on mounted storage
inputs_archive_path <- paste_path(mounted_path, "output/inputs-archive/")

# if run is a job, save files to "output/scheduled"; if manually, save to "output/manual"
run_type <- tryCatch(dbutils.widgets.get("run_type"), error = function(e) {return("manual")})
output_directory <- paste_path(mounted_path, "output/", run_type) #paste_path("output/", run_type)

# COMMAND ----------

error_delay <- tryCatch(dbutils.widgets.get("error_delay"), error = function(e) {return(F)})
error_delay <- if (error_delay) T else F

# COMMAND ----------

# HEALTH
lap_start()
ghsi_collect() %>% delay_error(return = NA, on = error_delay)
dons_collect() %>% delay_error(return = NA, on = error_delay)
ifrc_collect() %>% delay_error(return = NA, on = error_delay)
lap_print("Health dimension finished collecting")

# COMMAND ----------

# FOOD
lap_start()
fpi_collect_api() %>% delay_error(return = NA, on = error_delay)
proteus_collect() %>% delay_error(return = NA, on = error_delay)
fews_collect_api() %>% delay_error(return = NA, on = error_delay)
# fao_wfp_web_collect() %>% delay_error(return = NA, on = error_delay)
gfsi_collect() %>% delay_error(return = NA, on = error_delay)
lap_print("Food dimension finished collecting")

# COMMAND ----------

# MACRO FISCAL
lap_start()
eiu_collect_many() %>% delay_error(return = NA, on = error_delay)
mfr_watchlist_collect() %>% delay_error(return = NA, on = error_delay)
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
locust_collect() %>% delay_error(return = NA, on = error_delay)
inform_severity_collect() %>% delay_error(return = NA, on = error_delay)
acaps_risk_list_collect() %>% delay_error(return = NA, on = error_delay)
lap_print("Natural hazards dimension finished collecting")

# COMMAND ----------

# FRAGILITY AND CONFLICT
lap_start()
fsi_collect() %>% delay_error(return = NA, on = error_delay)
fcs_collect() %>% delay_error(return = NA, on = error_delay)
acled_collect() %>% delay_error(return = NA, on = error_delay)
# reign_collect()
gic_collect() %>% delay_error(return = NA, on = error_delay)
ifes_collect() %>% delay_error(return = NA, on = error_delay)
lap_print("Fragility dimension finished collecting")