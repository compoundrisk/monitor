if (dir.exists("/dbfs")) {
  mounted_path <- "/dbfs/mnt/CompoundRiskMonitor"
  working_path <- "/tmp/crm/monitor"
  setwd(mounted_path)
  system("bash src/clone-git-repos-into-tmp.sh")
  setwd(working_path)
  }
source('src/fns/prep.R')
source('src/db-notebooks/01-update-inputs.R')
source('src/db-notebooks/02-process-indicators.R')
