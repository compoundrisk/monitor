if (dir.exists("/dbfs")) {
  mounted_path <- "/dbfs/mnt/CompoundRiskMonitor"
  working_path <- "/tmp/crm/monitor"
  setwd(mounted_path)

  # GitHub PAT used by clone-git-repos-into-tmp.sh to clone/update the monitor and
  # hosted-data repos. Stored as a Databricks secret rather than a plain file in
  # .access/, so it isn't sitting in cleartext on the DBFS mount. One-time setup
  # (Databricks CLI):
  #   databricks secrets create-scope compoundriskmonitor
  #   databricks secrets put-secret compoundriskmonitor github-pat
  #   databricks secrets put-acl compoundriskmonitor <cluster-or-user-principal> READ
  github_pat <- tryCatch(
    dbutils.secrets.get(scope = "compoundriskmonitor", key = "github-pat"),
    error = function(e) stop(
      "Could not read GitHub PAT from secret scope 'compoundriskmonitor', key 'github-pat'. ",
      "Create it with `databricks secrets put-secret compoundriskmonitor github-pat` and make ",
      "sure this cluster's identity has READ access to the scope. Original error: ",
      conditionMessage(e)))
  Sys.setenv(GITHUB_PAT = github_pat)

  system("bash src/clone-git-repos-into-tmp.sh")
  setwd(working_path)
  }
source('src/fns/prep.R')
source('src/db-notebooks/01-update-inputs.R')
source('src/db-notebooks/02-process-indicators.R')
