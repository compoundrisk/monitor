source('src/fns/prep.R')
source('src/db-notebooks/01-update-inputs.R')
source('src/db-notebooks/02-process-indicators.R')
system('bash src/export.sh')
