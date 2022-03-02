#!/bin/bash
# cp -R output/risk-sheets/ ~/Documents/world-bank/crm/compoundriskdata/Risk_sheets
# cp -R output/ ~/Documents/world-bank/crm/compoundriskdata/external
# # R -e "rmarkdown::render('output-report.Rmd', output_format = 'html_document', output_file = paste0('output/reports/', Sys.Date(), '-report'))"

# From scheduled folder
# cp -R output/scheduled/dimensions/ ~/Documents/world-bank/crm/compoundriskdata/Risk_sheets
# cp -R output/scheduled/crm-excel/ ~/Documents/world-bank/crm/compoundriskdata/Risk_sheets/crm-excel
# cp -R output/scheduled ~/Documents/world-bank/crm/compoundriskdata/external

# From manual folder
cp -R output/manual/dimensions/ ~/Documents/world-bank/crm/compoundriskdata/Risk_sheets
cp -R output/manual/crm-excel/ ~/Documents/world-bank/crm/compoundriskdata/Risk_sheets/crm-excel
cp -R output/manual ~/Documents/world-bank/crm/compoundriskdata/external


# R -e "rmarkdown::render('output-report.Rmd', output_format = 'html_document', output_file = paste0('output/reports/', Sys.Date(), '-report'))"