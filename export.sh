#!/bin/bash
cp -R output/risk-sheets/ ~/Documents/world-bank/crm/compoundriskdata/Risk_sheets
cp -R output/ ~/Documents/world-bank/crm/compoundriskdata/external
# R -e "rmarkdown::render('external-report.Rmd', output_format = 'html_document', output_file = paste0('manual-reports/', Sys.Date(), '-report'))"