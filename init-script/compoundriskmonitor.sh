#!/bin/bash
# init script for databricks; saved in /dbfs/databricks/scripts/
# first line deletes reference to zulu package, which is lacking Release file
test -f /etc/apt/sources.list.d/zulu-openjdk.list && rm /etc/apt/sources.list.d/zulu-openjdk.list
sudo rm -r /var/lib/apt/lists/* 
sudo apt-get clean &&
  sudo apt-get update --fix-missing -y &&
  sudo apt-get install -y libmysqlclient21
sudo apt-get install -y gdal-bin

sudo apt-get update -y
sudo apt-get install -y libudunits2-dev proj-bin libgdal-dev libproj-dev