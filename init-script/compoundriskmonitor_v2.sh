#!/bin/bash
# init script for databricks; saved in /dbfs/databricks/scripts/
#
# This is the init script actually configured on the DECPY_CompoundRiskMonitor_Johan
# cluster (Databricks Runtime 16.4.x-scala2.12) as of 2026-09-15. It supersedes the
# older, simpler `compoundriskmonitor.sh` in this same folder, which does not include
# the spatial-package compilation steps below.
#
# INCIDENT (2026-09): a previous version of this script installed `r-base` and
# `r-base-dev` via apt-get in section 2. Databricks Runtime 16.4.x already bundles
# its own R build with a matching IRkernel; installing r-base via APT overwrites the
# bundled R (libR.so, /usr/bin/R), so the IRkernel (compiled against the original
# bundled R) crashes on an ABI mismatch the moment the R REPL tries to start. Every
# job run failed with `ReplStartFailureException: Kernel exited while we were
# waiting for the kernel_info_reply message` before any notebook code executed.
# Fix: do NOT install r-base / r-base-dev — the runtime already provides R and the
# headers needed to compile the packages below. If a future compile step genuinely
# needs headers not present, add back only r-base-dev, pinned to the exact version
# already bundled with the runtime (check with `dpkg -l r-base-core` on a fresh
# cluster) — never plain r-base.

# -----------------------------------------------
# 1. APT SETUP
# -----------------------------------------------
test -f /etc/apt/sources.list.d/zulu-openjdk.list && rm /etc/apt/sources.list.d/zulu-openjdk.list

sudo rm -r /var/lib/apt/lists/*
sudo apt-get clean
sudo apt-get update --fix-missing -y

# -----------------------------------------------
# 2. SYSTEM LIBRARIES
# -----------------------------------------------
sudo apt-get install -y libmysqlclient21

# NOTE: do not add r-base / r-base-dev here — see INCIDENT note above.
sudo apt-get install -y --no-install-recommends \
  gdal-bin \
  libgdal-dev \
  libproj-dev \
  proj-bin \
  proj-data \
  libudunits2-dev \
  libgeos-dev \
  libgsl-dev \
  build-essential \
  cmake

# Poppler for pdftools - available in noble base repos (no PPA needed)
sudo apt-get install -y --no-install-recommends \
  libpoppler-cpp-dev \
  poppler-utils

sudo ldconfig

# -----------------------------------------------
# 3. LOCAL COMPILE DIRECTORY
# DBFS does not support file ops needed during
# R package installation - compile locally first
# then copy to DBFS
# -----------------------------------------------
LOCAL_RLIBS=/tmp/rlibs
mkdir -p $LOCAL_RLIBS

# -----------------------------------------------
# 4. CLEAN STALE DBFS LOCKS
# -----------------------------------------------
echo "===== Cleaning stale DBFS lock directories ====="
find /dbfs/mnt/CompoundRiskMonitor/lib -maxdepth 1 -name "00LOCK-*" -exec rm -rf {} + 2>/dev/null
ls /dbfs/mnt/CompoundRiskMonitor/lib/ | grep "00LOCK" || echo "No stale locks found"

# -----------------------------------------------
# 5. COMPILE ALL PACKAGES LOCALLY
# Order matters - dependencies must come first:
#   Rcpp -> units -> sf -> lwgeom -> rgdal -> terra -> exactextractr
#   qpdf -> pdftools
#
# Broken packages and what they were compiled against:
#   lwgeom:       libproj.so.15  (very old - different Databricks runtime)
#   rgdal:        libgdal.so.26, libproj.so.15  (very old)
#   sf:           libgdal.so.30, libproj.so.22  (old)
#   terra:        libgdal.so.30, libproj.so.22  (old)
#   exactextractr: depends on terra + sf
#   pdftools:     libpoppler-cpp.so.0 (old runtime / missing PPA for noble)
# -----------------------------------------------
echo "===== Compiling spatial and PDF packages locally ====="

Rscript -e "
  local_lib <- '$LOCAL_RLIBS'
  dbfs_lib  <- '/dbfs/mnt/CompoundRiskMonitor/lib'

  # Full lib path so dependencies resolve during compile
  .libPaths(c(local_lib, dbfs_lib, .libPaths()))

  pkgs <- c(
    'Rcpp',          # Must be first - fixes gcc13 ABI issue
    'units',         # sf dependency
    'sf',            # core spatial - lwgeom/rgdal/exactextractr depend on it
    'lwgeom',        # was compiled against libproj.so.15
    'rgdal',         # was compiled against libgdal.so.26 + libproj.so.15
    'terra',         # was compiled against libgdal.so.30 + libproj.so.22
    'exactextractr', # depends on terra + sf
    'qpdf',          # pdftools dependency
    'pdftools'       # was compiled against libpoppler-cpp.so.0 (old runtime)
  )

  for (pkg in pkgs) {
    cat('\n========== Installing', pkg, '==========\n')

    # Remove from local lib to force fresh compile
    old <- file.path(local_lib, pkg)
    if (dir.exists(old)) unlink(old, recursive = TRUE)

    tryCatch({
      install.packages(
        pkg,
        lib   = local_lib,
        repos = 'https://cloud.r-project.org',
        type  = 'source'
      )
      if (dir.exists(file.path(local_lib, pkg))) {
        cat('SUCCESS:', pkg, '\n')
      } else {
        stop(paste('Directory not found after install:', pkg))
      }
    }, error = function(e) {
      cat('FAILED:', pkg, '-', conditionMessage(e), '\n')
      quit(status = 1)
    })
  }

  cat('\nAll packages compiled successfully\n')
"

# -----------------------------------------------
# 6. COPY ALL COMPILED PACKAGES TO DBFS
# -----------------------------------------------
echo "===== Copying compiled packages to DBFS ====="

for pkg in Rcpp units sf lwgeom rgdal terra exactextractr qpdf pdftools; do
  src="$LOCAL_RLIBS/$pkg"
  dest="/dbfs/mnt/CompoundRiskMonitor/lib/$pkg"

  if [ -d "$src" ]; then
    rm -rf "$dest"
    cp -r "$src" "/dbfs/mnt/CompoundRiskMonitor/lib/"
    echo "Copied: $pkg"
  else
    echo "WARNING: $pkg not found in local lib - compile may have failed"
  fi
done

# -----------------------------------------------
# 7. VERIFY - check all .so files for missing deps
# -----------------------------------------------
echo "===== Checking all .so files for missing deps ====="

ALL_CLEAR=true
for so in \
  /dbfs/mnt/CompoundRiskMonitor/lib/sf/libs/sf.so \
  /dbfs/mnt/CompoundRiskMonitor/lib/lwgeom/libs/lwgeom.so \
  /dbfs/mnt/CompoundRiskMonitor/lib/rgdal/libs/rgdal.so \
  /dbfs/mnt/CompoundRiskMonitor/lib/terra/libs/terra.so \
  /dbfs/mnt/CompoundRiskMonitor/lib/exactextractr/libs/exactextractr.so \
  /dbfs/mnt/CompoundRiskMonitor/lib/pdftools/libs/pdftools.so; do

  if [ -f "$so" ]; then
    missing=$(ldd "$so" 2>/dev/null | grep "not found")
    if [ -n "$missing" ]; then
      echo "BROKEN: $so"
      echo "$missing"
      ALL_CLEAR=false
    else
      echo "OK: $so"
    fi
  else
    echo "MISSING FILE: $so"
    ALL_CLEAR=false
  fi
done

if [ "$ALL_CLEAR" = true ]; then
  echo "===== ALL PACKAGES OK ====="
else
  echo "===== WARNING: SOME PACKAGES STILL BROKEN - check logs above ====="
fi

echo "===== Done ====="
