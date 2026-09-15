#!/bin/bash
# Clones (or updates) monitor and hosted-data into /tmp/crm on a Databricks cluster.
# Called from R via system("bash src/clone-git-repos-into-tmp.sh") — see run-notebooks.R.
#
# GITHUB_PAT is expected to already be set in the environment by run-notebooks.R,
# which reads it from the Databricks secret scope "compoundriskmonitor" (key
# "github-pat") rather than a plain file. If you're testing this script by hand,
# export GITHUB_PAT=<token> first.
#
# NOTE: this used to start with a stray `%sh` (a Databricks notebook cell-magic
# marker left over from pasting this in as a notebook cell). When run via
# `bash script.sh`, that line gets parsed as job-control syntax and prints a
# harmless but confusing "line 1: fg: no job control" on every run. Removed.

: "${GITHUB_PAT:?GITHUB_PAT is not set. run-notebooks.R should set it from the compoundriskmonitor/github-pat Databricks secret before calling this script.}"

REPO="monitor"
REPO_PATH="compoundrisk/$REPO.git"
HOSTED_REPO="hosted-data"
HOSTED_REPO_PATH="compoundrisk/$HOSTED_REPO.git"
OUT_REPO="output"
OUT_REPO_PATH="compoundrisk/$OUT_REPO.git"

# clone_or_update <repo_dir> <repo_path>
# Clones <repo_path> into ./<repo_dir> if it doesn't exist yet, else fetches +
# merges the databricks branch. Exits the whole script with a clear message on
# any failure instead of silently continuing with a missing/stale directory
# (this previously caused confusing downstream errors many steps later, e.g.
# "cannot open file 'hosted-data/ghsi/ghsi.csv'", instead of failing here).
clone_or_update() {
  local repo_dir="$1"
  local repo_path="$2"

  if [ ! -d "$repo_dir" ]; then
    echo "No $repo_dir directory; cloning $repo_dir"
    if ! git clone --depth=1 --single-branch --branch databricks "https://${GITHUB_PAT}@github.com/$repo_path"; then
      echo "FATAL: git clone failed for $repo_path (see 'fatal:' message above — likely an invalid/expired token in the compoundriskmonitor/github-pat secret)" >&2
      exit 1
    fi
    echo "$repo_dir repository cloned"
    cd "$repo_dir" || { echo "FATAL: $repo_dir was not created after clone" >&2; exit 1; }
    git config user.email "compoundriskmonitor@worldbank.org"
    git config user.name "Compound Risk Monitor"
  else
    cd "$repo_dir" || { echo "FATAL: cannot cd into existing $repo_dir" >&2; exit 1; }
    if ! git fetch origin databricks || ! git merge origin/databricks; then
      echo "FATAL: git fetch/merge failed for $repo_dir (check token validity / merge conflicts)" >&2
      exit 1
    fi
  fi
  cd ..
}

if [ -d "/dbfs" ]; then

	cd /tmp || exit 1

	if [ ! -d "crm" ]; then
	  echo "No /tmp/crm directory; making /tmp/crm"
	  mkdir crm;
	fi
	  cd crm || exit 1

	clone_or_update "$REPO" "$REPO_PATH"

	# Copy this shell script over so that it can be updated by git
	cp -R /tmp/crm/monitor/src/clone-git-repos-into-tmp.sh /dbfs/mnt/CompoundRiskMonitor/src

	cd "$REPO" || exit 1
	clone_or_update "$HOSTED_REPO" "$HOSTED_REPO_PATH"

	# if [ ! -d /tmp/crm/$REPO/$OUT_REPO ];
	#   then
	#     echo "No $OUT_REPO directory; cloning $OUT_REPO"
	#     git clone --depth=1 --single-branch --branch databricks https://${GITHUB_PAT}@github.com/OUT_REPO_PATH;
	#     echo "$OUT_REPO repository cloned"
	#     cd $OUT_REPO
	#     git config user.email "compoundriskmonitor@worldbank.org";
	#     git config user.name "Compound Risk Monitor";
	#   else
	#     cd $OUT_REPO
	#     git fetch https://${GITHUB_PAT}@github.com/$OUT_REPO_PATH
	#     git merge origin/databricks
	# fi
	cd /tmp/crm/$REPO
fi
