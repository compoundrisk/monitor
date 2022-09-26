%sh
REPO="monitor"
REPO_PATH="compoundrisk/$REPO.git"
HOSTED_REPO="output"
HOSTED_REPO_PATH="compoundrisk/$HOSTED_REPO.git"
OUT_REPO="output"
OUT_REPO_PATH="compoundrisk/$OUT_REPO.git"
PAT=$(<.access/github-pat.txt)

if [ -d "/dbfs" ]; then

	cd /tmp
	
	if [ ! -d "crm" ]; then
	  echo "No /tmp/crm directory; making /tmp/crm"
	  mkdir crm;
	fi
	  cd crm;
	if [ ! -d /tmp/crm/$REPO ];
	  then
	    echo "No $REPO directory; cloning $REPO"
	    git clone --depth=1 https://bennotkin:$PAT@github.com/$REPO_PATH;
	    echo "$REPO repository cloned"
	    cd $REPO
	    git config user.email "bnotkin@gmail.com";
	    git config user.name "Ben Notkin";
	  else
	    cd $REPO
	    git fetch https://bennotkin:$PAT@github.com/$REPO_PATH
	    git merge origin/master
	fi
	# Copy this shell script over so that it can be updated by git
	cp -R /tmp/crm/monitor/src/clone-git-repos-tmp.sh /dbfs/mnt/CompoundRiskMonitor/src
	if [ ! -d /tmp/crm/$REPO/$HOSTED_REPO ];
	  then
	    echo "No $HOSTED_REPO directory; cloning $HOSTED_REPO"
	    git clone --depth=1 https://bennotkin:$PAT@github.com/HOSTED_REPO_PATH;
	    echo "$HOSTED_REPO repository cloned"
	    cd $HOSTED_REPO
	    git config user.email "bnotkin@gmail.com";
	    git config user.name "Ben Notkin";
	  else
	    cd $HOSTED_REPO
	    git fetch https://bennotkin:$PAT@github.com/$HOSTED_REPO_PATH
	    git merge origin/master
	fi
	# if [ ! -d /tmp/crm/$REPO/$OUT_REPO ];
	#   then
	#     echo "No $OUT_REPO directory; cloning $OUT_REPO"
	#     git clone --depth=1 https://bennotkin:$PAT@github.com/OUT_REPO_PATH;
	#     echo "$OUT_REPO repository cloned"
	#     cd $OUT_REPO
	#     git config user.email "bnotkin@gmail.com";
	#     git config user.name "Ben Notkin";
	#   else
	#     cd $OUT_REPO
	#     git fetch https://bennotkin:$PAT@github.com/$OUT_REPO_PATH
	#     git merge origin/master
	# fi
	cd /tmp/crm/$REPO
fi