#!/usr/bin/env bash

#!/usr/bin/env bash
export SONAR_SCANNER_HOME=$HOME/.sonar/sonar-scanner-2.6
	rm -rf $SONAR_SCANNER_HOME
	mkdir -p $SONAR_SCANNER_HOME
	curl -sSLo $HOME/.sonar/sonar-scanner.zip http://repo1.maven.org/maven2/org/sonarsource/scanner/cli/sonar-scanner-cli/2.6/sonar-scanner-cli-2.6.zip
	unzip $HOME/.sonar/sonar-scanner.zip -d $HOME/.sonar/
	rm $HOME/.sonar/sonar-scanner.zip
	export PATH=$SONAR_SCANNER_HOME/bin:$PATH

sonar-scanner
