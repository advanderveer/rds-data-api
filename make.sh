#!/bin/bash
set -e

function print_help {
	printf "Available Commands:\n";
	awk -v sq="'" '/^function run_([a-zA-Z0-9-]*)\s*/ {print "-e " sq NR "p" sq " -e " sq NR-1 "p" sq }' make.sh \
		| while read line; do eval "sed -n $line make.sh"; done \
		| paste -d"|" - - \
		| sed -e 's/^/  /' -e 's/function run_//' -e 's/#//' -e 's/{/	/' \
		| awk -F '|' '{ print "  " $2 "\t" $1}' \
		| expand -t 30
}

function run_example { # run example program
	 DATA_API_SECRET_ARN=foo DATA_API_RESOURCE_ARN=bar go run example/main.go 
}

function run_test { # run tests with coverage report
	 go test -v
}

case $1 in
	"example") run_example ;;
	"example") run_test ;;
	*) print_help ;;
esac