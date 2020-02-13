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

function run_deploy { # use the default AWS credentials to setup a test database
	aws cloudformation deploy \
		--template-file cf.yaml \
		--stack-name rdsda-testing
}

function run_destroy { # destroy the test database and all data in it
  	aws cloudformation delete-stack \
		--stack-name=rdsda-testing
}

function run_example { # run example program
	 DATA_API_SECRET_ARN=foo DATA_API_RESOURCE_ARN=bar go run example/main.go 
}

function run_test { # run tests with coverage report
	DATA_API_SECRET_ARN=`aws cloudformation describe-stacks --stack-name rdsda-testing --query "Stacks[0].Outputs[?OutputKey=='SecretARN'].OutputValue" --output text` \
	DATA_API_RESOURCE_ARN=`aws cloudformation describe-stacks --stack-name rdsda-testing --query "Stacks[0].Outputs[?OutputKey=='ResourceARN'].OutputValue" --output text` \
	go test -v
}

case $1 in
	"deploy") run_deploy ;;
	"destroy") run_destroy ;;
	"example") run_example ;;
	"test") run_test ;;
	*) print_help ;;
esac