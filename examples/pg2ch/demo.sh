#!/bin/bash


#################################
# include the -=magic=-
# you can pass command line args
#
# example:
# to disable simulated typing
# . ../demo-magic.sh -d
#
# pass -h to see all options
#################################
. ../demo-magic.sh

bat transfer.yaml

pe "trcli validate --transfer transfer.yaml"

pe "trcli check --transfer transfer.yaml"

pe "trcli activate --transfer transfer.yaml"
