#!/bin/bash

trap 'rm -f /var/lock/coa.lock' EXIT
sudo docker rmi $COA_EXECUTOR -f || :
find ../../.. -maxdepth 3 -type d -name cromwell-executions -execdir rm -fdr cromwell-executions \;
sudo docker system prune --volumes -f
