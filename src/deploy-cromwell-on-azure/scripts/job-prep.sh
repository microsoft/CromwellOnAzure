#!/bin/bash

sudo echo docker rmi $COA_EXECUTOR -f > clean-executor.sh
sudo chmod a+x clean-executor.sh
sudo find $AZ_BATCH_NODE_ROOT_DIR/workitems -maxdepth 5 -path $(dirname $(dirname $(dirname $PWD))) -prune -o -type f -name clean-executor.sh -execdir '{}' \; || :
sudo find $AZ_BATCH_NODE_ROOT_DIR/workitems -maxdepth 5 -path $(dirname $(dirname $(dirname $PWD))) -prune -o -type d -name cromwell-executions -execdir rm -fdr '{}' \; || :
sudo docker system prune --volumes -f || :
