#!/bin/bash

# args:
# 1 folder to sync
# 2 dataset name
#
# example:
#   bash ../../scripts/upload.sh icij_offshoreleaks data/export

aws s3 sync --no-progress --cache-control "public, max-age=64600" --metadata-directive REPLACE --acl public-read $2 s3://data.opensanctions.org/graph/$1
