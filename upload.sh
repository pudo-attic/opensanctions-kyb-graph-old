#!/bin/bash

# args:
# 1 folder to sync
# 2 dataset name
#
# example:
#   bash ../../scripts/upload.sh icij_offshoreleaks data/export

aws s3 sync --no-progress --content-disposition attachment --metadata-directive REPLACE --acl public-read $2 s3://data.opensanctions.org/graph/$1
aws cloudfront create-invalidation --distribution-id ETROMAQBEJS91 --paths "/graph/$1/*"
