#!/bin/bash

set -eu -o pipefail

mkdir -p data
mkdir -p metadata

curl 'https://www.nomisweb.co.uk/census/2011/bulk/r2_2' > metadata/index.html

cat metadata/index.html | grep -o '/output/census/2011/[^/]*_oa.zip' > metadata/files.txt
sed 's/^/https:\/\/www.nomisweb.co.uk/' metadata/files.txt > metadata/urls.txt

cat metadata/urls.txt | grep 'ks10[17]' | while read f; do
    b=$(basename $f)
    curl "$f" > data/$b
    (cd data && unzip $b)
done
