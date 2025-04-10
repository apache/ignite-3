#!/bin/bash

PACKAGE_NAME="test-cluster.full.8.tar.xz"

mkdir -p resources/sample-clusters/test-cluster
cd resources/sample-clusters/
curl -O -v -u $CURL_CREDENTIALS \
  --resolve gridgainsystems.com:443:104.154.90.88 \
  https://gridgainsystems.com/nexus/service/local/repositories/external-binaries/content/migration-tools/sample-clusters/$PACKAGE_NAME
tar -Jxf $PACKAGE_NAME