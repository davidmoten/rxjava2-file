#!/bin/bash
set -e
mvn site
cd ../davidmoten.github.io
git pull
mkdir -p rxjava-file2
cp -r ../rxjava-file2/target/site/* rxjava-file2/
git add .
git commit -am "update site reports"
git push
