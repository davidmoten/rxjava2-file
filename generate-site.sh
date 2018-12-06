#!/bin/bash
set -e
mvn site
cd ../davidmoten.github.io
git pull
mkdir -p rxjava2-file
cp -r ../rxjava2-file/target/site/* rxjava2-file/
git add .
git commit -am "update site reports"
git push
