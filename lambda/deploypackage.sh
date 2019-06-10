#!/bin/bash
# Based on https://github.com/quiltdata/lambda
mkdir function

pip3 install -r /lambda/requirements.txt -t function/

# remove any old .zips
rm -f /lambda/function.zip

# grab existing products
cp -r /lambda/* function

# zip without any containing folder (or it won't work)
cd function
zip -r /lambda/function.zip *
