#!/bin/bash

# Update apt-get
apt-get update

# Install Linux Packages
## Issue 1
## ruby-rubyforge is not available, but it does not appear to be required.
## Patch 1 Start
## apt-get install sqlite3 libsqlite3-dev flex ruby-full ruby-rubyforge \
##                 libssl-dev libssh2-1-dev libcurl4-openssl-dev curl \
##                 libxml2-dev
apt-get install -y sqlite3 libsqlite3-dev flex ruby-full libssl-dev \
libssh2-1-dev libcurl4-openssl-dev curl libxml2-dev
## Patch 1 End

# Install ruby gems
gem install sqlite3
gem install json
gem install Text

# Install R packages
## Issue 2
## These packages are wrong.
## {devtools} is already installed in Databricks.
## {CB} used to contain mappp(), but it has been moved to its own CRAN package.
## {argparser} has been superseded with {docopt} in entrypoint.R
## Patch 2 Start
## R -e "install.packages('devtools', repos='https://cran.rstudio.com/')"
## R -e "devtools::install_github('cole-brokamp/CB')"
## R -e "install.packages('argparser', repos='https://cran.rstudio.com/')"
##
## Install to a library that will be available to Rscript.exe by default.
## Note, default install from an init script is /usr/local/lib/R/site-library.
## Note, install.packages() requires setting repos argument in non-interactive 
## mode but does not produce an error to stop init script if not set.
R -e 'devtools::install_github("degauss-org/dht")'
R -e 'install.packages("docopt", repos = "https://cloud.r-project.org")'
## Patch 2 End
## Issue 3
## mappp::mappp() is used in entrypoint.R, but both the CRAN and github 
## (cole-brokamp/mappp) versions produce the following error when using 
## entrypoint.R.
##
## ── Welcome to DeGAUSS! ──
## 
## • You are using , version
## • This container returns
## • <https://degauss.org/>
## 
## ℹ removing non-alphanumeric characters...
## ℹ removing excess whitespace...
## ℹ flagging PO boxes...
## ℹ flagging known Cincinnati foster & institutional addresses...
## ℹ flagging non-address text and missing addresses...
## ℹ now geocoding ...
## Error in formatTime(ETA) : could not find function "formatTime"
## Calls: <Anonymous> -> mclapply_pb -> pbb_eta -> up
## Error in formatTime(ETA) : could not find function "formatTime"
## Calls: <Anonymous> -> mclapply_pb -> pbb_eta -> up
## Execution halted
## Execution halted
## Patch 3 Start
R -e 'devtools::install_github("the-mad-statter/mappp@dev")'
## Patch 3 End

# Install Geocoder Ruby gems
## Issue 4
## The existing shp2sqlite Makefile generates an error when it tries to make 
## the ../../build/ directory which already exists. We need to use the 
## Makefile.nix version instead.
## Issue 5
## /root/geocoder/bin is not a directory.
## Issue 6
## geocode.rb does not get installed to /root/geocoder/bin. It gets installed 
## to /root/geocoder.
cd /root
git clone https://github.com/degauss-org/geocoder
cd geocoder
## Patch 4 Start
mv /root/geocoder/src/shp2sqlite/Makefile \
/root/geocoder/src/shp2sqlite/Makefile.old
cp /root/geocoder/src/shp2sqlite/Makefile.nix \
/root/geocoder/src/shp2sqlite/Makefile
## Patch 4 End
make -f Makefile.ruby install
gem install Geocoder-US-2.0.4.gem
## Patch 5 Start
mv /root/geocoder/bin /root/geocoder/bin.old
mkdir /root/geocoder/bin
## Patch 5 End
## Patch 6 Start
mv /root/geocoder/geocode.rb /root/geocoder/bin/geocode.rb
## Patch 6 End

# Install TIGER/Line Database
wget https://colebrokamp-dropbox.s3.amazonaws.com/geocoder.db -P /opt

## Issue 7
## entrypoint.R uses /app/geocode.rb.
## Patch 7 Start
mkdir /app
ln -s /root/geocoder/bin/geocode.rb /app/geocode.rb
## Patch 7 End

## Issue 8
## entrypoint.R gets some environment variables from the docker image.
## Cannot set environment variables from init scripts so write to the .Renviron 
## file for root
## Patch 8 Start
mkdir -p /root
echo 'degauss_name="geocoder"'        >  /root/.Renviron
echo 'degauss_version="3.3.0"'        >> /root/.Renviron
echo 'degauss_description="geocodes"' >> /root/.Renviron
## Patch 8 End

## Issue 9
## It might be nice to have a global shortcut.
## Patch 9 Start
## Add global shortcut
echo "#!/bin/bash" > /root/geocoder/bin/geocode.sh
echo 'ruby /root/geocoder/bin/geocode.rb "$1"' >> /root/geocoder/bin/geocode.sh
chmod +x /root/geocoder/bin/geocode.sh
ln -s /root/geocoder/bin/geocode.sh /usr/bin/geocode
## Patch 9 End
