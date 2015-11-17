#! /bin/bash

# This script installs the start STS related start/stop scripts
# to the proper directories under CentOS.   
#
# Script must be installed run as root
#
# Usage: install_service.sh [stssend | stsreceive]
#
# Brent Kolasinski
# Argonne National Laboratory
# brent@anl.gov

STSTYPE=$1

if [ $STSTYPE = 'stssend'  ]
  then
    cp ./stssend /etc/init.d/
    chkconfig --add stssend
elif [ $STSTYPE = 'stsreceive' ]
  then
    cp ./stsreceive /etc/init.d/
    chkconfig --add stsreceive
else
    echo -n "Usage: install_service.sh [stssend | stsreceive]\n"
fi

