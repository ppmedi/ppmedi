#!/bin/bash

# You first need to create a file called inp_headers.csv
# which is a csv file containing a single line of the column names
# found in inp_clm_saf_lds_100_2009.fts

# Example script, assuming inp file is in parent directory
#
./stripcols.sh inp_headers.csv > data.thin
./stripcols.sh ../inp_clm_saf_lds_100_2009.csv >> data.thin

# call DBTruck
#
importmydata.py inp ppdatabase data.thin
