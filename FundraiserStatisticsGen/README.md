# Fundraising Statistics Generator

Exports general statistics about our fundraising campaign performance to CSV files.

## Overview (incomplete)
The configuration file has database connection settings, and follows the format of example file fundstatgen.cfg

#### fundstatgen.py
This script generates general statistics about the number of donations and total amount of money raised per hour.
Usage:
    PYTHONPATH=`pwd` python3 FundraiserStatisticsGen/fundstatgen.py -c <configpath> <outputdir>
