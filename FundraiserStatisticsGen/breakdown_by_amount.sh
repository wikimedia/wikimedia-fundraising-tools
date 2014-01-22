#!/bin/bash

echo "bound\tusd_sum\tusd_avg\tcount" > fundraiser_amount_breakdown.tsv
mysql -BN civicrm < fundraiser_amount_breakdown.sql >> fundraiser_amount_breakdown.tsv

