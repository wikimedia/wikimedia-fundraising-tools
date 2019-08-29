# Paypal Audit Scripts

## Installation
```sh
pip install -r requirements.txt
cp config.yaml.example /etc/fundraising/paypal-audit.yaml
```
Update the paypal-audit.yaml config values to match your environment equivalents

## Overview (probably not complete)

#### download_nightly.py
This script uses an SFTP client to download files from the stfp server specified in the 'sftp:' config block. The files are then dropped in to the path specified in the 'incoming_path:' config value. Currently, the script is pulling PayPal audit CSV files down which are then used by the
parse_nightly.py script.

#### parse_nightly.py
This script processes the files pulled down by the download_nightly.py script. It looks for files in the path specified within the 'incoming_path:' config value and if a file is found and does not already exist in the path specified within 'archive_path' config value, it is processed.

Processing involves first confirming the filename matches the following pattern:
`^(?P<type>[A-Z]{3})-(?P<date>[0-9]{8})[.](?P<sequence>[0-9]{2})[.](?P<version>[0-9]{3})[.]CSV$`
e.g. TRR-20190101.01.001.CSV and if true the script begins iterating through the CSV treating each line item as a transaction record from PayPal. The transaction data is normalised and then a check is performed to see if the transaction already exists within the CiviCRM database using the details specified within the 'civicrm_db:' config block.
If the transaction does not exist, it is pushed to the appropriate donations/recurring/refund queue to be then processed by the Drupal queue consumers.