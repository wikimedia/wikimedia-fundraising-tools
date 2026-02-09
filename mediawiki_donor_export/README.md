# MediaWiki Donor Status Export

Exports donor status data from `silverpop_export_view_full` for sync to
MediaWiki user preferences. Produces a CSV containing `contact_id`,
`email_address`, and `donor_status_id`.

Requires the silverpop export to have run first (it builds the views this
module reads from).

## Usage

```bash
# Full export of all contacts
python -m mediawiki_donor_export.export

# Delta export: contacts modified in the last N days
python -m mediawiki_donor_export.export --days 3
```

If `--days` is not specified, falls back to `offset_in_days` from config,
or does a full export if that is also unset.

## Configuration

Copy `mediawiki_donor_export.yaml.example` to
`~/.fundraising/mediawiki_donor_export.yaml` and edit as needed.

## Output

See `sample_output.csv` for an example of the export format.

## Donor Status Values

The `donor_status_id` values come directly from `wmf_donor.donor_status_id`
in CiviCRM. See the
[technical implementation doc](https://phabricator.wikimedia.org/T416638)
for the full value table.
