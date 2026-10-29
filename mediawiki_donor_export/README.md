# MediaWiki Donor Status Export

Exports donor status data from `silverpop_export_view_full` for sync to
MediaWiki user preferences. Produces a headerless CSV with the columns
`email` and `relationship_type`.

Requires the silverpop export to have run first (it builds the views this
module reads from). Data is read from `silverpop_export_view_full`, but the
freshness check runs against the underlying `silverpop_export` table - views
have no update time of their own - and will abort if it has not been updated
within the last 36 hours.

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

## Relationship Type Values

`relationship_type` is set from `donor_status_otg` and
`donor_status_recur_overall`, using the approved mapping in
[T436548](https://phabricator.wikimedia.org/T436548). The rules are checked
in this order and the first rule that matches is used:

| Condition                                      | Description                                    | relationship_type               |
|------------------------------------------------|------------------------------------------------|---------------------------------|
| `donor_status_recur_overall` in 15, 25, 35, 45 | New, Active, Paused, Failing recurring         | 2 - Sustaining Donor            |
| `donor_status_otg` in 10, 20, 40, 50           | Consecutive or Reactivated OTG this FY or last | 4 - Returning / Loyal Supporter |
| `donor_status_otg` in 30, 60                   | New OTG this FY or last                        | 1 - New / Recent Supporter      |
| `donor_status_recur_overall` in 55, 65         | Failed or Cancelled recurring                  | 3 - Lapsed Supporter            |
| `donor_status_otg` in 70, 80, 90               | Lapsed, Deep Lapsed, Ultra Lapsed OTG          | 3 - Lapsed Supporter            |
| any other                                      |                                                | 5 - Contactable Reader          |
