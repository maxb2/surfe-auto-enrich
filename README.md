# Surfe Auto Enrichment

This is a cli that will submit a csv from CRM to Surfe for enrichment.
It will make an output csv with the data from Surfe and flag the records if there is a chnge.

## Getting Started

1. [Install uv](https://docs.astral.sh/uv/getting-started/installation/).
2. Add your API key to `example.env` and rename it to `.env`.
3. Run `uv run surfe-enrich` in this folder.
4. Profit!

## Output CSV fields

- The fields indicating data from surfe will have a `_surfe` suffix.
- The `crm_email_in_surfe_results` field indicates that the CRM email was in the list of valid/cacthall emails returned by surfe.
A true indicates the CRM email is probably still valid. A false indicates that you should take action on the results.
- The `has_diff` field indicates that the name or LinkedIn urls have changed and you should take action.
- Other data from Surfe is left as-is in case you want to review it.