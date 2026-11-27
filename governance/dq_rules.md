# Data quality rules

Pass rates below are estimated from the profiling run (before Silver cleaning), so the real numbers will differ slightly.

| Rule name | Dimension | What it checks | Severity | Threshold % | Reasoning | Est. pass % |
|---|---|---|---|---|---|---|
| values_parseable | Validity | Quantity, InvoiceDate and UnitPrice can be converted to their data types | critical | 99 | A value that cannot be parsed would silently become empty and corrupt revenue | about 100 |
| invoice_no_not_null | Completeness | InvoiceNo is present | critical | 99 | Every sale line must belong to an invoice | 100 |
| unit_price_positive | Validity | UnitPrice is greater than 0 | critical | 99 | A zero or negative price breaks revenue; sample rows look like stock adjustments | 99.5 |
| invoice_date_in_range | Validity | InvoiceDate is between 2010-12-01 and 2011-12-31 | critical | 99 | Data covers Dec 2010 to Dec 2011; anything else is a data entry or load error | 100 |
| quantity_sign_matches_invoice_type | Consistency | Sales have Quantity > 0; cancellations (C invoices) have Quantity < 0 | critical | 99 | 1,336 rows have negative quantity without a C invoice | 99.75 |
| country_in_reference | Reference data | Country exists in the approved country list | critical | 99 | Unspecified and European Community are not countries (507 rows) | 99.9 |
| exact_duplicate_rows | Uniqueness | Fully identical rows are removed in Silver | critical | 98 | 5,268 exact duplicates inflate sales | 99.0 |
| customer_id_not_null | Completeness | CustomerID is present | warning | 70 | About 25% are guest checkouts, which are legitimate, so rows are kept | 75 |
| description_not_null | Completeness | Description is present | warning | 99 | Descriptive field only, rows stay usable | 99.7 |
| stock_code_pattern | Consistency | StockCode matches the product pattern | warning | 98 | Non-product codes (POST, DOT, M, ...) are real business charges, not errors | 99.4 |
| invoice_stock_key_unique | Uniqueness | (InvoiceNo, StockCode) is unique | warning | 95 | Repeated lines for one product on one invoice can be legitimate | 98 |

**Critical** = rows that fail are moved to quarantine and a breach fails the pipeline run.
**Warning** = measured and reported, rows are kept.