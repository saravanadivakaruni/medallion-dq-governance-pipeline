# Data dictionary: silver_retail

| Column | Type | Description | Rule | Nullable | CDE | Classification |
|---|---|---|---|---|---|---|
| InvoiceNo | string | Invoice number; a C prefix marks a cancellation | Not null | No | Yes | Internal |
| StockCode | string | Product code, upper-cased | Product pattern: five digits plus up to two letters | No | Yes | Internal |
| Description | string | Product description, trimmed | Free text | Yes | No | Internal |
| Quantity | int | Units on the line | Positive for sales, negative for cancellations | No | Yes | Internal |
| InvoiceDate | timestamp | Date and time of the invoice | 2010-12-01 to 2011-12-31 (data runs to 2011-12-09) | No | Yes | Internal |
| UnitPrice | double | Price per unit | Greater than 0 | No | Yes | Internal |
| CustomerID | string | Customer identifier | Empty for guest checkouts | Yes | Yes | Confidential |
| Country | string | Customer country | Must exist in ref_country (aliases EIRE, USA, RSA mapped) | No | No | Internal |
| _bronze_row_id | bigint | Row id from the Bronze table, for traceability | System generated | No | No | Internal |
| _run_id | string | Pipeline run that produced the row (yyyyMMddHHmmss) | System generated | No | No | Internal |