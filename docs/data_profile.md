# Data profile: UCI Online Retail

Rows: 541,909 | Columns: 8 | Period: 2010-12-01 to 2011-12-09

## Findings
| # | Issue | Count | DQ dimension | Note |
|---|---|---|---|---|
| 1 | CustomerID missing | 135,080 (24.93%) | Completeness | Guest checkouts are legitimate, so a warning only |
| 2 | Description missing | 1,454 (0.27%) | Completeness | Warning |
| 3 | Exact duplicate rows | 5,268 | Uniqueness | Remove in Silver |
| 4 | Negative Quantity on a non-cancellation invoice | 1,336 | Consistency | Critical rule |
| 5 | UnitPrice <= 0 | 2,517 (2,515 zero + 2 negative) | Validity | Sample rows have no Description or CustomerID: looks like stock adjustments |
| 6 | Non-countries in Country | "Unspecified", "European Community" | Reference data | Fail the reference check |
| 7 | Country aliases | EIRE, USA, RSA | Consistency | Map to Ireland, United States, South Africa |
| 8 | Non-product stock codes | 2,995 rows (POST, DOT, M, C2, D, S, BANK CHARGES...) | Consistency | Warning |
| 9 | Lower-case letters in StockCode | 1,977 | Consistency | Upper-case in Silver |
| 10 | Same (InvoiceNo, StockCode), not exact duplicates | 10,615 rows | Uniqueness | Warning |
| 11 | Descriptions with leading/trailing spaces | 113,453 | Consistency | Trim in Silver (verify with examples) |