# Revisit after studying

## Rewrite in my own words
- [ ] governance/dq_rules.md: the Reasoning column
- [ ] governance/business_glossary.csv: all definitions
- [ ] docs/data_profile.md: the Note column

## Concepts to understand before interviews
- [ ] The six data quality dimensions (accuracy, completeness, consistency, timeliness, uniqueness, validity) with one example each from this project
- [ ] Medallion architecture: what belongs in Bronze, Silver and Gold, and why Bronze stays raw
- [ ] Why we read everything as strings in Bronze
- [ ] ANSI mode and why we use try_cast instead of cast
- [ ] Delta tables: what they add (history, safe overwrites)
- [ ] Critical vs warning severity, and why missing CustomerID is only a warning
- [ ] The quarantine pattern: why we move bad rows aside instead of deleting them
- [ ] Great Expectations: expectations, batches, results
- [ ] Soda Core: SodaCL checks and how they differ from Great Expectations
- [ ] Why two DQ engines and a cross-check
- [ ] Critical data elements, data classification, owner vs steward vs custodian
- [ ] Unity Catalog: catalog, schema, volume, tags, lineage
