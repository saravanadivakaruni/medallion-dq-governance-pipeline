"""Checks that your governance files agree with each other. Run from the project root:
python scripts/check_governance.py"""
import re
import sys
import pandas as pd

COLUMNS = ["InvoiceNo", "StockCode", "Description", "Quantity",
           "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
problems = []

def check(label, ok, detail=""):
    print(("PASS  " if ok else "FAIL  ") + label + (f"  -> {detail}" if detail and not ok else ""))
    if not ok:
        problems.append(label)

rules_text = open("governance/dq_rules.md", encoding="utf-8").read()
rule_names = set(re.findall(r"^\|\s*([a-z_]+)\s*\|", rules_text, flags=re.M))
check("dq_rules.md lists 11 rules", len(rule_names) == 11, f"found {len(rule_names)}: {sorted(rule_names)}")

glossary = pd.read_csv("governance/business_glossary.csv")
bad = [c.strip() for cell in glossary["related_columns"] for c in cell.split(";") if c.strip() not in COLUMNS]
check(f"glossary has {len(glossary)} terms and every related column exists", not bad, f"unknown columns: {bad}")

cde = pd.read_csv("governance/critical_data_elements.csv")
bad_cde = [c for c in cde["cde"] if c not in COLUMNS]
check(f"{len(cde)} CDEs are real columns", not bad_cde, f"unknown: {bad_cde}")
linked = {c.strip() for cell in cde["linked_dq_checks"] for c in cell.split(";")}
check("every CDE-linked check exists in dq_rules.md", linked <= rule_names, f"missing: {sorted(linked - rule_names)}")

cls = pd.read_csv("governance/data_classification.csv")
check("classification covers all 8 columns exactly once",
      sorted(cls["column"]) == sorted(COLUMNS), f"got {sorted(cls['column'])}")

own = pd.read_csv("governance/data_ownership_matrix.csv")
check(f"ownership matrix has {len(own)} rows with no empty owner/steward/custodian",
      not own[["data_owner_role", "data_steward_role", "data_custodian_role"]].isna().any().any())

ref = pd.read_csv("governance/ref_country.csv")
check(f"ref_country.csv has 36 countries (found {len(ref)})", len(ref) == 36)

print("\nALL CHECKS PASSED" if not problems else f"\n{len(problems)} CHECK(S) FAILED")
sys.exit(1 if problems else 0)