#!/usr/bin/env python3
"""Compare per-case outputs (OUTS/<case>/{user_daily,campaign_daily}) with fixtures/<case>/expected."""
import os, sys
from run_fixtures import SCHEMAS, load_expected, load_actual, rows

fx, outs = sys.argv[1], sys.argv[2]
cases = sorted(d for d in os.listdir(fx) if os.path.isdir(os.path.join(fx, d)))
bad = 0
for case in cases:
    ok = True
    for table, schema in SCHEMAS.items():
        exp = rows(load_expected(os.path.join(fx, case, "expected", f"{table}.csv"), schema))
        try:
            act = rows(load_actual(os.path.join(outs, case, table), schema))
        except Exception as e:
            ok = False; print(f"FAIL {case} {table}: {e}"); continue
        if exp != act:
            ok = False
            print(f"FAIL {case} {table}\n  expected {exp}\n  actual   {act}")
    bad += not ok
print(f"{len(cases)-bad}/{len(cases)} fixtures passed")
