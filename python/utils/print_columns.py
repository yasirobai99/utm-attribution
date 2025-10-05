#!/usr/bin/env python3
# python/utils/print_columns.py
# Quick helper to print the first 50 column names of a CSV.

import sys
import pandas as pd

if len(sys.argv) < 2:
    print("Usage: python python/utils/print_columns.py <path_to_csv>")
    sys.exit(1)

path = sys.argv[1]
df = pd.read_csv(path, nrows=5)
print("Columns:", list(df.columns))
print(df.head(3))
