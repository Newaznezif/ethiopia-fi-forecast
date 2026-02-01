# verify_csv_files.py
"""Verify all CSV files are properly formatted"""

import pandas as pd
import csv
from pathlib import Path

print("🔍 VERIFYING ALL CSV FILES")
print("="*60)

data_dir = Path("data/raw")
files_to_check = [
    "ethiopia_fi_unified_data.csv",
    "impact_links.csv", 
    "reference_codes.csv"
]

all_good = True

for filename in files_to_check:
    filepath = data_dir / filename
    
    print(f"\n📄 Checking: {filename}")
    
    if not filepath.exists():
        print(f"  ❌ File not found: {filepath}")
        all_good = False
        continue
    
    # Method 1: Try pandas read
    try:
        df = pd.read_csv(filepath)
        print(f"  ✅ Pandas read successful")
        print(f"     Records: {len(df)}")
        print(f"     Columns: {len(df.columns)}")
        print(f"     Columns: {list(df.columns)}")
        
        # Display first 2 rows
        print(f"     Sample data:")
        print(df.head(2).to_string())
        
    except Exception as e:
        print(f"  ❌ Pandas read failed: {e}")
        
        # Method 2: Try CSV module
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
                
            print(f"  ⚠️  CSV module read: {len(rows)} rows")
            
            # Show first few rows
            for i, row in enumerate(rows[:3]):
                print(f"     Row {i}: {len(row)} fields - {row}")
                
        except Exception as e2:
            print(f"  ❌ CSV module also failed: {e2}")
        
        all_good = False
    
    print()

print("="*60)
if all_good:
    print("🎉 ALL CSV FILES ARE PROPERLY FORMATTED!")
else:
    print("⚠️  SOME CSV FILES HAVE ISSUES")

print("\nTo fix reference_codes.csv manually:")
print("1. Open it in a text editor")
print("2. Ensure exactly 3 columns per row")
print("3. Use quotes if descriptions contain commas")
print("4. Save as UTF-8 encoding")
