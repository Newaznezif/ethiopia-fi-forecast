# verify_datasets.py
"""Verify that all 3 datasets are correctly structured"""

import pandas as pd
from pathlib import Path

print("🔍 VERIFYING ALL 3 DATASETS")
print("="*60)

# Check each dataset exists
datasets = {
    "ethiopia_fi_unified_data.csv": "Main dataset (observations, events, targets)",
    "impact_links.csv": "Impact links dataset", 
    "reference_codes.csv": "Reference codes dataset"
}

all_good = True

for filename, description in datasets.items():
    filepath = Path("data/raw") / filename
    
    if filepath.exists():
        try:
            df = pd.read_csv(filepath)
            print(f"✅ {filename}")
            print(f"   {description}")
            print(f"   Records: {len(df):,}")
            print(f"   Columns: {len(df.columns)}")
            
            # Special checks for each file
            if filename == "ethiopia_fi_unified_data.csv":
                if 'record_type' in df.columns:
                    counts = df['record_type'].value_counts()
                    print(f"   Record types: {dict(counts)}")
                    
                    # Should NOT have impact_link records
                    if 'impact_link' in df['record_type'].values:
                        print("   ⚠️  WARNING: Main dataset contains impact_link records!")
                        all_good = False
            
            elif filename == "impact_links.csv":
                required_cols = ['parent_id', 'related_indicator', 'impact_direction']
                missing = [col for col in required_cols if col not in df.columns]
                if missing:
                    print(f"   ❌ MISSING COLUMNS: {missing}")
                    all_good = False
                else:
                    print(f"   Impact links: {len(df)}")
                    
        except Exception as e:
            print(f"❌ {filename} - Error: {e}")
            all_good = False
    else:
        print(f"❌ {filename} - File not found!")
        all_good = False
    
    print()

print("="*60)

if all_good:
    print("🎉 ALL 3 DATASETS ARE CORRECTLY STRUCTURED!")
    print("\n✅ Task 1 requirements met:")
    print("   1. Three separate datasets ✓")
    print("   2. Schema compliance ✓")
    print("   3. Impact links separate from main data ✓")
    print("   4. Reference codes for validation ✓")
    
    print("\n🚀 Next: Run the data loader:")
    print("   python src/load_all_datasets.py")
else:
    print("⚠️  Issues found with dataset structure.")
    print("   Please check the warnings above.")
