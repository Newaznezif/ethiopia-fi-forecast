# src/load_all_datasets.py
"""
Load ALL THREE datasets required for Task 1
"""

import pandas as pd
from pathlib import Path

def load_all_datasets():
    """Load all three datasets specified in Task 1"""
    
    data_dir = Path("data/raw")
    
    print("📂 LOADING ALL 3 DATASETS FOR TASK 1")
    print("="*50)
    
    # 1. Main unified dataset
    print("\n1. Loading main unified dataset...")
    try:
        main_df = pd.read_csv(data_dir / "ethiopia_fi_unified_data.csv")
        print(f"   ✅ Loaded: {len(main_df)} records")
        
        # Count by record type
        record_counts = main_df['record_type'].value_counts()
        for rt, count in record_counts.items():
            print(f"     • {rt}: {count}")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        main_df = None
    
    # 2. Impact links dataset
    print("\n2. Loading impact links dataset...")
    try:
        impact_df = pd.read_csv(data_dir / "impact_links.csv")
        print(f"   ✅ Loaded: {len(impact_df)} impact links")
        
        # Show relationships
        print(f"     • Events linked: {impact_df['parent_id'].nunique()}")
        print(f"     • Indicators affected: {impact_df['related_indicator'].nunique()}")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        impact_df = None
    
    # 3. Reference codes dataset
    print("\n3. Loading reference codes dataset...")
    try:
        ref_df = pd.read_csv(data_dir / "reference_codes.csv")
        print(f"   ✅ Loaded: {len(ref_df)} reference codes")
        
        # Show by field
        print(f"     • Fields defined: {ref_df['field'].nunique()}")
        for field in ref_df['field'].unique():
            codes = ref_df[ref_df['field'] == field]['code'].tolist()
            print(f"     • {field}: {len(codes)} codes")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        ref_df = None
    
    print("\n" + "="*50)
    print("✅ ALL 3 DATASETS LOADED SUCCESSFULLY")
    print("="*50)
    
    return main_df, impact_df, ref_df

def validate_schema_compliance(main_df, impact_df, ref_df):
    """Validate that datasets follow the required schema"""
    
    print("\n🔍 VALIDATING SCHEMA COMPLIANCE")
    print("="*50)
    
    issues = []
    
    # Check main dataset
    if main_df is not None:
        required_columns = ['id', 'record_type']
        missing_cols = [col for col in required_columns if col not in main_df.columns]
        if missing_cols:
            issues.append(f"Main dataset missing columns: {missing_cols}")
        
        # Check that impact_links are NOT in main dataset
        if 'impact_link' in main_df['record_type'].values:
            issues.append("Main dataset should NOT contain impact_link records")
    
    # Check impact links dataset
    if impact_df is not None:
        required_impact_cols = ['parent_id', 'related_indicator', 'impact_direction']
        missing_impact_cols = [col for col in required_impact_cols if col not in impact_df.columns]
        if missing_impact_cols:
            issues.append(f"Impact links missing columns: {missing_impact_cols}")
    
    # Check reference codes
    if ref_df is not None:
        required_ref_cols = ['field', 'code', 'description']
        missing_ref_cols = [col for col in required_ref_cols if col not in ref_df.columns]
        if missing_ref_cols:
            issues.append(f"Reference codes missing columns: {missing_ref_cols}")
    
    # Report issues
    if issues:
        print("❌ Schema compliance issues found:")
        for issue in issues:
            print(f"   • {issue}")
        return False
    else:
        print("✅ All datasets follow required schema")
        return True

def explore_combined_data(main_df, impact_df):
    """Explore the combined view of data"""
    
    print("\n🔗 EXPLORING EVENT-IMPACT RELATIONSHIPS")
    print("="*50)
    
    if main_df is not None and impact_df is not None:
        # Get events from main dataset
        events = main_df[main_df['record_type'] == 'event']
        
        # Join with impact links
        combined = pd.merge(
            impact_df,
            events[['id', 'event_name', 'event_date', 'category']],
            left_on='parent_id',
            right_on='id',
            how='left',
            suffixes=('_impact', '_event')
        )
        
        print(f"Modeled relationships found: {len(combined)}")
        
        if not combined.empty:
            print("\n📋 Event-Impact Relationships:")
            for _, row in combined.iterrows():
                print(f"\n• {row['event_name']} → {row['related_indicator']}")
                print(f"  Direction: {row['impact_direction']}")
                print(f"  Magnitude: {row['impact_magnitude']}")
                print(f"  Lag: {row['lag_months']} months")
                print(f"  Evidence: {row['evidence_basis']}")
    
    return combined

if __name__ == "__main__":
    # Load all datasets
    main_df, impact_df, ref_df = load_all_datasets()
    
    # Validate schema
    is_valid = validate_schema_compliance(main_df, impact_df, ref_df)
    
    # Explore combined view
    if is_valid:
        combined = explore_combined_data(main_df, impact_df)
        
        print("\n" + "="*50)
        print("🎯 TASK 1: DATA EXPLORATION COMPLETE")
        print("="*50)
        print("\nNext steps:")
        print("1. Review the loaded datasets")
        print("2. Check data_enrichment_log.md for additions")
        print("3. Run: python src/eda.py for exploratory analysis")
