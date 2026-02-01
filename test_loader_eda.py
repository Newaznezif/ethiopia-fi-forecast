# test_loader_eda.py
"""Test the data loader and EDA setup"""

print("🧪 Testing Data Loader and EDA Setup...")
print("="*50)

try:
    # Test data loader
    from src.data_loader import EthiopiaFIDataLoader, test_data_loader
    
    print("1. Testing data loader...")
    loader = test_data_loader()
    
    if loader:
        print("\n✅ Data loader test successful!")
        
        # Test getting specific data
        print("\n2. Testing data access methods...")
        
        acc_data = loader.get_account_ownership_data()
        print(f"   • Account ownership data: {len(acc_data)} records")
        
        usage_data = loader.get_usage_data()
        print(f"   • Usage data: {len(usage_data)} records")
        
        infra_data = loader.get_infrastructure_data()
        print(f"   • Infrastructure data: {len(infra_data)} records")
        
        impacts = loader.get_event_impacts()
        print(f"   • Event impacts: {len(impacts)} records")
        
        print("\n🎉 All data access methods working!")
        
        print("\n3. Testing notebook data loading...")
        # Test direct loading like the notebook does
        import pandas as pd
        from pathlib import Path
        
        data_dir = Path("data/raw")
        
        main_df = pd.read_csv(data_dir / "ethiopia_fi_unified_data.csv")
        impact_df = pd.read_csv(data_dir / "impact_links.csv")
        ref_df = pd.read_csv(data_dir / "reference_codes.csv")
        
        print(f"   • Main data loaded: {len(main_df)} records")
        print(f"   • Impact links loaded: {len(impact_df)} records")
        print(f"   • Reference codes loaded: {len(ref_df)} records")
        
        print("\n✅ Notebook data loading test successful!")
        
    else:
        print("❌ Data loader test failed")
        
except Exception as e:
    print(f"❌ Error during testing: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*50)
print("🚀 Ready to run Task 2 EDA notebook!")
print("\nTo run the notebook:")
print("1. Open notebooks/02_eda.ipynb in VS Code")
print("2. Install Jupyter extension if not already")
print("3. Run cells sequentially")
print("\nOr run: jupyter notebook notebooks/02_eda.ipynb")
