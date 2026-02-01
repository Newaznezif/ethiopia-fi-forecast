# Simple test script
print("🧪 Testing Data Loader...")
print("="*40)

try:
    from src.data_loader import DataLoader
    
    # Create loader instance
    loader = DataLoader()
    
    # Run analysis
    print("\nRunning full analysis pipeline:")
    df, data_dict = loader.run_full_analysis()
    
    # Show some stats
    if df is not None:
        print("\n📋 Quick Stats:")
        print(f"Total records: {len(df)}")
        
        # Check record types
        for rt in df['record_type'].unique():
            count = len(df[df['record_type'] == rt])
            print(f"{rt}: {count}")
        
        # Show account ownership trend
        if 'observations' in data_dict:
            acc_data = data_dict['observations'][
                data_dict['observations']['indicator_code'] == 'ACC_OWNERSHIP'
            ]
            if len(acc_data) > 0:
                print(f"\n📈 Account Ownership points: {len(acc_data)}")
                print(f"From {acc_data['observation_date'].min().year} to {acc_data['observation_date'].max().year}")
    
    print("\n✅ Test completed successfully!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
