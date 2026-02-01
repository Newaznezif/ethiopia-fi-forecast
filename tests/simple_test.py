"""
Simple test file for quick validation of data_loader
"""
import pandas as pd
import numpy as np
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from data_loader import EthiopiaFIDataLoader

def test_basic_functionality():
    """Test basic data loader functionality"""
    print("🧪 Testing basic data loader functionality...")
    
    # Create a simple test
    test_data = pd.DataFrame({
        'record_id': ['test_obs_001'],
        'record_type': ['observation'],
        'pillar': ['Access'],
        'indicator': ['Test Indicator'],
        'indicator_code': ['TEST_INDICATOR'],
        'value_numeric': [50.0],
        'observation_date': ['2024-01-01'],
        'source_name': ['Test Source'],
        'source_url': ['https://test.com'],
        'confidence': ['high']
    })
    
    test_ref = pd.DataFrame({
        'field': ['record_type', 'pillar', 'confidence'],
        'code': ['observation', 'Access', 'high'],
        'description': ['Test', 'Test', 'Test']
    })
    
    # Save test files
    data_path = 'test_data.csv'
    ref_path = 'test_ref.csv'
    
    test_data.to_csv(data_path, index=False)
    test_ref.to_csv(ref_path, index=False)
    
    try:
        # Create loader
        loader = EthiopiaFIDataLoader(data_path, ref_path)
        
        # Load data
        data, ref_codes = loader.load_all_data()
        
        # Basic assertions
        assert len(data) == 1, f"Expected 1 record, got {len(data)}"
        assert len(ref_codes) == 3, f"Expected 3 reference codes, got {len(ref_codes)}"
        assert data.iloc[0]['record_type'] == 'observation'
        assert data.iloc[0]['value_numeric'] == 50.0
        
        # Test adding new observation
        new_id = loader.add_observation(
            pillar='Usage',
            indicator='New Test',
            indicator_code='NEW_TEST',
            value_numeric=75.0,
            observation_date='2024-12-01',
            source_name='New Source',
            source_url='https://new.test.com',
            confidence='medium'
        )
        
        assert new_id.startswith('obs_'), f"ID should start with 'obs_', got {new_id}"
        assert len(loader.data) == 2, f"Expected 2 records after addition, got {len(loader.data)}"
        
        # Test statistics
        stats = loader.get_record_type_stats()
        assert 'counts' in stats
        assert stats['counts']['observation'] == 2
        
        print("✅ All basic tests passed!")
        
    finally:
        # Clean up
        if os.path.exists(data_path):
            os.remove(data_path)
        if os.path.exists(ref_path):
            os.remove(ref_path)

def test_error_handling():
    """Test error handling"""
    print("\n🧪 Testing error handling...")
    
    # Test with non-existent files
    loader = EthiopiaFIDataLoader('nonexistent.csv', 'nonexistent_ref.csv')
    data, ref_codes = loader.load_all_data()
    
    assert data.empty, "Data should be empty for non-existent file"
    assert ref_codes.empty, "Ref codes should be empty for non-existent file"
    
    print("✅ Error handling tests passed!")

def test_schema_validation():
    """Test schema validation"""
    print("\n🧪 Testing schema validation...")
    
    # Create test data with missing columns
    incomplete_data = pd.DataFrame({
        'record_id': ['test1'],
        'record_type': ['observation']
        # Missing other columns
    })
    
    test_ref = pd.DataFrame({
        'field': ['record_type'],
        'code': ['observation'],
        'description': ['Test']
    })
    
    data_path = 'incomplete_test.csv'
    ref_path = 'incomplete_ref.csv'
    
    incomplete_data.to_csv(data_path, index=False)
    test_ref.to_csv(ref_path, index=False)
    
    try:
        loader = EthiopiaFIDataLoader(data_path, ref_path)
        data, _ = loader.load_all_data()
        
        # Should add missing columns
        assert 'pillar' in data.columns, "Should add missing pillar column"
        assert 'indicator' in data.columns, "Should add missing indicator column"
        
        print("✅ Schema validation tests passed!")
        
    finally:
        if os.path.exists(data_path):
            os.remove(data_path)
        if os.path.exists(ref_path):
            os.remove(ref_path)

if __name__ == '__main__':
    print("=" * 60)
    print("RUNNING DATA LOADER TESTS")
    print("=" * 60)
    
    try:
        test_basic_functionality()
        test_error_handling()
        test_schema_validation()
        
        print("\n" + "=" * 60)
        print("🎉 ALL TESTS PASSED SUCCESSFULLY!")
        print("=" * 60)
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        sys.exit(1)
