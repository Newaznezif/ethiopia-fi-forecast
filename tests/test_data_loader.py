"""
Unit tests for data_loader module - Task 1: Data Exploration & Enrichment
Tests ensure schema compliance and data validation
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from data_loader import EthiopiaFIDataLoader

# ============================================
# TEST DATA LOADER INITIALIZATION
# ============================================

class TestDataLoaderInitialization:
    """Test data loader initialization"""
    
    def test_init_with_paths(self, tmp_path):
        """Test initialization with custom paths"""
        data_path = tmp_path / "data.csv"
        ref_path = tmp_path / "ref.csv"
        
        loader = EthiopiaFIDataLoader(str(data_path), str(ref_path))
        
        assert loader.data_path == str(data_path)
        assert loader.ref_codes_path == str(ref_path)
        assert loader.data is None
        assert loader.reference_codes is None
    
    def test_init_default_paths(self):
        """Test initialization with default paths"""
        loader = EthiopiaFIDataLoader()
        
        expected_data_path = os.path.join('data', 'raw', 'ethiopia_fi_unified_data.csv')
        expected_ref_path = os.path.join('data', 'raw', 'reference_codes.csv')
        
        assert expected_data_path in loader.data_path
        assert expected_ref_path in loader.ref_codes_path

# ============================================
# TEST DATA LOADING FUNCTIONALITY
# ============================================

@pytest.fixture
def sample_data():
    """Create sample data for testing"""
    return pd.DataFrame({
        'record_id': ['obs_001', 'evt_001', 'obs_002'],
        'record_type': ['observation', 'event', 'observation'],
        'pillar': ['Access', '', 'Usage'],
        'indicator': ['Account Ownership', 'Telebirr Launch', 'Digital Payments'],
        'indicator_code': ['ACC_OWNERSHIP', 'EVENT_PRODUCT_LAUNCH', 'USG_DIGITAL_PAYMENT'],
        'value_numeric': [49.0, np.nan, 35.0],
        'observation_date': ['2024-01-01', np.nan, '2024-06-01'],
        'event_date': [np.nan, '2021-05-01', np.nan],
        'source_name': ['World Bank', 'NBE', 'NBE'],
        'source_url': ['https://example.com/wb', 'https://example.com/nbe', 'https://example.com/nbe2'],
        'confidence': ['high', 'high', 'high'],
        'notes': ['Global Findex 2024', 'Launch of Telebirr', 'NBE report'],
        'collected_by': ['system', 'system', 'system'],
        'collection_date': ['2024-01-01', '2024-01-01', '2024-06-01']
    })

@pytest.fixture
def sample_ref_codes():
    """Create sample reference codes"""
    return pd.DataFrame({
        'field': ['record_type', 'record_type', 'record_type',
                  'pillar', 'pillar', 'pillar',
                  'confidence', 'confidence', 'confidence'],
        'code': ['observation', 'event', 'target',
                 'Access', 'Usage', 'Infrastructure',
                 'high', 'medium', 'low'],
        'description': ['Observation record', 'Event record', 'Target record',
                       'Access pillar', 'Usage pillar', 'Infrastructure pillar',
                       'High confidence', 'Medium confidence', 'Low confidence']
    })

class TestDataLoading:
    """Test data loading functionality"""
    
    def test_load_all_data(self, tmp_path, sample_data, sample_ref_codes):
        """Test loading all data"""
        data_path = tmp_path / "test_data.csv"
        ref_path = tmp_path / "test_ref.csv"
        
        sample_data.to_csv(data_path, index=False)
        sample_ref_codes.to_csv(ref_path, index=False)
        
        loader = EthiopiaFIDataLoader(str(data_path), str(ref_path))
        data, ref_codes = loader.load_all_data()
        
        assert data is not None
        assert ref_codes is not None
        assert len(data) == 3
        assert len(ref_codes) == 9
        assert 'record_type' in data.columns
        assert 'field' in ref_codes.columns
    
    def test_load_missing_data_file(self, tmp_path):
        """Test loading when data file doesn't exist"""
        data_path = tmp_path / "nonexistent_data.csv"
        ref_path = tmp_path / "nonexistent_ref.csv"
        
        loader = EthiopiaFIDataLoader(str(data_path), str(ref_path))
        data, ref_codes = loader.load_all_data()
        
        assert data is not None
        assert ref_codes is not None
        assert data.empty
        assert ref_codes.empty
    
    def test_date_conversion(self, tmp_path, sample_data, sample_ref_codes):
        """Test that date columns are converted correctly"""
        data_path = tmp_path / "test_data.csv"
        ref_path = tmp_path / "test_ref.csv"
        
        sample_data.to_csv(data_path, index=False)
        sample_ref_codes.to_csv(ref_path, index=False)
        
        loader = EthiopiaFIDataLoader(str(data_path), str(ref_path))
        data, _ = loader.load_all_data()
        
        # Check date columns are datetime
        assert pd.api.types.is_datetime64_any_dtype(data['observation_date'])
        assert pd.api.types.is_datetime64_any_dtype(data['event_date'])
        assert pd.api.types.is_datetime64_any_dtype(data['collection_date'])

# ============================================
# TEST SCHEMA VALIDATION
# ============================================

class TestSchemaValidation:
    """Test schema validation functionality"""
    
    def test_validate_categorical_fields(self, tmp_path, sample_data, sample_ref_codes):
        """Test validation of categorical fields"""
        data_path = tmp_path / "test_data.csv"
        ref_path = tmp_path / "test_ref.csv"
        
        sample_data.to_csv(data_path, index=False)
        sample_ref_codes.to_csv(ref_path, index=False)
        
        loader = EthiopiaFIDataLoader(str(data_path), str(ref_path))
        data, _ = loader.load_all_data()
        
        # Check that valid codes cache is populated
        assert 'record_type' in loader.valid_codes_cache
        assert 'pillar' in loader.valid_codes_cache
        assert 'confidence' in loader.valid_codes_cache
        
        # Check valid values
        valid_record_types = loader.valid_codes_cache['record_type']
        assert 'observation' in valid_record_types
        assert 'event' in valid_record_types
    
    def test_validate_invalid_data(self, tmp_path, sample_ref_codes):
        """Test validation with invalid data"""
        invalid_data = pd.DataFrame({
            'record_id': ['obs1', 'obs2'],
            'record_type': ['observation', 'INVALID_TYPE'],  # Invalid record type
            'pillar': ['Access', 'INVALID_PILLAR'],  # Invalid pillar
            'confidence': ['high', 'INVALID_CONF'],  # Invalid confidence
            'indicator': ['Test1', 'Test2'],
            'indicator_code': ['TEST1', 'TEST2']
        })
        
        data_path = tmp_path / "invalid_data.csv"
        ref_path = tmp_path / "ref.csv"
        
        invalid_data.to_csv(data_path, index=False)
        sample_ref_codes.to_csv(ref_path, index=False)
        
        loader = EthiopiaFIDataLoader(str(data_path), str(ref_path))
        loader.load_all_data()
        
        # Should still load but log warnings
        assert loader.data is not None
        assert len(loader.data) == 2

# ============================================
# TEST DATA ADDITION
# ============================================

class TestDataAddition:
    """Test adding new data"""
    
    def test_add_observation(self, tmp_path, sample_data, sample_ref_codes):
        """Test adding a new observation"""
        data_path = tmp_path / "test_data.csv"
        ref_path = tmp_path / "test_ref.csv"
        
        sample_data.to_csv(data_path, index=False)
        sample_ref_codes.to_csv(ref_path, index=False)
        
        loader = EthiopiaFIDataLoader(str(data_path), str(ref_path))
        loader.load_all_data()
        initial_count = len(loader.data)
        
        new_id = loader.add_observation(
            pillar='Access',
            indicator='Mobile Money Accounts',
            indicator_code='ACC_MM_ACCOUNT',
            value_numeric=9.45,
            observation_date='2024-12-01',
            source_name='Global Findex',
            source_url='https://globalfindex.worldbank.org/',
            confidence='high',
            notes='2024 estimate'
        )
        
        assert new_id.startswith('obs_')
        assert len(loader.data) == initial_count + 1
        
        # Verify the new record
        new_record = loader.data[loader.data['record_id'] == new_id].iloc[0]
        assert new_record['record_type'] == 'observation'
        assert new_record['pillar'] == 'Access'
        assert new_record['value_numeric'] == 9.45
        assert new_record['confidence'] == 'high'
    
    def test_add_event(self, tmp_path, sample_data, sample_ref_codes):
        """Test adding a new event"""
        data_path = tmp_path / "test_data.csv"
        ref_path = tmp_path / "test_ref.csv"
        
        sample_data.to_csv(data_path, index=False)
        sample_ref_codes.to_csv(ref_path, index=False)
        
        loader = EthiopiaFIDataLoader(str(data_path), str(ref_path))
        loader.load_all_data()
        initial_count = len(loader.data)
        
        new_id = loader.add_event(
            event_name='M-Pesa Launch',
            event_date='2023-08-01',
            category='product_launch',
            description='Commercial launch of M-Pesa in Ethiopia',
            source_name='Safaricom',
            source_url='https://safaricom.et',
            confidence='high'
        )
        
        assert new_id.startswith('evt_')
        assert len(loader.data) == initial_count + 1
        
        # Verify the new record
        new_record = loader.data[loader.data['record_id'] == new_id].iloc[0]
        assert new_record['record_type'] == 'event'
        assert new_record['event_name'] == 'M-Pesa Launch'
        assert new_record['event_category'] == 'product_launch'
        assert new_record['confidence'] == 'high'

# ============================================
# TEST DATA ANALYSIS
# ============================================

class TestDataAnalysis:
    """Test data analysis functions"""
    
    def test_get_record_type_stats(self, tmp_path, sample_data, sample_ref_codes):
        """Test getting record type statistics"""
        data_path = tmp_path / "test_data.csv"
        ref_path = tmp_path / "test_ref.csv"
        
        sample_data.to_csv(data_path, index=False)
        sample_ref_codes.to_csv(ref_path, index=False)
        
        loader = EthiopiaFIDataLoader(str(data_path), str(ref_path))
        loader.load_all_data()
        stats = loader.get_record_type_stats()
        
        assert 'counts' in stats
        assert 'total_records' in stats
        
        counts = stats['counts']
        assert counts['observation'] == 2
        assert counts['event'] == 1
        assert stats['total_records'] == 3
    
    def test_get_temporal_coverage(self, tmp_path, sample_data, sample_ref_codes):
        """Test getting temporal coverage"""
        data_path = tmp_path / "test_data.csv"
        ref_path = tmp_path / "test_ref.csv"
        
        sample_data.to_csv(data_path, index=False)
        sample_ref_codes.to_csv(ref_path, index=False)
        
        loader = EthiopiaFIDataLoader(str(data_path), str(ref_path))
        loader.load_all_data()
        coverage = loader.get_temporal_coverage()
        
        # Should return a DataFrame
        assert isinstance(coverage, pd.DataFrame)
        
        if not coverage.empty:
            assert 'first_date' in coverage.columns
            assert 'last_date' in coverage.columns
            assert 'count' in coverage.columns

# ============================================
# TEST DATA SAVING
# ============================================

class TestDataSaving:
    """Test data saving functionality"""
    
    def test_save_enriched_data(self, tmp_path, sample_data, sample_ref_codes):
        """Test saving enriched data"""
        data_path = tmp_path / "test_data.csv"
        ref_path = tmp_path / "test_ref.csv"
        
        sample_data.to_csv(data_path, index=False)
        sample_ref_codes.to_csv(ref_path, index=False)
        
        loader = EthiopiaFIDataLoader(str(data_path), str(ref_path))
        loader.load_all_data()
        
        # Add some data
        loader.add_observation(
            pillar='Usage',
            indicator='Test',
            indicator_code='TEST',
            value_numeric=50.0,
            observation_date='2025-01-01',
            source_name='Test',
            source_url='https://test.com',
            confidence='high'
        )
        
        # Save to temporary location
        output_path = tmp_path / "enriched_data.csv"
        loader.save_enriched_data(str(output_path))
        
        # Verify file was created
        assert output_path.exists()
        
        # Load and verify
        saved_data = pd.read_csv(output_path)
        assert len(saved_data) == 4  # Original 3 + 1 new
        assert 'TEST' in saved_data['indicator_code'].values

# ============================================
# TEST INTEGRATION SCENARIO
# ============================================

class TestIntegration:
    """Test integration scenario"""
    
    def test_full_workflow(self, tmp_path):
        """Test complete workflow: load, enrich, save"""
        # Create initial data
        initial_data = pd.DataFrame({
            'record_id': ['obs_init'],
            'record_type': ['observation'],
            'pillar': ['Access'],
            'indicator': ['Initial'],
            'indicator_code': ['INIT'],
            'value_numeric': [10.0],
            'observation_date': ['2024-01-01'],
            'source_name': ['Test'],
            'source_url': ['https://test.com'],
            'confidence': ['high']
        })
        
        ref_codes = pd.DataFrame({
            'field': ['record_type', 'pillar', 'confidence'],
            'code': ['observation', 'Access', 'high'],
            'description': ['Test', 'Test', 'Test']
        })
        
        data_path = tmp_path / "workflow_data.csv"
        ref_path = tmp_path / "workflow_ref.csv"
        
        initial_data.to_csv(data_path, index=False)
        ref_codes.to_csv(ref_path, index=False)
        
        # Create loader and load data
        loader = EthiopiaFIDataLoader(str(data_path), str(ref_path))
        data, refs = loader.load_all_data()
        assert len(data) == 1
        
        # Add new data
        loader.add_observation(
            pillar='Access',
            indicator='Account Ownership',
            indicator_code='ACC_OWNERSHIP',
            value_numeric=49.0,
            observation_date='2024-06-01',
            source_name='World Bank',
            source_url='https://worldbank.org',
            confidence='high'
        )
        
        # Add event
        loader.add_event(
            event_name='Test Event',
            event_date='2024-03-01',
            category='policy',
            description='Test policy',
            source_name='Government',
            source_url='https://gov.et',
            confidence='high'
        )
        
        # Verify data
        assert len(loader.data) == 3
        assert loader.data['record_type'].value_counts()['observation'] == 2
        assert loader.data['record_type'].value_counts()['event'] == 1
        
        # Save enriched data
        output_path = tmp_path / "output.csv"
        loader.save_enriched_data(str(output_path))
        
        # Verify saved file
        saved_data = pd.read_csv(output_path)
        assert len(saved_data) == 3
        assert 'ACC_OWNERSHIP' in saved_data['indicator_code'].values

# ============================================
# MAIN TEST RUNNER
# ============================================

if __name__ == '__main__':
    print("Running data loader tests...")
    print("=" * 50)
    
    # Run tests
    import pytest
    pytest.main([__file__, '-v', '--tb=short'])
    
    print("\n✅ All tests completed successfully!")
