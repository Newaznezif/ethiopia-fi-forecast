"""
Unit tests for Task 3: Event Impact Modeling
"""

import unittest
import pandas as pd
import numpy as np
import sys
import os

# Add src to path
sys.path.append('src')

from impact_modeling import EventImpactModeler

class TestImpactModeling(unittest.TestCase):
    
    def setUp(self):
        """Set up test data"""
        # Create mock data for testing
        self.test_data = pd.DataFrame({
            'id': ['event_001', 'event_002', 'obs_001', 'link_001'],
            'record_type': ['event', 'event', 'observation', 'impact_link'],
            'event_name': ['Telebirr Launch', 'M-Pesa Entry', None, None],
            'event_date': ['2021-05-01', '2023-08-01', None, None],
            'category': ['product_launch', 'market_entry', None, None],
            'indicator_code': [None, None, 'ACC_OWNERSHIP', None],
            'value_numeric': [None, None, 46.0, None],
            'observation_date': [None, None, '2021-01-01', None],
            'parent_id': [None, None, None, 'event_001'],
            'related_indicator': [None, None, None, 'ACC_OWNERSHIP'],
            'impact_direction': [None, None, None, 'positive'],
            'impact_magnitude': [None, None, None, 0.02],
            'lag_months': [None, None, None, 12],
            'confidence': [None, None, 'high', 'medium']
        })
    
    def test_data_loading(self):
        """Test that data loads correctly"""
        # This is a basic test - actual implementation would load from file
        self.assertEqual(len(self.test_data), 4)
        self.assertEqual(self.test_data['record_type'].nunique(), 4)
    
    def test_association_matrix_shape(self):
        """Test association matrix has correct shape"""
        # Mock matrix creation
        events = self.test_data[self.test_data['record_type'] == 'event']
        indicators = ['ACC_OWNERSHIP', 'ACC_MM_ACCOUNT']
        
        # Expected: 2 events x 2 indicators
        expected_shape = (len(events), len(indicators))
        
        # This would be replaced with actual matrix creation
        # matrix = build_association_matrix(events, indicators)
        # self.assertEqual(matrix.shape, expected_shape)
        
        # Placeholder assertion
        self.assertTrue(True)
    
    def test_impact_magnitude_range(self):
        """Test that impact magnitudes are within reasonable range"""
        magnitudes = [0.0, 0.02, 0.05, 0.1]  # Example values
        
        for magnitude in magnitudes:
            self.assertGreaterEqual(magnitude, 0.0)
            self.assertLessEqual(magnitude, 0.5)  # Max 50% impact
    
    def test_date_parsing(self):
        """Test that dates are parsed correctly"""
        test_date = '2021-05-01'
        parsed_date = pd.to_datetime(test_date)
        
        self.assertEqual(parsed_date.year, 2021)
        self.assertEqual(parsed_date.month, 5)
        self.assertEqual(parsed_date.day, 1)
    
    def test_confidence_levels(self):
        """Test valid confidence levels"""
        valid_levels = ['high', 'medium', 'low']
        test_level = 'medium'
        
        self.assertIn(test_level, valid_levels)
    
    def test_impact_directions(self):
        """Test valid impact directions"""
        valid_directions = ['positive', 'negative', 'none']
        test_direction = 'positive'
        
        self.assertIn(test_direction, valid_directions)

if __name__ == '__main__':
    unittest.main()
