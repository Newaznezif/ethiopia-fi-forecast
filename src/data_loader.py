"""
Data loading and validation module for Ethiopia Financial Inclusion Forecasting
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import warnings
from datetime import datetime
import logging
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EthiopiaFIDataLoader:
    """Load and validate Ethiopia financial inclusion data"""
    
    def __init__(self, data_path: str = None, ref_codes_path: str = None):
        """
        Initialize data loader
        
        Args:
            data_path: Path to unified data CSV
            ref_codes_path: Path to reference codes CSV
        """
        self.data_path = data_path or os.path.join('data', 'raw', 'ethiopia_fi_unified_data.csv')
        self.ref_codes_path = ref_codes_path or os.path.join('data', 'raw', 'reference_codes.csv')
        self.data = None
        self.reference_codes = None
        self.valid_codes_cache = {}
        self.impact_links = None
        
    def load_all_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load all datasets
        
        Returns:
            Tuple of (main_data, reference_codes)
        """
        logger.info(f"Loading data from {self.data_path}")
        
        try:
            self.data = pd.read_csv(self.data_path)
            logger.info(f"Loaded main dataset with {len(self.data)} records")
        except FileNotFoundError:
            logger.error(f"Main data file not found at {self.data_path}")
            self.data = pd.DataFrame()
        
        try:
            self.reference_codes = pd.read_csv(self.ref_codes_path)
            logger.info(f"Loaded reference codes with {len(self.reference_codes)} records")
        except FileNotFoundError:
            logger.error(f"Reference codes file not found at {self.ref_codes_path}")
            self.reference_codes = pd.DataFrame()
        
        if not self.data.empty:
            self._validate_schema()
            self._validate_categorical_fields()
            self._convert_dates()
            self._load_impact_links()
        
        return self.data, self.reference_codes
    
    def _validate_schema(self):
        """Validate that required columns exist"""
        required_columns = ['record_id', 'record_type', 'indicator', 'indicator_code']
        
        missing_required = [col for col in required_columns if col not in self.data.columns]
        if missing_required:
            logger.warning(f"Missing required columns: {missing_required}")
    
    def _validate_categorical_fields(self):
        """Validate categorical fields against reference codes"""
        if self.reference_codes.empty:
            return
            
        categorical_fields = ['record_type', 'pillar', 'source_type', 'confidence']
        
        for field in categorical_fields:
            if field in self.data.columns:
                valid_values = self.reference_codes[
                    self.reference_codes['field'] == field
                ]['code'].tolist()
                
                if valid_values:
                    invalid_mask = ~self.data[field].isin(valid_values) & ~self.data[field].isna()
                    invalid_count = invalid_mask.sum()
                    
                    if invalid_count > 0:
                        invalid_values_list = self.data.loc[invalid_mask, field].unique()[:5]
                        logger.warning(f"Found {invalid_count} invalid values in {field}: {invalid_values_list}")
                
                self.valid_codes_cache[field] = valid_values
    
    def _convert_dates(self):
        """Convert date columns to datetime"""
        date_columns = ['observation_date', 'event_date', 'target_date', 'collection_date']
        
        for col in date_columns:
            if col in self.data.columns:
                try:
                    self.data[col] = pd.to_datetime(self.data[col], errors='coerce')
                except Exception as e:
                    logger.warning(f"Failed to convert {col} to datetime: {e}")
    
    def _load_impact_links(self):
        """Load impact links from data"""
        if 'impact_direction' in self.data.columns:
            self.impact_links = self.data[
                self.data['impact_direction'].notna()
            ].copy()
            logger.info(f"Loaded {len(self.impact_links)} impact links")
        else:
            self.impact_links = pd.DataFrame()
    
    def get_record_type_stats(self) -> Dict[str, Any]:
        """Get statistics by record type"""
        if self.data is None or self.data.empty:
            return {}
        
        stats = {
            'counts': self.data['record_type'].value_counts().to_dict(),
            'total_records': len(self.data)
        }
        
        return stats
    
    def add_observation(self, 
                       pillar: str,
                       indicator: str,
                       indicator_code: str,
                       value_numeric: float,
                       observation_date: str,
                       source_name: str,
                       source_url: str,
                       confidence: str = 'medium',
                       notes: str = '',
                       **kwargs) -> str:
        """
        Add a new observation record
        """
        record_id = f"obs_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        new_record = {
            'record_id': record_id,
            'record_type': 'observation',
            'pillar': pillar,
            'indicator': indicator,
            'indicator_code': indicator_code,
            'value_numeric': value_numeric,
            'observation_date': observation_date,
            'source_name': source_name,
            'source_url': source_url,
            'confidence': confidence,
            'notes': notes,
            'collected_by': kwargs.get('collected_by', 'system'),
            'collection_date': kwargs.get('collection_date', datetime.now().strftime('%Y-%m-%d'))
        }
        
        new_df = pd.DataFrame([new_record])
        self.data = pd.concat([self.data, new_df], ignore_index=True)
        
        logger.info(f"Added observation: {record_id}")
        return record_id
    
    def add_event(self,
                  event_name: str,
                  event_date: str,
                  category: str,
                  description: str = '',
                  source_name: str = '',
                  source_url: str = '',
                  confidence: str = 'medium',
                  **kwargs) -> str:
        """
        Add a new event record
        """
        record_id = f"evt_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        new_record = {
            'record_id': record_id,
            'record_type': 'event',
            'pillar': '',  # Events don't have pillar
            'indicator': event_name,
            'indicator_code': f"EVENT_{category.upper()}",
            'event_name': event_name,
            'event_date': event_date,
            'event_category': category,
            'description': description,
            'source_name': source_name,
            'source_url': source_url,
            'confidence': confidence,
            'notes': description,
            'collected_by': kwargs.get('collected_by', 'system'),
            'collection_date': kwargs.get('collection_date', datetime.now().strftime('%Y-%m-%d'))
        }
        
        new_df = pd.DataFrame([new_record])
        self.data = pd.concat([self.data, new_df], ignore_index=True)
        
        logger.info(f"Added event: {record_id}")
        return record_id
    
    def add_impact_link(self,
                       parent_id: str,
                       pillar: str,
                       related_indicator: str,
                       impact_direction: str,
                       impact_magnitude: float,
                       lag_months: int = 0,
                       evidence_basis: str = '',
                       **kwargs) -> str:
        """
        Add a new impact link record
        """
        record_id = f"imp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        new_record = {
            'record_id': record_id,
            'record_type': 'impact_link',
            'pillar': pillar,
            'indicator': 'Impact Link',
            'indicator_code': 'IMPACT_LINK',
            'parent_id': parent_id,
            'related_indicator': related_indicator,
            'impact_direction': impact_direction,
            'impact_magnitude': impact_magnitude,
            'lag_months': lag_months,
            'evidence_basis': evidence_basis,
            'notes': f"Impact of {parent_id} on {related_indicator}",
            'collected_by': kwargs.get('collected_by', 'system'),
            'collection_date': kwargs.get('collection_date', datetime.now().strftime('%Y-%m-%d'))
        }
        
        new_df = pd.DataFrame([new_record])
        self.data = pd.concat([self.data, new_df], ignore_index=True)
        
        logger.info(f"Added impact link: {record_id}")
        return record_id
    
    def save_enriched_data(self, output_path: str = None):
        """Save enriched dataset"""
        if self.data is None:
            raise ValueError("No data to save")
        
        if output_path is None:
            output_path = os.path.join('data', 'processed', 'ethiopia_fi_enriched.csv')
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        self.data.to_csv(output_path, index=False)
        logger.info(f"Saved data to {output_path}")
    
    def get_temporal_coverage(self) -> pd.DataFrame:
        """Get temporal coverage by indicator"""
        if self.data is None or self.data.empty:
            return pd.DataFrame()
        
        # Filter observation records
        observations = self.data[self.data['record_type'] == 'observation'].copy()
        
        if observations.empty or 'observation_date' not in observations.columns:
            return pd.DataFrame()
        
        # Group by indicator and get date range
        temporal_coverage = observations.groupby('indicator_code').agg({
            'observation_date': ['min', 'max', 'count'],
            'source_name': lambda x: list(x.unique())[:5],
            'confidence': lambda x: x.mode()[0] if not x.empty else None
        }).round(2)
        
        # Flatten column names
        temporal_coverage.columns = ['first_date', 'last_date', 'count', 'sources', 'confidence']
        
        return temporal_coverage
    
    def get_events_timeline(self) -> pd.DataFrame:
        """Get timeline of events"""
        if self.data is None or self.data.empty:
            return pd.DataFrame()
        
        events = self.data[self.data['record_type'] == 'event'].copy()
        
        if events.empty:
            return pd.DataFrame()
        
        timeline = events[[
            'record_id', 'event_name', 'event_date', 'event_category',
            'source_name', 'confidence', 'notes'
        ]].sort_values('event_date')
        
        return timeline
    
    def get_observations_by_indicator(self, indicator_code: str = None) -> pd.DataFrame:
        """Get observations for specific indicator or all indicators"""
        if self.data is None or self.data.empty:
            return pd.DataFrame()
        
        observations = self.data[self.data['record_type'] == 'observation'].copy()
        
        if observations.empty:
            return pd.DataFrame()
        
        if indicator_code:
            observations = observations[observations['indicator_code'] == indicator_code]
        
        return observations.sort_values('observation_date')