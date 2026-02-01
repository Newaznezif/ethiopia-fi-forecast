# src/data_loader.py
"""
Data loader for Ethiopia Financial Inclusion project
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import csv
import warnings
warnings.filterwarnings('ignore')

class EthiopiaFIDataLoader:
    """Load and prepare Ethiopia Financial Inclusion data"""
    
    def __init__(self, data_dir="data/raw"):
        self.data_dir = Path(data_dir)
        self.main_data = None
        self.impact_links = None
        self.reference_codes = None
        self.observations = None
        self.events = None
        self.targets = None
        
    def load_all_data(self):
        """Load all three datasets with robust error handling"""
        print("📂 Loading Ethiopia Financial Inclusion data...")
        success = True
        
        try:
            # 1. Main dataset
            self.main_data = pd.read_csv(self.data_dir / "ethiopia_fi_unified_data.csv")
            print(f"  ✅ Main data: {len(self.main_data)} records")
            
        except Exception as e:
            print(f"  ❌ Error loading main data: {e}")
            success = False
        
        try:
            # 2. Impact links
            self.impact_links = pd.read_csv(self.data_dir / "impact_links.csv")
            print(f"  ✅ Impact links: {len(self.impact_links)} records")
            
        except Exception as e:
            print(f"  ❌ Error loading impact links: {e}")
            success = False
        
        try:
            # 3. Reference codes - try multiple methods
            ref_path = self.data_dir / "reference_codes.csv"
            
            # Try standard read first
            try:
                self.reference_codes = pd.read_csv(ref_path)
                print(f"  ✅ Reference codes: {len(self.reference_codes)} records")
                
            except Exception as e:
                print(f"  ⚠️  Standard read failed for reference codes: {e}")
                print(f"  Trying alternative methods...")
                
                # Try with different parameters
                try:
                    self.reference_codes = pd.read_csv(ref_path, encoding='utf-8', 
                                                      on_bad_lines='skip')
                    print(f"  ✅ Reference codes (skip bad lines): {len(self.reference_codes)} records")
                except:
                    # Try manual parsing
                    try:
                        with open(ref_path, 'r', encoding='utf-8') as f:
                            lines = f.readlines()
                        
                        # Simple manual parsing
                        data = []
                        for line in lines:
                            line = line.strip()
                            if line and not line.startswith('#'):
                                parts = line.split(',', 2)  # Split into max 3 parts
                                if len(parts) == 3:
                                    data.append([p.strip('\"\' ') for p in parts])
                        
                        if data:
                            self.reference_codes = pd.DataFrame(data, 
                                                              columns=['field', 'code', 'description'])
                            print(f"  ✅ Reference codes (manual parse): {len(self.reference_codes)} records")
                        else:
                            print(f"  ❌ Could not parse reference codes")
                            success = False
                            
                    except Exception as e2:
                        print(f"  ❌ All methods failed for reference codes: {e2}")
                        success = False
        
        except Exception as e:
            print(f"  ❌ Unexpected error: {e}")
            success = False
        
        if success:
            # Convert dates
            self._process_dates()
            
            # Separate by record type
            self._separate_data()
            
            print("✅ All data loaded successfully!")
        else:
            print("⚠️  Some data loading issues - check above errors")
        
        return success
    
    def _process_dates(self):
        """Convert date columns to datetime"""
        # Main data dates
        if self.main_data is not None:
            date_cols = [col for col in self.main_data.columns if 'date' in col]
            for col in date_cols:
                if col in self.main_data.columns:
                    self.main_data[col] = pd.to_datetime(self.main_data[col], errors='coerce')
        
        # Impact links numeric conversion
        if self.impact_links is not None and 'lag_months' in self.impact_links.columns:
            self.impact_links['lag_months'] = pd.to_numeric(
                self.impact_links['lag_months'], errors='coerce'
            )
    
    def _separate_data(self):
        """Separate main data by record type"""
        if self.main_data is not None:
            self.observations = self.main_data[
                self.main_data['record_type'] == 'observation'
            ].copy()
            
            self.events = self.main_data[
                self.main_data['record_type'] == 'event'
            ].copy()
            
            self.targets = self.main_data[
                self.main_data['record_type'] == 'target'
            ].copy()
            
            print(f"  • Observations: {len(self.observations)}")
            print(f"  • Events: {len(self.events)}")
            print(f"  • Targets: {len(self.targets)}")
    
    def get_account_ownership_data(self):
        """Get account ownership time series"""
        if self.observations is None:
            return pd.DataFrame()
        
        acc_data = self.observations[
            self.observations['indicator_code'] == 'ACC_OWNERSHIP'
        ].copy()
        
        if not acc_data.empty:
            acc_data = acc_data.sort_values('observation_date')
            acc_data['growth_pp'] = acc_data['value_numeric'].diff()
            acc_data['growth_pct'] = acc_data['value_numeric'].pct_change() * 100
        
        return acc_data
    
    def get_usage_data(self):
        """Get usage indicators data"""
        if self.observations is None:
            return pd.DataFrame()
        
        usage_data = self.observations[
            self.observations['pillar'] == 'usage'
        ].copy()
        
        return usage_data
    
    def get_infrastructure_data(self):
        """Get infrastructure and enabler data"""
        if self.observations is None:
            return pd.DataFrame()
        
        infra_data = self.observations[
            self.observations['pillar'].isin(['infrastructure', 'enabler'])
        ].copy()
        
        return infra_data
    
    def get_event_impacts(self):
        """Get combined event-impact analysis"""
        if self.events is None or self.impact_links is None:
            return pd.DataFrame()
        
        # Merge impact links with events
        impact_analysis = pd.merge(
            self.impact_links,
            self.events[['id', 'event_name', 'event_date', 'category']],
            left_on='parent_id',
            right_on='id',
            how='left',
            suffixes=('_impact', '_event')
        )
        
        return impact_analysis
    
    def get_data_summary(self):
        """Get comprehensive data summary"""
        summary = {
            'total_records': len(self.main_data) if self.main_data is not None else 0,
            'observations': len(self.observations) if self.observations is not None else 0,
            'events': len(self.events) if self.events is not None else 0,
            'targets': len(self.targets) if self.targets is not None else 0,
            'impact_links': len(self.impact_links) if self.impact_links is not None else 0,
            'reference_codes': len(self.reference_codes) if self.reference_codes is not None else 0,
        }
        
        # Temporal coverage
        if self.observations is not None and not self.observations.empty:
            obs_years = self.observations['observation_date'].dt.year
            summary['observation_years'] = sorted(obs_years.unique().tolist())
            summary['year_range'] = f"{obs_years.min()} - {obs_years.max()}"
        
        # Event timeline
        if self.events is not None and not self.events.empty:
            event_years = self.events['event_date'].dt.year
            summary['event_years'] = sorted(event_years.unique().tolist())
        
        return summary
    
    def validate_data_quality(self):
        """Validate data quality"""
        issues = []
        
        # Check for missing values in key columns
        if self.observations is not None:
            required_cols = ['indicator', 'value_numeric', 'observation_date']
            for col in required_cols:
                if col in self.observations.columns:
                    missing = self.observations[col].isnull().sum()
                    if missing > 0:
                        issues.append(f"Observations: {missing} missing values in {col}")
        
        # Check impact links reference valid events
        if self.impact_links is not None and self.events is not None:
            event_ids = set(self.events['id'])
            impact_event_ids = set(self.impact_links['parent_id'])
            missing_refs = impact_event_ids - event_ids
            if missing_refs:
                issues.append(f"Impact links reference non-existent events: {len(missing_refs)}")
        
        return issues

# Simple data loader without the EthiopiaFIDataLoader class
def load_data_simple():
    """Simple function to load data for the notebook"""
    print("📂 Loading data for EDA notebook...")
    
    data = {}
    
    try:
        # Load main data
        data['main'] = pd.read_csv("data/raw/ethiopia_fi_unified_data.csv")
        print(f"✅ Main data: {len(data['main'])} records")
        
        # Load impact links
        data['impact'] = pd.read_csv("data/raw/impact_links.csv")
        print(f"✅ Impact links: {len(data['impact'])} records")
        
        # Load reference codes (with error handling)
        try:
            data['ref'] = pd.read_csv("data/raw/reference_codes.csv")
            print(f"✅ Reference codes: {len(data['ref'])} records")
        except:
            # Create minimal reference codes if file is corrupted
            print("⚠️  Using default reference codes")
            data['ref'] = pd.DataFrame({
                'field': ['record_type', 'pillar', 'confidence'],
                'code': ['observation,event,impact_link,target', 
                        'access,usage,infrastructure,enabler',
                        'high,medium,low'],
                'description': ['Record types', 'Pillars', 'Confidence levels']
            })
        
        return data
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None

# Quick test function
def test_data_loader():
    """Test the data loader"""
    print("🧪 Testing Data Loader...")
    
    loader = EthiopiaFIDataLoader()
    
    if loader.load_all_data():
        print("\n📊 Data Summary:")
        summary = loader.get_data_summary()
        for key, value in summary.items():
            if key not in ['observation_years', 'event_years', 'year_range']:
                print(f"  {key}: {value}")
        
        print("\n🔍 Data Quality Check:")
        issues = loader.validate_data_quality()
        if issues:
            print("  Issues found:")
            for issue in issues:
                print(f"  • {issue}")
        else:
            print("  ✅ No data quality issues found")
        
        return loader
    else:
        print("❌ Failed to load data")
        return None

if __name__ == "__main__":
    test_data_loader()
