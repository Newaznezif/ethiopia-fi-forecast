# src/data_loader.py
"""Load and process Ethiopia FI data from our discussion"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

class DataLoader:
    def __init__(self):
        self.raw_path = Path("data/raw/ethiopia_fi_unified_data.csv")
        self.processed_path = Path("data/processed/cleaned_data.csv")
        
    def load_raw_data(self):
        """Load the CSV data we have from our discussion"""
        print("📂 Loading unified dataset...")
        try:
            df = pd.read_csv(self.raw_path)
            print(f"   Loaded {len(df)} records")
            print(f"   Record types: {df['record_type'].value_counts().to_dict()}")
            return df
        except Exception as e:
            print(f"❌ Error: {e}")
            return None
    
    def explore_data(self, df):
        """Basic exploration of the data"""
        print("\n🔍 Data Exploration:")
        print("="*40)
        print(f"Shape: {df.shape}")
        print(f"\nColumns: {list(df.columns)}")
        
        # Record type analysis
        print("\n📊 Record Type Distribution:")
        for rt, count in df['record_type'].value_counts().items():
            print(f"   {rt}: {count}")
        
        # Date analysis
        date_cols = [col for col in df.columns if 'date' in col]
        for col in date_cols:
            if col in df.columns and pd.notna(df[col]).any():
                df[col] = pd.to_datetime(df[col], errors='coerce')
                dates = df[col].dropna()
                if len(dates) > 0:
                    print(f"\n📅 {col} range: {dates.min().date()} to {dates.max().date()}")
        
        # Key indicators
        print("\n🎯 Key Indicators:")
        if 'indicator' in df.columns:
            indicators = df['indicator'].dropna().unique()
            for i, indicator in enumerate(indicators[:10]):  # Show first 10
                print(f"   {i+1}. {indicator}")
        
        return df
    
    def clean_data(self, df):
        """Clean the dataset"""
        print("\n🧹 Cleaning data...")
        
        # 1. Handle dates
        date_cols = [col for col in df.columns if 'date' in col]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # 2. Handle numeric values
        if 'value_numeric' in df.columns:
            df['value_numeric'] = pd.to_numeric(df['value_numeric'], errors='coerce')
        
        # 3. Clean text columns
        text_cols = ['record_type', 'pillar', 'indicator', 'indicator_code']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.lower()
        
        print(f"   Cleaned data shape: {df.shape}")
        return df
    
    def separate_by_type(self, df):
        """Separate data by record type as discussed"""
        print("\n📦 Separating data by record type...")
        
        data_dict = {
            'observations': df[df['record_type'] == 'observation'].copy(),
            'events': df[df['record_type'] == 'event'].copy(),
            'impact_links': df[df['record_type'] == 'impact_link'].copy(),
            'targets': df[df['record_type'] == 'target'].copy()
        }
        
        for key, df_sub in data_dict.items():
            print(f"   {key}: {len(df_sub)} records")
        
        return data_dict
    
    def analyze_access_trend(self, observations_df):
        """Analyze account ownership trend"""
        print("\n📈 Analyzing Account Ownership Trend:")
        print("="*40)
        
        access_data = observations_df[
            observations_df['indicator_code'] == 'ACC_OWNERSHIP'
        ].sort_values('observation_date')
        
        if len(access_data) > 0:
            print("Historical Account Ownership (%):")
            for _, row in access_data.iterrows():
                year = row['observation_date'].year
                value = row['value_numeric']
                print(f"   {year}: {value}%")
            
            # Calculate growth
            access_data['growth'] = access_data['value_numeric'].diff()
            print(f"\n📊 Average growth: {access_data['growth'].mean():.2f} pp per survey")
            
            # Identify slowdown
            if len(access_data) >= 2:
                recent_growth = access_data['growth'].iloc[-1]
                print(f"⚠️  Recent growth (2021-2024): {recent_growth} pp")
        
        return access_data
    
    def save_processed(self, df, filename="cleaned_data.csv"):
        """Save processed data"""
        save_path = self.processed_path.parent / filename
        df.to_csv(save_path, index=False)
        print(f"\n💾 Saved processed data to: {save_path}")
        return save_path
    
    def run_full_analysis(self):
        """Run complete analysis pipeline"""
        print("🚀 Starting Data Analysis Pipeline")
        print("="*50)
        
        # 1. Load
        df = self.load_raw_data()
        if df is None:
            return
        
        # 2. Explore
        df = self.explore_data(df)
        
        # 3. Clean
        df = self.clean_data(df)
        
        # 4. Separate
        data_dict = self.separate_by_type(df)
        
        # 5. Analyze trends
        if 'observations' in data_dict:
            self.analyze_access_trend(data_dict['observations'])
        
        # 6. Save
        self.save_processed(df)
        
        print("\n✅ Analysis complete!")
        return df, data_dict

# Quick usage example
if __name__ == "__main__":
    loader = DataLoader()
    df, data_dict = loader.run_full_analysis()
