"""
Dashboard Utility Functions
Helper functions for data processing and visualization
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from typing import Dict, List, Any, Optional
import plotly.express as px

class DashboardUtils:
    """Utility functions for the dashboard"""
    
    @staticmethod
    def load_json_data(filepath: str) -> Dict:
        """Load JSON data from file"""
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except:
            return {}
    
    @staticmethod
    def format_percentage(value: float) -> str:
        """Format a float as percentage string"""
        return f"{value:.1f}%"
    
    @staticmethod
    def calculate_growth_rate(start_value: float, end_value: float, 
                            years: int) -> float:
        """Calculate annual growth rate"""
        if start_value == 0:
            return 0
        return ((end_value / start_value) ** (1/years) - 1) * 100
    
    @staticmethod
    def filter_by_date_range(df: pd.DataFrame, date_col: str,
                           start_date: str, end_date: str) -> pd.DataFrame:
        """Filter DataFrame by date range"""
        df[date_col] = pd.to_datetime(df[date_col])
        mask = (df[date_col] >= pd.to_datetime(start_date)) & \
               (df[date_col] <= pd.to_datetime(end_date))
        return df[mask].copy()
    
    @staticmethod
    def create_summary_stats(df: pd.DataFrame, value_col: str) -> Dict:
        """Create summary statistics"""
        return {
            'mean': df[value_col].mean(),
            'median': df[value_col].median(),
            'min': df[value_col].min(),
            'max': df[value_col].max(),
            'std': df[value_col].std(),
            'count': len(df)
        }
    
    @staticmethod
    def prepare_forecast_data(forecast_df: pd.DataFrame, 
                            indicator: str, scenario: str) -> pd.DataFrame:
        """Prepare forecast data for visualization"""
        filtered = forecast_df[
            (forecast_df['Indicator'] == indicator) &
            (forecast_df['Scenario'] == scenario)
        ].copy()
        
        if filtered.empty:
            return pd.DataFrame()
        
        # Add year as datetime for proper plotting
        filtered['Date'] = pd.to_datetime(filtered['Year'].astype(str) + '-01-01')
        
        return filtered.sort_values('Date')
    
    @staticmethod
    def get_color_palette(n_colors: int) -> List[str]:
        """Get a color palette for charts"""
        palettes = {
            3: ['#2E86AB', '#A23B72', '#F18F01'],
            5: ['#2E86AB', '#A23B72', '#F18F01', '#73AB84', '#C44900'],
            7: ['#2E86AB', '#A23B72', '#F18F01', '#73AB84', '#C44900', 
                '#6D6875', '#FFB4A2']
        }
        
        return palettes.get(n_colors, px.colors.qualitative.Set3[:n_colors])
    
    @staticmethod
    def generate_tooltip_html(row: Dict) -> str:
        """Generate HTML for tooltips"""
        return f"""
        <div style="padding: 10px;">
            <b>{row.get('event_name', 'N/A')}</b><br>
            Date: {row.get('event_date', 'N/A')}<br>
            Type: {row.get('category', 'N/A')}<br>
            Impact: {row.get('impact_magnitude', 'N/A')}
        </div>
        """
    
    @staticmethod
    def validate_data(df: pd.DataFrame, required_cols: List[str]) -> bool:
        """Validate data has required columns"""
        return all(col in df.columns for col in required_cols)
