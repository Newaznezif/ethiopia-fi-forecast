"""
EDA utilities for Ethiopia Financial Inclusion analysis
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

class EthiopiaFIEDA:
    """Exploratory Data Analysis utilities"""
    
    def __init__(self):
        # Color palette
        self.colors = {
            'Access': '#2E86AB',
            'Usage': '#A23B72',
            'Infrastructure': '#F18F01',
            'Enabler': '#73AB84',
            'Target': '#C73E1D',
            'Male': '#4C72B0',
            'Female': '#DD8452'
        }
        
        # Set style
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")
        
    def analyze_record_types(self, df: pd.DataFrame) -> Dict:
        """
        Analyze record type distribution
        
        Args:
            df: DataFrame with 'record_type' column
            
        Returns:
            Dictionary with analysis results
        """
        if 'record_type' not in df.columns:
            return {}
        
        record_counts = df['record_type'].value_counts()
        total = len(df)
        
        analysis = {
            'counts': record_counts.to_dict(),
            'percentages': {k: (v/total)*100 for k, v in record_counts.items()},
            'total_records': total
        }
        
        return analysis
    
    def analyze_pillar_distribution(self, observations: pd.DataFrame) -> Dict:
        """
        Analyze pillar distribution for observations
        
        Args:
            observations: DataFrame with 'pillar' column
            
        Returns:
            Dictionary with analysis results
        """
        if 'pillar' not in observations.columns:
            return {}
        
        pillar_counts = observations['pillar'].value_counts()
        total = len(observations)
        
        analysis = {
            'counts': pillar_counts.to_dict(),
            'percentages': {k: (v/total)*100 for k, v in pillar_counts.items()},
            'total_observations': total
        }
        
        return analysis
    
    def plot_trend(self, data: pd.DataFrame, x_col: str, y_col: str, 
                  title: str, save_path: Optional[str] = None) -> plt.Figure:
        """
        Plot time series trend
        
        Args:
            data: DataFrame with time series data
            x_col: Column name for x-axis
            y_col: Column name for y-axis
            title: Plot title
            save_path: Optional path to save figure
            
        Returns:
            Matplotlib figure
        """
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Sort by x
        data_sorted = data.sort_values(x_col)
        
        # Plot
        ax.plot(data_sorted[x_col], data_sorted[y_col], 
                marker='o', markersize=8, linewidth=2)
        
        ax.set_title(title)
        ax.set_xlabel(x_col)
        ax.set_ylabel(y_col)
        ax.grid(True, alpha=0.3)
        
        # Format x-axis if dates
        if pd.api.types.is_datetime64_any_dtype(data_sorted[x_col]):
            fig.autofmt_xdate()
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def plot_comparison(self, data_list: List[pd.DataFrame], 
                       labels: List[str], x_col: str, y_col: str,
                       title: str, save_path: Optional[str] = None) -> plt.Figure:
        """
        Plot comparison of multiple time series
        
        Args:
            data_list: List of DataFrames
            labels: List of labels for each series
            x_col: Column name for x-axis
            y_col: Column name for y-axis
            title: Plot title
            save_path: Optional path to save figure
            
        Returns:
            Matplotlib figure
        """
        fig, ax = plt.subplots(figsize=(12, 6))
        
        colors = plt.cm.Set3(np.linspace(0, 1, len(data_list)))
        
        for i, (data, label) in enumerate(zip(data_list, labels)):
            if not data.empty:
                data_sorted = data.sort_values(x_col)
                ax.plot(data_sorted[x_col], data_sorted[y_col], 
                       marker='o', markersize=8, linewidth=2,
                       label=label, color=colors[i])
        
        ax.set_title(title)
        ax.set_xlabel(x_col)
        ax.set_ylabel(y_col)
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Format x-axis if dates
        if data_list and pd.api.types.is_datetime64_any_dtype(data_list[0][x_col]):
            fig.autofmt_xdate()
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def calculate_growth_rates(self, data: pd.DataFrame, 
                              date_col: str, value_col: str) -> pd.DataFrame:
        """
        Calculate growth rates between periods
        
        Args:
            data: Time series data
            date_col: Column with dates
            value_col: Column with values
            
        Returns:
            DataFrame with growth calculations
        """
        if data.empty:
            return pd.DataFrame()
        
        data_sorted = data.sort_values(date_col).copy()
        
        # Calculate differences
        data_sorted['period'] = data_sorted[date_col].dt.year
        data_sorted['prev_value'] = data_sorted[value_col].shift(1)
        data_sorted['prev_period'] = data_sorted['period'].shift(1)
        
        # Calculate growth
        data_sorted['absolute_growth'] = data_sorted[value_col] - data_sorted['prev_value']
        data_sorted['relative_growth'] = (data_sorted['absolute_growth'] / data_sorted['prev_value']) * 100
        
        # Calculate annual growth
        data_sorted['years_diff'] = data_sorted['period'] - data_sorted['prev_period']
        data_sorted['annual_growth'] = data_sorted['absolute_growth'] / data_sorted['years_diff']
        
        return data_sorted.dropna(subset=['prev_value'])
    
    def analyze_correlations(self, data_dict: Dict[str, pd.DataFrame],
                           indicator_col: str, value_col: str) -> pd.DataFrame:
        """
        Analyze correlations between different indicators
        
        Args:
            data_dict: Dictionary of {indicator_name: dataframe}
            indicator_col: Column name for indicator identification
            value_col: Column name for values
            
        Returns:
            Correlation matrix
        """
        # Create combined dataframe
        combined_data = []
        
        for indicator_name, df in data_dict.items():
            if not df.empty:
                df_copy = df.copy()
                df_copy['indicator'] = indicator_name
                combined_data.append(df_copy[[indicator_col, value_col, 'indicator']])
        
        if not combined_data:
            return pd.DataFrame()
        
        combined_df = pd.concat(combined_data, ignore_index=True)
        
        # Pivot to wide format
        pivot_df = combined_df.pivot_table(
            values=value_col,
            index=indicator_col,
            columns='indicator'
        )
        
        # Calculate correlation
        correlation_matrix = pivot_df.corr()
        
        return correlation_matrix
    
    def plot_correlation_heatmap(self, correlation_matrix: pd.DataFrame,
                                title: str, save_path: Optional[str] = None) -> plt.Figure:
        """
        Plot correlation heatmap
        
        Args:
            correlation_matrix: Correlation matrix
            title: Plot title
            save_path: Optional path to save figure
            
        Returns:
            Matplotlib figure
        """
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Create heatmap
        im = ax.imshow(correlation_matrix, cmap='coolwarm', vmin=-1, vmax=1)
        
        # Set labels
        ax.set_xticks(range(len(correlation_matrix.columns)))
        ax.set_yticks(range(len(correlation_matrix.columns)))
        ax.set_xticklabels(correlation_matrix.columns, rotation=45, ha='right')
        ax.set_yticklabels(correlation_matrix.columns)
        ax.set_title(title)
        
        # Add correlation values
        for i in range(len(correlation_matrix.columns)):
            for j in range(len(correlation_matrix.columns)):
                value = correlation_matrix.iloc[i, j]
                if not np.isnan(value):
                    ax.text(j, i, f'{value:.2f}', 
                           ha='center', va='center',
                           color='white' if abs(value) > 0.5 else 'black',
                           fontsize=8)
        
        plt.colorbar(im, ax=ax)
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def generate_insights_report(self, analysis_results: Dict) -> str:
        """
        Generate insights report from analysis results
        
        Args:
            analysis_results: Dictionary with analysis results
            
        Returns:
            Markdown formatted report
        """
        report = "# Ethiopia Financial Inclusion EDA Report\n\n"
        
        # Record type analysis
        if 'record_types' in analysis_results:
            report += "## Record Type Analysis\n\n"
            record_data = analysis_results['record_types']
            
            for record_type, count in record_data.get('counts', {}).items():
                percentage = record_data.get('percentages', {}).get(record_type, 0)
                report += f"- **{record_type}**: {count} records ({percentage:.1f}%)\n"
            report += "\n"
        
        # Pillar analysis
        if 'pillars' in analysis_results:
            report += "## Pillar Analysis\n\n"
            pillar_data = analysis_results['pillars']
            
            for pillar, count in pillar_data.get('counts', {}).items():
                percentage = pillar_data.get('percentages', {}).get(pillar, 0)
                report += f"- **{pillar}**: {count} observations ({percentage:.1f}%)\n"
            report += "\n"
        
        # Growth analysis
        if 'growth_rates' in analysis_results:
            report += "## Growth Analysis\n\n"
            growth_data = analysis_results['growth_rates']
            
            if isinstance(growth_data, pd.DataFrame) and not growth_data.empty:
                for _, row in growth_data.iterrows():
                    report += (f"- {row.get('period', 'N/A')}: "
                              f"Growth of {row.get('absolute_growth', 0):.1f}pp "
                              f"({row.get('relative_growth', 0):.1f}%)\n")
            report += "\n"
        
        # Correlation insights
        if 'correlations' in analysis_results:
            report += "## Key Correlations\n\n"
            corr_data = analysis_results['correlations']
            
            if isinstance(corr_data, pd.DataFrame) and not corr_data.empty:
                # Find strong correlations
                strong_corrs = []
                for i in range(len(corr_data.columns)):
                    for j in range(i+1, len(corr_data.columns)):
                        corr = corr_data.iloc[i, j]
                        if abs(corr) > 0.7:
                            strong_corrs.append({
                                'ind1': corr_data.columns[i],
                                'ind2': corr_data.columns[j],
                                'corr': corr
                            })
                
                for corr in strong_corrs:
                    direction = "positive" if corr['corr'] > 0 else "negative"
                    report += (f"- **{corr['ind1']}** ↔ **{corr['ind2']}**: "
                              f"{corr['corr']:.3f} ({direction})\n")
            report += "\n"
        
        # Data quality
        if 'data_quality' in analysis_results:
            report += "## Data Quality Assessment\n\n"
            quality_data = analysis_results['data_quality']
            
            report += f"- **Missing Values**: {quality_data.get('missing_pct', 0):.1f}%\n"
            report += f"- **High Confidence**: {quality_data.get('high_conf_pct', 0):.1f}%\n"
            report += f"- **Coverage**: {quality_data.get('coverage_years', 0)} years\n"
            report += "\n"
        
        return report
