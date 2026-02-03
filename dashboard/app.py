"""
Dashboard for Ethiopia Financial Inclusion Forecast
Streamlit application for interactive visualization
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import json
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="Ethiopia Financial Inclusion Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #2E86AB;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #A23B72;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #2E86AB;
        margin-bottom: 1rem;
    }
    .stButton button {
        background-color: #2E86AB;
        color: white;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

class FinancialInclusionDashboard:
    """Main dashboard class"""
    
    def __init__(self):
        self.load_data()
    
    def load_data(self):
        """Load all required data"""
        # Load forecast data
        try:
            self.forecast_df = pd.read_csv('data/processed/forecasts_2025_2027.csv')
        except:
            st.error("Forecast data not found. Please run Task 4 first.")
            self.forecast_df = pd.DataFrame()
        
        # Load historical data
        try:
            self.historical_df = pd.read_csv('data/processed/ethiopia_fi_enriched.csv')
            self.historical_obs = self.historical_df[self.historical_df['record_type'] == 'observation']
        except:
            st.warning("Historical data not found. Using sample data.")
            self.historical_obs = pd.DataFrame()
        
        # Load impact matrix
        try:
            self.impact_matrix = pd.read_csv('data/processed/impact_matrix.csv', index_col=0)
        except:
            self.impact_matrix = pd.DataFrame()
        
        # Load event data
        try:
            self.events_df = self.historical_df[self.historical_df['record_type'] == 'event']
        except:
            self.events_df = pd.DataFrame()
    
    def create_sidebar(self):
        """Create sidebar with controls"""
        with st.sidebar:
            st.title("📊 Dashboard Controls")
            
            # Scenario selection
            st.subheader("Forecast Scenario")
            self.scenario = st.selectbox(
                "Select Scenario",
                ["base", "optimistic", "pessimistic"],
                index=0,
                help="Choose the forecast scenario to display"
            )
            
            # Year range
            st.subheader("Time Range")
            self.start_year = st.slider(
                "Start Year",
                min_value=2011,
                max_value=2027,
                value=2011,
                step=1
            )
            self.end_year = st.slider(
                "End Year",
                min_value=2011,
                max_value=2027,
                value=2027,
                step=1
            )
            
            # Indicator selection
            st.subheader("Indicators")
            indicators = ["ACC_OWNERSHIP", "ACC_MM_ACCOUNT", "USG_DIGITAL_PAYMENT"]
            self.selected_indicators = st.multiselect(
                "Select Indicators",
                indicators,
                default=indicators,
                help="Choose which indicators to display"
            )
            
            # Display options
            st.subheader("Display Options")
            self.show_confidence = st.checkbox("Show Confidence Intervals", value=True)
            self.show_events = st.checkbox("Show Events Timeline", value=True)
            self.show_targets = st.checkbox("Show National Targets", value=True)
            
            # Refresh button
            if st.button("🔄 Refresh Data", use_container_width=True):
                self.load_data()
                st.rerun()
            
            st.divider()
            
            # Information
            st.info("""
            **Data Sources:**
            - World Bank Global Findex
            - National Bank of Ethiopia
            - GSMA Mobile Money
            """)
    
    def create_header(self):
        """Create dashboard header"""
        st.markdown('<h1 class="main-header">📈 Ethiopia Financial Inclusion Forecast Dashboard</h1>', 
                   unsafe_allow_html=True)
        
        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Current Account Ownership",
                value="49%",
                delta="+3pp since 2021",
                delta_color="normal"
            )
        
        with col2:
            st.metric(
                label="2027 Forecast (Base)",
                value="55%",
                delta="+6pp expected",
                delta_color="normal"
            )
        
        with col3:
            st.metric(
                label="Gender Gap",
                value="14pp",
                delta="Needs improvement",
                delta_color="inverse"
            )
        
        with col4:
            st.metric(
                label="Mobile Money Users",
                value="65M+",
                delta="54M Telebirr + 10M M-Pesa",
                delta_color="normal"
            )
    
    def create_forecast_chart(self):
        """Create interactive forecast chart"""
        st.markdown('<h2 class="sub-header">📊 Forecast Visualization</h2>', 
                   unsafe_allow_html=True)
        
        if self.forecast_df.empty:
            st.warning("No forecast data available")
            return
        
        # Filter data based on selections
        filtered_df = self.forecast_df[
            (self.forecast_df['Scenario'] == self.scenario) &
            (self.forecast_df['Indicator'].isin(self.selected_indicators)) &
            (self.forecast_df['Year'] >= self.start_year) &
            (self.forecast_df['Year'] <= self.end_year)
        ]
        
        if filtered_df.empty:
            st.warning("No data matches current filters")
            return
        
        # Create Plotly figure
        fig = go.Figure()
        
        colors = {
            'ACC_OWNERSHIP': '#2E86AB',
            'ACC_MM_ACCOUNT': '#A23B72',
            'USG_DIGITAL_PAYMENT': '#F18F01'
        }
        
        # Add traces for each indicator
        for indicator in self.selected_indicators:
            indicator_data = filtered_df[filtered_df['Indicator'] == indicator]
            
            # Main forecast line
            fig.add_trace(go.Scatter(
                x=indicator_data['Year'],
                y=indicator_data['Forecast (%)'],
                name=indicator.replace('_', ' ').title(),
                mode='lines+markers',
                line=dict(color=colors.get(indicator, '#333'), width=3),
                marker=dict(size=8)
            ))
            
            # Confidence interval
            if self.show_confidence:
                fig.add_trace(go.Scatter(
                    x=indicator_data['Year'].tolist() + indicator_data['Year'].tolist()[::-1],
                    y=indicator_data['Upper Bound'].tolist() + indicator_data['Lower Bound'].tolist()[::-1],
                    fill='toself',
                    fillcolor=f'rgba{(*plt.colors.to_rgb(colors.get(indicator, "#333")), 0.2)}',
                    line=dict(color='rgba(255,255,255,0)'),
                    hoverinfo='skip',
                    showlegend=False,
                    name=f'{indicator} Confidence'
                ))
        
        # Add event markers
        if self.show_events and not self.events_df.empty:
            self.events_df['event_date'] = pd.to_datetime(self.events_df['event_date'])
            events_years = self.events_df['event_date'].dt.year.unique()
            
            for year in events_years:
                if self.start_year <= year <= self.end_year:
                    fig.add_vline(
                        x=year,
                        line_dash="dash",
                        line_color="orange",
                        opacity=0.5,
                        annotation_text=f"Event: {year}",
                        annotation_position="top"
                    )
        
        # Add target line
        if self.show_targets:
            fig.add_hline(
                y=60,
                line_dash="dot",
                line_color="purple",
                opacity=0.7,
                annotation_text="NFIS-II Target (60%)",
                annotation_position="bottom right"
            )
        
        # Update layout
        fig.update_layout(
            title=f"Financial Inclusion Forecast ({self.scenario.title()} Scenario)",
            xaxis_title="Year",
            yaxis_title="Percentage (%)",
            hovermode="x unified",
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            ),
            height=600,
            template="plotly_white"
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def create_impact_matrix(self):
        """Create impact matrix visualization"""
        st.markdown('<h2 class="sub-header">🎯 Event Impact Analysis</h2>', 
                   unsafe_allow_html=True)
        
        if self.impact_matrix.empty:
            st.warning("Impact matrix data not available")
            return
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=self.impact_matrix.values,
            x=self.impact_matrix.columns,
            y=self.impact_matrix.index,
            colorscale='RdBu',
            zmid=0,
            text=np.round(self.impact_matrix.values, 3),
            texttemplate='%{text}',
            textfont={"size": 10},
            hoverongaps=False
        ))
        
        fig.update_layout(
            title="Event-Impact Association Matrix",
            xaxis_title="Indicators",
            yaxis_title="Events",
            height=400,
            template="plotly_white"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Impact summary
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Top Positive Impacts")
            positive_impacts = []
            for event in self.impact_matrix.index:
                for indicator in self.impact_matrix.columns:
                    impact = self.impact_matrix.loc[event, indicator]
                    if impact > 0:
                        positive_impacts.append((event, indicator, impact))
            
            positive_impacts.sort(key=lambda x: x[2], reverse=True)
            
            for event, indicator, impact in positive_impacts[:3]:
                st.write(f"**{event}** → {indicator}: **+{impact:.3f}**")
        
        with col2:
            st.subheader("Key Insights")
            st.info("""
            - Telebirr launch had strongest impact on mobile money
            - M-Pesa entry boosted digital payments
            - Policy events show longer-term effects
            """)
    
    def create_scenario_comparison(self):
        """Create scenario comparison chart"""
        st.markdown('<h2 class="sub-header">🔍 Scenario Comparison</h2>', 
                   unsafe_allow_html=True)
        
        if self.forecast_df.empty:
            return
        
        # Filter for 2027 forecasts
        comparison_df = self.forecast_df[self.forecast_df['Year'] == 2027]
        
        if comparison_df.empty:
            return
        
        # Create grouped bar chart
        fig = go.Figure()
        
        scenarios = ['optimistic', 'base', 'pessimistic']
        colors = {'optimistic': '#2ecc71', 'base': '#3498db', 'pessimistic': '#e74c3c'}
        
        for scenario in scenarios:
            scenario_data = comparison_df[comparison_df['Scenario'] == scenario]
            fig.add_trace(go.Bar(
                x=scenario_data['Indicator'],
                y=scenario_data['Forecast (%)'],
                name=scenario.title(),
                marker_color=colors[scenario],
                error_y=dict(
                    type='data',
                    array=scenario_data['Upper Bound'] - scenario_data['Forecast (%)'],
                    arrayminus=scenario_data['Forecast (%)'] - scenario_data['Lower Bound'],
                    visible=True
                )
            ))
        
        fig.update_layout(
            title="2027 Forecast by Scenario",
            xaxis_title="Indicator",
            yaxis_title="Percentage (%)",
            barmode='group',
            height=500,
            template="plotly_white"
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def create_historical_trend(self):
        """Create historical trend visualization"""
        st.markdown('<h2 class="sub-header">📈 Historical Trends</h2>', 
                   unsafe_allow_html=True)
        
        if self.historical_obs.empty:
            st.warning("Historical data not available")
            return
        
        # Filter and prepare data
        historical_filtered = self.historical_obs[
            (self.historical_obs['indicator_code'].isin(self.selected_indicators)) &
            (self.historical_obs['observation_date'].str[:4].astype(int) >= self.start_year) &
            (self.historical_obs['observation_date'].str[:4].astype(int) <= 2024)
        ]
        
        if historical_filtered.empty:
            return
        
        # Create line chart
        fig = go.Figure()
        
        for indicator in self.selected_indicators:
            indicator_data = historical_filtered[historical_filtered['indicator_code'] == indicator]
            if not indicator_data.empty:
                fig.add_trace(go.Scatter(
                    x=pd.to_datetime(indicator_data['observation_date']),
                    y=indicator_data['value_numeric'],
                    name=indicator.replace('_', ' ').title(),
                    mode='lines+markers',
                    line=dict(width=3)
                ))
        
        # Add event markers
        if self.show_events and not self.events_df.empty:
            for _, event in self.events_df.iterrows():
                event_date = pd.to_datetime(event['event_date'])
                fig.add_vline(
                    x=event_date,
                    line_dash="dash",
                    line_color="orange",
                    opacity=0.5,
                    annotation_text=event['event_name'],
                    annotation_position="top"
                )
        
        fig.update_layout(
            title="Historical Financial Inclusion Trends",
            xaxis_title="Date",
            yaxis_title="Percentage (%)",
            hovermode="x unified",
            height=500,
            template="plotly_white"
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def create_download_section(self):
        """Create data download section"""
        st.markdown('<h2 class="sub-header">📥 Download Data</h2>', 
                   unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Forecast data
            forecast_csv = self.forecast_df.to_csv(index=False)
            st.download_button(
                label="Download Forecasts (CSV)",
                data=forecast_csv,
                file_name="ethiopia_fi_forecasts.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col2:
            # Historical data
            historical_csv = self.historical_obs.to_csv(index=False)
            st.download_button(
                label="Download Historical Data (CSV)",
                data=historical_csv,
                file_name="ethiopia_fi_historical.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col3:
            # Summary report
            with open('reports/results_summary.md', 'r') as f:
                summary_text = f.read()
            
            st.download_button(
                label="Download Summary Report (MD)",
                data=summary_text,
                file_name="ethiopia_fi_summary.md",
                mime="text/markdown",
                use_container_width=True
            )
    
    def run(self):
        """Run the dashboard"""
        self.create_sidebar()
        self.create_header()
        
        # Create tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "📈 Forecasts", 
            "🎯 Impact Analysis", 
            "📊 Comparison",
            "📥 Data"
        ])
        
        with tab1:
            col1, col2 = st.columns([3, 1])
            with col1:
                self.create_forecast_chart()
            with col2:
                st.subheader("Current Status")
                st.metric("Account Ownership", "49%", "2024")
                st.metric("Mobile Money", "9.45%", "2024")
                st.metric("Digital Payments", "~35%", "2024")
            
            self.create_historical_trend()
        
        with tab2:
            self.create_impact_matrix()
            
            # Event timeline
            st.subheader("Event Timeline")
            if not self.events_df.empty:
                events_display = self.events_df[['event_name', 'event_date', 'category']].copy()
                events_display['event_date'] = pd.to_datetime(events_display['event_date'])
                events_display = events_display.sort_values('event_date')
                st.dataframe(events_display, use_container_width=True)
        
        with tab3:
            self.create_scenario_comparison()
            
            # Growth rates
            st.subheader("Growth Rate Analysis")
            col1, col2, col3 = st.columns(3)
            
            growth_data = {
                "Period": ["2011-2014", "2014-2017", "2017-2021", "2021-2024", "2024-2027 (Forecast)"],
                "Annual Growth": [2.0, 4.3, 2.8, 1.0, 2.0],
                "Status": ["Slow", "Fast", "Moderate", "Slow", "Expected"]
            }
            
            growth_df = pd.DataFrame(growth_data)
            st.dataframe(growth_df, use_container_width=True)
        
        with tab4:
            self.create_download_section()
            
            # Raw data preview
            st.subheader("Data Preview")
            preview_tab1, preview_tab2 = st.tabs(["Forecasts", "Historical"])
            
            with preview_tab1:
                st.dataframe(self.forecast_df, use_container_width=True)
            
            with preview_tab2:
                st.dataframe(self.historical_obs.head(20), use_container_width=True)
        
        # Footer
        st.divider()
        st.caption(f"Dashboard last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        st.caption("Data Sources: World Bank Findex, National Bank of Ethiopia, GSMA")

def main():
    """Main function to run the dashboard"""
    dashboard = FinancialInclusionDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()
