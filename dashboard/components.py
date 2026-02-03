"""
Dashboard UI Components
Reusable widgets for the financial inclusion dashboard
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from typing import List, Dict, Optional

class DashboardComponents:
    """Reusable UI components"""
    
    @staticmethod
    def create_metric_card(title: str, value: str, delta: str = None, 
                          delta_color: str = "normal"):
        """Create a metric card"""
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.metric(
                label=title,
                value=value,
                delta=delta,
                delta_color=delta_color
            )
    
    @staticmethod
    def create_info_box(title: str, content: str, icon: str = "ℹ️"):
        """Create an information box"""
        with st.expander(f"{icon} {title}"):
            st.markdown(content)
    
    @staticmethod
    def create_data_table(data: pd.DataFrame, title: str = None, 
                         height: int = 300):
        """Create an interactive data table"""
        if title:
            st.subheader(title)
        
        st.dataframe(data, height=height, use_container_width=True)
    
    @staticmethod
    def create_bar_chart(data: pd.DataFrame, x_col: str, y_col: str,
                        title: str = None, color: str = None):
        """Create a bar chart"""
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=data[x_col],
            y=data[y_col],
            marker_color=color,
            text=data[y_col],
            textposition='auto'
        ))
        
        if title:
            fig.update_layout(title=title)
        
        fig.update_layout(
            xaxis_title=x_col,
            yaxis_title=y_col,
            template="plotly_white"
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def create_gauge_chart(value: float, title: str, 
                          min_val: float = 0, max_val: float = 100,
                          threshold: float = None):
        """Create a gauge chart"""
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=value,
            title={'text': title},
            domain={'x': [0, 1], 'y': [0, 1]},
            gauge={
                'axis': {'range': [min_val, max_val]},
                'bar': {'color': "#2E86AB"},
                'steps': [
                    {'range': [min_val, threshold or max_val*0.7], 'color': "lightgray"},
                    {'range': [threshold or max_val*0.7, max_val], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': threshold or max_val*0.8
                }
            }
        ))
        
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def create_timeline(events: List[Dict], title: str = "Event Timeline"):
        """Create a timeline visualization"""
        fig = go.Figure()
        
        for i, event in enumerate(events):
            fig.add_trace(go.Scatter(
                x=[event['date'], event['date']],
                y=[i, i],
                mode='markers+text',
                marker=dict(size=15, color=event.get('color', '#FFA500')),
                text=event['name'],
                textposition="top center",
                name=event['name']
            ))
        
        fig.update_layout(
            title=title,
            showlegend=False,
            xaxis_title="Date",
            yaxis=dict(showticklabels=False),
            height=400,
            template="plotly_white"
        )
        
        st.plotly_chart(fig, use_container_width=True)
