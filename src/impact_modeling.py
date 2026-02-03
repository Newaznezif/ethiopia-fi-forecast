"""
Event Impact Modeling for Ethiopia Financial Inclusion Forecasting
Task 3: Build event-indicator association matrix and quantify impacts
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import warnings
import logging
import json
import os
from dataclasses import dataclass
from enum import Enum

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ImpactDirection(Enum):
    """Impact direction enumeration"""
    POSITIVE = "positive"
    NEGATIVE = "negative" 
    NEUTRAL = "neutral"
    MIXED = "mixed"

class EvidenceBasis(Enum):
    """Evidence basis for impact estimates"""
    DIRECT_OBSERVATION = "direct_observation"
    COMPARABLE_COUNTRY = "comparable_country"
    EXPERT_JUDGMENT = "expert_judgment"
    REGRESSION_ANALYSIS = "regression_analysis"
    SIMULATION_MODEL = "simulation_model"

@dataclass
class EventImpact:
    """Data class for event impact relationships"""
    event_id: str
    event_name: str
    event_date: datetime
    event_category: str
    indicator_code: str
    pillar: str
    impact_direction: ImpactDirection
    impact_magnitude: float  # In percentage points
    lag_months: int
    evidence_basis: EvidenceBasis
    confidence: float  # 0.0 to 1.0
    notes: str = ""
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "event_id": self.event_id,
            "event_name": self.event_name,
            "event_date": self.event_date.strftime("%Y-%m-%d"),
            "event_category": self.event_category,
            "indicator_code": self.indicator_code,
            "pillar": self.pillar,
            "impact_direction": self.impact_direction.value,
            "impact_magnitude": self.impact_magnitude,
            "lag_months": self.lag_months,
            "evidence_basis": self.evidence_basis.value,
            "confidence": self.confidence,
            "notes": self.notes
        }

class EthiopiaFIImpactModeler:
    """
    Event impact modeling for Ethiopia financial inclusion
    
    Builds event-indicator association matrix and quantifies impacts
    based on impact links and comparable country evidence
    """
    
    def __init__(self, data_path: str = None):
        """
        Initialize impact modeler
        
        Args:
            data_path: Path to enriched data CSV
        """
        self.data_path = data_path or os.path.join('data', 'processed', 'ethiopia_fi_enriched.csv')
        self.data = None
        self.events = None
        self.impact_links = None
        self.observations = None
        self.impact_matrix = None
        self.event_impacts = []
        
        # Comparable country evidence database
        self.comparable_evidence = self._load_comparable_evidence()
        
        # Default impact functions
        self.impact_functions = {
            "immediate": self._immediate_impact,
            "gradual": self._gradual_impact,
            "saturating": self._saturating_impact,
            "network": self._network_impact
        }
        
    def _load_comparable_evidence(self) -> Dict:
        """
        Load comparable country evidence database
        
        Returns:
            Dictionary of comparable evidence
        """
        # Base evidence from similar contexts
        evidence = {
            # East Africa mobile money launches
            "mobile_money_launch": {
                "kenya": {
                    "event": "M-Pesa launch (2007)",
                    "impact": {
                        "ACC_MM_ACCOUNT": {"magnitude": 15.0, "lag": 36, "confidence": 0.9},
                        "USG_DIGITAL_PAYMENT": {"magnitude": 12.0, "lag": 24, "confidence": 0.8}
                    }
                },
                "tanzania": {
                    "event": "M-Pesa launch (2008)",
                    "impact": {
                        "ACC_MM_ACCOUNT": {"magnitude": 10.0, "lag": 48, "confidence": 0.8},
                        "USG_DIGITAL_PAYMENT": {"magnitude": 8.0, "lag": 36, "confidence": 0.7}
                    }
                }
            },
            
            # Interoperability impacts
            "interoperability": {
                "kenya": {
                    "event": "PesaLink launch (2018)",
                    "impact": {
                        "USG_DIGITAL_PAYMENT": {"magnitude": 5.0, "lag": 12, "confidence": 0.8},
                        "INF_TRANSACTION_VOLUME": {"magnitude": 40.0, "lag": 6, "confidence": 0.9}
                    }
                }
            },
            
            # QR standardization
            "qr_standardization": {
                "india": {
                    "event": "UPI QR standardization (2016)",
                    "impact": {
                        "USG_MERCHANT_PAYMENT": {"magnitude": 8.0, "lag": 18, "confidence": 0.85},
                        "USG_DIGITAL_PAYMENT": {"magnitude": 5.0, "lag": 12, "confidence": 0.8}
                    }
                }
            },
            
            # Agent network expansion
            "agent_expansion": {
                "bangladesh": {
                    "event": "Agent banking expansion (2013)",
                    "impact": {
                        "ACC_OWNERSHIP": {"magnitude": 7.0, "lag": 36, "confidence": 0.8},
                        "INF_AGENT_DENSITY": {"magnitude": 15.0, "lag": 24, "confidence": 0.9}
                    }
                }
            }
        }
        
        return evidence
    
    def load_data(self) -> pd.DataFrame:
        """
        Load and prepare data for impact modeling
        
        Returns:
            Prepared DataFrame
        """
        logger.info(f"Loading data from {self.data_path}")
        
        try:
            self.data = pd.read_csv(self.data_path)
            logger.info(f"Loaded data: {len(self.data)} records")
            
            # Convert dates
            date_cols = [col for col in self.data.columns if 'date' in col.lower()]
            for col in date_cols:
                if col in self.data.columns:
                    self.data[col] = pd.to_datetime(self.data[col], errors='coerce')
            
            # Separate by record type
            self.observations = self.data[self.data['record_type'] == 'observation'].copy()
            self.events = self.data[self.data['record_type'] == 'event'].copy()
            self.impact_links = self.data[self.data['record_type'] == 'impact_link'].copy()
            
            logger.info(f"Observations: {len(self.observations)}")
            logger.info(f"Events: {len(self.events)}")
            logger.info(f"Impact links: {len(self.impact_links)}")
            
            return self.data
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def build_impact_matrix(self) -> pd.DataFrame:
        """
        Build event-indicator association matrix
        
        Returns:
            Impact matrix DataFrame
        """
        if self.data is None:
            self.load_data()
        
        logger.info("Building impact matrix...")
        
        # Get unique events and indicators
        events_list = self.events['indicator'].unique()  # Using indicator as event name
        indicators_list = self.observations['indicator_code'].unique()
        
        # Create empty matrix
        self.impact_matrix = pd.DataFrame(
            index=events_list,
            columns=indicators_list,
            dtype=object
        )
        
        # Fill matrix from impact links
        if not self.impact_links.empty and 'parent_id' in self.impact_links.columns:
            for _, link in self.impact_links.iterrows():
                parent_id = link['parent_id']
                related_indicator = link.get('related_indicator', '')
                
                # Find event name
                event = self.events[self.events['record_id'] == parent_id]
                if not event.empty:
                    event_name = event.iloc[0]['indicator']
                    
                    # Get impact details
                    impact_direction = link.get('impact_direction', 'positive')
                    impact_magnitude = link.get('impact_magnitude', 0.0)
                    lag_months = link.get('lag_months', 0)
                    
                    # Format impact string
                    impact_str = f"{impact_direction[0].upper()}: {impact_magnitude}pp (lag: {lag_months}m)"
                    
                    # Add to matrix
                    if event_name in self.impact_matrix.index and related_indicator in self.impact_matrix.columns:
                        self.impact_matrix.loc[event_name, related_indicator] = impact_str
        
        # Fill missing values with comparable evidence
        self._enhance_with_comparable_evidence()
        
        logger.info(f"Impact matrix built: {self.impact_matrix.shape}")
        return self.impact_matrix
    
    def _enhance_with_comparable_evidence(self):
        """Enhance impact matrix with comparable country evidence"""
        if self.impact_matrix is None:
            return
        
        # Map event categories to comparable evidence
        event_category_mapping = {
            'product_launch': 'mobile_money_launch',
            'policy': ['interoperability', 'qr_standardization'],
            'infrastructure': 'agent_expansion'
        }
        
        for event_name in self.impact_matrix.index:
            # Find event category
            event_row = self.events[self.events['indicator'] == event_name]
            if event_row.empty:
                continue
                
            event_category = event_row.iloc[0].get('event_category', '')
            
            # Get comparable evidence keys
            evidence_keys = []
            if event_category in event_category_mapping:
                keys = event_category_mapping[event_category]
                if isinstance(keys, list):
                    evidence_keys.extend(keys)
                else:
                    evidence_keys.append(keys)
            
            # Apply comparable evidence
            for evidence_key in evidence_keys:
                if evidence_key in self.comparable_evidence:
                    for country, evidence in self.comparable_evidence[evidence_key].items():
                        for indicator, impact in evidence['impact'].items():
                            if indicator in self.impact_matrix.columns:
                                current_value = self.impact_matrix.loc[event_name, indicator]
                                if pd.isna(current_value) or current_value == '':
                                    # Scale impact for Ethiopia context
                                    scaled_magnitude = impact['magnitude'] * 0.7  # Conservative scaling
                                    impact_str = f"C*: {scaled_magnitude:.1f}pp (lag: {impact['lag']}m)"
                                    self.impact_matrix.loc[event_name, indicator] = impact_str
    
    def estimate_event_impacts(self) -> List[EventImpact]:
        """
        Estimate detailed event impacts
        
        Returns:
            List of EventImpact objects
        """
        if self.data is None:
            self.load_data()
        
        logger.info("Estimating event impacts...")
        self.event_impacts = []
        
        for _, event in self.events.iterrows():
            event_id = event['record_id']
            event_name = event['indicator']
            event_date = event.get('event_date')
            event_category = event.get('event_category', 'unknown')
            
            # Skip if no date
            if pd.isna(event_date):
                continue
            
            # Find related impact links
            related_links = self.impact_links[self.impact_links['parent_id'] == event_id]
            
            if not related_links.empty:
                # Use existing impact links
                for _, link in related_links.iterrows():
                    impact = self._create_impact_from_link(event, link)
                    if impact:
                        self.event_impacts.append(impact)
            else:
                # Estimate impacts using comparable evidence
                impacts = self._estimate_using_comparable_evidence(event)
                self.event_impacts.extend(impacts)
        
        logger.info(f"Estimated {len(self.event_impacts)} event impacts")
        return self.event_impacts
    
    def _create_impact_from_link(self, event: pd.Series, link: pd.Series) -> Optional[EventImpact]:
        """Create EventImpact from impact link"""
        try:
            # Parse impact direction
            direction_str = link.get('impact_direction', 'positive').lower()
            if direction_str == 'positive':
                direction = ImpactDirection.POSITIVE
            elif direction_str == 'negative':
                direction = ImpactDirection.NEGATIVE
            else:
                direction = ImpactDirection.NEUTRAL
            
            # Parse evidence basis
            evidence_str = link.get('evidence_basis', 'expert_judgment').lower()
            if 'direct' in evidence_str:
                evidence = EvidenceBasis.DIRECT_OBSERVATION
            elif 'comparable' in evidence_str:
                evidence = EvidenceBasis.COMPARABLE_COUNTRY
            elif 'regression' in evidence_str:
                evidence = EvidenceBasis.REGRESSION_ANALYSIS
            else:
                evidence = EvidenceBasis.EXPERT_JUDGMENT
            
            impact = EventImpact(
                event_id=event['record_id'],
                event_name=event['indicator'],
                event_date=event.get('event_date'),
                event_category=event.get('event_category', 'unknown'),
                indicator_code=link.get('related_indicator', ''),
                pillar=link.get('pillar', ''),
                impact_direction=direction,
                impact_magnitude=float(link.get('impact_magnitude', 0.0)),
                lag_months=int(link.get('lag_months', 0)),
                evidence_basis=evidence,
                confidence=float(link.get('confidence', 0.5)),
                notes=link.get('notes', '')
            )
            
            return impact
            
        except Exception as e:
            logger.warning(f"Error creating impact from link: {e}")
            return None
    
    def _estimate_using_comparable_evidence(self, event: pd.Series) -> List[EventImpact]:
        """Estimate impacts using comparable country evidence"""
        impacts = []
        event_category = event.get('event_category', 'unknown')
        
        # Map event category to evidence
        category_to_evidence = {
            'product_launch': 'mobile_money_launch',
            'policy': ['interoperability', 'qr_standardization'],
            'infrastructure': 'agent_expansion',
            'market_entry': 'mobile_money_launch'
        }
        
        if event_category in category_to_evidence:
            evidence_keys = category_to_evidence[event_category]
            if isinstance(evidence_keys, str):
                evidence_keys = [evidence_keys]
            
            for evidence_key in evidence_keys:
                if evidence_key in self.comparable_evidence:
                    # Take average across comparable countries
                    countries = self.comparable_evidence[evidence_key]
                    
                    for country, evidence in countries.items():
                        for indicator, impact_details in evidence['impact'].items():
                            # Scale for Ethiopia context (conservative)
                            scaled_magnitude = impact_details['magnitude'] * 0.7
                            confidence = impact_details['confidence'] * 0.9  # Reduce confidence
                            
                            impact = EventImpact(
                                event_id=event['record_id'],
                                event_name=event['indicator'],
                                event_date=event.get('event_date'),
                                event_category=event_category,
                                indicator_code=indicator,
                                pillar=self._get_pillar_from_indicator(indicator),
                                impact_direction=ImpactDirection.POSITIVE,
                                impact_magnitude=scaled_magnitude,
                                lag_months=impact_details['lag'],
                                evidence_basis=EvidenceBasis.COMPARABLE_COUNTRY,
                                confidence=confidence,
                                notes=f"Based on {country} experience with {evidence['event']}"
                            )
                            impacts.append(impact)
        
        return impacts
    
    def _get_pillar_from_indicator(self, indicator_code: str) -> str:
        """Get pillar from indicator code"""
        if indicator_code.startswith('ACC_'):
            return 'Access'
        elif indicator_code.startswith('USG_'):
            return 'Usage'
        elif indicator_code.startswith('INF_'):
            return 'Infrastructure'
        elif indicator_code.startswith('ENA_'):
            return 'Enabler'
        else:
            return ''
    
    def _immediate_impact(self, magnitude: float, months: int) -> float:
        """Immediate impact function"""
        if months <= 1:
            return magnitude
        elif months <= 3:
            return magnitude * 0.8
        else:
            return magnitude * (1 - (months - 3) * 0.1)
    
    def _gradual_impact(self, magnitude: float, months: int) -> float:
        """Gradual impact function"""
        if months <= 0:
            return 0
        max_months = 24
        if months >= max_months:
            return magnitude
        return magnitude * (months / max_months)
    
    def _saturating_impact(self, magnitude: float, months: int) -> float:
        """Saturating impact function (logistic)"""
        if months <= 0:
            return 0
        k = 0.2  # Growth rate
        return magnitude / (1 + np.exp(-k * (months - 12)))
    
    def _network_impact(self, magnitude: float, months: int) -> float:
        """Network effect impact function"""
        if months <= 0:
            return 0
        # Network effects grow with square root of time
        return magnitude * np.sqrt(months / 12)
    
    def calculate_aggregate_impacts(self, target_date: datetime = None) -> pd.DataFrame:
        """
        Calculate aggregate impacts by indicator
        
        Args:
            target_date: Date to calculate impacts for
            
        Returns:
            DataFrame with aggregate impacts
        """
        if target_date is None:
            target_date = datetime.now()
        
        if not self.event_impacts:
            self.estimate_event_impacts()
        
        # Group impacts by indicator
        indicator_impacts = {}
        
        for impact in self.event_impacts:
            indicator = impact.indicator_code
            
            if indicator not in indicator_impacts:
                indicator_impacts[indicator] = {
                    'total_positive': 0.0,
                    'total_negative': 0.0,
                    'net_impact': 0.0,
                    'event_count': 0,
                    'earliest_impact': None,
                    'latest_impact': None
                }
            
            # Calculate time-adjusted impact
            impact_date = impact.event_date + timedelta(days=30 * impact.lag_months)
            
            if impact_date <= target_date:
                # Impact has already occurred
                adjusted_impact = impact.impact_magnitude * impact.confidence
            else:
                # Future impact, discount it
                months_until = (impact_date - target_date).days / 30
                discount_factor = 1 / (1 + 0.05 * months_until)  # 5% monthly discount
                adjusted_impact = impact.impact_magnitude * impact.confidence * discount_factor
            
            # Add to aggregates
            if impact.impact_direction == ImpactDirection.POSITIVE:
                indicator_impacts[indicator]['total_positive'] += adjusted_impact
                indicator_impacts[indicator]['net_impact'] += adjusted_impact
            elif impact.impact_direction == ImpactDirection.NEGATIVE:
                indicator_impacts[indicator]['total_negative'] += adjusted_impact
                indicator_impacts[indicator]['net_impact'] -= adjusted_impact
            
            indicator_impacts[indicator]['event_count'] += 1
            
            # Track impact dates
            if indicator_impacts[indicator]['earliest_impact'] is None or impact_date < indicator_impacts[indicator]['earliest_impact']:
                indicator_impacts[indicator]['earliest_impact'] = impact_date
            
            if indicator_impacts[indicator]['latest_impact'] is None or impact_date > indicator_impacts[indicator]['latest_impact']:
                indicator_impacts[indicator]['latest_impact'] = impact_date
        
        # Convert to DataFrame
        impact_df = pd.DataFrame.from_dict(indicator_impacts, orient='index')
        impact_df.index.name = 'indicator_code'
        
        # Format dates
        for col in ['earliest_impact', 'latest_impact']:
            impact_df[col] = impact_df[col].apply(
                lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None
            )
        
        return impact_df.sort_values('net_impact', ascending=False)
    
    def validate_against_historical(self, indicator_code: str = None) -> pd.DataFrame:
        """
        Validate impact model against historical data
        
        Args:
            indicator_code: Specific indicator to validate
            
        Returns:
            DataFrame with validation results
        """
        if not self.event_impacts:
            self.estimate_event_impacts()
        
        validation_results = []
        
        # Filter events with historical data
        historical_events = []
        for impact in self.event_impacts:
            event_date = impact.event_date
            
            # Find observations before and after event
            pre_obs = self.observations[
                (self.observations['indicator_code'] == impact.indicator_code) &
                (self.observations['observation_date'] < event_date)
            ]
            
            post_obs = self.observations[
                (self.observations['indicator_code'] == impact.indicator_code) &
                (self.observations['observation_date'] > event_date)
            ]
            
            if not pre_obs.empty and not post_obs.empty:
                # Get closest observations
                pre_value = pre_obs.sort_values('observation_date').iloc[-1]['value_numeric']
                post_value = post_obs.sort_values('observation_date').iloc[0]['value_numeric']
                
                actual_change = post_value - pre_value
                predicted_change = impact.impact_magnitude
                
                # Calculate error
                if predicted_change != 0:
                    error_pct = abs((actual_change - predicted_change) / predicted_change) * 100
                else:
                    error_pct = 100 if actual_change != 0 else 0
                
                validation_results.append({
                    'event_name': impact.event_name,
                    'indicator_code': impact.indicator_code,
                    'event_date': impact.event_date.strftime('%Y-%m-%d'),
                    'pre_value': pre_value,
                    'post_value': post_value,
                    'actual_change': actual_change,
                    'predicted_change': predicted_change,
                    'error_pp': actual_change - predicted_change,
                    'error_pct': error_pct,
                    'lag_months': impact.lag_months,
                    'confidence': impact.confidence
                })
        
        validation_df = pd.DataFrame(validation_results)
        
        if not validation_df.empty:
            # Calculate overall metrics
            overall_metrics = {
                'mean_absolute_error_pp': validation_df['error_pp'].abs().mean(),
                'mean_absolute_error_pct': validation_df['error_pct'].abs().mean(),
                'r_squared': max(0, 1 - (validation_df['error_pp'].var() / validation_df['actual_change'].var())),
                'validation_count': len(validation_df)
            }
            
            logger.info(f"Validation metrics: {overall_metrics}")
        
        return validation_df
    
    def save_impact_matrix(self, output_path: str = None):
        """Save impact matrix to CSV"""
        if self.impact_matrix is None:
            self.build_impact_matrix()
        
        if output_path is None:
            output_path = os.path.join('models', 'impact_matrix.csv')
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        self.impact_matrix.to_csv(output_path)
        logger.info(f"Impact matrix saved to {output_path}")
    
    def save_event_impacts(self, output_path: str = None):
        """Save event impacts to JSON"""
        if not self.event_impacts:
            self.estimate_event_impacts()
        
        if output_path is None:
            output_path = os.path.join('models', 'event_impacts.json')
        
        # Convert to dict
        impacts_dict = [impact.to_dict() for impact in self.event_impacts]
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(impacts_dict, f, indent=2, default=str)
        
        logger.info(f"Event impacts saved to {output_path}")
    
    def visualize_impact_matrix(self, save_path: str = None):
        """
        Create visualization of impact matrix
        
        Args:
            save_path: Path to save visualization
        """
        if self.impact_matrix is None:
            self.build_impact_matrix()
        
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            
            # Create heatmap data
            heatmap_data = self.impact_matrix.copy()
            
            # Convert impact strings to numeric scores
            def impact_to_score(impact_str):
                if pd.isna(impact_str) or impact_str == '':
                    return 0
                
                # Extract magnitude from string
                import re
                match = re.search(r'([+-]?\d+\.?\d*)pp', str(impact_str))
                if match:
                    magnitude = float(match.group(1))
                    return magnitude
                return 0
            
            # Apply conversion
            score_matrix = heatmap_data.applymap(impact_to_score)
            
            # Create visualization
            plt.figure(figsize=(16, 10))
            
            # Create heatmap
            sns.heatmap(
                score_matrix,
                cmap='RdYlGn',
                center=0,
                annot=True,
                fmt='.1f',
                cbar_kws={'label': 'Impact Magnitude (pp)'},
                linewidths=0.5,
                linecolor='gray'
            )
            
            plt.title('Event-Impact Association Matrix', fontsize=16, pad=20)
            plt.xlabel('Indicators', fontsize=12)
            plt.ylabel('Events', fontsize=12)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            if save_path:
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"Impact matrix visualization saved to {save_path}")
            
            plt.show()
            
        except ImportError:
            logger.warning("Matplotlib/Seaborn not available for visualization")
        except Exception as e:
            logger.error(f"Error creating visualization: {e}")

# Example usage
if __name__ == "__main__":
    # Initialize modeler
    modeler = EthiopiaFIImpactModeler()
    
    # Load data
    modeler.load_data()
    
    # Build impact matrix
    impact_matrix = modeler.build_impact_matrix()
    print("Impact Matrix:")
    print(impact_matrix.head())
    
    # Save results
    modeler.save_impact_matrix()
    modeler.save_event_impacts()
    
    # Create visualization
    modeler.visualize_impact_matrix('reports/figures/impact_matrix.png')
    
    print("\nTask 3: Event Impact Modeling complete!")
