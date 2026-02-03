# Task 3: Event Impact Modeling Methodology

## Overview
This document outlines the methodology for modeling event impacts on Ethiopia's financial inclusion indicators. The approach combines existing impact links with comparable country evidence to build a comprehensive event-indicator association matrix.

## 1. Data Sources and Preparation

### 1.1 Primary Data Sources
- **Enriched Dataset**: Processed data from Task 1 (`ethiopia_fi_enriched.csv`)
- **Impact Links**: Explicit relationships between events and indicators
- **Event Catalog**: All recorded events with dates and categories
- **Observation Data**: Historical values for validation

### 1.2 Data Preparation Steps
1. **Date Standardization**: Convert all date fields to datetime format
2. **Record Separation**: Split data into observations, events, and impact links
3. **Data Validation**: Check for missing critical fields
4. **Indicator Mapping**: Map indicators to pillars (Access, Usage, Infrastructure, Enabler)

## 2. Impact Modeling Approach

### 2.1 Impact Matrix Construction
The event-indicator association matrix is built using a three-tier approach:

1. **Tier 1: Direct Impact Links**
   - Use explicitly defined impact relationships from the dataset
   - Preserve direction, magnitude, and lag specifications
   - Confidence: High (based on documented relationships)

2. **Tier 2: Comparable Country Evidence**
   - Apply evidence from similar contexts (Kenya, Tanzania, India, Bangladesh)
   - Scale impacts conservatively for Ethiopia context (70% scaling factor)
   - Confidence: Medium (adjusted for context differences)

3. **Tier 3: Event Category Inference**
   - For events without direct or comparable evidence
   - Infer likely impacts based on event category
   - Confidence: Low (requires validation)

### 2.2 Comparable Evidence Database
The model includes evidence from four key comparable contexts:

| Context | Event Type | Key Indicators | Impact Range | Source |
|---------|------------|----------------|--------------|--------|
| Kenya | M-Pesa Launch (2007) | Mobile money accounts, Digital payments | 12-15pp over 3 years | GSMA, Findex |
| Tanzania | M-Pesa Launch (2008) | Mobile money accounts, Digital payments | 8-10pp over 4 years | GSMA, Findex |
| India | UPI QR Standardization (2016) | Merchant payments, Digital payments | 5-8pp over 18 months | RBI, NPCI |
| Bangladesh | Agent Banking Expansion (2013) | Account ownership, Agent density | 5-7pp over 3 years | Bangladesh Bank |

### 2.3 Impact Scaling Factors
- **East Africa contexts**: 0.7x scaling (conservative adjustment)
- **South Asia contexts**: 0.6x scaling (more distant context)
- **Direct observation**: 1.0x scaling (when available)
- **Expert judgment**: 0.8x scaling (uncertainty adjustment)

## 3. Mathematical Framework

### 3.1 Impact Functions
Four functional forms are used to model different impact types:

1. **Immediate Impact**: `f(t) = m * exp(-λt)`
   - For policy announcements, regulatory changes
   - Rapid initial effect decaying over time

2. **Gradual Impact**: `f(t) = m * (1 - exp(-kt))`
   - For infrastructure investments, network expansion
   - Slow buildup to maximum effect

3. **Saturating Impact**: `f(t) = m / (1 + exp(-k(t - t₀)))`
   - For market adoption, technology diffusion
   - S-curve pattern with inflection point

4. **Network Impact**: `f(t) = m * sqrt(t/τ)`
   - For interoperability, platform effects
   - Square root growth (network effects)

### 3.2 Aggregate Impact Calculation
Total impact on indicator i at time t:Where:
- `w_j` = impact magnitude for event j
- `f_j` = impact function for event j
- `t_event_j` = occurrence time of event j
- `c_j` = confidence factor (0-1)

### 3.3 Confidence Weighting
Each impact estimate includes a confidence score:
- **High confidence (0.8-1.0)**: Direct observation, robust evidence
- **Medium confidence (0.5-0.8)**: Comparable evidence, expert consensus
- **Low confidence (0.3-0.5)**: Inferred impacts, limited evidence

## 4. Validation Methodology

### 4.1 Historical Validation
For events with pre- and post-observation data:
Acceptance criteria: Error < 50% for high-confidence impacts

### 4.2 Cross-validation Metrics
- **Mean Absolute Error (pp)**: Average error in percentage points
- **Mean Absolute Percentage Error**: Relative error magnitude
- **R-squared**: Proportion of variance explained
- **Bias Assessment**: Systematic over/under prediction

### 4.3 Sensitivity Analysis
Key parameters tested for sensitivity:
- Impact scaling factors (±20%)
- Lag periods (±3 months)
- Confidence thresholds (±0.1)
- Functional form selection

## 5. Key Assumptions

### 5.1 General Assumptions
1. **Additivity**: Impacts from multiple events sum linearly
2. **Independence**: Events affect indicators independently
3. **Time Invariance**: Impact functions don't change over time
4. **Context Transferability**: Comparable evidence is applicable to Ethiopia

### 5.2 Ethiopia-Specific Assumptions
1. **Mobile Money Context**: Similar to East Africa but with later adoption
2. **Regulatory Environment**: Supportive but evolving
3. **Infrastructure Level**: Lower baseline than comparator countries
4. **Consumer Behavior**: Similar patterns to peer countries

## 6. Limitations and Uncertainties

### 6.1 Data Limitations
- **Sparse time series**: Limited observation points
- **Event coverage**: Not all relevant events cataloged
- **Impact specification**: Many impacts not quantified
- **Context differences**: Ethiopia's unique characteristics

### 6.2 Modeling Limitations
- **Simplified functions**: Real impacts may be more complex
- **Parameter uncertainty**: Key parameters estimated, not calibrated
- **Interaction effects**: Events may interact non-linearly
- **External factors**: Macro conditions not fully incorporated

### 6.3 Uncertainty Quantification
- **Parameter uncertainty**: ±20-30% for key parameters
- **Model uncertainty**: Different functional forms yield ±15% variation
- **Evidence uncertainty**: Comparable evidence varies ±25% across contexts
- **Total uncertainty**: Combined uncertainty ±40-50%

## 7. Implementation Details

### 7.1 Software Architecture
- **Core class**: `EthiopiaFIImpactModeler`
- **Data classes**: `EventImpact`, `ImpactDirection`, `EvidenceBasis`
- **Database**: Comparable evidence as nested dictionaries
- **Visualization**: Heatmaps for impact matrix

### 7.2 Output Files
1. **Impact Matrix CSV**: Event × Indicator associations
2. **Event Impacts JSON**: Detailed impact objects
3. **Visualization PNG**: Heatmap of impact matrix
4. **Validation Report**: Historical validation results

### 7.3 Performance Considerations
- **Scalability**: Handles up to 100 events and 50 indicators
- **Speed**: Near-instantaneous for current dataset size
- **Memory**: Minimal requirements (<100MB)
- **Parallelization**: Not currently implemented but possible

## 8. Future Improvements

### 8.1 Short-term Enhancements
1. **Bayesian updating**: Incorporate new evidence as it becomes available
2. **Interaction modeling**: Account for event-event interactions
3. **Time-varying parameters**: Allow parameters to change over time
4. **Uncertainty propagation**: Better quantification of total uncertainty

### 8.2 Long-term Development
1. **Machine learning**: Train impact models on broader dataset
2. **Causal inference**: Apply difference-in-differences or synthetic control
3. **Agent-based modeling**: Micro-level simulation of adoption
4. **Real-time updating**: Automatically incorporate new data

## 9. References

1. GSMA (2024). Mobile Money Metrics - East Africa
2. World Bank (2024). Global Findex Database
3. National Bank of Ethiopia (2023). Financial Inclusion Reports
4. IMF (2023). Financial Access Survey
5. Academic literature on digital financial inclusion impacts

## 10. Version History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | 2026-02-01 | Initial methodology document | Data Science Team |
| 1.1 | 2026-02-01 | Added validation methodology | Data Science Team |

---

*This methodology document will be updated as the modeling approach evolves during Task 3 implementation.*
