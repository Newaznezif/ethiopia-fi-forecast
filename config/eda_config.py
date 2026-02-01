# EDA Configuration for Ethiopia Financial Inclusion

## Analysis Settings
start_year = 2011
end_year = 2024
confidence_threshold = 0.7  # For correlation analysis

## Key Indicators to Analyze
access_indicators = [
    "ACC_OWNERSHIP",
    "ACC_MM_ACCOUNT",
    "ACC_OWNERSHIP_FEMALE",
    "ACC_OWNERSHIP_MALE"
]

usage_indicators = [
    "USG_DIGITAL_PAYMENT",
    "USG_DIGITAL_PAYMENT_FEMALE",
    "USG_DIGITAL_PAYMENT_MALE",
    "USG_P2P_PAYMENT",
    "USG_MERCHANT_PAYMENT"
]

infrastructure_indicators = [
    "INF_AGENT_DENSITY",
    "INF_4G_COVERAGE",
    "INF_ATM_DENSITY",
    "INF_BRANCH_DENSITY"
]

enabler_indicators = [
    "ENA_SMARTPHONE_PEN",
    "ENA_INTERNET_ACCESS",
    "ENA_DIGITAL_LITERACY",
    "ENA_ELECTRICITY_ACCESS"
]

## Visualization Settings
figure_size = (12, 6)
dpi = 300
color_palette = "husl"

## Report Settings
min_insights = 5
confidence_levels = ["high", "medium", "low"]
output_formats = ["png", "json", "md"]
