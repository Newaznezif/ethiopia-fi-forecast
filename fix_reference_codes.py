# fix_reference_codes.py
"""Automatically fix reference_codes.csv"""

print("🛠️  Fixing reference_codes.csv...")

# Create a clean reference codes CSV
clean_data = '''field,code,description
record_type,observation,Measured values from surveys or reports
record_type,event,Events like policies, launches, milestones
record_type,impact_link,Modeled relationships between events and indicators
record_type,target,Official policy goals or targets
pillar,access,Account ownership and access to financial services
pillar,usage,Usage of digital financial services
pillar,infrastructure,Physical and digital infrastructure enabling FI
pillar,enabler,Social, economic, and regulatory enablers
category,policy,Policy or regulatory changes
category,product_launch,Launch of new financial products or services
category,market_entry,Entry of new market players
category,infrastructure,Infrastructure investments or improvements
confidence,high,High confidence in data accuracy
confidence,medium,Medium confidence in data accuracy
confidence,low,Low confidence in data accuracy
impact_direction,positive,Positive impact on indicator
impact_direction,negative,Negative impact on indicator
impact_magnitude,very_low,Very low magnitude of impact
impact_magnitude,low,Low magnitude of impact
impact_magnitude,medium,Medium magnitude of impact
impact_magnitude,high,High magnitude of impact
evidence_basis,direct_observation,Based on direct pre/post observations
evidence_basis,comparable_country,Based on comparable country evidence
evidence_basis,expert_judgment,Based on expert judgment or qualitative assessment'''

# Save to file
with open('data/raw/reference_codes.csv', 'w', encoding='utf-8') as f:
    f.write(clean_data)

print("✅ Created clean reference_codes.csv")
print("\nTesting the new file...")

import pandas as pd
try:
    df = pd.read_csv('data/raw/reference_codes.csv')
    print(f"✅ Test passed: {len(df)} records loaded")
    print(f"   Columns: {list(df.columns)}")
    print(f"   Sample:")
    print(df.head(3).to_string(index=False))
except Exception as e:
    print(f"❌ Test failed: {e}")

print("\nNow test the data loader:")
print("python test_loader_eda.py")
