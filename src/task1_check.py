# src/task1_check.py
import pandas as pd
import os
from pathlib import Path

def check_task1_completion():
    """Check if Task 1 requirements are met"""
    
    print("🔍 TASK 1 COMPLETION CHECK")
    print("="*50)
    
    requirements = {
        "Files exist": False,
        "Data loaded successfully": False,
        "Enrichment log exists": False,
        "Added new observations": False,
        "Added new events": False,
        "Branch committed": False
    }
    
    # Check 1: Files exist
    required_files = [
        "data/raw/ethiopia_fi_unified_data.csv",
        "data/raw/reference_codes.csv",
        "src/core_analysis.py",
        "data_enrichment_log.md"
    ]
    
    missing_files = []
    for file in required_files:
        if os.path.exists(file):
            print(f"✅ {file}")
        else:
            print(f"❌ {file}")
            missing_files.append(file)
    
    requirements["Files exist"] = len(missing_files) == 0
    
    # Check 2: Data loads
    try:
        df = pd.read_csv("data/raw/ethiopia_fi_unified_data.csv")
        original_count = 19  # Initial record count
        current_count = len(df)
        
        print(f"\n📊 Data Status:")
        print(f"   Original records: {original_count}")
        print(f"   Current records: {current_count}")
        print(f"   Records added: {current_count - original_count}")
        
        if current_count > original_count:
            requirements["Added new observations"] = True
        
        requirements["Data loaded successfully"] = True
        
    except Exception as e:
        print(f"❌ Data loading failed: {e}")
    
    # Check 3: Enrichment log
    if os.path.exists("data_enrichment_log.md"):
        with open("data_enrichment_log.md", 'r') as f:
            log_content = f.read()
            if len(log_content) > 500:  # More than just header
                requirements["Enrichment log exists"] = True
                print(f"\n📝 Enrichment log: {len(log_content)} characters")
    
    # Check 4: Git status (simplified)
    if os.path.exists(".git"):
        requirements["Branch committed"] = True
        print("\n✅ Git repository initialized")
    
    # Final assessment
    print("\n" + "="*50)
    print("TASK 1 COMPLETION STATUS")
    print("="*50)
    
    completed = sum(requirements.values())
    total = len(requirements)
    
    for req, status in requirements.items():
        status_icon = "✅" if status else "❌"
        print(f"{status_icon} {req}")
    
    print(f"\n📈 Progress: {completed}/{total} requirements met")
    
    if completed >= 4:
        print("\n🎉 TASK 1 READY FOR SUBMISSION!")
        print("\nNext steps:")
        print("1. git add .")
        print("2. git commit -m 'Task 1: Data exploration and enrichment'")
        print("3. git push origin task-1-data-exploration")
        print("4. Create Pull Request to main branch")
    else:
        print(f"\n⚠️  Missing {total - completed} requirements")
        print("Continue with data enrichment before submission.")

if __name__ == "__main__":
    check_task1_completion()
