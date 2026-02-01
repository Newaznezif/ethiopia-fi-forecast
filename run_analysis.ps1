# PowerShell script to run the analysis
Write-Host "Starting Ethiopia FI Analysis..." -ForegroundColor Cyan

# Check if Python is installed
try {
    python --version
} catch {
    Write-Host "❌ Python not found. Please install Python first." -ForegroundColor Red
    exit 1
}

# Install requirements
Write-Host "
📦 Installing requirements..." -ForegroundColor Yellow
pip install -r requirements.txt

# Run the test
Write-Host "
🚀 Running data analysis..." -ForegroundColor Green
python test_loader.py

# Show files created
Write-Host "
📁 Files created:" -ForegroundColor Cyan
Get-ChildItem "data/raw" -Name
Write-Host "
📊 Processed data available in: data/processed/" -ForegroundColor Cyan

Write-Host "
✅ Analysis complete! Check the output above." -ForegroundColor Green
Write-Host "
Next steps:"
Write-Host "1. Review the data in data/raw/"
Write-Host "2. Add more data to data_enrichment_log.md"
Write-Host "3. Run: python src/data_loader.py for detailed analysis"
