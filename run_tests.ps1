# PowerShell script to run data loader tests
Write-Host "`n===========================================" -ForegroundColor Cyan
Write-Host "RUNNING DATA LOADER UNIT TESTS" -ForegroundColor Cyan
Write-Host "===========================================`n" -ForegroundColor Cyan

# Check if pytest is installed
try {
    $pytestVersion = python -m pytest --version 2>&1
    Write-Host "✅ pytest is installed" -ForegroundColor Green
} catch {
    Write-Host "⚠️ pytest not found. Installing..." -ForegroundColor Yellow
    pip install pytest pytest-cov
}

# Run the comprehensive tests
Write-Host "`n🧪 Running comprehensive tests..." -ForegroundColor White
python -m pytest tests/test_data_loader.py -v --tb=short

# Run the simple test (for quick validation)
Write-Host "`n🧪 Running simple validation test..." -ForegroundColor White
python tests/simple_test.py

# Run with coverage if available
try {
    Write-Host "`n📊 Running tests with coverage..." -ForegroundColor White
    python -m pytest tests/test_data_loader.py --cov=src --cov-report=term-missing
} catch {
    Write-Host "⚠️ Coverage not available. Install pytest-cov for coverage reports" -ForegroundColor Yellow
}

Write-Host "`n===========================================" -ForegroundColor Cyan
Write-Host "TEST EXECUTION COMPLETE" -ForegroundColor Cyan
Write-Host "===========================================`n" -ForegroundColor Cyan

Write-Host "📁 Test files created:" -ForegroundColor White
Write-Host "  • tests/test_data_loader.py - Main test file" -ForegroundColor Gray
Write-Host "  • tests/simple_test.py - Quick validation" -ForegroundColor Gray
Write-Host "  • tests/__init__.py - Package file" -ForegroundColor Gray
Write-Host "  • pytest.ini - Test configuration" -ForegroundColor Gray

Write-Host "`n🚀 Quick test commands:" -ForegroundColor White
Write-Host "  python -m pytest tests/test_data_loader.py -v" -ForegroundColor Yellow
Write-Host "  python tests/simple_test.py" -ForegroundColor Yellow
Write-Host "  python -m pytest tests/ -k 'TestDataLoading'" -ForegroundColor Yellow

Write-Host "`n✅ Ready for CI/CD integration!" -ForegroundColor Green
