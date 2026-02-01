# Git setup for the project
Write-Host "Setting up Git for Ethiopia FI Project..." -ForegroundColor Cyan

# Initialize git
git init
Write-Host "✅ Git repository initialized" -ForegroundColor Green

# Add all files
git add .
Write-Host "✅ Files added to staging" -ForegroundColor Green

# Initial commit
git commit -m "Initial commit: Ethiopia FI project setup with data loader"
Write-Host "✅ Initial commit created" -ForegroundColor Green

# Create branches for tasks
git branch task-1-data-exploration
git branch task-2-eda
git branch task-3-impact-modeling
git branch task-4-forecasting
git branch task-5-dashboard

# Switch to task 1
git checkout task-1-data-exploration
Write-Host "✅ Switched to task-1-data-exploration branch" -ForegroundColor Green

Write-Host "
🎉 Git setup complete!" -ForegroundColor Cyan
Write-Host "
Current branch: task-1-data-exploration"
Write-Host "Run 'git branch' to see all branches"
Write-Host "Run 'git status' to see current status"
