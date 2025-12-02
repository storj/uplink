# GitHub Actions Workflows

This directory contains automated workflows for the Uplink repository.

## Weekly Copilot Task

**File:** `weekly-copilot-task.yml`

**Schedule:** Every Wednesday at 9:00 AM UTC

**Purpose:** This workflow demonstrates how to set up a scheduled GitHub Action that runs weekly. It can be customized to integrate with GitHub Copilot or other automation tools.

**Trigger Methods:**
- **Automatic:** Runs every Wednesday at 9:00 AM UTC via cron schedule
- **Manual:** Can be triggered manually from the Actions tab using the "workflow_dispatch" feature

**Customization Ideas:**
- Create automated code review issues
- Generate weekly reports
- Run automated maintenance tasks
- Trigger Copilot-based code analysis
- Check for dependency updates
- Generate documentation

To manually trigger the workflow:
1. Go to the "Actions" tab in the GitHub repository
2. Select "Weekly Copilot Task" from the workflows list
3. Click "Run workflow"
