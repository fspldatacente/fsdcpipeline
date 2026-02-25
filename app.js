// app.js - Main Dashboard
document.addEventListener('DOMContentLoaded', function() {
    const lastRunEl = document.getElementById('last-run');
    const statusEl = document.getElementById('status');
    const nextRunEl = document.getElementById('next-run');
    const runNowBtn = document.getElementById('run-now-btn');
    const viewLogsBtn = document.getElementById('view-logs-btn');
    const refreshBtn = document.getElementById('refresh-btn');
    const dataOverviewEl = document.getElementById('data-overview');
    
    // Initialize
    updateStatus();
    updateDataOverview();
    
    // Auto-refresh every 60 seconds
    setInterval(() => {
        updateStatus();
        updateDataOverview();
    }, 60000);
    
    // Event Listeners
    refreshBtn.addEventListener('click', () => {
        updateStatus();
        updateDataOverview();
    });
    
    viewLogsBtn.addEventListener('click', () => {
        window.open('https://github.com/fspldatacente/fsdcpipeline/actions', '_blank');
    });
    
    runNowBtn.addEventListener('click', async () => {
        runNowBtn.disabled = true;
        runNowBtn.textContent = 'Running...';
        
        alert('To trigger the pipeline, we need to set up GitHub token. Coming soon!');
        
        runNowBtn.disabled = false;
        runNowBtn.textContent = 'Run Pipeline Now';
    });
    
    // Update pipeline status from GitHub Actions
    async function updateStatus() {
        try {
            const response = await fetch('https://api.github.com/repos/fspldatacente/fsdcpipeline/actions/runs?per_page=1');
            const data = await response.json();
            
            if (data.workflow_runs && data.workflow_runs.length > 0) {
                const latestRun = data.workflow_runs[0];
                
                const runDate = new Date(latestRun.created_at);
                lastRunEl.textContent = runDate.toLocaleString();
                
                statusEl.textContent = latestRun.status === 'completed' ? 
                    (latestRun.conclusion === 'success' ? 'SUCCESS' : 'FAILED') : 
                    latestRun.status.toUpperCase();
                
                statusEl.className = 'value status-badge ' + 
                    (latestRun.status === 'completed' ? 
                        (latestRun.conclusion === 'success' ? 'success' : 'failed') : 
                        (latestRun.status === 'in_progress' ? 'running' : 'pending'));
                
                // Calculate next run (hourly at :15)
                const now = new Date();
                const nextRun = new Date(now);
                if (now.getMinutes() < 15) {
                    nextRun.setMinutes(15, 0, 0);
                } else {
                    nextRun.setHours(now.getHours() + 1, 15, 0, 0);
                }
                nextRunEl.textContent = nextRun.toLocaleString();
            } else {
                lastRunEl.textContent = 'No runs yet';
                statusEl.textContent = 'PENDING';
                statusEl.className = 'value status-badge pending';
            }
        } catch (error) {
            console.error('Error fetching status:', error);
            lastRunEl.textContent = 'Error loading';
            statusEl.textContent = 'ERROR';
            statusEl.className = 'value status-badge failed';
        }
    }
    
    // Update data overview - fetches from API
    async function updateDataOverview() {
        try {
            const response = await fetch('/.netlify/functions/get-fixtures');
            const data = await response.json();
            
            dataOverviewEl.innerHTML = `
                <div class="data-item">
                    <h3>Finished Matches</h3>
                    <div class="count">${data.finished_matches?.length || 0}</div>
                </div>
                <div class="data-item">
                    <h3>Upcoming Fixtures</h3>
                    <div class="count">${data.upcoming_fixtures?.length || 0}</div>
                </div>
                <div class="data-item">
                    <h3>Total Matches</h3>
                    <div class="count">${(data.finished_matches?.length || 0) + (data.upcoming_fixtures?.length || 0)}</div>
                </div>
                <div class="data-item">
                    <h3>Last Fetch</h3>
                    <div class="count">${new Date().toLocaleTimeString()}</div>
                </div>
            `;
        } catch (error) {
            console.error('Error fetching overview:', error);
            dataOverviewEl.innerHTML = '<div class="data-item loading">Failed to load data</div>';
        }
    }
});