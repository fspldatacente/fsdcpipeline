// FSDC Pipeline Dashboard
document.addEventListener('DOMContentLoaded', function() {
    // Elements
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
    
    // Event Listeners
    refreshBtn.addEventListener('click', () => {
        updateStatus();
        updateDataOverview();
    });
    
    runNowBtn.addEventListener('click', async () => {
        runNowBtn.disabled = true;
        runNowBtn.textContent = 'Running...';
        
        alert('To trigger the pipeline, you need to set up a GitHub token. We\'ll add this later.');
        
        runNowBtn.disabled = false;
        runNowBtn.textContent = 'Run Pipeline Now';
    });
    
    viewLogsBtn.addEventListener('click', () => {
        window.open('https://github.com/fspldatacente/fsdcpipeline/actions', '_blank');
    });
    
    // Update status from GitHub Actions
    async function updateStatus() {
        try {
            const response = await fetch('https://api.github.com/repos/fspldatacente/fsdcpipeline/actions/runs?per_page=1');
            const data = await response.json();
            
            if (data.workflow_runs && data.workflow_runs.length > 0) {
                const latestRun = data.workflow_runs[0];
                
                // Format date
                const runDate = new Date(latestRun.created_at);
                lastRunEl.textContent = runDate.toLocaleString();
                
                // Set status
                statusEl.textContent = latestRun.status === 'completed' ? 
                    (latestRun.conclusion === 'success' ? 'SUCCESS' : 'FAILED') : 
                    latestRun.status.toUpperCase();
                
                statusEl.className = 'value status-badge ' + 
                    (latestRun.status === 'completed' ? 
                        (latestRun.conclusion === 'success' ? 'success' : 'failed') : 
                        (latestRun.status === 'in_progress' ? 'running' : 'pending'));
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
    
    // Update data overview (placeholder for now)
    async function updateDataOverview() {
        dataOverviewEl.innerHTML = `
            <div class="data-item">
                <h3>Players Processed</h3>
                <div class="count">--</div>
            </div>
            <div class="data-item">
                <h3>Teams Processed</h3>
                <div class="count">--</div>
            </div>
            <div class="data-item">
                <h3>Last Data Fetch</h3>
                <div class="count">--</div>
            </div>
            <div class="data-item">
                <h3>Database Size</h3>
                <div class="count">--</div>
            </div>
        `;
    }
    
    // Auto-refresh every 60 seconds
    setInterval(updateStatus, 60000);
});