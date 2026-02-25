// FSDC Pipeline Dashboard
document.addEventListener('DOMContentLoaded', function() {
    // ===== ELEMENTS =====
    // Status elements
    const lastRunEl = document.getElementById('last-run');
    const statusEl = document.getElementById('status');
    const nextRunEl = document.getElementById('next-run');
    
    // Button elements
    const runNowBtn = document.getElementById('run-now-btn');
    const viewLogsBtn = document.getElementById('view-logs-btn');
    const refreshBtn = document.getElementById('refresh-btn');
    
    // Data overview element
    const dataOverviewEl = document.getElementById('data-overview');
    
    // ===== NEW: Fixtures Tab Elements =====
    const tabFinished = document.getElementById('tab-finished');
    const tabUnfinished = document.getElementById('tab-unfinished');
    const finishedContent = document.getElementById('finished-content');
    const unfinishedContent = document.getElementById('unfinished-content');
    const finishedTable = document.getElementById('finished-table');
    const unfinishedTable = document.getElementById('unfinished-table');
    
    // ===== STATE =====
    let finishedMatches = [];
    let unfinishedMatches = [];
    
    // ===== INITIALIZE =====
    updateStatus();
    updateDataOverview();
    fetchFixturesData();
    
    // Auto-refresh every 60 seconds
    setInterval(() => {
        updateStatus();
        fetchFixturesData();
    }, 60000);
    
    // ===== EVENT LISTENERS =====
    
    // Refresh button
    refreshBtn.addEventListener('click', () => {
        updateStatus();
        fetchFixturesData();
    });
    
    // View logs button
    viewLogsBtn.addEventListener('click', () => {
        window.open('https://github.com/fspldatacente/fsdcpipeline/actions', '_blank');
    });
    
    // Run now button (placeholder for now)
    runNowBtn.addEventListener('click', async () => {
        runNowBtn.disabled = true;
        runNowBtn.textContent = 'Running...';
        
        alert('To trigger the pipeline, we need to set up GitHub token. Coming soon!');
        
        runNowBtn.disabled = false;
        runNowBtn.textContent = 'Run Pipeline Now';
    });
    
    // ===== NEW: Tab Switching =====
    tabFinished.addEventListener('click', () => {
        // Update tab buttons
        tabFinished.classList.add('active');
        tabUnfinished.classList.remove('active');
        
        // Show/hide content
        finishedContent.classList.add('active');
        unfinishedContent.classList.remove('active');
    });
    
    tabUnfinished.addEventListener('click', () => {
        // Update tab buttons
        tabUnfinished.classList.add('active');
        tabFinished.classList.remove('active');
        
        // Show/hide content
        unfinishedContent.classList.add('active');
        finishedContent.classList.remove('active');
    });
    
    // ===== FUNCTIONS =====
    
    // Update pipeline status from GitHub Actions
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
    
    // ===== NEW: Fetch fixtures data (mock data for now) =====
    async function fetchFixturesData() {
        // Show loading states
        finishedTable.innerHTML = '<div class="loading">Loading finished matches...</div>';
        unfinishedTable.innerHTML = '<div class="loading">Loading upcoming fixtures...</div>';
        
        try {
            // TODO: Replace with actual API calls to your pipeline
            // For now, using mock data to test the UI
            
            // Simulate API delay
            await new Promise(resolve => setTimeout(resolve, 1000));
            
            // Mock finished matches (results)
            finishedMatches = [
                { round: 22, home: "Al Hilal", away: "Al Nassr", homeScore: 2, awayScore: 1, status: "finished" },
                { round: 22, home: "Al Ittihad", away: "Al Ahli", homeScore: 3, awayScore: 0, status: "finished" },
                { round: 22, home: "Al Shabab", away: "Al Taawoun", homeScore: 1, awayScore: 1, status: "finished" },
                { round: 21, home: "Al Nassr", away: "Al Fateh", homeScore: 4, awayScore: 2, status: "finished" },
                { round: 21, home: "Al Hilal", away: "Al Raed", homeScore: 3, awayScore: 0, status: "finished" },
                { round: 21, home: "Al Ittihad", away: "Al Khaleej", homeScore: 2, awayScore: 2, status: "finished" },
                { round: 20, home: "Al Ahli", away: "Al Wehda", homeScore: 1, awayScore: 0, status: "finished" },
                { round: 20, home: "Al Taawoun", away: "Al Fateh", homeScore: 2, awayScore: 1, status: "finished" }
            ];
            
            // Mock unfinished matches (fixtures)
            unfinishedMatches = [
                { round: 23, home: "Al Nassr", away: "Al Hilal", kickoff: "2026-03-01 20:00", status: "upcoming" },
                { round: 23, home: "Al Ahli", away: "Al Ittihad", kickoff: "2026-03-01 18:00", status: "upcoming" },
                { round: 23, home: "Al Fateh", away: "Al Shabab", kickoff: "2026-03-02 16:00", status: "upcoming" },
                { round: 23, home: "Al Raed", away: "Al Taawoun", kickoff: "2026-03-02 18:30", status: "upcoming" },
                { round: 24, home: "Al Hilal", away: "Al Ittihad", kickoff: "2026-03-08 20:00", status: "upcoming" },
                { round: 24, home: "Al Nassr", away: "Al Ahli", kickoff: "2026-03-08 18:00", status: "upcoming" }
            ];
            
            // Render the tables
            renderFinishedTable();
            renderUnfinishedTable();
            
        } catch (error) {
            console.error('Error fetching fixtures:', error);
            finishedTable.innerHTML = '<div class="empty-state">Failed to load finished matches</div>';
            unfinishedTable.innerHTML = '<div class="empty-state">Failed to load upcoming fixtures</div>';
        }
    }
    
    // Render finished matches table
    function renderFinishedTable() {
        if (finishedMatches.length === 0) {
            finishedTable.innerHTML = '<div class="empty-state">No finished matches found</div>';
            return;
        }
        
        let html = '';
        finishedMatches.forEach(match => {
            const homeScore = match.homeScore;
            const awayScore = match.awayScore;
            
            // Determine winner class
            let homeClass = '';
            let awayClass = '';
            if (homeScore > awayScore) {
                homeClass = 'score-winner';
                awayClass = 'score-draw';
            } else if (awayScore > homeScore) {
                homeClass = 'score-draw';
                awayClass = 'score-winner';
            } else {
                homeClass = 'score-draw';
                awayClass = 'score-draw';
            }
            
            html += `
                <div class="fixture-row">
                    <span>RD${match.round}</span>
                    <span class="team-name ${homeClass}">${match.home}</span>
                    <span class="score">${homeScore} - ${awayScore}</span>
                    <span class="team-name ${awayClass}">${match.away}</span>
                    <span><span class="status-badge-finished">Finished</span></span>
                </div>
            `;
        });
        
        finishedTable.innerHTML = html;
    }
    
    // Render unfinished matches table
    function renderUnfinishedTable() {
        if (unfinishedMatches.length === 0) {
            unfinishedTable.innerHTML = '<div class="empty-state">No upcoming fixtures found</div>';
            return;
        }
        
        let html = '';
        unfinishedMatches.forEach(match => {
            // Format kickoff time nicely
            const kickoffDate = new Date(match.kickoff);
            const formattedKickoff = kickoffDate.toLocaleString('en-US', {
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
            
            html += `
                <div class="fixture-row">
                    <span>RD${match.round}</span>
                    <span class="team-name">${match.home}</span>
                    <span class="team-name">${match.away}</span>
                    <span class="kickoff-time">${formattedKickoff}</span>
                    <span><span class="status-badge-unfinished">Scheduled</span></span>
                </div>
            `;
        });
        
        unfinishedTable.innerHTML = html;
    }
    
    // Update data overview (placeholder for now)
    async function updateDataOverview() {
        dataOverviewEl.innerHTML = `
            <div class="data-item">
                <h3>Finished Matches</h3>
                <div class="count">${finishedMatches.length}</div>
            </div>
            <div class="data-item">
                <h3>Upcoming Fixtures</h3>
                <div class="count">${unfinishedMatches.length}</div>
            </div>
            <div class="data-item">
                <h3>Total Matches</h3>
                <div class="count">${finishedMatches.length + unfinishedMatches.length}</div>
            </div>
            <div class="data-item">
                <h3>Last Fetch</h3>
                <div class="count">${new Date().toLocaleTimeString()}</div>
            </div>
        `;
    }
});