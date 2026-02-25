// fixtures.js - Fixtures Module
document.addEventListener('DOMContentLoaded', function() {
    const tabFinished = document.getElementById('tab-finished');
    const tabUnfinished = document.getElementById('tab-unfinished');
    const finishedContent = document.getElementById('finished-content');
    const unfinishedContent = document.getElementById('unfinished-content');
    const finishedTable = document.getElementById('finished-table');
    const unfinishedTable = document.getElementById('unfinished-table');
    
    // Tab switching
    tabFinished.addEventListener('click', () => {
        tabFinished.classList.add('active');
        tabUnfinished.classList.remove('active');
        finishedContent.classList.add('active');
        unfinishedContent.classList.remove('active');
    });
    
    tabUnfinished.addEventListener('click', () => {
        tabUnfinished.classList.add('active');
        tabFinished.classList.remove('active');
        unfinishedContent.classList.add('active');
        finishedContent.classList.remove('active');
    });
    
    // Load data from API
    async function loadFixtures() {
        try {
            const response = await fetch('/.netlify/functions/get-fixtures');
            if (!response.ok) throw new Error('Failed to fetch');
            
            const data = await response.json();
            
            renderFinished(data.finished_matches || []);
            renderUnfinished(data.upcoming_fixtures || []);
            
        } catch (error) {
            console.error('Failed to load fixtures:', error);
            finishedTable.innerHTML = '<div class="empty-state">Failed to load data</div>';
            unfinishedTable.innerHTML = '<div class="empty-state">Failed to load data</div>';
        }
    }
    
    function renderFinished(matches) {
        if (matches.length === 0) {
            finishedTable.innerHTML = '<div class="empty-state">No finished matches found</div>';
            return;
        }
        
        let html = '';
        matches.forEach(match => {
            const homeClass = match.home_score > match.away_score ? 'score-winner' : 'score-draw';
            const awayClass = match.away_score > match.home_score ? 'score-winner' : 'score-draw';
            
            html += `
                <div class="fixture-row">
                    <span>RD${match.round}</span>
                    <span class="team-name ${homeClass}">${match.home}</span>
                    <span class="score">${match.home_score} - ${match.away_score}</span>
                    <span class="team-name ${awayClass}">${match.away}</span>
                    <span><span class="status-badge-finished">Finished</span></span>
                </div>
            `;
        });
        
        finishedTable.innerHTML = html;
    }
    
    function renderUnfinished(matches) {
        if (matches.length === 0) {
            unfinishedTable.innerHTML = '<div class="empty-state">No upcoming fixtures found</div>';
            return;
        }
        
        let html = '';
        matches.forEach(match => {
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
    
    // Load initial data
    loadFixtures();
    
    // Auto-refresh every 60 seconds
    setInterval(loadFixtures, 60000);
});