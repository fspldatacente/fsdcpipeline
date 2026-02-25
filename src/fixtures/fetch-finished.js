// src/fixtures/fetch-finished.js
// Fetches finished matches (results) from 365scores API
// Stores them in the finished_matches table in TiDB

import dbClient from '../database/tidb-client.js';

// 365scores API configuration
const BASE_URL = 'https://webws.365scores.com';
const COMPETITION_ID = 649; // Saudi Pro League
const SEASON_NUM = 53; // Current season
const HEADERS = {
    'User-Agent': 'FSDC-Pipeline/1.0',
    'Accept': 'application/json'
};

/**
 * Fetch finished matches from 365scores
 * Uses the /results endpoint which returns completed games
 */
async function fetchFinishedMatches() {
    console.log('📥 Fetching finished matches from 365scores...');
    
    const resultsUrl = '/web/games/results/?appTypeId=5&langId=1&timezoneName=UTC&userCountryId=1&competitions=649';
    let allFinishedMatches = [];
    let nextPage = resultsUrl;
    let pageCount = 0;
    
    try {
        // Paginate through all results
        while (nextPage) {
            pageCount++;
            console.log(`   Fetching page ${pageCount}...`);
            
            const response = await fetch(`${BASE_URL}${nextPage}`, { headers: HEADERS });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();
            
            // Extract games from response
            if (data.games && Array.isArray(data.games)) {
                // Filter for current season and competition
                const seasonGames = data.games.filter(game => 
                    game.seasonNum === SEASON_NUM && 
                    game.competitions?.some(comp => comp.id === COMPETITION_ID)
                );
                
                allFinishedMatches = [...allFinishedMatches, ...seasonGames];
                console.log(`   Found ${seasonGames.length} finished matches in this page`);
            }
            
            // Check for next page
            nextPage = data.paging?.nextPage || null;
        }
        
        console.log(`✅ Total finished matches fetched: ${allFinishedMatches.length}`);
        return allFinishedMatches;
        
    } catch (error) {
        console.error('❌ Error fetching finished matches:', error.message);
        throw error;
    }
}

/**
 * Transform 365scores game data to our database schema
 */
function transformFinishedMatch(game) {
    // Extract home and away teams
    const homeTeam = game.homeCompetitor?.name || 'Unknown';
    const awayTeam = game.awayCompetitor?.name || 'Unknown';
    
    // Extract scores
    const homeScore = game.homeScore || 0;
    const awayScore = game.awayScore || 0;
    
    // Extract round number
    const roundNum = game.roundNum || 0;
    
    // Extract match date
    const matchDate = game.startTime ? new Date(game.startTime) : new Date();
    
    // Determine status
    const status = game.status || 'finished';
    
    // Create fixture ID (combine round + teams to create a unique ID)
    const fixtureId = game.id || `${roundNum}_${homeTeam}_${awayTeam}`.replace(/\s+/g, '_');
    
    return {
        fixture_id: fixtureId,
        round_num: roundNum,
        home_team: homeTeam,
        away_team: awayTeam,
        home_score: homeScore,
        away_score: awayScore,
        match_date: matchDate,
        status: status,
        full_data: game // Store complete original data as JSON
    };
}

/**
 * Save finished matches to TiDB
 */
async function saveFinishedMatches(matches) {
    console.log('💾 Saving finished matches to TiDB...');
    
    let inserted = 0;
    let updated = 0;
    let errors = 0;
    
    for (const game of matches) {
        try {
            const match = transformFinishedMatch(game);
            
            // Check if match already exists
            const existing = await dbClient.query(
                'SELECT fixture_id FROM finished_matches WHERE fixture_id = ?',
                [match.fixture_id]
            );
            
            if (existing.length > 0) {
                // Update existing match
                await dbClient.query(
                    `UPDATE finished_matches 
                     SET home_score = ?, away_score = ?, match_date = ?, 
                         status = ?, full_data = ?, updated_at = CURRENT_TIMESTAMP
                     WHERE fixture_id = ?`,
                    [
                        match.home_score, match.away_score, match.match_date,
                        match.status, JSON.stringify(match.full_data), match.fixture_id
                    ]
                );
                updated++;
            } else {
                // Insert new match
                await dbClient.query(
                    `INSERT INTO finished_matches 
                     (fixture_id, round_num, home_team, away_team, home_score, away_score, match_date, status, full_data)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [
                        match.fixture_id, match.round_num, match.home_team, match.away_team,
                        match.home_score, match.away_score, match.match_date, match.status,
                        JSON.stringify(match.full_data)
                    ]
                );
                inserted++;
            }
            
        } catch (error) {
            console.error(`   Error saving match:`, error.message);
            errors++;
        }
    }
    
    console.log(`   ✅ Inserted: ${inserted}, Updated: ${updated}, Errors: ${errors}`);
    return { inserted, updated, errors };
}

/**
 * Main function to run the finished matches fetch
 */
export default async function runFinishedMatchesFetch(runId) {
    console.log('\n🏁 Starting Finished Matches Fetch...');
    console.log(`   Run ID: ${runId}`);
    console.log(`   Time: ${new Date().toISOString()}`);
    
    let syncLogId = null;
    
    try {
        // Initialize database connection and schema
        await dbClient.initialize();
        await dbClient.initializeSchema();
        
        // Log start of sync
        const syncLogResult = await dbClient.query(
            `INSERT INTO sync_log (run_id, source, status) VALUES (?, ?, ?)`,
            [runId, 'finished-matches', 'running']
        );
        syncLogId = syncLogResult.insertId;
        
        // Fetch finished matches
        const finishedMatches = await fetchFinishedMatches();
        
        if (finishedMatches.length === 0) {
            console.log('⚠️ No finished matches found');
            
            // Update sync log
            await dbClient.query(
                `UPDATE sync_log SET status = ?, completed_at = NOW() WHERE id = ?`,
                ['success', syncLogId]
            );
            
            return {
                success: true,
                count: 0,
                message: 'No finished matches found'
            };
        }
        
        // Save to database
        const stats = await saveFinishedMatches(finishedMatches);
        
        // Update sync log with stats
        await dbClient.query(
            `UPDATE sync_log 
             SET status = ?, completed_at = NOW(), finished_fetched = ?
             WHERE id = ?`,
            ['success', finishedMatches.length, syncLogId]
        );
        
        console.log(`✅ Finished matches fetch completed successfully`);
        
        return {
            success: true,
            count: finishedMatches.length,
            inserted: stats.inserted,
            updated: stats.updated,
            errors: stats.errors,
            runId
        };
        
    } catch (error) {
        console.error('❌ Finished matches fetch failed:', error.message);
        
        // Update sync log with error
        if (syncLogId) {
            await dbClient.query(
                `UPDATE sync_log 
                 SET status = ?, completed_at = NOW(), error_message = ?
                 WHERE id = ?`,
                ['failed', error.message.substring(0, 500), syncLogId]
            );
        }
        
        throw error;
    } finally {
        // Close database connection
        await dbClient.close();
    }
}

// If running directly (not imported)
if (import.meta.url === `file://${process.argv[1]}`) {
    const runId = `finished-${Date.now()}`;
    runFinishedMatchesFetch(runId)
        .then(result => {
            console.log('\n📊 Summary:', result);
            process.exit(0);
        })
        .catch(error => {
            console.error('\n💥 Fatal error:', error);
            process.exit(1);
        });
}