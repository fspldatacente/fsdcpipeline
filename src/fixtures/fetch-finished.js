// src/fixtures/fetch-finished.js
// Fetches finished matches from 365scores API
// Stores them in finished_matches table

import dbClient from '../database/tidb-client.js';

const BASE_URL = 'https://webws.365scores.com';
const COMPETITION_ID = 649;
const SEASON_NUM = 53;
const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
};

async function fetchFinishedMatches() {
    console.log('📥 Fetching finished matches from 365scores...');
    
    const resultsUrl = '/web/games/results/?appTypeId=5&langId=1&timezoneName=UTC&userCountryId=1&competitions=649';
    let allFinishedMatches = [];
    let currentPage = resultsUrl;
    let pageCount = 0;
    
    try {
        while (currentPage) {
            pageCount++;
            console.log(`   Fetching page ${pageCount}...`);
            console.log(`   URL: ${BASE_URL}${currentPage}`);
            
            const response = await fetch(`${BASE_URL}${currentPage}`, { headers: HEADERS });
            
            if (!response.ok) {
                console.error(`   HTTP error! status: ${response.status}`);
                break;
            }
            
            const data = await response.json();
            
            // Log the response structure to debug
            console.log(`   Response keys:`, Object.keys(data));
            console.log(`   Has games array:`, Array.isArray(data.games));
            console.log(`   Games count:`, data.games?.length || 0);
            
            if (data.games && Array.isArray(data.games)) {
                // Don't filter by seasonNum - just take all games for this competition
                // The API already filtered by competition=649
                allFinishedMatches = [...allFinishedMatches, ...data.games];
                console.log(`   Found ${data.games.length} matches in this page`);
                
                // Log first game structure to understand it
                if (pageCount === 1 && data.games.length > 0) {
                    console.log('   Sample game structure:', JSON.stringify(data.games[0], null, 2).substring(0, 500) + '...');
                }
            }
            
            // CRITICAL FIX: For results, we need previousPage, not nextPage
            currentPage = data.paging?.previousPage || null;
            console.log(`   Next page:`, currentPage || 'none');
        }
        
        console.log(`✅ Total finished matches fetched: ${allFinishedMatches.length}`);
        return allFinishedMatches;
        
    } catch (error) {
        console.error('❌ Error fetching finished matches:', error.message);
        throw error;
    }
}

function transformFinishedMatch(game) {
    // Check different possible property names
    const homeTeam = game.homeCompetitor?.name || game.home_team || 'Unknown';
    const awayTeam = game.awayCompetitor?.name || game.away_team || 'Unknown';
    const homeScore = game.homeScore || game.home_score || 0;
    const awayScore = game.awayScore || game.away_score || 0;
    const roundNum = game.roundNum || game.round_num || 0;
    const matchDate = game.startTime || game.kickoff_time || game.date || new Date().toISOString();
    const status = game.status || 'finished';
    const fixtureId = game.id || `${roundNum}_${homeTeam}_${awayTeam}`.replace(/\s+/g, '_');
    
    return {
        fixture_id: fixtureId,
        round_num: roundNum,
        home_team: homeTeam,
        away_team: awayTeam,
        home_score: homeScore,
        away_score: awayScore,
        match_date: new Date(matchDate),
        status: status,
        full_data: game
    };
}

async function saveFinishedMatches(matches) {
    console.log('💾 Saving finished matches to TiDB...');
    
    let inserted = 0;
    let updated = 0;
    let errors = 0;
    
    for (const game of matches) {
        try {
            const match = transformFinishedMatch(game);
            
            const existing = await dbClient.query(
                'SELECT fixture_id FROM finished_matches WHERE fixture_id = ?',
                [match.fixture_id]
            );
            
            if (existing.length > 0) {
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

export default async function runFinishedMatchesFetch(runId) {
    console.log('\n🏁 Starting Finished Matches Fetch...');
    console.log(`   Run ID: ${runId}`);
    console.log(`   Time: ${new Date().toISOString()}`);
    
    let syncLogId = null;
    
    try {
        await dbClient.initialize();
        
        // Log start of sync
        const syncLogResult = await dbClient.query(
            `INSERT INTO sync_log (run_id, source, status) VALUES (?, ?, ?)`,
            [runId, 'finished-matches', 'running']
        );
        syncLogId = syncLogResult.insertId;
        
        const finishedMatches = await fetchFinishedMatches();
        
        if (finishedMatches.length === 0) {
            console.log('⚠️ No finished matches found');
            await dbClient.query(
                `UPDATE sync_log SET status = ?, completed_at = NOW() WHERE id = ?`,
                ['success', syncLogId]
            );
            return { success: true, count: 0 };
        }
        
        const stats = await saveFinishedMatches(finishedMatches);
        
        await dbClient.query(
            `UPDATE sync_log 
             SET status = ?, completed_at = NOW(), finished_fetched = ?
             WHERE id = ?`,
            ['success', finishedMatches.length, syncLogId]
        );
        
        return {
            success: true,
            count: finishedMatches.length,
            ...stats
        };
        
    } catch (error) {
        console.error('❌ Finished matches fetch failed:', error.message);
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
        await dbClient.close();
    }
}