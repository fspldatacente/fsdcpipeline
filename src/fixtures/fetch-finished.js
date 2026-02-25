// src/fixtures/fetch-finished.js
// Fetches finished matches from 365scores API
// Stores them in finished_matches AND adds to unprocessed_fixtures

import dbClient from '../database/tidb-client.js';

const BASE_URL = 'https://webws.365scores.com';
const COMPETITION_ID = 649;
const SEASON_NUM = 53;
const HEADERS = {
    'User-Agent': 'FSDC-Pipeline/1.0',
    'Accept': 'application/json'
};

async function fetchFinishedMatches() {
    console.log('📥 Fetching finished matches from 365scores...');
    
    const resultsUrl = '/web/games/results/?appTypeId=5&langId=1&timezoneName=UTC&userCountryId=1&competitions=649';
    let allFinishedMatches = [];
    let nextPage = resultsUrl;
    let pageCount = 0;
    
    try {
        while (nextPage) {
            pageCount++;
            console.log(`   Fetching page ${pageCount}...`);
            
            const response = await fetch(`${BASE_URL}${nextPage}`, { headers: HEADERS });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();
            
            if (data.games && Array.isArray(data.games)) {
                const seasonGames = data.games.filter(game => 
                    game.seasonNum === SEASON_NUM && 
                    game.competitions?.some(comp => comp.id === COMPETITION_ID)
                );
                
                allFinishedMatches = [...allFinishedMatches, ...seasonGames];
                console.log(`   Found ${seasonGames.length} finished matches in this page`);
            }
            
            nextPage = data.paging?.nextPage || null;
        }
        
        console.log(`✅ Total finished matches fetched: ${allFinishedMatches.length}`);
        return allFinishedMatches;
        
    } catch (error) {
        console.error('❌ Error fetching finished matches:', error.message);
        throw error;
    }
}

function transformFinishedMatch(game) {
    const homeTeam = game.homeCompetitor?.name || 'Unknown';
    const awayTeam = game.awayCompetitor?.name || 'Unknown';
    const homeScore = game.homeScore || 0;
    const awayScore = game.awayScore || 0;
    const roundNum = game.roundNum || 0;
    const matchDate = game.startTime ? new Date(game.startTime) : new Date();
    const status = game.status || 'finished';
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
        full_data: game  // Keep full raw data for reference
    };
}

async function saveFinishedMatches(matches) {
    console.log('💾 Saving finished matches to TiDB...');
    
    let inserted = 0;
    let updated = 0;
    let addedToUnprocessed = 0;
    let skipped = 0;
    let errors = 0;
    
    for (const game of matches) {
        try {
            const match = transformFinishedMatch(game);
            
            // 1. Save/Update in finished_matches table
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
            
            // 2. Check if this fixture is already in unprocessed_fixtures
            const unprocessedCheck = await dbClient.query(
                'SELECT fixture_id FROM unprocessed_fixtures WHERE fixture_id = ?',
                [match.fixture_id]
            );
            
            // 3. Check if it's already in processed_fixtures (already done)
            const processedCheck = await dbClient.query(
                'SELECT fixture_id FROM processed_fixtures WHERE fixture_id = ?',
                [match.fixture_id]
            );
            
            // 4. If not in unprocessed AND not in processed, add to unprocessed
            if (unprocessedCheck.length === 0 && processedCheck.length === 0) {
                await dbClient.query(
                    `INSERT INTO unprocessed_fixtures 
                     (fixture_id, round_num, home_team, away_team, home_score, away_score, match_date, full_data)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
                    [
                        match.fixture_id, match.round_num, match.home_team, match.away_team,
                        match.home_score, match.away_score, match.match_date,
                        JSON.stringify(match.full_data)
                    ]
                );
                addedToUnprocessed++;
            } else if (processedCheck.length > 0) {
                skipped++;
            }
            
        } catch (error) {
            console.error(`   Error saving match ${game.id}:`, error.message);
            errors++;
        }
    }
    
    console.log(`   📊 Finished matches: Inserted ${inserted}, Updated ${updated}`);
    console.log(`   📋 Unprocessed queue: Added ${addedToUnprocessed} new fixtures`);
    console.log(`   ⏭️  Skipped (already processed): ${skipped}`);
    console.log(`   ❌ Errors: ${errors}`);
    
    return { inserted, updated, addedToUnprocessed, skipped, errors };
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