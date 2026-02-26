// src/fixtures/fetch-finished.js
// Fetches finished matches from 365scores API
// Stores them in finished_matches AND adds to unprocessed_fixtures with status tracking

import dbClient from '../database/tidb-client.js';

const BASE_URL = 'https://webws.365scores.com';
const COMPETITION_ID = 649;
const SEASON_NUM = 53; // Current season
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
    let earlyExit = false;
    
    // Get the latest match date from database to check for new data
    let latestMatchDate = null;
    try {
        const [latestResult] = await dbClient.query(
            'SELECT MAX(match_date) as latest FROM finished_matches'
        );
        latestMatchDate = latestResult[0]?.latest;
        if (latestMatchDate) {
            console.log(`   Latest match in DB: ${new Date(latestMatchDate).toISOString()}`);
        }
    } catch (error) {
        console.log('   Could not fetch latest match date, will fetch all pages');
    }
    
    try {
        while (currentPage && !earlyExit) {
            pageCount++;
            console.log(`   Fetching page ${pageCount}...`);
            console.log(`   URL: ${BASE_URL}${currentPage}`);
            
            const response = await fetch(`${BASE_URL}${currentPage}`, { headers: HEADERS });
            
            if (!response.ok) {
                console.error(`   HTTP error! status: ${response.status}`);
                break;
            }
            
            const data = await response.json();
            
            if (data.games && Array.isArray(data.games)) {
                // CRITICAL: Only keep current season matches (seasonNum = 53)
                const currentSeasonGames = data.games.filter(game => game.seasonNum === SEASON_NUM);
                
                console.log(`   Found ${data.games.length} matches in this page, ${currentSeasonGames.length} from current season`);
                
                if (currentSeasonGames.length > 0) {
                    allFinishedMatches = [...allFinishedMatches, ...currentSeasonGames];
                    
                    // Check if we have old data and can exit early
                    if (latestMatchDate && pageCount === 1) {
                        const newestInBatch = new Date(currentSeasonGames[0].startTime);
                        if (newestInBatch <= new Date(latestMatchDate)) {
                            console.log('   No new matches since last run, stopping early');
                            earlyExit = true;
                            break;
                        }
                    }
                }
                
                // Sample first game for debugging
                if (pageCount === 1 && currentSeasonGames.length > 0) {
                    console.log('   Sample current season game:', JSON.stringify(currentSeasonGames[0], null, 2).substring(0, 500) + '...');
                }
            }
            
            currentPage = data.paging?.previousPage || null;
            console.log(`   Next page:`, currentPage || 'none');
        }
        
        console.log(`✅ Total current season matches fetched: ${allFinishedMatches.length}`);
        return allFinishedMatches;
        
    } catch (error) {
        console.error('❌ Error fetching finished matches:', error.message);
        throw error;
    }
}

function transformFinishedMatch(game) {
    const homeTeam = game.homeCompetitor?.name || 'Unknown';
    const awayTeam = game.awayCompetitor?.name || 'Unknown';
    const homeScore = game.homeCompetitor?.score || 0;
    const awayScore = game.awayCompetitor?.score || 0;
    const roundNum = game.roundNum || 0;
    const matchDate = game.startTime || new Date().toISOString();
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
            
            // 4. Check if status record already exists
            const statusCheck = await dbClient.query(
                'SELECT fixture_id FROM match_processing_status WHERE fixture_id = ?',
                [match.fixture_id]
            );
            
            // 5. If not in unprocessed AND not in processed, add to unprocessed
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
                
                // 6. Create status record if it doesn't exist
                if (statusCheck.length === 0) {
                    await dbClient.query(
                        `INSERT INTO match_processing_status 
                         (fixture_id, round_num, home_team, away_team, match_date, overall_status)
                         VALUES (?, ?, ?, ?, ?, 'pending')`,
                        [
                            match.fixture_id, 
                            match.round_num, 
                            match.home_team, 
                            match.away_team, 
                            match.match_date
                        ]
                    );
                }
                
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
    console.log(`   📊 Status records: Created/verified for ${addedToUnprocessed} fixtures`);
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
            console.log('⚠️ No current season finished matches found');
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