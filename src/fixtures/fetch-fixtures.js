// src/fixtures/fetch-fixtures.js
// Fetches and updates fixtures intelligently:
// 1. First run: fetches ALL historical data
// 2. Subsequent runs: compares upcoming lists to find newly finished games
// 3. Handles live games correctly

import dbClient from '../database/tidb-client.js';

const BASE_URL = 'https://webws.365scores.com';
const COMPETITION_ID = 649;
const SEASON_NUM = 53;
const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
};

// Fetch all upcoming fixtures (includes scheduled and live)
async function fetchAllUpcomingFixtures() {
    console.log('📥 Fetching all upcoming fixtures from 365scores...');
    
    const fixturesUrl = '/web/games/fixtures/?appTypeId=5&langId=1&timezoneName=UTC&userCountryId=1&competitions=649';
    let allGames = [];
    let currentPage = fixturesUrl;
    let pageCount = 0;
    
    try {
        while (currentPage) {
            pageCount++;
            console.log(`   Fetching upcoming page ${pageCount}...`);
            
            const response = await fetch(`${BASE_URL}${currentPage}`, { headers: HEADERS });
            
            if (!response.ok) {
                console.error(`   HTTP error! status: ${response.status}`);
                break;
            }
            
            const data = await response.json();
            
            if (data.games && Array.isArray(data.games)) {
                console.log(`   Found ${data.games.length} games in this page`);
                allGames = [...allGames, ...data.games];
            }
            
            currentPage = data.paging?.nextPage || null;
        }
        
        // Filter for current season
        const currentSeasonGames = allGames.filter(game => game.seasonNum === SEASON_NUM);
        console.log(`✅ Total upcoming games fetched: ${allGames.length}`);
        console.log(`✅ Current season upcoming games: ${currentSeasonGames.length}`);
        
        return currentSeasonGames;
        
    } catch (error) {
        console.error('❌ Error fetching upcoming fixtures:', error.message);
        throw error;
    }
}

// Fetch all historical finished matches (for first run only)
async function fetchAllHistoricalFinished() {
    console.log('📥 Fetching all historical finished matches (first run)...');
    
    const resultsUrl = '/web/games/results/?appTypeId=5&langId=1&timezoneName=UTC&userCountryId=1&competitions=649';
    let allGames = [];
    let currentPage = resultsUrl;
    let pageCount = 0;
    
    try {
        while (currentPage) {
            pageCount++;
            console.log(`   Fetching results page ${pageCount}...`);
            
            const response = await fetch(`${BASE_URL}${currentPage}`, { headers: HEADERS });
            
            if (!response.ok) {
                console.error(`   HTTP error! status: ${response.status}`);
                break;
            }
            
            const data = await response.json();
            
            if (data.games && Array.isArray(data.games)) {
                console.log(`   Found ${data.games.length} games in this page`);
                allGames = [...allGames, ...data.games];
            }
            
            currentPage = data.paging?.previousPage || null;
        }
        
        // Filter for current season
        const currentSeasonGames = allGames.filter(game => game.seasonNum === SEASON_NUM);
        console.log(`✅ Total historical games fetched: ${allGames.length}`);
        console.log(`✅ Current season historical games: ${currentSeasonGames.length}`);
        
        return currentSeasonGames;
        
    } catch (error) {
        console.error('❌ Error fetching historical finished matches:', error.message);
        throw error;
    }
}

// Fetch details for a specific game by ID
async function fetchGameDetails(gameId) {
    console.log(`   📥 Fetching details for game ID: ${gameId}`);
    
    const url = `${BASE_URL}/web/game/?appTypeId=5&langId=1&timezoneName=UTC&userCountryId=1&gameId=${gameId}`;
    
    const response = await fetch(url, { headers: HEADERS });
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    
    if (!data?.game) {
        throw new Error(`No game data for ID ${gameId}`);
    }
    
    return data.game;
}

// Save upcoming fixtures to database
async function saveUpcomingFixtures(fixtures, connection) {
    console.log('💾 Saving upcoming fixtures to TiDB...');
    
    let inserted = 0;
    let updated = 0;
    
    for (const game of fixtures) {
        const homeTeam = game.homeCompetitor?.name || 'Unknown';
        const awayTeam = game.awayCompetitor?.name || 'Unknown';
        const roundNum = game.roundNum || 0;
        const kickoffTime = game.startTime || new Date().toISOString();
        const status = game.status || 'scheduled';
        const fixtureId = game.id;
        
        const [existing] = await connection.execute(
            'SELECT fixture_id FROM upcoming_fixtures WHERE fixture_id = ?',
            [fixtureId]
        );
        
        if (existing.length > 0) {
            await connection.execute(
                `UPDATE upcoming_fixtures 
                 SET kickoff_time = ?, status = ?, full_data = ?, updated_at = CURRENT_TIMESTAMP
                 WHERE fixture_id = ?`,
                [new Date(kickoffTime), status, JSON.stringify(game), fixtureId]
            );
            updated++;
        } else {
            await connection.execute(
                `INSERT INTO upcoming_fixtures 
                 (fixture_id, round_num, home_team, away_team, kickoff_time, status, full_data)
                 VALUES (?, ?, ?, ?, ?, ?, ?)`,
                [fixtureId, roundNum, homeTeam, awayTeam, new Date(kickoffTime), status, JSON.stringify(game)]
            );
            inserted++;
        }
    }
    
    console.log(`   ✅ Upcoming: Inserted ${inserted}, Updated ${updated}`);
    return { inserted, updated };
}

// Save finished match to database and add to unprocessed queue
async function saveFinishedMatch(game, connection) {
    const homeTeam = game.homeCompetitor?.name || 'Unknown';
    const awayTeam = game.awayCompetitor?.name || 'Unknown';
    const homeScore = game.homeCompetitor?.score || 0;
    const awayScore = game.awayCompetitor?.score || 0;
    const roundNum = game.roundNum || 0;
    const matchDate = game.startTime || new Date().toISOString();
    const fixtureId = game.id;
    
    // Save to finished_matches
    const [existing] = await connection.execute(
        'SELECT fixture_id FROM finished_matches WHERE fixture_id = ?',
        [fixtureId]
    );
    
    if (existing.length > 0) {
        await connection.execute(
            `UPDATE finished_matches 
             SET home_score = ?, away_score = ?, match_date = ?, 
                 status = ?, full_data = ?, updated_at = CURRENT_TIMESTAMP
             WHERE fixture_id = ?`,
            [homeScore, awayScore, new Date(matchDate), 'finished', JSON.stringify(game), fixtureId]
        );
    } else {
        await connection.execute(
            `INSERT INTO finished_matches 
             (fixture_id, round_num, home_team, away_team, home_score, away_score, match_date, status, full_data)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [fixtureId, roundNum, homeTeam, awayTeam, homeScore, awayScore, new Date(matchDate), 'finished', JSON.stringify(game)]
        );
        
        // Add to unprocessed_fixtures
        await connection.execute(
            `INSERT INTO unprocessed_fixtures 
             (fixture_id, round_num, home_team, away_team, home_score, away_score, match_date, full_data)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [fixtureId, roundNum, homeTeam, awayTeam, homeScore, awayScore, new Date(matchDate), JSON.stringify(game)]
        );
        
        // Create status record
        await connection.execute(
            `INSERT INTO match_processing_status 
             (fixture_id, round_num, home_team, away_team, match_date, overall_status)
             VALUES (?, ?, ?, ?, ?, 'pending')`,
            [fixtureId, roundNum, homeTeam, awayTeam, new Date(matchDate)]
        );
    }
    
    console.log(`   ✅ Finished match ${fixtureId} saved and queued for processing`);
}

// Main function
export default async function runFetchFixtures(runId, connection) {
    console.log('\n🏁 Starting Fetch Fixtures...');
    console.log(`   Run ID: ${runId}`);
    console.log(`   Time: ${new Date().toISOString()}`);
    
    try {
        // Check if this is first run (no data in finished_matches)
        const [countResult] = await connection.execute(
            'SELECT COUNT(*) as count FROM finished_matches'
        );
        const isFirstRun = countResult[0].count === 0;
        
        if (isFirstRun) {
            console.log('📊 First run detected - fetching all historical data...');
            
            // Fetch all historical finished matches
            const historicalGames = await fetchAllHistoricalFinished();
            
            // Save each historical game
            for (const game of historicalGames) {
                await saveFinishedMatch(game, connection);
            }
            
            // Fetch and save upcoming fixtures
            const upcomingGames = await fetchAllUpcomingFixtures();
            await saveUpcomingFixtures(upcomingGames, connection);
            
            console.log(`✅ First run complete: ${historicalGames.length} historical games, ${upcomingGames.length} upcoming games`);
            
        } else {
            console.log('📊 Subsequent run - checking for newly finished games...');
            
            // Get current upcoming fixtures from database
            const [dbUpcoming] = await connection.execute(
                'SELECT fixture_id, full_data FROM upcoming_fixtures'
            );
            const dbUpcomingMap = new Map(dbUpcoming.map(u => [u.fixture_id, u.full_data]));
            
            // Fetch fresh upcoming fixtures from API
            const apiUpcoming = await fetchAllUpcomingFixtures();
            
            // Find which fixtures are missing from API (these might be finished)
            const missingFixtureIds = [];
            for (const [fixtureId, fullData] of dbUpcomingMap) {
                if (!apiUpcoming.some(api => api.id === fixtureId)) {
                    missingFixtureIds.push({ id: fixtureId, fullData });
                }
            }
            
            console.log(`   Found ${missingFixtureIds.length} potentially finished games`);
            
            // Check each missing fixture
            for (const missing of missingFixtureIds) {
                try {
                    const gameDetails = await fetchGameDetails(missing.id);
                    
                    // StatusGroup: 2 = Scheduled, 3 = Live, 4 = Finished
                    if (gameDetails.statusGroup === 4) {
                        console.log(`   ✅ Game ${missing.id} is finished - adding to queue`);
                        await saveFinishedMatch(gameDetails, connection);
                    } else if (gameDetails.statusGroup === 3) {
                        console.log(`   ⚽ Game ${missing.id} is live - keeping in upcoming`);
                        // Keep in upcoming (don't remove)
                        await saveUpcomingFixtures([gameDetails], connection);
                    } else {
                        console.log(`   ℹ️ Game ${missing.id} has status ${gameDetails.statusGroup} - updating`);
                        await saveUpcomingFixtures([gameDetails], connection);
                    }
                } catch (error) {
                    console.error(`   ❌ Error checking game ${missing.id}:`, error.message);
                }
            }
            
            // Update upcoming fixtures with fresh data
            await saveUpcomingFixtures(apiUpcoming, connection);
            
            console.log(`✅ Subsequent run complete: ${missingFixtureIds.length} checked, updates applied`);
        }
        
        return { success: true };
        
    } catch (error) {
        console.error('❌ Fetch fixtures failed:', error.message);
        throw error;
    }
}