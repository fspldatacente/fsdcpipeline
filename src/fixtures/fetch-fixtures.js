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
            console.log(`   📄 Fetching upcoming page ${pageCount}...`);
            console.log(`   🔗 URL: ${BASE_URL}${currentPage}`);
            
            const response = await fetch(`${BASE_URL}${currentPage}`, { headers: HEADERS });
            
            if (!response.ok) {
                console.error(`   ❌ HTTP error! status: ${response.status}`);
                break;
            }
            
            const data = await response.json();
            console.log(`   📊 Response received. Has games: ${data.games ? 'yes' : 'no'}`);
            
            if (data.games && Array.isArray(data.games)) {
                console.log(`   ✅ Found ${data.games.length} games in this page`);
                allGames = [...allGames, ...data.games];
            } else {
                console.log(`   ⚠️ No games array in response`);
            }
            
            currentPage = data.paging?.nextPage || null;
            console.log(`   ⏩ Next page: ${currentPage || 'none'}`);
        }
        
        console.log(`   🔍 Filtering for season ${SEASON_NUM}...`);
        const currentSeasonGames = allGames.filter(game => {
            const match = game.seasonNum === SEASON_NUM;
            if (!match) {
                console.log(`   🗑️ Filtering out game ${game.id} - season ${game.seasonNum} (not current)`);
            }
            return match;
        });
        
        console.log(`   ✅ Total upcoming games fetched: ${allGames.length}`);
        console.log(`   ✅ Current season upcoming games: ${currentSeasonGames.length}`);
        
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
            console.log(`   📄 Fetching results page ${pageCount}...`);
            console.log(`   🔗 URL: ${BASE_URL}${currentPage}`);
            
            const response = await fetch(`${BASE_URL}${currentPage}`, { headers: HEADERS });
            
            if (!response.ok) {
                console.error(`   ❌ HTTP error! status: ${response.status}`);
                break;
            }
            
            const data = await response.json();
            console.log(`   📊 Response received. Has games: ${data.games ? 'yes' : 'no'}`);
            
            if (data.games && Array.isArray(data.games)) {
                console.log(`   ✅ Found ${data.games.length} games in this page`);
                allGames = [...allGames, ...data.games];
            } else {
                console.log(`   ⚠️ No games array in response`);
            }
            
            currentPage = data.paging?.previousPage || null;
            console.log(`   ⏪ Previous page: ${currentPage || 'none'}`);
        }
        
        console.log(`   🔍 Filtering for season ${SEASON_NUM}...`);
        const currentSeasonGames = allGames.filter(game => {
            const match = game.seasonNum === SEASON_NUM;
            if (!match) {
                console.log(`   🗑️ Filtering out game ${game.id} - season ${game.seasonNum} (not current)`);
            }
            return match;
        });
        
        console.log(`   ✅ Total historical games fetched: ${allGames.length}`);
        console.log(`   ✅ Current season historical games: ${currentSeasonGames.length}`);
        
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
    console.log(`   🔗 Details URL: ${url}`);
    
    const response = await fetch(url, { headers: HEADERS });
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    console.log(`   📊 Game details response received. Has game: ${data.game ? 'yes' : 'no'}`);
    
    if (!data?.game) {
        throw new Error(`No game data for ID ${gameId}`);
    }
    
    console.log(`   ✅ Game details fetched. Status: ${data.game.statusText}, StatusGroup: ${data.game.statusGroup}`);
    
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
        
        console.log(`   🔄 Processing upcoming fixture ${fixtureId}: ${homeTeam} vs ${awayTeam} (Round ${roundNum})`);
        
        const [existing] = await connection.execute(
            'SELECT fixture_id FROM upcoming_fixtures WHERE fixture_id = ?',
            [fixtureId]
        );
        
        if (existing.length > 0) {
            console.log(`   🔄 Updating existing fixture ${fixtureId}`);
            await connection.execute(
                `UPDATE upcoming_fixtures 
                 SET kickoff_time = ?, status = ?, full_data = ?, updated_at = CURRENT_TIMESTAMP
                 WHERE fixture_id = ?`,
                [new Date(kickoffTime), status, JSON.stringify(game), fixtureId]
            );
            updated++;
        } else {
            console.log(`   ➕ Inserting new fixture ${fixtureId}`);
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
    
    console.log(`   🏁 Processing finished match ${fixtureId}: ${homeTeam} ${homeScore}-${awayScore} ${awayTeam} (Round ${roundNum})`);
    
    // Save to finished_matches
    const [existing] = await connection.execute(
        'SELECT fixture_id FROM finished_matches WHERE fixture_id = ?',
        [fixtureId]
    );
    
    if (existing.length > 0) {
        console.log(`   🔄 Updating existing finished match ${fixtureId}`);
        await connection.execute(
            `UPDATE finished_matches 
             SET home_score = ?, away_score = ?, match_date = ?, 
                 status = ?, full_data = ?, updated_at = CURRENT_TIMESTAMP
             WHERE fixture_id = ?`,
            [homeScore, awayScore, new Date(matchDate), 'finished', JSON.stringify(game), fixtureId]
        );
    } else {
        console.log(`   ➕ Inserting new finished match ${fixtureId}`);
        await connection.execute(
            `INSERT INTO finished_matches 
             (fixture_id, round_num, home_team, away_team, home_score, away_score, match_date, status, full_data)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [fixtureId, roundNum, homeTeam, awayTeam, homeScore, awayScore, new Date(matchDate), 'finished', JSON.stringify(game)]
        );
        
        console.log(`   📋 Adding to unprocessed_fixtures queue`);
        await connection.execute(
            `INSERT INTO unprocessed_fixtures 
             (fixture_id, round_num, home_team, away_team, home_score, away_score, match_date, full_data)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [fixtureId, roundNum, homeTeam, awayTeam, homeScore, awayScore, new Date(matchDate), JSON.stringify(game)]
        );
        
        console.log(`   📊 Creating status record`);
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
    console.log('\n' + '='.repeat(60));
    console.log('🏁 STARTING FETCH FIXTURES');
    console.log('='.repeat(60));
    console.log(`   Run ID: ${runId}`);
    console.log(`   Time: ${new Date().toISOString()}`);
    console.log(`   Season: ${SEASON_NUM}`);
    console.log('-'.repeat(60));
    
    try {
        // Check if this is first run (no data in finished_matches)
        console.log('\n🔍 Checking database state...');
        const [countResult] = await connection.execute(
            'SELECT COUNT(*) as count FROM finished_matches'
        );
        const isFirstRun = countResult[0].count === 0;
        console.log(`   📊 finished_matches count: ${countResult[0].count}`);
        console.log(`   📋 First run: ${isFirstRun ? 'YES' : 'NO'}`);
        
        if (isFirstRun) {
            console.log('\n' + '-'.repeat(60));
            console.log('📊 FIRST RUN DETECTED - Fetching all historical data...');
            console.log('-'.repeat(60));
            
            // Fetch all historical finished matches
            console.log('\n📥 Step 1: Fetching historical finished matches...');
            const historicalGames = await fetchAllHistoricalFinished();
            console.log(`\n📊 Found ${historicalGames.length} historical games to process`);
            
            // Save each historical game
            console.log('\n💾 Step 2: Saving historical games to database...');
            let savedCount = 0;
            for (const game of historicalGames) {
                await saveFinishedMatch(game, connection);
                savedCount++;
                if (savedCount % 20 === 0) {
                    console.log(`   Progress: ${savedCount}/${historicalGames.length} games saved`);
                }
            }
            
            // Fetch and save upcoming fixtures
            console.log('\n📥 Step 3: Fetching upcoming fixtures...');
            const upcomingGames = await fetchAllUpcomingFixtures();
            console.log(`\n📊 Found ${upcomingGames.length} upcoming games`);
            
            console.log('\n💾 Step 4: Saving upcoming fixtures...');
            await saveUpcomingFixtures(upcomingGames, connection);
            
            console.log('\n' + '-'.repeat(60));
            console.log('✅ FIRST RUN COMPLETE:');
            console.log(`   Historical games: ${historicalGames.length}`);
            console.log(`   Upcoming games: ${upcomingGames.length}`);
            console.log('-'.repeat(60));
            
        } else {
            console.log('\n' + '-'.repeat(60));
            console.log('📊 SUBSEQUENT RUN - Checking for newly finished games...');
            console.log('-'.repeat(60));
            
            // Get current upcoming fixtures from database
            console.log('\n🔍 Step 1: Fetching current upcoming fixtures from database...');
            const [dbUpcoming] = await connection.execute(
                'SELECT fixture_id, full_data FROM upcoming_fixtures'
            );
            console.log(`   📊 Found ${dbUpcoming.length} upcoming games in database`);
            
            const dbUpcomingMap = new Map(dbUpcoming.map(u => [u.fixture_id, u.full_data]));
            
            // Fetch fresh upcoming fixtures from API
            console.log('\n📥 Step 2: Fetching fresh upcoming fixtures from API...');
            const apiUpcoming = await fetchAllUpcomingFixtures();
            console.log(`   📊 Found ${apiUpcoming.length} upcoming games from API`);
            
            // Find which fixtures are missing from API (these might be finished)
            console.log('\n🔍 Step 3: Comparing database vs API...');
            const missingFixtureIds = [];
            for (const [fixtureId, fullData] of dbUpcomingMap) {
                if (!apiUpcoming.some(api => api.id === fixtureId)) {
                    console.log(`   🔍 Game ${fixtureId} is missing from API - might be finished`);
                    missingFixtureIds.push({ id: fixtureId, fullData });
                }
            }
            
            console.log(`   📊 Found ${missingFixtureIds.length} potentially finished games`);
            
            // Check each missing fixture
            console.log('\n🔍 Step 4: Checking each missing game status...');
            let finishedCount = 0;
            let liveCount = 0;
            let otherCount = 0;
            
            for (const missing of missingFixtureIds) {
                console.log(`\n   🔍 Checking game ${missing.id}...`);
                try {
                    const gameDetails = await fetchGameDetails(missing.id);
                    
                    // StatusGroup: 2 = Scheduled, 3 = Live, 4 = Finished
                    console.log(`      StatusGroup: ${gameDetails.statusGroup}, StatusText: ${gameDetails.statusText}`);
                    
                    if (gameDetails.statusGroup === 4) {
                        console.log(`      ✅ Game is FINISHED - adding to queue`);
                        await saveFinishedMatch(gameDetails, connection);
                        finishedCount++;
                    } else if (gameDetails.statusGroup === 3) {
                        console.log(`      ⚽ Game is LIVE - keeping in upcoming`);
                        await saveUpcomingFixtures([gameDetails], connection);
                        liveCount++;
                    } else {
                        console.log(`      ℹ️ Game has status ${gameDetails.statusGroup} - updating`);
                        await saveUpcomingFixtures([gameDetails], connection);
                        otherCount++;
                    }
                } catch (error) {
                    console.error(`      ❌ Error checking game ${missing.id}:`, error.message);
                }
            }
            
            // Update upcoming fixtures with fresh data
            console.log('\n💾 Step 5: Updating upcoming fixtures with fresh API data...');
            await saveUpcomingFixtures(apiUpcoming, connection);
            
            console.log('\n' + '-'.repeat(60));
            console.log('✅ SUBSEQUENT RUN COMPLETE:');
            console.log(`   Missing games checked: ${missingFixtureIds.length}`);
            console.log(`   Finished games added: ${finishedCount}`);
            console.log(`   Live games updated: ${liveCount}`);
            console.log(`   Other updates: ${otherCount}`);
            console.log('-'.repeat(60));
        }
        
        console.log('\n' + '='.repeat(60));
        console.log('✅ FETCH FIXTURES COMPLETED SUCCESSFULLY');
        console.log('='.repeat(60));
        
        return { success: true };
        
    } catch (error) {
        console.error('\n❌ FETCH FIXTURES FAILED:');
        console.error('   Error:', error.message);
        console.error('   Stack:', error.stack);
        throw error;
    }
}

// If running directly
if (import.meta.url === `file://${process.argv[1]}`) {
    const runId = `fetch-${Date.now()}`;
    
    // Initialize connection and run
    const run = async () => {
        let connection = null;
        try {
            await dbClient.initialize();
            connection = await dbClient.getConnection();
            console.log('✅ Database connection established');
            
            await runFetchFixtures(runId, connection);
            
        } catch (error) {
            console.error('❌ Fatal error:', error);
            process.exit(1);
        } finally {
            if (connection) {
                await connection.release();
                console.log('✅ Database connection released');
            }
            await dbClient.close();
        }
    };
    
    run();
}