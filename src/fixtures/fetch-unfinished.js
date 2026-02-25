// src/fixtures/fetch-unfinished.js
// Fetches upcoming fixtures (unfinished matches) from 365scores API
// Stores them in the upcoming_fixtures table in TiDB

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
 * Fetch upcoming fixtures from 365scores
 * Uses the /fixtures endpoint which returns scheduled/unfinished games
 */
async function fetchUnfinishedFixtures() {
    console.log('📥 Fetching upcoming fixtures from 365scores...');
    
    const fixturesUrl = '/web/games/fixtures/?appTypeId=5&langId=1&timezoneName=UTC&userCountryId=1&competitions=649';
    let allUnfinishedFixtures = [];
    let nextPage = fixturesUrl;
    let pageCount = 0;
    
    try {
        // Paginate through all fixtures
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
                
                allUnfinishedFixtures = [...allUnfinishedFixtures, ...seasonGames];
                console.log(`   Found ${seasonGames.length} upcoming fixtures in this page`);
            }
            
            // Check for next page
            nextPage = data.paging?.nextPage || null;
        }
        
        console.log(`✅ Total upcoming fixtures fetched: ${allUnfinishedFixtures.length}`);
        return allUnfinishedFixtures;
        
    } catch (error) {
        console.error('❌ Error fetching upcoming fixtures:', error.message);
        throw error;
    }
}

/**
 * Transform 365scores game data to our database schema
 */
function transformUnfinishedFixture(game) {
    // Extract home and away teams
    const homeTeam = game.homeCompetitor?.name || 'Unknown';
    const awayTeam = game.awayCompetitor?.name || 'Unknown';
    
    // Extract round number
    const roundNum = game.roundNum || 0;
    
    // Extract kickoff time
    const kickoffTime = game.startTime ? new Date(game.startTime) : new Date();
    
    // Determine status (scheduled, postponed, etc.)
    const status = game.status || 'scheduled';
    
    // Create fixture ID (combine round + teams to create a unique ID)
    const fixtureId = game.id || `${roundNum}_${homeTeam}_${awayTeam}`.replace(/\s+/g, '_');
    
    return {
        fixture_id: fixtureId,
        round_num: roundNum,
        home_team: homeTeam,
        away_team: awayTeam,
        kickoff_time: kickoffTime,
        status: status,
        full_data: game // Store complete original data as JSON
    };
}

/**
 * Save upcoming fixtures to TiDB
 */
async function saveUnfinishedFixtures(fixtures) {
    console.log('💾 Saving upcoming fixtures to TiDB...');
    
    let inserted = 0;
    let updated = 0;
    let errors = 0;
    
    for (const game of fixtures) {
        try {
            const fixture = transformUnfinishedFixture(game);
            
            // Check if fixture already exists
            const existing = await dbClient.query(
                'SELECT fixture_id FROM upcoming_fixtures WHERE fixture_id = ?',
                [fixture.fixture_id]
            );
            
            if (existing.length > 0) {
                // Update existing fixture
                await dbClient.query(
                    `UPDATE upcoming_fixtures 
                     SET kickoff_time = ?, status = ?, full_data = ?, updated_at = CURRENT_TIMESTAMP
                     WHERE fixture_id = ?`,
                    [
                        fixture.kickoff_time, fixture.status, 
                        JSON.stringify(fixture.full_data), fixture.fixture_id
                    ]
                );
                updated++;
            } else {
                // Insert new fixture
                await dbClient.query(
                    `INSERT INTO upcoming_fixtures 
                     (fixture_id, round_num, home_team, away_team, kickoff_time, status, full_data)
                     VALUES (?, ?, ?, ?, ?, ?, ?)`,
                    [
                        fixture.fixture_id, fixture.round_num, fixture.home_team, fixture.away_team,
                        fixture.kickoff_time, fixture.status, JSON.stringify(fixture.full_data)
                    ]
                );
                inserted++;
            }
            
        } catch (error) {
            console.error(`   Error saving fixture:`, error.message);
            errors++;
        }
    }
    
    console.log(`   ✅ Inserted: ${inserted}, Updated: ${updated}, Errors: ${errors}`);
    return { inserted, updated, errors };
}

/**
 * Main function to run the unfinished fixtures fetch
 */
export default async function runUnfinishedFixturesFetch(runId) {
    console.log('\n🏁 Starting Unfinished Fixtures Fetch...');
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
            [runId, 'unfinished-fixtures', 'running']
        );
        syncLogId = syncLogResult.insertId;
        
        // Fetch upcoming fixtures
        const upcomingFixtures = await fetchUnfinishedFixtures();
        
        if (upcomingFixtures.length === 0) {
            console.log('⚠️ No upcoming fixtures found');
            
            // Update sync log
            await dbClient.query(
                `UPDATE sync_log SET status = ?, completed_at = NOW() WHERE id = ?`,
                ['success', syncLogId]
            );
            
            return {
                success: true,
                count: 0,
                message: 'No upcoming fixtures found'
            };
        }
        
        // Save to database
        const stats = await saveUnfinishedFixtures(upcomingFixtures);
        
        // Update sync log with stats
        await dbClient.query(
            `UPDATE sync_log 
             SET status = ?, completed_at = NOW(), unfinished_fetched = ?
             WHERE id = ?`,
            ['success', upcomingFixtures.length, syncLogId]
        );
        
        console.log(`✅ Unfinished fixtures fetch completed successfully`);
        
        return {
            success: true,
            count: upcomingFixtures.length,
            inserted: stats.inserted,
            updated: stats.updated,
            errors: stats.errors,
            runId
        };
        
    } catch (error) {
        console.error('❌ Unfinished fixtures fetch failed:', error.message);
        
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
    const runId = `unfinished-${Date.now()}`;
    runUnfinishedFixturesFetch(runId)
        .then(result => {
            console.log('\n📊 Summary:', result);
            process.exit(0);
        })
        .catch(error => {
            console.error('\n💥 Fatal error:', error);
            process.exit(1);
        });
}