// src/fixtures/fetch-unfinished.js
// Fetches upcoming fixtures from 365scores API
// Stores them in upcoming_fixtures table only

import dbClient from '../database/tidb-client.js';

const BASE_URL = 'https://webws.365scores.com';
const COMPETITION_ID = 649;
const SEASON_NUM = 53;
const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
};

async function fetchUnfinishedFixtures() {
    console.log('📥 Fetching upcoming fixtures from 365scores...');
    
    const fixturesUrl = '/web/games/fixtures/?appTypeId=5&langId=1&timezoneName=UTC&userCountryId=1&competitions=649';
    let allUnfinishedFixtures = [];
    let currentPage = fixturesUrl;
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
                allUnfinishedFixtures = [...allUnfinishedFixtures, ...data.games];
                console.log(`   Found ${data.games.length} fixtures in this page`);
                
                // Log first game structure to understand it
                if (pageCount === 1 && data.games.length > 0) {
                    console.log('   Sample fixture structure:', JSON.stringify(data.games[0], null, 2).substring(0, 500) + '...');
                }
            }
            
            // For fixtures, we use nextPage
            currentPage = data.paging?.nextPage || null;
            console.log(`   Next page:`, currentPage || 'none');
        }
        
        console.log(`✅ Total upcoming fixtures fetched: ${allUnfinishedFixtures.length}`);
        return allUnfinishedFixtures;
        
    } catch (error) {
        console.error('❌ Error fetching upcoming fixtures:', error.message);
        throw error;
    }
}

function transformUnfinishedFixture(game) {
    // Check different possible property names
    const homeTeam = game.homeCompetitor?.name || game.home_team || 'Unknown';
    const awayTeam = game.awayCompetitor?.name || game.away_team || 'Unknown';
    const roundNum = game.roundNum || game.round_num || 0;
    const kickoffTime = game.startTime || game.kickoff_time || game.date || new Date().toISOString();
    const status = game.status || 'scheduled';
    const fixtureId = game.id || `${roundNum}_${homeTeam}_${awayTeam}`.replace(/\s+/g, '_');
    
    return {
        fixture_id: fixtureId,
        round_num: roundNum,
        home_team: homeTeam,
        away_team: awayTeam,
        kickoff_time: new Date(kickoffTime),
        status: status,
        full_data: game
    };
}

async function saveUnfinishedFixtures(fixtures) {
    console.log('💾 Saving upcoming fixtures to TiDB...');
    
    let inserted = 0;
    let updated = 0;
    let errors = 0;
    
    for (const game of fixtures) {
        try {
            const fixture = transformUnfinishedFixture(game);
            
            const existing = await dbClient.query(
                'SELECT fixture_id FROM upcoming_fixtures WHERE fixture_id = ?',
                [fixture.fixture_id]
            );
            
            if (existing.length > 0) {
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

export default async function runUnfinishedFixturesFetch(runId) {
    console.log('\n🏁 Starting Unfinished Fixtures Fetch...');
    console.log(`   Run ID: ${runId}`);
    console.log(`   Time: ${new Date().toISOString()}`);
    
    let syncLogId = null;
    
    try {
        await dbClient.initialize();
        
        const syncLogResult = await dbClient.query(
            `INSERT INTO sync_log (run_id, source, status) VALUES (?, ?, ?)`,
            [runId, 'unfinished-fixtures', 'running']
        );
        syncLogId = syncLogResult.insertId;
        
        const upcomingFixtures = await fetchUnfinishedFixtures();
        
        if (upcomingFixtures.length === 0) {
            console.log('⚠️ No upcoming fixtures found');
            await dbClient.query(
                `UPDATE sync_log SET status = ?, completed_at = NOW() WHERE id = ?`,
                ['success', syncLogId]
            );
            return { success: true, count: 0 };
        }
        
        const stats = await saveUnfinishedFixtures(upcomingFixtures);
        
        await dbClient.query(
            `UPDATE sync_log 
             SET status = ?, completed_at = NOW(), unfinished_fetched = ?
             WHERE id = ?`,
            ['success', upcomingFixtures.length, syncLogId]
        );
        
        return {
            success: true,
            count: upcomingFixtures.length,
            ...stats
        };
        
    } catch (error) {
        console.error('❌ Unfinished fixtures fetch failed:', error.message);
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