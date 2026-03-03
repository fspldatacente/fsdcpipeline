// src/fixtures/update-penalties-from-365scores.js
// Updates penalties_scored in score365_players table by fetching data from 365scores API
// Processes all finished fixtures to extract penalty information

import dbClient from '../database/tidb-client.js';
import createLogger from '../utils/logger.js';

const BASE_URL = 'https://webws.365scores.com';
const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
};

const DELAY_BETWEEN_FIXTURES = 1000; // 1 second delay to respect rate limits
const BATCH_SIZE = 20; // Log progress every 20 fixtures

// Helper function to get penalties scored from goals stat (copied from process-stats.js)
function getPenaltiesScored(player) {
    if (!player || !player.stats) return 0;
    
    const goalsStat = player.stats.find(s => s && s.type === 27);
    if (goalsStat && goalsStat.value && String(goalsStat.value).includes('Pk')) {
        const match = String(goalsStat.value).match(/(\d+)Pk/);
        return match ? parseInt(match[1], 10) : 0;
    }
    return 0;
}

// Fetch detailed game data from 365scores (copied from process-stats.js)
async function fetchGameDetails(gameId, logger) {
    logger.debug(`   📥 Fetching details for game ID: ${gameId}`);
    
    const url = `${BASE_URL}/web/game/?appTypeId=5&langId=1&timezoneName=UTC&userCountryId=1&gameId=${gameId}`;
    
    const response = await fetch(url, { headers: HEADERS });
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    
    if (!data?.game) {
        logger.error(`   ❌ No game data in response for game ${gameId}`);
        throw new Error('No game data in response');
    }
    
    return data.game;
}

// Process a single fixture to extract penalties
async function processFixture(fixture, connection, logger) {
    const fixtureId = fixture.fixture_id;
    const roundNum = fixture.round_num;
    
    logger.info(`\n   🎮 Processing fixture ${fixtureId}: ${fixture.home_team} vs ${fixture.away_team} (Round ${roundNum})`);
    
    try {
        // Fetch game details from 365scores
        const gameData = await fetchGameDetails(fixtureId, logger);
        
        const { homeCompetitor, awayCompetitor, members, chartEvents } = gameData;
        
        if (!homeCompetitor || !awayCompetitor) {
            throw new Error('Missing home or away competitor data');
        }
        
        // Track penalties from chart events (for missed penalties)
        const gamePenaltyMissedMap = new Map();
        
        // Process penalty events to track missed penalties
        if (chartEvents?.events && Array.isArray(chartEvents.events)) {
            chartEvents.events.filter(e => e && e.subType === 9).forEach(event => {
                const playerId = String(event.playerId || '');
                if (event.outcome?.name !== 'Goal') {
                    gamePenaltyMissedMap.set(playerId, (gamePenaltyMissedMap.get(playerId) || 0) + 1);
                }
            });
        }
        
        // Create member name map
        const memberNameMap = new Map();
        if (members && Array.isArray(members)) {
            members.forEach(m => {
                if (m && m.id) {
                    memberNameMap.set(m.id, m.name || 'Unknown Player');
                }
            });
        }
        
        // Process players from both teams
        const processPlayerList = async (playerList, teamName) => {
            if (!playerList || !Array.isArray(playerList)) {
                logger.debug(`   ⚠️ No player list for ${teamName}`);
                return [];
            }
            
            const updatedPlayers = [];
            
            for (const player of playerList) {
                if (!player) continue;
                
                // Skip if minutes played = 0
                const minutesStat = player.stats?.find(s => s && s.type === 30);
                if (minutesStat && parseInt(minutesStat.value || 0, 10) === 0) continue;
                
                const playerName = memberNameMap.get(player.id) || player.name || 'Unknown Player';
                const playerId = player.id;
                const penaltiesScored = getPenaltiesScored(player);
                const penaltiesMissed = gamePenaltyMissedMap.get(String(playerId)) || 0;
                
                if (penaltiesScored > 0 || penaltiesMissed > 0) {
                    logger.debug(`      🎯 Player ${playerName}: scored ${penaltiesScored}, missed ${penaltiesMissed}`);
                }
                
                // Update the player's penalties in score365_players
                // We need to identify the correct row - match by player_name, team_name, round_num
                const [result] = await connection.execute(
                    `UPDATE stats.score365_players 
                     SET penalties_scored = ?, penalties_missed = ?
                     WHERE player_name = ? AND team_name = ? AND round_num = ?`,
                    [penaltiesScored, penaltiesMissed, playerName, teamName, roundNum]
                );
                
                if (result.affectedRows > 0) {
                    updatedPlayers.push({ playerName, penaltiesScored, penaltiesMissed });
                } else {
                    logger.warn(`      ⚠️ No matching row found for ${playerName} in ${teamName} round ${roundNum}`);
                }
            }
            
            return updatedPlayers;
        };
        
        // Process home team
        const homeUpdated = await processPlayerList(
            homeCompetitor?.lineups?.members || [],
            homeCompetitor.name || 'Unknown'
        );
        
        // Process away team
        const awayUpdated = await processPlayerList(
            awayCompetitor?.lineups?.members || [],
            awayCompetitor.name || 'Unknown'
        );
        
        const totalUpdated = homeUpdated.length + awayUpdated.length;
        logger.success(`   ✅ Fixture ${fixtureId} processed: updated ${totalUpdated} players with penalty data`);
        
        return { fixtureId, updatedCount: totalUpdated };
        
    } catch (error) {
        logger.error(`   ❌ Failed to process fixture ${fixtureId}:`, error.message);
        throw error;
    }
}

export default async function runUpdatePenaltiesFrom365scores(runId) {
    const logger = createLogger('update-penalties-365');
    
    console.log('\n' + '='.repeat(60));
    console.log('🏁 STARTING UPDATE PENALTIES FROM 365SCORES');
    console.log('='.repeat(60));
    console.log(`   Run ID: ${runId}`);
    console.log(`   Time: ${new Date().toISOString()}`);
    console.log('-'.repeat(60));
    
    let connection;
    try {
        await dbClient.initialize();
        connection = await dbClient.getConnection();
        
        // Create sync log entry
        await connection.execute(
            `INSERT INTO fantasy_stats.fantasy_sync_log (run_id, function_name, status)
             VALUES (?, 'update-penalties-365', 'running')`,
            [runId]
        );
        
        // =========================================================
        // STEP 1: Get all finished fixtures
        // =========================================================
        console.log('\n🔍 Step 1: Fetching finished fixtures...');
        const [fixtures] = await connection.execute(`
            SELECT fixture_id, round_num, home_team, away_team 
            FROM fixtures.finished_matches 
            ORDER BY round_num ASC, match_date ASC
        `);
        console.log(`   ✅ Found ${fixtures.length} finished fixtures`);
        
        if (fixtures.length === 0) {
            console.log('\n📭 No fixtures to process - exiting');
            return { success: true, processed: 0 };
        }
        
        // =========================================================
        // STEP 2: Process each fixture
        // =========================================================
        console.log('\n⚙️ Step 2: Processing fixtures...');
        
        let processed = 0;
        let errors = 0;
        let totalPlayersUpdated = 0;
        const failedFixtures = [];
        
        for (let i = 0; i < fixtures.length; i++) {
            const fixture = fixtures[i];
            
            console.log(`\n📋 [${i + 1}/${fixtures.length}] Processing fixture ${fixture.fixture_id}...`);
            
            try {
                const result = await processFixture(fixture, connection, logger);
                processed++;
                totalPlayersUpdated += result.updatedCount;
                
            } catch (error) {
                logger.error(`   ❌ Failed to process fixture ${fixture.fixture_id}`);
                errors++;
                failedFixtures.push({
                    id: fixture.fixture_id,
                    round: fixture.round_num,
                    error: error.message
                });
            }
            
            // Log progress every BATCH_SIZE fixtures
            if ((i + 1) % BATCH_SIZE === 0) {
                console.log(`\n📊 Progress: ${i + 1}/${fixtures.length} fixtures processed`);
                console.log(`   Players updated so far: ${totalPlayersUpdated}`);
            }
            
            // Delay between fixtures to respect rate limits
            await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_FIXTURES));
        }
        
        // =========================================================
        // STEP 3: Summary
        // =========================================================
        console.log('\n' + '='.repeat(60));
        console.log('📊 PROCESSING COMPLETE');
        console.log('='.repeat(60));
        console.log(`   Total fixtures: ${fixtures.length}`);
        console.log(`   Successfully processed: ${processed}`);
        console.log(`   Failed: ${errors}`);
        console.log(`   Total players updated with penalty data: ${totalPlayersUpdated}`);
        
        if (failedFixtures.length > 0) {
            console.log('\n❌ Failed fixtures:');
            failedFixtures.slice(0, 10).forEach(f => {
                console.log(`   - Fixture ${f.id} (Round ${f.round}): ${f.error}`);
            });
            if (failedFixtures.length > 10) {
                console.log(`   ... and ${failedFixtures.length - 10} more`);
            }
        }
        
        // Update sync log
        await connection.execute(
            `UPDATE fantasy_stats.fantasy_sync_log 
             SET status = ?, players_processed = ?, errors = ?, completed_at = NOW()
             WHERE run_id = ?`,
            [errors === 0 ? 'success' : 'partial', totalPlayersUpdated, errors, runId]
        );
        
        console.log('\n✅ UPDATE PENALTIES FROM 365SCORES COMPLETED');
        console.log('='.repeat(60));
        
        return {
            success: errors === 0,
            processed,
            playersUpdated: totalPlayersUpdated,
            errors,
            failedFixtures
        };
        
    } catch (error) {
        logger.error('Fatal error:', error);
        
        if (connection) {
            await connection.execute(
                `UPDATE fantasy_stats.fantasy_sync_log 
                 SET status = 'failed', error_message = ?, completed_at = NOW()
                 WHERE run_id = ?`,
                [error.message.substring(0, 500), runId]
            );
        }
        
        throw error;
    } finally {
        if (connection) await connection.release();
        await dbClient.close();
    }
}

// If running directly
if (import.meta.url === `file://${process.argv[1]}`) {
    const runId = `update-penalties-365-${Date.now()}`;
    
    runUpdatePenaltiesFrom365scores(runId).catch(error => {
        console.error('❌ Fatal error:', error);
        process.exit(1);
    });
}