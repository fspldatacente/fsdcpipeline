// src/fixtures/process-stats.js
// Processes unprocessed fixtures to extract player, goalkeeper, and team stats
// Saves stats to score365_* tables in stats database
// Updates match_processing_status at each stage
// Moves processed fixtures to processed_fixtures table

import dbClient from '../database/tidb-client.js';

const BASE_URL = 'https://webws.365scores.com';
const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
};

// Helper function to get stat value from player stats array
function getStatValue(player, statType, defaultValue = 0) {
    const stat = player.stats?.find(s => s.type === statType);
    if (stat && typeof stat.value !== 'undefined') {
        // Handle special case for penalties saved (format: "1/2")
        if (statType === 44) {
            const parts = stat.value.split('/');
            return {
                saved: parseInt(parts[0], 10) || 0,
                faced: parseInt(parts[1], 10) || 0
            };
        }
        return parseFloat(stat.value) || 0;
    }
    return defaultValue;
}

// Helper function to get penalties scored from goals stat
function getPenaltiesScored(player) {
    const goalsStat = player.stats?.find(s => s.type === 27);
    if (goalsStat?.value?.includes('Pk')) {
        const match = goalsStat.value.match(/(\d+)Pk/);
        return match ? parseInt(match[1], 10) : 0;
    }
    return 0;
}

// Fetch detailed game data from 365scores
async function fetchGameDetails(gameId) {
    console.log(`   📥 Fetching details for game ID: ${gameId}`);
    
    const url = `${BASE_URL}/web/game/?appTypeId=5&langId=1&timezoneName=UTC&userCountryId=1&gameId=${gameId}`;
    
    const response = await fetch(url, { headers: HEADERS });
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    if (!data?.game) {
        throw new Error('No game data in response');
    }
    
    return data.game;
}

// Process a single game and save stats
async function processGame(game, connection) {
    const fixtureId = game.id;
    const roundNum = game.roundNum;
    const homeTeam = game.homeCompetitor.name;
    const awayTeam = game.awayCompetitor.name;
    
    console.log(`\n   🎮 Processing: ${homeTeam} vs ${awayTeam} (Round ${roundNum})`);
    
    // Update status to processing
    await connection.execute(
        `UPDATE match_processing_status 
         SET overall_status = 'processing', 
             updated_at = CURRENT_TIMESTAMP 
         WHERE fixture_id = ?`,
        [fixtureId]
    );
    
    try {
        // STAGE 1: Fetch game details
        console.log(`   ⏳ Stage 1: Fetching game details...`);
        await connection.execute(
            `UPDATE match_processing_status 
             SET fetch_status = 'processing', fetch_attempts = fetch_attempts + 1
             WHERE fixture_id = ?`,
            [fixtureId]
        );
        
        let gameData;
        try {
            gameData = await fetchGameDetails(fixtureId);
            
            await connection.execute(
                `UPDATE match_processing_status 
                 SET fetch_status = 'success', fetch_completed_at = NOW()
                 WHERE fixture_id = ?`,
                [fixtureId]
            );
            console.log(`   ✅ Stage 1: Game details fetched successfully`);
            
        } catch (fetchError) {
            await connection.execute(
                `UPDATE match_processing_status 
                 SET fetch_status = 'failed', fetch_error = ?,
                     overall_status = 'failed'
                 WHERE fixture_id = ?`,
                [fetchError.message.substring(0, 500), fixtureId]
            );
            console.error(`   ❌ Stage 1 failed: ${fetchError.message}`);
            throw fetchError; // Re-throw to stop processing this game
        }
        
        // STAGE 2: Process stats
        console.log(`   ⏳ Stage 2: Processing player and team stats...`);
        await connection.execute(
            `UPDATE match_processing_status 
             SET process_status = 'processing'
             WHERE fixture_id = ?`,
            [fixtureId]
        );
        
        try {
            // Extract data from gameData
            const { homeCompetitor, awayCompetitor } = gameData;
            
            // Maps for team stats
            const matchTeamStats = {
                home: { xg: 0, npxg: 0, penalties_scored: 0, penalties_missed: 0 },
                away: { xg: 0, npxg: 0, penalties_scored: 0, penalties_missed: 0 }
            };
            
            // Track penalties for xG calculation
            const gamePenaltyXgMap = new Map();
            const gamePenaltyMissedMap = new Map();
            
            // Process penalty events
            gameData.chartEvents?.events?.filter(e => e.subType === 9).forEach(event => {
                const playerId = String(event.playerId);
                gamePenaltyXgMap.set(playerId, (gamePenaltyXgMap.get(playerId) || 0) + (parseFloat(event.xg) || 0));
                if (event.outcome?.name !== 'Goal') {
                    gamePenaltyMissedMap.set(playerId, (gamePenaltyMissedMap.get(playerId) || 0) + 1);
                }
            });
            
            // Create member name map
            const memberNameMap = new Map(gameData.members.map(m => [m.id, m.name]));
            
            // Process players and goalkeepers
            const processPlayerList = async (playerList, teamName, venue, opponentName) => {
                for (const player of playerList) {
                    // Skip if minutes played = 0
                    if (parseInt(player.stats?.find(s => s.type === 30)?.value || 0, 10) === 0) continue;
                    
                    const playerName = memberNameMap.get(player.id) || 'Unknown Player';
                    const playerIdStr = String(player.id);
                    const shirtNumber = player.shirtNum || null;
                    
                    if (player.position?.id === 1) { // Goalkeeper
                        const pensData = getStatValue(player, 44); // Penalties saved
                        
                        const gkStats = {
                            player_name: playerName,
                            team_name: teamName,
                            shirt_number: shirtNumber,
                            round_num: roundNum,
                            game_id: fixtureId,
                            venue: venue,
                            mp: 1,
                            clean_sheets: getStatValue(player, 35) === 0 ? 1 : 0,
                            saves: getStatValue(player, 23),
                            xg_prevented: getStatValue(player, 83),
                            penalties_saved: pensData.saved || 0,
                            penalties_faced: pensData.faced || 0,
                            game_timestamp: gameData.startTime
                        };
                        
                        // Insert or update goalkeeper stats
                        await connection.execute(
                            `INSERT INTO stats.score365_goalkeepers 
                             (player_name, team_name, shirt_number, round_num, game_id, venue,
                              mp, clean_sheets, saves, xg_prevented, penalties_saved, penalties_faced, game_timestamp)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                             ON DUPLICATE KEY UPDATE
                                team_name = VALUES(team_name),
                                shirt_number = VALUES(shirt_number),
                                mp = VALUES(mp),
                                clean_sheets = VALUES(clean_sheets),
                                saves = VALUES(saves),
                                xg_prevented = VALUES(xg_prevented),
                                penalties_saved = VALUES(penalties_saved),
                                penalties_faced = VALUES(penalties_faced),
                                game_timestamp = VALUES(game_timestamp)`,
                            [
                                gkStats.player_name, gkStats.team_name, gkStats.shirt_number,
                                gkStats.round_num, gkStats.game_id, gkStats.venue,
                                gkStats.mp, gkStats.clean_sheets, gkStats.saves,
                                gkStats.xg_prevented, gkStats.penalties_saved, gkStats.penalties_faced,
                                gkStats.game_timestamp
                            ]
                        );
                        
                    } else { // Outfield player
                        const xg = getStatValue(player, 76);
                        const penaltyXg = gamePenaltyXgMap.get(playerIdStr) || 0;
                        
                        const playerStats = {
                            player_name: playerName,
                            team_name: teamName,
                            shirt_number: shirtNumber,
                            round_num: roundNum,
                            game_id: fixtureId,
                            venue: venue,
                            mp: 1,
                            goals: getStatValue(player, 27),
                            xg: xg,
                            npxg: xg - penaltyXg,
                            assists: getStatValue(player, 26),
                            xa: getStatValue(player, 78),
                            penalties_scored: getPenaltiesScored(player),
                            penalties_missed: gamePenaltyMissedMap.get(playerIdStr) || 0,
                            game_timestamp: gameData.startTime
                        };
                        
                        // Insert or update player stats
                        await connection.execute(
                            `INSERT INTO stats.score365_players 
                             (player_name, team_name, shirt_number, round_num, game_id, venue,
                              mp, goals, xg, npxg, assists, xa, penalties_scored, penalties_missed, game_timestamp)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                             ON DUPLICATE KEY UPDATE
                                team_name = VALUES(team_name),
                                shirt_number = VALUES(shirt_number),
                                mp = VALUES(mp),
                                goals = VALUES(goals),
                                xg = VALUES(xg),
                                npxg = VALUES(npxg),
                                assists = VALUES(assists),
                                xa = VALUES(xa),
                                penalties_scored = VALUES(penalties_scored),
                                penalties_missed = VALUES(penalties_missed),
                                game_timestamp = VALUES(game_timestamp)`,
                            [
                                playerStats.player_name, playerStats.team_name, playerStats.shirt_number,
                                playerStats.round_num, playerStats.game_id, playerStats.venue,
                                playerStats.mp, playerStats.goals, playerStats.xg, playerStats.npxg,
                                playerStats.assists, playerStats.xa,
                                playerStats.penalties_scored, playerStats.penalties_missed,
                                playerStats.game_timestamp
                            ]
                        );
                        
                        // Update team stats aggregates
                        matchTeamStats[venue].xg += playerStats.xg;
                        matchTeamStats[venue].npxg += playerStats.npxg;
                        matchTeamStats[venue].penalties_scored += playerStats.penalties_scored;
                        matchTeamStats[venue].penalties_missed += playerStats.penalties_missed;
                    }
                }
            };
            
            // Process home team players
            await processPlayerList(
                homeCompetitor?.lineups?.members || [],
                homeCompetitor.name,
                'home',
                awayCompetitor.name
            );
            
            // Process away team players
            await processPlayerList(
                awayCompetitor?.lineups?.members || [],
                awayCompetitor.name,
                'away',
                homeCompetitor.name
            );
            
            // Process team stats
            const processTeam = async (team, venue, teamStats, opponentScore) => {
                const teamName = team.name;
                
                // Calculate non-penalty scores
                const teamNpScore = team.score - teamStats.penalties_scored;
                const opponentNpScore = opponentScore - (venue === 'home' ? matchTeamStats.away.penalties_scored : matchTeamStats.home.penalties_scored);
                
                const teamStatsRow = {
                    team_name: teamName,
                    round_num: roundNum,
                    game_id: fixtureId,
                    venue: venue,
                    mp: 1,
                    goals_for: team.score,
                    goals_against: opponentScore,
                    penalties_scored: teamStats.penalties_scored,
                    penalties_missed: teamStats.penalties_missed,
                    penalties_conceded: venue === 'home' ? matchTeamStats.away.penalties_scored : matchTeamStats.home.penalties_scored,
                    xg_for: teamStats.xg,
                    npxg_for: teamStats.npxg,
                    xg_against: venue === 'home' ? matchTeamStats.away.xg : matchTeamStats.home.xg,
                    npxg_against: venue === 'home' ? matchTeamStats.away.npxg : matchTeamStats.home.npxg,
                    score_str: venue === 'home' ? `${team.score}-${opponentScore}` : `${opponentScore}-${team.score}`,
                    npscore_str: venue === 'home' ? `${teamNpScore}-${opponentNpScore}` : `${opponentNpScore}-${teamNpScore}`,
                    game_timestamp: gameData.startTime
                };
                
                await connection.execute(
                    `INSERT INTO stats.score365_teams 
                     (team_name, round_num, game_id, venue, mp,
                      goals_for, goals_against, penalties_scored, penalties_missed, penalties_conceded,
                      xg_for, npxg_for, xg_against, npxg_against, score_str, npscore_str, game_timestamp)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON DUPLICATE KEY UPDATE
                        mp = VALUES(mp),
                        goals_for = VALUES(goals_for),
                        goals_against = VALUES(goals_against),
                        penalties_scored = VALUES(penalties_scored),
                        penalties_missed = VALUES(penalties_missed),
                        penalties_conceded = VALUES(penalties_conceded),
                        xg_for = VALUES(xg_for),
                        npxg_for = VALUES(npxg_for),
                        xg_against = VALUES(xg_against),
                        npxg_against = VALUES(npxg_against),
                        score_str = VALUES(score_str),
                        npscore_str = VALUES(npscore_str),
                        game_timestamp = VALUES(game_timestamp)`,
                    [
                        teamStatsRow.team_name, teamStatsRow.round_num, teamStatsRow.game_id,
                        teamStatsRow.venue, teamStatsRow.mp,
                        teamStatsRow.goals_for, teamStatsRow.goals_against,
                        teamStatsRow.penalties_scored, teamStatsRow.penalties_missed,
                        teamStatsRow.penalties_conceded,
                        teamStatsRow.xg_for, teamStatsRow.npxg_for,
                        teamStatsRow.xg_against, teamStatsRow.npxg_against,
                        teamStatsRow.score_str, teamStatsRow.npscore_str,
                        teamStatsRow.game_timestamp
                    ]
                );
            };
            
            await processTeam(homeCompetitor, 'home', matchTeamStats.home, awayCompetitor.score);
            await processTeam(awayCompetitor, 'away', matchTeamStats.away, homeCompetitor.score);
            
            // Update process status to success
            await connection.execute(
                `UPDATE match_processing_status 
                 SET process_status = 'success', process_completed_at = NOW()
                 WHERE fixture_id = ?`,
                [fixtureId]
            );
            console.log(`   ✅ Stage 2: Stats processed successfully`);
            
            // STAGE 3: Update save status
            await connection.execute(
                `UPDATE match_processing_status 
                 SET save_players_status = 'success',
                     save_gks_status = 'success',
                     save_teams_status = 'success',
                     save_completed_at = NOW(),
                     overall_status = 'completed'
                 WHERE fixture_id = ?`,
                [fixtureId]
            );
            
            // Move to processed_fixtures
            await connection.execute(
                `INSERT INTO processed_fixtures (fixture_id, round_num, processed_at)
                 SELECT fixture_id, round_num, NOW()
                 FROM unprocessed_fixtures
                 WHERE fixture_id = ?`,
                [fixtureId]
            );
            
            await connection.execute(
                `DELETE FROM unprocessed_fixtures WHERE fixture_id = ?`,
                [fixtureId]
            );
            
            console.log(`   ✅ Game fully processed and moved to processed_fixtures`);
            
        } catch (processError) {
            await connection.execute(
                `UPDATE match_processing_status 
                 SET process_status = 'failed', process_error = ?,
                     overall_status = 'failed'
                 WHERE fixture_id = ?`,
                [processError.message.substring(0, 500), fixtureId]
            );
            console.error(`   ❌ Stage 2 failed: ${processError.message}`);
            throw processError;
        }
        
    } catch (error) {
        // Error already handled in stages, just re-throw
        throw error;
    }
}

// Main function to process unprocessed fixtures
export default async function runStatsProcessing(runId) {
    console.log('\n🏁 Starting Stats Processing...');
    console.log(`   Run ID: ${runId}`);
    console.log(`   Time: ${new Date().toISOString()}`);
    
    let processedCount = 0;
    let failedCount = 0;
    
    try {
        await dbClient.initialize();
        
        // Get one unprocessed fixture
        const [fixtures] = await dbClient.pool.execute(
            `SELECT u.* 
             FROM unprocessed_fixtures u
             LEFT JOIN match_processing_status m ON u.fixture_id = m.fixture_id
             WHERE m.overall_status != 'processing' OR m.overall_status IS NULL
             ORDER BY u.match_date ASC
             LIMIT 1`
        );
        
        if (fixtures.length === 0) {
            console.log('📭 No unprocessed fixtures found');
            return { success: true, processed: 0, failed: 0 };
        }
        
        const fixture = fixtures[0];
        console.log(`\n📋 Processing fixture ID: ${fixture.fixture_id}`);
        console.log(`   ${fixture.home_team} vs ${fixture.away_team} (Round ${fixture.round_num})`);
        
        try {
            await processGame(fixture, dbClient.pool);
            processedCount++;
        } catch (error) {
            console.error(`❌ Failed to process game ${fixture.fixture_id}:`, error.message);
            failedCount++;
        }
        
        console.log(`\n📊 Processing complete:`);
        console.log(`   ✅ Processed: ${processedCount}`);
        console.log(`   ❌ Failed: ${failedCount}`);
        
        return {
            success: failedCount === 0,
            processed: processedCount,
            failed: failedCount
        };
        
    } catch (error) {
        console.error('❌ Stats processing failed:', error.message);
        throw error;
    } finally {
        await dbClient.close();
    }
}

// If running directly
if (import.meta.url === `file://${process.argv[1]}`) {
    const runId = `process-${Date.now()}`;
    runStatsProcessing(runId)
        .then(result => {
            process.exit(result.success ? 0 : 1);
        })
        .catch(error => {
            console.error(error);
            process.exit(1);
        });
}