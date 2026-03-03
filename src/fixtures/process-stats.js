// src/fixtures/process-stats.js
// FINAL VERSION - Includes all required stats:
// - Minutes played (actual minutes)
// - Yellow/red cards (with second yellow logic)
// - xG, npxG, xA
// - Penalties scored/missed (for all players including GKs)
// - All team stats

import dbClient from '../database/tidb-client.js';

const BASE_URL = 'https://webws.365scores.com';
const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
};

function getStatValue(player, statType, defaultValue = 0) {
    if (!player || !player.stats) return defaultValue;
    
    const stat = player.stats.find(s => s && s.type === statType);
    if (stat && typeof stat.value !== 'undefined') {
        if (statType === 44) {
            const parts = String(stat.value).split('/');
            return {
                saved: parseInt(parts[0], 10) || 0,
                faced: parseInt(parts[1], 10) || 0
            };
        }
        return parseFloat(stat.value) || 0;
    }
    return defaultValue;
}

function getPenaltiesScored(player) {
    if (!player || !player.stats) return 0;
    
    const goalsStat = player.stats.find(s => s && s.type === 27);
    if (goalsStat && goalsStat.value && String(goalsStat.value).includes('Pk')) {
        const match = String(goalsStat.value).match(/(\d+)Pk/);
        return match ? parseInt(match[1], 10) : 0;
    }
    return 0;
}

function getYellowCardsFromEvents(playerId, gameEvents) {
    if (!gameEvents || !Array.isArray(gameEvents)) return 0;
    
    const regularYellows = gameEvents.filter(event => 
        event.playerId === playerId && 
        event.eventType && 
        event.eventType.id === 2
    ).length;
    
    const secondYellowReds = gameEvents.filter(event => 
        event.playerId === playerId && 
        event.eventType && 
        event.eventType.id === 3 &&
        event.eventType.subTypeId === 21
    ).length;
    
    return regularYellows + secondYellowReds;
}

function getRedCardsFromEvents(playerId, gameEvents) {
    if (!gameEvents || !Array.isArray(gameEvents)) return 0;
    
    return gameEvents.filter(event => 
        event.playerId === playerId && 
        event.eventType && 
        event.eventType.id === 3
    ).length;
}

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

async function processGame(game, connection) {
    const fixtureId = game.fixture_id;
    const roundNum = game.round_num;
    const homeTeam = game.home_team;
    const awayTeam = game.away_team;
    
    console.log(`\n   🎮 Processing: ${homeTeam} vs ${awayTeam} (Round ${roundNum})`);
    
    await connection.execute(
        `UPDATE match_processing_status 
         SET overall_status = 'processing', updated_at = CURRENT_TIMESTAMP 
         WHERE fixture_id = ?`,
        [fixtureId]
    );
    
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
        throw fetchError;
    }
    
    console.log(`   ⏳ Stage 2: Processing player and team stats...`);
    await connection.execute(
        `UPDATE match_processing_status 
         SET process_status = 'processing'
         WHERE fixture_id = ?`,
        [fixtureId]
    );
    
    try {
        const { homeCompetitor, awayCompetitor, members, chartEvents, events } = gameData;
        
        if (!homeCompetitor || !awayCompetitor) {
            throw new Error('Missing home or away competitor data');
        }
        
        const matchTeamStats = {
            home: { xg: 0, npxg: 0, penalties_scored: 0, penalties_missed: 0 },
            away: { xg: 0, npxg: 0, penalties_scored: 0, penalties_missed: 0 }
        };
        
        const gamePenaltyXgMap = new Map();
        const gamePenaltyMissedMap = new Map();
        
        if (chartEvents?.events && Array.isArray(chartEvents.events)) {
            chartEvents.events.filter(e => e && e.subType === 9).forEach(event => {
                const playerId = String(event.playerId || '');
                gamePenaltyXgMap.set(playerId, (gamePenaltyXgMap.get(playerId) || 0) + (parseFloat(event.xg) || 0));
                if (event.outcome?.name !== 'Goal') {
                    gamePenaltyMissedMap.set(playerId, (gamePenaltyMissedMap.get(playerId) || 0) + 1);
                }
            });
        }
        
        const memberNameMap = new Map();
        if (members && Array.isArray(members)) {
            members.forEach(m => {
                if (m && m.id) {
                    memberNameMap.set(m.id, m.name || 'Unknown Player');
                }
            });
        }
        
        const processPlayerList = async (playerList, teamName, venue, opponentName) => {
            if (!playerList || !Array.isArray(playerList)) return;
            
            for (const player of playerList) {
                if (!player) continue;
                
                // Get actual minutes played (type 30)
                const minutes = getStatValue(player, 30);
                if (minutes === 0) continue;
                
                const playerName = memberNameMap.get(player.id) || player.name || 'Unknown Player';
                const playerIdStr = String(player.id || '');
                const shirtNumber = player.shirtNum || null;
                
                const yellowCards = getYellowCardsFromEvents(player.id, events);
                const redCards = getRedCardsFromEvents(player.id, events);
                
                const xg = getStatValue(player, 76);
                const goals = getStatValue(player, 27);
                const assists = getStatValue(player, 26);
                const xa = getStatValue(player, 78);
                const cleanSheets = getStatValue(player, 35);
                const saves = getStatValue(player, 23);
                
                // ALL PLAYERS (including GKs) can score penalties
                const penaltiesScored = getPenaltiesScored(player);
                
                if (player.position?.id === 1) { // Goalkeeper
                    const pensData = getStatValue(player, 44);
                    
                    const gkStats = {
                        player_name: playerName,
                        team_name: teamName,
                        shirt_number: shirtNumber,
                        round_num: roundNum,
                        game_id: fixtureId,
                        venue: venue,
                        mp: 1,
                        minutes_played: minutes,
                        clean_sheets: cleanSheets === 0 ? 1 : 0,
                        saves: saves,
                        yellow_cards: yellowCards,
                        red_cards: redCards,
                        xg_prevented: getStatValue(player, 83),
                        penalties_saved: (pensData && pensData.saved) || 0,
                        penalties_faced: (pensData && pensData.faced) || 0,
                        penalties_scored: penaltiesScored, // ADDED for GK
                        game_timestamp: gameData.startTime
                    };
                    
                    console.log(`      🧤 GK ${playerName}: MIN=${minutes}, CS=${gkStats.clean_sheets}, Saves=${saves}, YC=${yellowCards}, RC=${redCards}, PS=${penaltiesScored}`);
                    
                    await connection.execute(
                        `INSERT INTO stats.score365_goalkeepers 
                         (player_name, team_name, shirt_number, round_num, game_id, venue,
                          mp, minutes_played, clean_sheets, saves, yellow_cards, red_cards, xg_prevented, 
                          penalties_saved, penalties_faced, penalties_scored, game_timestamp)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                         ON DUPLICATE KEY UPDATE
                            team_name = VALUES(team_name),
                            shirt_number = VALUES(shirt_number),
                            mp = VALUES(mp),
                            minutes_played = VALUES(minutes_played),
                            clean_sheets = VALUES(clean_sheets),
                            saves = VALUES(saves),
                            yellow_cards = VALUES(yellow_cards),
                            red_cards = VALUES(red_cards),
                            xg_prevented = VALUES(xg_prevented),
                            penalties_saved = VALUES(penalties_saved),
                            penalties_faced = VALUES(penalties_faced),
                            penalties_scored = VALUES(penalties_scored),
                            game_timestamp = VALUES(game_timestamp)`,
                        [
                            gkStats.player_name, gkStats.team_name, gkStats.shirt_number,
                            gkStats.round_num, gkStats.game_id, gkStats.venue,
                            gkStats.mp, gkStats.minutes_played, gkStats.clean_sheets, gkStats.saves,
                            gkStats.yellow_cards, gkStats.red_cards,
                            gkStats.xg_prevented, gkStats.penalties_saved, gkStats.penalties_faced,
                            gkStats.penalties_scored,
                            gkStats.game_timestamp
                        ]
                    );
                    
                } else { // Outfield player
                    const penaltyXg = gamePenaltyXgMap.get(playerIdStr) || 0;
                    
                    const playerStats = {
                        player_name: playerName,
                        team_name: teamName,
                        shirt_number: shirtNumber,
                        round_num: roundNum,
                        game_id: fixtureId,
                        venue: venue,
                        mp: 1,
                        minutes_played: minutes,
                        goals: goals,
                        yellow_cards: yellowCards,
                        red_cards: redCards,
                        xg: xg,
                        npxg: Math.max(0, xg - penaltyXg),
                        assists: assists,
                        xa: xa,
                        penalties_scored: penaltiesScored,
                        penalties_missed: gamePenaltyMissedMap.get(playerIdStr) || 0,
                        game_timestamp: gameData.startTime
                    };
                    
                    console.log(`      👤 ${playerName}: MIN=${minutes}, G=${goals}, A=${assists}, xG=${xg.toFixed(2)}, xA=${xa.toFixed(2)}, YC=${yellowCards}, RC=${redCards}, PS=${penaltiesScored}`);
                    
                    await connection.execute(
                        `INSERT INTO stats.score365_players 
                         (player_name, team_name, shirt_number, round_num, game_id, venue,
                          mp, minutes_played, goals, yellow_cards, red_cards, xg, npxg, assists, xa, 
                          penalties_scored, penalties_missed, game_timestamp)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                         ON DUPLICATE KEY UPDATE
                            team_name = VALUES(team_name),
                            shirt_number = VALUES(shirt_number),
                            mp = VALUES(mp),
                            minutes_played = VALUES(minutes_played),
                            goals = VALUES(goals),
                            yellow_cards = VALUES(yellow_cards),
                            red_cards = VALUES(red_cards),
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
                            playerStats.mp, playerStats.minutes_played, playerStats.goals, 
                            playerStats.yellow_cards, playerStats.red_cards,
                            playerStats.xg, playerStats.npxg,
                            playerStats.assists, playerStats.xa,
                            playerStats.penalties_scored, playerStats.penalties_missed,
                            playerStats.game_timestamp
                        ]
                    );
                    
                    matchTeamStats[venue].xg += playerStats.xg || 0;
                    matchTeamStats[venue].npxg += playerStats.npxg || 0;
                    matchTeamStats[venue].penalties_scored += playerStats.penalties_scored || 0;
                    matchTeamStats[venue].penalties_missed += playerStats.penalties_missed || 0;
                }
            }
        };
        
        await processPlayerList(
            homeCompetitor?.lineups?.members || [],
            homeCompetitor.name || 'Unknown',
            'home',
            awayCompetitor.name || 'Unknown'
        );
        
        await processPlayerList(
            awayCompetitor?.lineups?.members || [],
            awayCompetitor.name || 'Unknown',
            'away',
            homeCompetitor.name || 'Unknown'
        );
        
        const processTeam = async (team, venue, teamStats, opponentScore) => {
            if (!team || !team.name) return;
            
            const teamName = team.name;
            const teamScore = team.score || 0;
            
            const teamNpScore = teamScore - (teamStats.penalties_scored || 0);
            const opponentNpScore = opponentScore - (venue === 'home' ? (matchTeamStats.away.penalties_scored || 0) : (matchTeamStats.home.penalties_scored || 0));
            
            const teamStatsRow = {
                team_name: teamName,
                round_num: roundNum,
                game_id: fixtureId,
                venue: venue,
                mp: 1,
                goals_for: teamScore,
                goals_against: opponentScore,
                penalties_scored: teamStats.penalties_scored || 0,
                penalties_missed: teamStats.penalties_missed || 0,
                penalties_conceded: venue === 'home' ? (matchTeamStats.away.penalties_scored || 0) : (matchTeamStats.home.penalties_scored || 0),
                xg_for: teamStats.xg || 0,
                npxg_for: teamStats.npxg || 0,
                xg_against: venue === 'home' ? (matchTeamStats.away.xg || 0) : (matchTeamStats.home.xg || 0),
                npxg_against: venue === 'home' ? (matchTeamStats.away.npxg || 0) : (matchTeamStats.home.npxg || 0),
                score_str: venue === 'home' ? `${teamScore}-${opponentScore}` : `${opponentScore}-${teamScore}`,
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
        
        await processTeam(homeCompetitor, 'home', matchTeamStats.home, awayCompetitor.score || 0);
        await processTeam(awayCompetitor, 'away', matchTeamStats.away, homeCompetitor.score || 0);
        
        await connection.execute(
            `UPDATE match_processing_status 
             SET process_status = 'success', process_completed_at = NOW()
             WHERE fixture_id = ?`,
            [fixtureId]
        );
        
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
        throw processError;
    }
}

export default async function runStatsProcessing(runId, connection) {
    console.log('\n' + '='.repeat(60));
    console.log('🏁 STARTING STATS PROCESSING (Batch Mode)');
    console.log('='.repeat(60));
    console.log(`   Run ID: ${runId}`);
    console.log(`   Time: ${new Date().toISOString()}`);
    console.log('-'.repeat(60));
    
    let totalProcessed = 0;
    let totalFailed = 0;
    let failedGames = [];
    const processedIds = new Set();
    
    try {
        const [countResult] = await connection.execute(
            'SELECT COUNT(*) as count FROM unprocessed_fixtures'
        );
        const initialQueueSize = countResult[0].count;
        console.log(`\n📊 Initial unprocessed queue: ${initialQueueSize} games\n`);
        
        if (initialQueueSize === 0) {
            console.log('📭 No games to process - exiting early');
            return { success: true, processed: 0, failed: 0 };
        }
        
        let gamesInQueue = initialQueueSize;
        let loopCount = 0;
        
        while (gamesInQueue > 0) {
            loopCount++;
            console.log(`\n${'-'.repeat(50)}`);
            console.log(`🔄 Processing cycle #${loopCount} (${gamesInQueue} games remaining)`);
            console.log(`${'-'.repeat(50)}`);
            
            const [fixtures] = await connection.execute(
                `SELECT * 
                 FROM unprocessed_fixtures 
                 ORDER BY round_num ASC, match_date ASC 
                 LIMIT 1`
            );
            
            if (fixtures.length === 0) break;
            
            const fixture = fixtures[0];
            console.log(`\n📋 Processing fixture ID: ${fixture.fixture_id}`);
            console.log(`   ${fixture.home_team} vs ${fixture.away_team} (Round ${fixture.round_num})`);
            
            try {
                await processGame(fixture, connection);
                totalProcessed++;
                processedIds.add(fixture.fixture_id);
                console.log(`   ✅ Game #${totalProcessed + totalFailed} completed successfully`);
            } catch (error) {
                console.error(`   ❌ Game #${totalProcessed + totalFailed + 1} failed:`, error.message);
                totalFailed++;
                
                if (!processedIds.has(fixture.fixture_id)) {
                    failedGames.push({
                        id: fixture.fixture_id,
                        round: fixture.round_num,
                        home: fixture.home_team,
                        away: fixture.away_team
                    });
                    processedIds.add(fixture.fixture_id);
                }
            }
            
            const [newCount] = await connection.execute(
                'SELECT COUNT(*) as count FROM unprocessed_fixtures'
            );
            gamesInQueue = newCount[0].count;
            console.log(`\n   📊 Queue remaining: ${gamesInQueue} games`);
        }
        
        console.log('\n' + '='.repeat(60));
        console.log('📊 BATCH PROCESSING COMPLETE:');
        console.log('='.repeat(60));
        console.log(`   Initial queue size: ${initialQueueSize}`);
        console.log(`   Successfully processed: ${totalProcessed}`);
        console.log(`   Failed: ${totalFailed}`);
        console.log(`   Final queue size: ${gamesInQueue}`);
        
        if (failedGames.length > 0) {
            console.log('\n' + '-'.repeat(60));
            console.log('❌ FAILED GAMES (need retry):');
            console.log('-'.repeat(60));
            
            const uniqueFailed = [...new Map(failedGames.map(game => [game.id, game])).values()];
            uniqueFailed.forEach((game, index) => {
                console.log(`   ${index + 1}. Game ${game.id}: ${game.home} vs ${game.away} (Round ${game.round})`);
            });
            
            console.log('\n   ⚠️  Run the workflow again to retry these failed games.');
        } else {
            console.log('\n   ✅ All games processed successfully!');
        }
        console.log('='.repeat(60));
        
        return {
            success: totalFailed === 0,
            processed: totalProcessed,
            failed: totalFailed,
            failedGames: [...new Map(failedGames.map(game => [game.id, game])).values()]
        };
        
    } catch (error) {
        console.error('\n❌ STATS PROCESSING FATAL ERROR:');
        console.error('   Error:', error.message);
        console.error('   Stack:', error.stack);
        throw error;
    }
}

if (import.meta.url === `file://${process.argv[1]}`) {
    const runId = `process-${Date.now()}`;
    
    const run = async () => {
        let connection = null;
        try {
            await dbClient.initialize();
            connection = await dbClient.getConnection();
            console.log('✅ Database connection established');
            
            await runStatsProcessing(runId, connection);
            
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