// src/matching/match-players.js
// Matches score365 players to RSL players by comparing stats
// Handles both outfield players and goalkeepers separately
// Outfield stats: team, minutes, goals, yellow cards, red cards
// GK stats: team, minutes, saves, yellow cards, red cards

import dbClient from '../database/tidb-client.js';
import createLogger from '../utils/logger.js';

// Configuration
const BATCH_SIZE = 50;
const MIN_ROUNDS_TO_MATCH = 3;
const CONFIDENCE_THRESHOLD = 0.66;
const TEST_OUTFIELD_LIMIT = 80;
const TEST_GK_LIMIT = 20;

// Special case for second yellows
function compareCards(score365YC, score365RC, rslYC, rslRC) {
    if (score365YC === rslYC && score365RC === rslRC) return true;
    if (score365YC === 2 && score365RC === 1 && rslYC === 0 && rslRC === 1) return true;
    return false;
}

function compareOutfieldRound(score365Round, rslRound) {
    let matches = 0;
    let totalCompared = 0;
    
    if (score365Round.minutes === rslRound.minutes) matches++;
    totalCompared++;
    
    if (score365Round.goals === rslRound.goals) matches++;
    totalCompared++;
    
    if (compareCards(
        score365Round.yellow_cards || 0, score365Round.red_cards || 0,
        rslRound.yellow_cards || 0, rslRound.red_cards || 0
    )) matches++;
    totalCompared++;
    
    return { matches, totalCompared, ratio: matches / totalCompared };
}

function compareGkRound(score365Round, rslRound) {
    let matches = 0;
    let totalCompared = 0;
    
    if (score365Round.minutes === rslRound.minutes) matches++;
    totalCompared++;
    
    if ((score365Round.saves || 0) === (rslRound.saves || 0)) matches++;
    totalCompared++;
    
    if (compareCards(
        score365Round.yellow_cards || 0, score365Round.red_cards || 0,
        rslRound.yellow_cards || 0, rslRound.red_cards || 0
    )) matches++;
    totalCompared++;
    
    return { matches, totalCompared, ratio: matches / totalCompared };
}

async function getRSLOutfieldPlayers(connection) {
    const logger = createLogger('getRSLOutfield');
    logger.info('Loading RSL outfield players by team...');
    
    const [players] = await connection.execute(`
        SELECT 
            pd.id as rsl_id,
            pd.web_name as rsl_name,
            pd.team as rsl_team_id,
            mt.rsl_name as rsl_team_name,
            mt.score365_name as score365_team_name,
            pfs.round,
            pfs.minutes,
            pfs.goals_scored as goals,
            pfs.yellow_cards,
            pfs.red_cards
        FROM fantasy_stats.players_directory pd
        JOIN fantasy_stats.players_fantasy_stats pfs ON pd.id = pfs.id
        JOIN matching_names.matched_teams mt ON pd.team = mt.rsl_team_id
        WHERE pfs.minutes > 0 AND pd.element_type != 1
        ORDER BY pd.id, pfs.round
    `);
    
    const byTeam = {};
    const byPlayer = {};
    
    players.forEach(row => {
        const teamKey = row.score365_team_name;
        if (!byTeam[teamKey]) byTeam[teamKey] = {};
        
        const playerKey = row.rsl_id;
        if (!byTeam[teamKey][playerKey]) {
            byTeam[teamKey][playerKey] = {
                rsl_id: row.rsl_id,
                rsl_name: row.rsl_name,
                rsl_team_id: row.rsl_team_id,
                rsl_team_name: row.rsl_team_name,
                rounds: {}
            };
        }
        
        byTeam[teamKey][playerKey].rounds[row.round] = {
            minutes: row.minutes,
            goals: row.goals,
            yellow_cards: row.yellow_cards,
            red_cards: row.red_cards
        };
        
        byPlayer[playerKey] = byTeam[teamKey][playerKey];
    });
    
    logger.success(`Loaded ${Object.keys(byPlayer).length} RSL outfield players`);
    return { byTeam, byPlayer };
}

async function getRSLGoalkeepers(connection) {
    const logger = createLogger('getRSLGKs');
    logger.info('Loading RSL goalkeepers by team...');
    
    const [players] = await connection.execute(`
        SELECT 
            pd.id as rsl_id,
            pd.web_name as rsl_name,
            pd.team as rsl_team_id,
            mt.rsl_name as rsl_team_name,
            mt.score365_name as score365_team_name,
            pfs.round,
            pfs.minutes,
            pfs.saves,
            pfs.yellow_cards,
            pfs.red_cards
        FROM fantasy_stats.players_directory pd
        JOIN fantasy_stats.players_fantasy_stats pfs ON pd.id = pfs.id
        JOIN matching_names.matched_teams mt ON pd.team = mt.rsl_team_id
        WHERE pfs.minutes > 0 AND pd.element_type = 1
        ORDER BY pd.id, pfs.round
    `);
    
    const byTeam = {};
    const byPlayer = {};
    
    players.forEach(row => {
        const teamKey = row.score365_team_name;
        if (!byTeam[teamKey]) byTeam[teamKey] = {};
        
        const playerKey = row.rsl_id;
        if (!byTeam[teamKey][playerKey]) {
            byTeam[teamKey][playerKey] = {
                rsl_id: row.rsl_id,
                rsl_name: row.rsl_name,
                rsl_team_id: row.rsl_team_id,
                rsl_team_name: row.rsl_team_name,
                rounds: {}
            };
        }
        
        byTeam[teamKey][playerKey].rounds[row.round] = {
            minutes: row.minutes,
            saves: row.saves,
            yellow_cards: row.yellow_cards,
            red_cards: row.red_cards
        };
        
        byPlayer[playerKey] = byTeam[teamKey][playerKey];
    });
    
    logger.success(`Loaded ${Object.keys(byPlayer).length} RSL goalkeepers`);
    return { byTeam, byPlayer };
}

function hasEnoughData(player) {
    const roundsWithMinutes = Object.values(player.rounds_data || {})
        .filter(r => (r.minutes || 0) > 0).length;
    return roundsWithMinutes >= MIN_ROUNDS_TO_MATCH;
}

async function findOutfieldMatches(score365Player, rslPlayersByTeam, logger) {
    const playerName = score365Player.player_name;
    const score365Team = score365Player.team_name;
    const score365Rounds = score365Player.rounds_data || {};
    
    const potentialRSLPlayers = rslPlayersByTeam[score365Team] || {};
    const potentialIds = Object.keys(potentialRSLPlayers);
    
    if (potentialIds.length === 0) return [];
    
    const matches = [];
    
    for (const rslId of potentialIds) {
        const rslPlayer = potentialRSLPlayers[rslId];
        let commonRounds = [];
        let totalMatches = 0;
        let totalCompared = 0;
        
        Object.entries(score365Rounds).forEach(([roundNum, score365Round]) => {
            if ((score365Round.minutes || 0) === 0) return;
            
            const rslRound = rslPlayer.rounds[roundNum];
            if (rslRound && (rslRound.minutes || 0) > 0) {
                commonRounds.push(parseInt(roundNum));
            }
        });
        
        if (commonRounds.length === 0) continue;
        
        for (const roundNum of commonRounds) {
            const score365Round = score365Rounds[roundNum];
            const rslRound = rslPlayer.rounds[roundNum];
            const comparison = compareOutfieldRound(score365Round, rslRound);
            totalMatches += comparison.matches;
            totalCompared += comparison.totalCompared;
        }
        
        const confidence = totalMatches / totalCompared;
        
        if (commonRounds.length >= MIN_ROUNDS_TO_MATCH && confidence >= CONFIDENCE_THRESHOLD) {
            matches.push({
                rsl_id: rslPlayer.rsl_id,
                rsl_name: rslPlayer.rsl_name || '',
                rsl_team_name: rslPlayer.rsl_team_name || '',
                rounds_compared: commonRounds.length,
                matches: totalMatches,
                total_compared: totalCompared,
                confidence: confidence,
                common_rounds: commonRounds
            });
        }
    }
    
    matches.sort((a, b) => b.confidence - a.confidence);
    return matches;
}

async function findGkMatches(score365Player, rslPlayersByTeam, logger) {
    const playerName = score365Player.player_name;
    const score365Team = score365Player.team_name;
    const score365Rounds = score365Player.rounds_data || {};
    
    const potentialRSLPlayers = rslPlayersByTeam[score365Team] || {};
    const potentialIds = Object.keys(potentialRSLPlayers);
    
    if (potentialIds.length === 0) return [];
    
    const matches = [];
    
    for (const rslId of potentialIds) {
        const rslPlayer = potentialRSLPlayers[rslId];
        let commonRounds = [];
        let totalMatches = 0;
        let totalCompared = 0;
        
        Object.entries(score365Rounds).forEach(([roundNum, score365Round]) => {
            if ((score365Round.minutes || 0) === 0) return;
            
            const rslRound = rslPlayer.rounds[roundNum];
            if (rslRound && (rslRound.minutes || 0) > 0) {
                commonRounds.push(parseInt(roundNum));
            }
        });
        
        if (commonRounds.length === 0) continue;
        
        for (const roundNum of commonRounds) {
            const score365Round = score365Rounds[roundNum];
            const rslRound = rslPlayer.rounds[roundNum];
            const comparison = compareGkRound(score365Round, rslRound);
            totalMatches += comparison.matches;
            totalCompared += comparison.totalCompared;
        }
        
        const confidence = totalMatches / totalCompared;
        
        if (commonRounds.length >= MIN_ROUNDS_TO_MATCH && confidence >= CONFIDENCE_THRESHOLD) {
            matches.push({
                rsl_id: rslPlayer.rsl_id,
                rsl_name: rslPlayer.rsl_name || '',
                rsl_team_name: rslPlayer.rsl_team_name || '',
                rounds_compared: commonRounds.length,
                matches: totalMatches,
                total_compared: totalCompared,
                confidence: confidence,
                common_rounds: commonRounds
            });
        }
    }
    
    matches.sort((a, b) => b.confidence - a.confidence);
    return matches;
}

async function processPlayerBatch(players, playerType, rslData, connection, logger, stats) {
    for (let i = 0; i < players.length; i++) {
        const player = players[i];
        const playerNum = stats.processed + i + 1;
        const totalPlayers = stats.totalPlayers;
        
        console.log(`\n📋 [${playerNum}/${totalPlayers}] Processing ${playerType}: ${player.player_name} (${player.team_name})`);
        console.log(`   Total minutes: ${player.total_minutes || 0}, Rounds played: ${player.rounds_played || 0}`);
        
        try {
            if (!player.has_played) {
                console.log(`   ⚠️ Player has no minutes - moving to manual matching`);
                
                await connection.execute(
                    `INSERT INTO matching_names.manual_matching_players 
                     (player_name, team_name, player_type, reason, total_minutes, rounds_available)
                     VALUES (?, ?, ?, 'no_stats', ?, ?)`,
                    [
                        player.player_name || '',
                        player.team_name || '',
                        playerType,
                        player.total_minutes || 0,
                        player.rounds_played || 0
                    ]
                );
                
                await connection.execute(
                    `UPDATE matching_names.${playerType === 'outfield' ? 'unprocessed_outfield_players' : 'unprocessed_gk_players'}
                     SET status = 'manual_needed' WHERE id = ?`,
                    [player.id]
                );
                
                stats.noStats++;
                continue;
            }
            
            if (!hasEnoughData(player)) {
                console.log(`   ⚠️ Player has only ${player.rounds_played || 0} rounds - need at least ${MIN_ROUNDS_TO_MATCH}`);
                
                await connection.execute(
                    `INSERT INTO matching_names.manual_matching_players 
                     (player_name, team_name, player_type, reason, total_minutes, rounds_available)
                     VALUES (?, ?, ?, 'insufficient_rounds', ?, ?)`,
                    [
                        player.player_name || '',
                        player.team_name || '',
                        playerType,
                        player.total_minutes || 0,
                        player.rounds_played || 0
                    ]
                );
                
                await connection.execute(
                    `UPDATE matching_names.${playerType === 'outfield' ? 'unprocessed_outfield_players' : 'unprocessed_gk_players'}
                     SET status = 'manual_needed' WHERE id = ?`,
                    [player.id]
                );
                
                stats.manualNeeded++;
                continue;
            }
            
            const matches = playerType === 'outfield' 
                ? await findOutfieldMatches(player, rslData, logger)
                : await findGkMatches(player, rslData, logger);
            
            if (matches.length === 0) {
                console.log(`   ❌ No matches found`);
                
                await connection.execute(
                    `INSERT INTO matching_names.manual_matching_players 
                     (player_name, team_name, player_type, reason, total_minutes, rounds_available, potential_matches)
                     VALUES (?, ?, ?, 'no_match_found', ?, ?, ?)`,
                    [
                        player.player_name || '',
                        player.team_name || '',
                        playerType,
                        player.total_minutes || 0,
                        player.rounds_played || 0,
                        JSON.stringify(matches.slice(0, 3))
                    ]
                );
                
                await connection.execute(
                    `UPDATE matching_names.${playerType === 'outfield' ? 'unprocessed_outfield_players' : 'unprocessed_gk_players'}
                     SET status = 'manual_needed' WHERE id = ?`,
                    [player.id]
                );
                
                stats.manualNeeded++;
                
            } else if (matches.length === 1) {
                const match = matches[0];
                console.log(`   ✅ MATCHED with ${match.rsl_name} (confidence: ${(match.confidence * 100).toFixed(1)}%)`);
                console.log(`      Rounds compared: ${match.rounds_compared}, Matches: ${match.matches}/${match.total_compared}`);
                
                await connection.execute(
                    `INSERT INTO matching_names.matched_players 
                     (score365_name, score365_team, rsl_name, rsl_player_id, rsl_team_id, player_type,
                      confidence_score, rounds_matched, rounds_compared, match_method)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'auto')`,
                    [
                        player.player_name || '',
                        player.team_name || '',
                        match.rsl_name || '',
                        match.rsl_id,
                        match.rsl_team_id,
                        playerType,
                        match.confidence,
                        match.matches,
                        match.total_compared
                    ]
                );
                
                await connection.execute(
                    `UPDATE matching_names.${playerType === 'outfield' ? 'unprocessed_outfield_players' : 'unprocessed_gk_players'}
                     SET status = 'matched' WHERE id = ?`,
                    [player.id]
                );
                
                stats.matched++;
                
            } else {
                console.log(`   ⚠️ Multiple matches found (${matches.length}) - needs manual review`);
                matches.slice(0, 3).forEach((m, idx) => {
                    console.log(`      ${idx + 1}. ${m.rsl_name} (${(m.confidence * 100).toFixed(1)}%)`);
                });
                
                await connection.execute(
                    `INSERT INTO matching_names.manual_matching_players 
                     (player_name, team_name, player_type, reason, total_minutes, rounds_available, potential_matches)
                     VALUES (?, ?, ?, 'ambiguous_match', ?, ?, ?)`,
                    [
                        player.player_name || '',
                        player.team_name || '',
                        playerType,
                        player.total_minutes || 0,
                        player.rounds_played || 0,
                        JSON.stringify(matches.slice(0, 5))
                    ]
                );
                
                await connection.execute(
                    `UPDATE matching_names.${playerType === 'outfield' ? 'unprocessed_outfield_players' : 'unprocessed_gk_players'}
                     SET status = 'manual_needed' WHERE id = ?`,
                    [player.id]
                );
                
                stats.manualNeeded++;
            }
            
        } catch (error) {
            logger.error(`   ❌ Error processing ${playerType} ${player.player_name}:`, error.message);
            stats.errors++;
            
            // Use INSERT IGNORE to avoid duplicate key errors
            await connection.execute(
                `INSERT IGNORE INTO matching_names.directory_errors_ids (id, error_message, retry_count)
                 VALUES (?, ?, 1)`,
                [player.id, error.message.substring(0, 500)]
            );
        }
        
        if ((playerNum) % BATCH_SIZE === 0) {
            console.log(`\n📊 Progress: ${playerNum}/${totalPlayers} processed`);
            console.log(`   ✅ Matched: ${stats.matched}, ⚠️ Manual: ${stats.manualNeeded}, ❌ Errors: ${stats.errors}`);
        }
    }
    
    stats.processed += players.length;
    return stats;
}

export default async function runPlayerMatching(runId, testMode = true) {
    const logger = createLogger('match-players');
    const mode = testMode ? 'TEST MODE' : 'FULL RUN';
    
    console.log('\n' + '='.repeat(60));
    console.log(`🏁 STARTING PLAYER NAME MATCHING (${mode})`);
    console.log('='.repeat(60));
    console.log(`   Run ID: ${runId}`);
    console.log(`   Time: ${new Date().toISOString()}`);
    console.log(`   Min rounds to match: ${MIN_ROUNDS_TO_MATCH}`);
    console.log(`   Confidence threshold: ${CONFIDENCE_THRESHOLD * 100}%`);
    if (testMode) console.log(`   Test mode: ${TEST_OUTFIELD_LIMIT} outfield + ${TEST_GK_LIMIT} goalkeepers`);
    console.log('-'.repeat(60));
    
    let connection;
    try {
        await dbClient.initialize();
        connection = await dbClient.getConnection();
        
        await connection.execute(
            `INSERT INTO matching_names.matching_log (run_id, function_name, status)
             VALUES (?, 'match-players', 'running')`,
            [runId]
        );
        
        console.log('\n🔍 Step 1: Loading RSL players data...');
        const rslOutfield = await getRSLOutfieldPlayers(connection);
        const rslGks = await getRSLGoalkeepers(connection);
        console.log(`   ✅ Loaded ${Object.keys(rslOutfield.byPlayer).length} outfield players`);
        console.log(`   ✅ Loaded ${Object.keys(rslGks.byPlayer).length} goalkeepers`);
        
        console.log('\n🔍 Step 2: Fetching unprocessed players...');
        
        let outfieldPlayers = [];
        let gkPlayers = [];
        
        if (testMode) {
            const [outfield] = await connection.execute(
                'SELECT * FROM matching_names.unprocessed_outfield_players WHERE status = "pending" ORDER BY id LIMIT 80'
            );
            outfieldPlayers = outfield;
            
            const [gks] = await connection.execute(
                'SELECT * FROM matching_names.unprocessed_gk_players WHERE status = "pending" ORDER BY id LIMIT 20'
            );
            gkPlayers = gks;
        } else {
            const [outfield] = await connection.execute(
                'SELECT * FROM matching_names.unprocessed_outfield_players WHERE status = "pending" ORDER BY id'
            );
            outfieldPlayers = outfield;
            
            const [gks] = await connection.execute(
                'SELECT * FROM matching_names.unprocessed_gk_players WHERE status = "pending" ORDER BY id'
            );
            gkPlayers = gks;
        }
        
        const totalPlayers = outfieldPlayers.length + gkPlayers.length;
        console.log(`   📊 Found ${outfieldPlayers.length} outfield players, ${gkPlayers.length} goalkeepers (Total: ${totalPlayers})`);
        
        if (totalPlayers === 0) {
            console.log('\n📭 No players to process - exiting');
            await connection.execute(
                `UPDATE matching_names.matching_log SET status = 'completed', completed_at = NOW() WHERE run_id = ?`,
                [runId]
            );
            return { success: true, processed: 0, matched: 0, manual: 0 };
        }
        
        console.log('\n⚙️ Step 3: Processing players...');
        
        const stats = {
            processed: 0,
            matched: 0,
            manualNeeded: 0,
            noStats: 0,
            errors: 0,
            totalPlayers: totalPlayers
        };
        
        if (outfieldPlayers.length > 0) {
            console.log(`\n📦 Processing ${outfieldPlayers.length} outfield players...`);
            await processPlayerBatch(outfieldPlayers, 'outfield', rslOutfield.byTeam, connection, logger, stats);
        }
        
        if (gkPlayers.length > 0) {
            console.log(`\n📦 Processing ${gkPlayers.length} goalkeepers...`);
            await processPlayerBatch(gkPlayers, 'gk', rslGks.byTeam, connection, logger, stats);
        }
        
        console.log('\n' + '='.repeat(60));
        console.log('📊 MATCHING COMPLETE');
        console.log('='.repeat(60));
        console.log(`   Total players processed: ${totalPlayers}`);
        console.log(`   ✅ Successfully matched: ${stats.matched}`);
        console.log(`   ⚠️ Manual review needed: ${stats.manualNeeded}`);
        console.log(`   📊 No stats (0 minutes): ${stats.noStats}`);
        console.log(`   ❌ Errors: ${stats.errors}`);
        
        const [remainingOutfield] = await connection.execute(
            'SELECT COUNT(*) as count FROM matching_names.unprocessed_outfield_players WHERE status = "pending"'
        );
        const [remainingGks] = await connection.execute(
            'SELECT COUNT(*) as count FROM matching_names.unprocessed_gk_players WHERE status = "pending"'
        );
        
        console.log(`\n   📋 Remaining in queue: ${remainingOutfield[0].count} outfield, ${remainingGks[0].count} GKs`);
        
        await connection.execute(
            `UPDATE matching_names.matching_log 
             SET status = ?, players_processed = ?, players_matched = ?, 
                 players_manual_needed = ?, players_no_stats = ?, errors = ?, 
                 completed_at = NOW()
             WHERE run_id = ?`,
            [
                stats.errors === 0 ? 'completed' : 'completed_with_errors',
                totalPlayers, stats.matched, stats.manualNeeded, stats.noStats, stats.errors,
                runId
            ]
        );
        
        console.log('\n✅ PLAYER NAME MATCHING COMPLETED');
        console.log('='.repeat(60));
        
        return {
            success: stats.errors === 0,
            processed: totalPlayers,
            matched: stats.matched,
            manualNeeded: stats.manualNeeded,
            noStats: stats.noStats,
            errors: stats.errors
        };
        
    } catch (error) {
        logger.error('Fatal error:', error);
        
        if (connection) {
            await connection.execute(
                `UPDATE matching_names.matching_log SET status = 'failed', error_message = ?, completed_at = NOW() WHERE run_id = ?`,
                [error.message.substring(0, 500), runId]
            );
        }
        
        throw error;
    } finally {
        if (connection) await connection.release();
        await dbClient.close();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) {
    const runId = `match-players-${Date.now()}`;
    const mode = process.argv[2] || 'test';
    const testMode = mode === 'test';
    
    runPlayerMatching(runId, testMode).catch(error => {
        console.error('❌ Fatal error:', error);
        process.exit(1);
    });
}