// src/matching/match-players.js
// Matches score365 players to RSL players by comparing stats
// Handles both outfield players and goalkeepers separately
// Progressive round-by-round elimination until unique match found
// No team filtering - compares against ALL RSL players
// Stats compared:
//   Outfield: minutes, goals, yellow cards, red cards
//   Goalkeepers: minutes, saves, yellow cards, red cards

import dbClient from '../database/tidb-client.js';
import createLogger from '../utils/logger.js';

// Configuration
const BATCH_SIZE = 50;
const MIN_ROUNDS_TO_MATCH = 3;
const TEST_OUTFIELD_LIMIT = 80;
const TEST_GK_LIMIT = 20;

// Special case for second yellows (365scores: YC=2, RC=1 | RSL: YC=0, RC=1)
function compareCards(score365YC, score365RC, rslYC, rslRC) {
    // Direct match
    if (score365YC === rslYC && score365RC === rslRC) return true;
    // Second yellow special case
    if (score365YC === 2 && score365RC === 1 && rslYC === 0 && rslRC === 1) return true;
    return false;
}

// Compare a single round for outfield player - returns true if ALL stats match
function compareOutfieldRound(score365Round, rslRound) {
    // Minutes must match exactly
    if (score365Round.minutes !== rslRound.minutes) return false;
    
    // Goals must match exactly
    if (score365Round.goals !== rslRound.goals) return false;
    
    // Cards with special case
    if (!compareCards(
        score365Round.yellow_cards || 0, score365Round.red_cards || 0,
        rslRound.yellow_cards || 0, rslRound.red_cards || 0
    )) return false;
    
    return true;
}

// Compare a single round for goalkeeper - returns true if ALL stats match
function compareGkRound(score365Round, rslRound) {
    // Minutes must match exactly
    if (score365Round.minutes !== rslRound.minutes) return false;
    
    // Saves must match exactly
    if ((score365Round.saves || 0) !== (rslRound.saves || 0)) return false;
    
    // Cards with special case
    if (!compareCards(
        score365Round.yellow_cards || 0, score365Round.red_cards || 0,
        rslRound.yellow_cards || 0, rslRound.red_cards || 0
    )) return false;
    
    return true;
}

// Get ALL RSL outfield players (no team filtering)
async function getRSLOutfieldPlayers(connection) {
    const logger = createLogger('getRSLOutfield');
    logger.info('Loading ALL RSL outfield players...');
    
    const [players] = await connection.execute(`
        SELECT 
            pd.id as rsl_id,
            pd.web_name as rsl_name,
            pd.team as rsl_team_id,
            pfs.round,
            pfs.minutes,
            pfs.goals_scored as goals,
            pfs.yellow_cards,
            pfs.red_cards
        FROM fantasy_stats.players_directory pd
        JOIN fantasy_stats.players_fantasy_stats pfs ON pd.id = pfs.id
        WHERE pfs.minutes > 0 AND pd.element_type != 1
        ORDER BY pd.id, pfs.round
    `);
    
    // Group by player
    const playersMap = {};
    
    players.forEach(row => {
        const playerId = row.rsl_id;
        if (!playersMap[playerId]) {
            playersMap[playerId] = {
                rsl_id: row.rsl_id,
                rsl_name: row.rsl_name || '',
                rsl_team_id: row.rsl_team_id,
                rounds: {}
            };
        }
        
        playersMap[playerId].rounds[row.round] = {
            minutes: row.minutes || 0,
            goals: row.goals || 0,
            yellow_cards: row.yellow_cards || 0,
            red_cards: row.red_cards || 0
        };
    });
    
    logger.success(`Loaded ${Object.keys(playersMap).length} RSL outfield players`);
    return playersMap;
}

// Get ALL RSL goalkeepers (no team filtering)
async function getRSLGoalkeepers(connection) {
    const logger = createLogger('getRSLGKs');
    logger.info('Loading ALL RSL goalkeepers...');
    
    const [players] = await connection.execute(`
        SELECT 
            pd.id as rsl_id,
            pd.web_name as rsl_name,
            pd.team as rsl_team_id,
            pfs.round,
            pfs.minutes,
            pfs.saves,
            pfs.yellow_cards,
            pfs.red_cards
        FROM fantasy_stats.players_directory pd
        JOIN fantasy_stats.players_fantasy_stats pfs ON pd.id = pfs.id
        WHERE pfs.minutes > 0 AND pd.element_type = 1
        ORDER BY pd.id, pfs.round
    `);
    
    // Group by player
    const playersMap = {};
    
    players.forEach(row => {
        const playerId = row.rsl_id;
        if (!playersMap[playerId]) {
            playersMap[playerId] = {
                rsl_id: row.rsl_id,
                rsl_name: row.rsl_name || '',
                rsl_team_id: row.rsl_team_id,
                rounds: {}
            };
        }
        
        playersMap[playerId].rounds[row.round] = {
            minutes: row.minutes || 0,
            saves: row.saves || 0,
            yellow_cards: row.yellow_cards || 0,
            red_cards: row.red_cards || 0
        };
    });
    
    logger.success(`Loaded ${Object.keys(playersMap).length} RSL goalkeepers`);
    return playersMap;
}

// Find matches for a player using progressive round elimination
function findMatches(score365Player, rslPlayersMap, compareFunc, logger) {
    const playerName = score365Player.player_name;
    const score365Rounds = score365Player.rounds_data || {};
    
    // Get all rounds where player actually played
    const playedRounds = Object.entries(score365Rounds)
        .filter(([_, round]) => (round.minutes || 0) > 0)
        .map(([roundNum, _]) => parseInt(roundNum))
        .sort((a, b) => a - b);
    
    if (playedRounds.length < MIN_ROUNDS_TO_MATCH) {
        return { status: 'insufficient', rounds: playedRounds.length };
    }
    
    logger.debug(`   Found ${playedRounds.length} rounds with minutes for ${playerName}`);
    
    // Start with ALL RSL players as potential matches
    let potentialMatches = Object.values(rslPlayersMap);
    logger.debug(`   Initial pool: ${potentialMatches.length} players`);
    
    // Track which rounds we've compared
    const roundsCompared = [];
    let finalMatch = null;
    
    // Progressive elimination through each round
    for (let i = 0; i < playedRounds.length; i++) {
        const roundNum = playedRounds[i];
        const score365Round = score365Rounds[roundNum];
        
        roundsCompared.push(roundNum);
        
        // Filter players who played this round and match all stats
        potentialMatches = potentialMatches.filter(rslPlayer => {
            const rslRound = rslPlayer.rounds[roundNum];
            if (!rslRound || (rslRound.minutes || 0) === 0) return false;
            
            return compareFunc(score365Round, rslRound);
        });
        
        logger.debug(`   After round ${roundNum}: ${potentialMatches.length} players remain`);
        
        // If only one match left, we're done
        if (potentialMatches.length === 1) {
            finalMatch = potentialMatches[0];
            break;
        }
        
        // If no matches left, stop
        if (potentialMatches.length === 0) {
            break;
        }
    }
    
    // Determine result based on final state
    if (potentialMatches.length === 0) {
        return { status: 'no_match', rounds_compared: roundsCompared };
    }
    
    if (potentialMatches.length === 1) {
        const match = potentialMatches[0];
        return {
            status: 'matched',
            match: {
                rsl_id: match.rsl_id,
                rsl_name: match.rsl_name || '',
                rsl_team_id: match.rsl_team_id,
                rounds_compared: roundsCompared.length,
                total_possible_rounds: playedRounds.length
            },
            rounds_compared: roundsCompared
        };
    }
    
    // Multiple matches remain after all rounds
    return {
        status: 'ambiguous',
        matches: potentialMatches.map(m => ({
            rsl_id: m.rsl_id,
            rsl_name: m.rsl_name || '',
            rsl_team_id: m.rsl_team_id
        })),
        rounds_compared: roundsCompared
    };
}

// Process a batch of players
async function processPlayerBatch(players, playerType, rslPlayersMap, compareFunc, connection, logger, stats) {
    for (let i = 0; i < players.length; i++) {
        const player = players[i];
        const playerNum = stats.processed + i + 1;
        const totalPlayers = stats.totalPlayers;
        
        console.log(`\n📋 [${playerNum}/${totalPlayers}] Processing ${playerType}: ${player.player_name} (${player.team_name})`);
        console.log(`   Total minutes: ${player.total_minutes || 0}, Rounds played: ${player.rounds_played || 0}`);
        
        try {
            // STEP 1: Check if player has any minutes at all
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
            
            // STEP 2: Check if player has at least MIN_ROUNDS_TO_MATCH rounds
            const roundsWithMinutes = Object.values(player.rounds_data || {})
                .filter(r => (r.minutes || 0) > 0).length;
            
            if (roundsWithMinutes < MIN_ROUNDS_TO_MATCH) {
                console.log(`   ⚠️ Player has only ${roundsWithMinutes} rounds - need at least ${MIN_ROUNDS_TO_MATCH}`);
                
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
            
            // STEP 3: Find matches using progressive elimination
            const result = findMatches(player, rslPlayersMap, compareFunc, logger);
            
            if (result.status === 'insufficient') {
                console.log(`   ⚠️ Player has only ${result.rounds} rounds - need at least ${MIN_ROUNDS_TO_MATCH}`);
                
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
                
            } else if (result.status === 'no_match') {
                console.log(`   ❌ No matches found after ${result.rounds_compared.length} rounds`);
                
                await connection.execute(
                    `INSERT INTO matching_names.manual_matching_players 
                     (player_name, team_name, player_type, reason, total_minutes, rounds_available)
                     VALUES (?, ?, ?, 'no_match_found', ?, ?)`,
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
                
            } else if (result.status === 'matched') {
                const match = result.match;
                console.log(`   ✅ MATCHED with ${match.rsl_name} after ${match.rounds_compared} rounds`);
                
                await connection.execute(
                    `INSERT INTO matching_names.matched_players 
                     (score365_name, score365_team, rsl_name, rsl_player_id, rsl_team_id, player_type,
                      rounds_compared, match_method)
                     VALUES (?, ?, ?, ?, ?, ?, ?, 'auto')`,
                    [
                        player.player_name || '',
                        player.team_name || '',
                        match.rsl_name || '',
                        match.rsl_id,
                        match.rsl_team_id,
                        playerType,
                        match.rounds_compared
                    ]
                );
                
                await connection.execute(
                    `UPDATE matching_names.${playerType === 'outfield' ? 'unprocessed_outfield_players' : 'unprocessed_gk_players'}
                     SET status = 'matched' WHERE id = ?`,
                    [player.id]
                );
                
                stats.matched++;
                
            } else if (result.status === 'ambiguous') {
                console.log(`   ⚠️ Multiple matches found after ${result.rounds_compared.length} rounds - needs manual review`);
                result.matches.slice(0, 3).forEach((m, idx) => {
                    console.log(`      ${idx + 1}. ${m.rsl_name}`);
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
                        JSON.stringify(result.matches.slice(0, 5))
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
            
            // Handle duplicate errors with ON DUPLICATE KEY UPDATE
            await connection.execute(
                `INSERT INTO matching_names.directory_errors_ids (id, error_message, retry_count)
                 VALUES (?, ?, 1)
                 ON DUPLICATE KEY UPDATE
                    error_message = VALUES(error_message),
                    retry_count = retry_count + 1`,
                [player.id, error.message.substring(0, 500)]
            );
        }
        
        // Log progress
        if ((playerNum) % BATCH_SIZE === 0) {
            console.log(`\n📊 Progress: ${playerNum}/${totalPlayers} processed`);
            console.log(`   ✅ Matched: ${stats.matched}, ⚠️ Manual: ${stats.manualNeeded}, ❌ Errors: ${stats.errors}`);
        }
    }
    
    stats.processed += players.length;
    return stats;
}

// Main function
export default async function runPlayerMatching(runId, testMode = true) {
    const logger = createLogger('match-players');
    const mode = testMode ? 'TEST MODE' : 'FULL RUN';
    
    console.log('\n' + '='.repeat(60));
    console.log(`🏁 STARTING PLAYER NAME MATCHING (${mode})`);
    console.log('='.repeat(60));
    console.log(`   Run ID: ${runId}`);
    console.log(`   Time: ${new Date().toISOString()}`);
    console.log(`   Min rounds required: ${MIN_ROUNDS_TO_MATCH}`);
    if (testMode) console.log(`   Test mode: ${TEST_OUTFIELD_LIMIT} outfield + ${TEST_GK_LIMIT} goalkeepers`);
    console.log('-'.repeat(60));
    
    let connection;
    try {
        await dbClient.initialize();
        connection = await dbClient.getConnection();
        
        // Create sync log entry - Note: error_count column name
        await connection.execute(
            `INSERT INTO matching_names.matching_log (run_id, function_name, status)
             VALUES (?, 'match-players', 'running')`,
            [runId]
        );
        
        // =========================================================
        // STEP 1: Load ALL RSL players (no team filtering)
        // =========================================================
        console.log('\n🔍 Step 1: Loading ALL RSL players...');
        
        const rslOutfield = await getRSLOutfieldPlayers(connection);
        const rslGks = await getRSLGoalkeepers(connection);
        
        console.log(`   ✅ Loaded ${Object.keys(rslOutfield).length} outfield players`);
        console.log(`   ✅ Loaded ${Object.keys(rslGks).length} goalkeepers`);
        
        // =========================================================
        // STEP 2: Get unprocessed players
        // =========================================================
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
        
        // =========================================================
        // STEP 3: Process players
        // =========================================================
        console.log('\n⚙️ Step 3: Processing players...');
        
        const stats = {
            processed: 0,
            matched: 0,
            manualNeeded: 0,
            noStats: 0,
            errors: 0,
            totalPlayers: totalPlayers
        };
        
        // Process outfield players first
        if (outfieldPlayers.length > 0) {
            console.log(`\n📦 Processing ${outfieldPlayers.length} outfield players...`);
            await processPlayerBatch(
                outfieldPlayers, 
                'outfield', 
                rslOutfield, 
                compareOutfieldRound, 
                connection, 
                logger, 
                stats
            );
        }
        
        // Then process goalkeepers
        if (gkPlayers.length > 0) {
            console.log(`\n📦 Processing ${gkPlayers.length} goalkeepers...`);
            await processPlayerBatch(
                gkPlayers, 
                'gk', 
                rslGks, 
                compareGkRound, 
                connection, 
                logger, 
                stats
            );
        }
        
        // =========================================================
        // STEP 4: Summary
        // =========================================================
        console.log('\n' + '='.repeat(60));
        console.log('📊 MATCHING COMPLETE');
        console.log('='.repeat(60));
        console.log(`   Total players processed: ${totalPlayers}`);
        console.log(`   ✅ Successfully matched: ${stats.matched}`);
        console.log(`   ⚠️ Manual review needed: ${stats.manualNeeded}`);
        console.log(`   📊 No stats (0 minutes): ${stats.noStats}`);
        console.log(`   ❌ Errors: ${stats.errors}`);
        
        // Check remaining queues
        const [remainingOutfield] = await connection.execute(
            'SELECT COUNT(*) as count FROM matching_names.unprocessed_outfield_players WHERE status = "pending"'
        );
        const [remainingGks] = await connection.execute(
            'SELECT COUNT(*) as count FROM matching_names.unprocessed_gk_players WHERE status = "pending"'
        );
        
        console.log(`\n   📋 Remaining in queue: ${remainingOutfield[0].count} outfield, ${remainingGks[0].count} GKs`);
        
        // Update sync log - Note: using error_count instead of errors
        await connection.execute(
            `UPDATE matching_names.matching_log 
             SET status = ?, players_processed = ?, players_matched = ?, 
                 players_manual_needed = ?, players_no_stats = ?, error_count = ?, 
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

// If running directly
if (import.meta.url === `file://${process.argv[1]}`) {
    const runId = `match-players-${Date.now()}`;
    const mode = process.argv[2] || 'test';
    const testMode = mode === 'test';
    
    runPlayerMatching(runId, testMode).catch(error => {
        console.error('❌ Fatal error:', error);
        process.exit(1);
    });
}