// src/matching/match-players.js
// Matches score365 players to RSL players by comparing stats
// Uses 5 key stats: team, minutes, goals, yellow cards, red cards
// Processes players from unprocessed_score365_players table
// Outputs to matched_players and manual_matching_players tables

import dbClient from '../database/tidb-client.js';
import createLogger from '../utils/logger.js';

// Configuration
const BATCH_SIZE = 50; // Log progress every 50 players
const MIN_ROUNDS_TO_MATCH = 3; // Need at least 3 rounds with stats to auto-match
const CONFIDENCE_THRESHOLD = 0.66; // 66% match required (2 out of 3 rounds)
const TEST_LIMIT = 100; // Number of players to process in test mode

// Stats to compare (in order of importance)
const COMPARE_STATS = [
    'team',      // Must match exactly (using team mapping)
    'minutes',   // Should match exactly
    'goals',     // Should match exactly
    'yellow_cards', // Should match exactly
    'red_cards'     // Should match exactly (with special case for second yellows)
];

// Special case for second yellows (365scores: YC=2, RC=1 | RSL: YC=0, RC=1)
function compareCards(score365YC, score365RC, rslYC, rslRC) {
    // Direct match
    if (score365YC === rslYC && score365RC === rslRC) {
        return true;
    }
    
    // Second yellow special case
    if (score365YC === 2 && score365RC === 1 && rslYC === 0 && rslRC === 1) {
        return true;
    }
    
    return false;
}

// Compare a single round's stats
function compareRound(score365Round, rslRound) {
    let matches = 0;
    let totalCompared = 0;
    
    // Compare minutes
    if (score365Round.minutes === rslRound.minutes) {
        matches++;
    }
    totalCompared++;
    
    // Compare goals
    if (score365Round.goals === rslRound.goals) {
        matches++;
    }
    totalCompared++;
    
    // Compare cards with special logic
    if (compareCards(
        score365Round.yellow_cards, score365Round.red_cards,
        rslRound.yellow_cards, rslRound.red_cards
    )) {
        matches++;
    }
    totalCompared++;
    
    return {
        matches,
        totalCompared,
        ratio: matches / totalCompared
    };
}

// Get all RSL players grouped by team for faster lookup
async function getRSLPlayersByTeam(connection) {
    const logger = createLogger('getRSLPlayers');
    logger.info('Loading RSL players by team...');
    
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
        WHERE pfs.minutes > 0
        ORDER BY pd.id, pfs.round
    `);
    
    // Group by team first, then by player
    const byTeam = {};
    const byPlayer = {};
    
    players.forEach(row => {
        // Group by score365 team name (for filtering)
        const teamKey = row.score365_team_name;
        if (!byTeam[teamKey]) {
            byTeam[teamKey] = {};
        }
        
        // Group by player
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
        
        // Add round data
        byTeam[teamKey][playerKey].rounds[row.round] = {
            minutes: row.minutes,
            goals: row.goals,
            yellow_cards: row.yellow_cards,
            red_cards: row.red_cards
        };
        
        // Also store in flat byPlayer map for quick lookup
        byPlayer[playerKey] = byTeam[teamKey][playerKey];
    });
    
    logger.success(`Loaded ${Object.keys(byPlayer).length} RSL players across ${Object.keys(byTeam).length} teams`);
    return { byTeam, byPlayer };
}

// Check if a player has enough data to be matched
function hasEnoughData(score365Player) {
    const roundsWithMinutes = Object.values(score365Player.rounds_data || {})
        .filter(r => r.minutes > 0).length;
    
    return roundsWithMinutes >= MIN_ROUNDS_TO_MATCH;
}

// Find potential matches for a score365 player
async function findMatches(score365Player, rslPlayersByTeam, logger) {
    const playerName = score365Player.player_name;
    const score365Team = score365Player.team_name;
    const score365Rounds = score365Player.rounds_data || {};
    
    logger.debug(`   Finding matches for ${playerName} (${score365Team})`);
    
    // Step 1: Get potential RSL players from the same team (using team mapping)
    const potentialRSLPlayers = rslPlayersByTeam[score365Team] || {};
    const potentialIds = Object.keys(potentialRSLPlayers);
    
    if (potentialIds.length === 0) {
        logger.debug(`   No RSL players found for team ${score365Team}`);
        return [];
    }
    
    logger.debug(`   Found ${potentialIds.length} potential RSL players from same team`);
    
    // Step 2: Compare rounds
    const matches = [];
    
    for (const rslId of potentialIds) {
        const rslPlayer = potentialRSLPlayers[rslId];
        let commonRounds = [];
        let totalMatches = 0;
        let totalCompared = 0;
        
        // Find common rounds where both players have minutes > 0
        Object.entries(score365Rounds).forEach(([roundNum, score365Round]) => {
            if (score365Round.minutes === 0) return;
            
            const rslRound = rslPlayer.rounds[roundNum];
            if (rslRound && rslRound.minutes > 0) {
                commonRounds.push(parseInt(roundNum));
            }
        });
        
        if (commonRounds.length === 0) continue;
        
        // Compare each common round
        for (const roundNum of commonRounds) {
            const score365Round = score365Rounds[roundNum];
            const rslRound = rslPlayer.rounds[roundNum];
            
            const comparison = compareRound(score365Round, rslRound);
            totalMatches += comparison.matches;
            totalCompared += comparison.totalCompared;
        }
        
        const confidence = totalMatches / totalCompared;
        
        if (commonRounds.length >= MIN_ROUNDS_TO_MATCH && confidence >= CONFIDENCE_THRESHOLD) {
            matches.push({
                rsl_id: rslPlayer.rsl_id,
                rsl_name: rslPlayer.rsl_name,
                rsl_team_name: rslPlayer.rsl_team_name,
                rounds_compared: commonRounds.length,
                matches: totalMatches,
                total_compared: totalCompared,
                confidence: confidence,
                common_rounds: commonRounds
            });
        }
    }
    
    // Sort by confidence (highest first)
    matches.sort((a, b) => b.confidence - a.confidence);
    
    return matches;
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
    console.log(`   Min rounds to match: ${MIN_ROUNDS_TO_MATCH}`);
    console.log(`   Confidence threshold: ${CONFIDENCE_THRESHOLD * 100}%`);
    if (testMode) console.log(`   Test limit: ${TEST_LIMIT} players`);
    console.log('-'.repeat(60));
    
    let connection;
    try {
        await dbClient.initialize();
        connection = await dbClient.getConnection();
        
        // Create sync log entry
        await connection.execute(
            `INSERT INTO matching_names.matching_log (run_id, function_name, status)
             VALUES (?, 'match-players', 'running')`,
            [runId]
        );
        
        // =========================================================
        // STEP 1: Load RSL players data
        // =========================================================
        console.log('\n🔍 Step 1: Loading RSL players data...');
        const { byTeam, byPlayer } = await getRSLPlayersByTeam(connection);
        console.log(`   ✅ Loaded ${Object.keys(byPlayer).length} RSL players`);
        
        // =========================================================
        // STEP 2: Get unprocessed score365 players
        // =========================================================
        console.log('\n🔍 Step 2: Fetching unprocessed score365 players...');
        
        let query = 'SELECT * FROM matching_names.unprocessed_score365_players WHERE status = "pending" ORDER BY id';
        if (testMode) {
            query += ' LIMIT ?';
        }
        
        const [unprocessed] = testMode 
            ? await connection.execute(query, [TEST_LIMIT])
            : await connection.execute(query);
        
        console.log(`   📊 Found ${unprocessed.length} unprocessed players`);
        
        if (unprocessed.length === 0) {
            console.log('\n📭 No players to process - exiting');
            
            await connection.execute(
                `UPDATE matching_names.matching_log 
                 SET status = 'success', completed_at = NOW()
                 WHERE run_id = ?`,
                [runId]
            );
            
            return { success: true, processed: 0, matched: 0, manual: 0 };
        }
        
        // =========================================================
        // STEP 3: Process each player
        // =========================================================
        console.log('\n⚙️ Step 3: Processing players...');
        
        let matched = 0;
        let manualNeeded = 0;
        let noStats = 0;
        let errors = 0;
        const results = [];
        
        for (let i = 0; i < unprocessed.length; i++) {
            const player = unprocessed[i];
            
            console.log(`\n📋 [${i + 1}/${unprocessed.length}] Processing player: ${player.player_name} (${player.team_name})`);
            console.log(`   Total minutes: ${player.total_minutes}, Rounds played: ${player.rounds_played}`);
            
            try {
                // Check if player has any minutes
                if (!player.has_played) {
                    console.log(`   ⚠️ Player has no minutes - moving to manual matching`);
                    
                    await connection.execute(
                        `INSERT INTO matching_names.manual_matching_players 
                         (player_name, team_name, reason, total_minutes, rounds_available)
                         VALUES (?, ?, 'no_stats', ?, ?)`,
                        [player.player_name, player.team_name, player.total_minutes, player.rounds_played]
                    );
                    
                    await connection.execute(
                        `UPDATE matching_names.unprocessed_score365_players 
                         SET status = 'manual_needed' WHERE id = ?`,
                        [player.id]
                    );
                    
                    noStats++;
                    results.push({ player: player.player_name, status: 'no_stats' });
                    continue;
                }
                
                // Check if player has enough data
                if (!hasEnoughData(player)) {
                    console.log(`   ⚠️ Player has only ${player.rounds_played} rounds with minutes - need at least ${MIN_ROUNDS_TO_MATCH}`);
                    
                    await connection.execute(
                        `INSERT INTO matching_names.manual_matching_players 
                         (player_name, team_name, reason, total_minutes, rounds_available)
                         VALUES (?, ?, 'insufficient_rounds', ?, ?)`,
                        [player.player_name, player.team_name, player.total_minutes, player.rounds_played]
                    );
                    
                    await connection.execute(
                        `UPDATE matching_names.unprocessed_score365_players 
                         SET status = 'manual_needed' WHERE id = ?`,
                        [player.id]
                    );
                    
                    manualNeeded++;
                    results.push({ player: player.player_name, status: 'insufficient_data' });
                    continue;
                }
                
                // Find matches
                const matches = await findMatches(player, byTeam, logger);
                
                if (matches.length === 0) {
                    console.log(`   ❌ No matches found`);
                    
                    await connection.execute(
                        `INSERT INTO matching_names.manual_matching_players 
                         (player_name, team_name, reason, total_minutes, rounds_available, potential_matches)
                         VALUES (?, ?, 'no_match_found', ?, ?, ?)`,
                        [player.player_name, player.team_name, player.total_minutes, player.rounds_played, JSON.stringify(matches.slice(0, 3))]
                    );
                    
                    await connection.execute(
                        `UPDATE matching_names.unprocessed_score365_players 
                         SET status = 'manual_needed' WHERE id = ?`,
                        [player.id]
                    );
                    
                    manualNeeded++;
                    results.push({ player: player.player_name, status: 'no_match' });
                    
                } else if (matches.length === 1) {
                    // Perfect match
                    const match = matches[0];
                    console.log(`   ✅ MATCHED with ${match.rsl_name} (confidence: ${(match.confidence * 100).toFixed(1)}%)`);
                    console.log(`      Rounds compared: ${match.rounds_compared}, Matches: ${match.matches}/${match.total_compared}`);
                    
                    await connection.execute(
                        `INSERT INTO matching_names.matched_players 
                         (score365_name, score365_team, rsl_name, rsl_player_id, rsl_team_id, 
                          confidence_score, rounds_matched, rounds_compared, match_method)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'auto')`,
                        [
                            player.player_name, player.team_name, 
                            match.rsl_name, match.rsl_id, match.rsl_team_id,
                            match.confidence, match.matches, match.total_compared
                        ]
                    );
                    
                    await connection.execute(
                        `UPDATE matching_names.unprocessed_score365_players 
                         SET status = 'matched' WHERE id = ?`,
                        [player.id]
                    );
                    
                    matched++;
                    results.push({ player: player.player_name, status: 'matched', rsl_id: match.rsl_id });
                    
                } else {
                    // Multiple matches - need manual review
                    console.log(`   ⚠️ Multiple matches found (${matches.length}) - needs manual review`);
                    matches.slice(0, 3).forEach((m, idx) => {
                        console.log(`      ${idx + 1}. ${m.rsl_name} (${(m.confidence * 100).toFixed(1)}%)`);
                    });
                    
                    await connection.execute(
                        `INSERT INTO matching_names.manual_matching_players 
                         (player_name, team_name, reason, total_minutes, rounds_available, potential_matches)
                         VALUES (?, ?, 'ambiguous_match', ?, ?, ?)`,
                        [
                            player.player_name, player.team_name, 
                            player.total_minutes, player.rounds_played,
                            JSON.stringify(matches.slice(0, 5))
                        ]
                    );
                    
                    await connection.execute(
                        `UPDATE matching_names.unprocessed_score365_players 
                         SET status = 'manual_needed' WHERE id = ?`,
                        [player.id]
                    );
                    
                    manualNeeded++;
                    results.push({ player: player.player_name, status: 'ambiguous' });
                }
                
            } catch (error) {
                logger.error(`   ❌ Error processing player ${player.player_name}:`, error.message);
                errors++;
                
                await connection.execute(
                    `INSERT INTO matching_names.directory_errors_ids (id, error_message)
                     VALUES (?, ?)`,
                    [player.id, error.message.substring(0, 500)]
                );
            }
            
            // Log progress
            if ((i + 1) % BATCH_SIZE === 0) {
                console.log(`\n📊 Progress: ${i + 1}/${unprocessed.length} players processed`);
                console.log(`   ✅ Matched: ${matched}, ⚠️ Manual: ${manualNeeded}, ❌ Errors: ${errors}`);
            }
        }
        
        // =========================================================
        // STEP 4: Summary
        // =========================================================
        console.log('\n' + '='.repeat(60));
        console.log('📊 MATCHING COMPLETE');
        console.log('='.repeat(60));
        console.log(`   Total players processed: ${unprocessed.length}`);
        console.log(`   ✅ Successfully matched: ${matched}`);
        console.log(`   ⚠️ Manual review needed: ${manualNeeded}`);
        console.log(`   📊 No stats (0 minutes): ${noStats}`);
        console.log(`   ❌ Errors: ${errors}`);
        
        if (manualNeeded > 0) {
            console.log('\n⚠️ Players needing manual review:');
            const manualResults = results.filter(r => 
                r.status === 'insufficient_data' || 
                r.status === 'no_match' || 
                r.status === 'ambiguous' ||
                r.status === 'no_stats'
            ).slice(0, 10);
            
            manualResults.forEach(r => {
                let reason = '';
                if (r.status === 'no_stats') reason = 'no minutes';
                else if (r.status === 'insufficient_data') reason = 'insufficient rounds';
                else if (r.status === 'no_match') reason = 'no match found';
                else if (r.status === 'ambiguous') reason = 'multiple matches';
                console.log(`   - ${r.player}: ${reason}`);
            });
            
            if (manualResults.length < manualNeeded) {
                console.log(`   ... and ${manualNeeded - manualResults.length} more`);
            }
            
            console.log('\n   👤 Check manual_matching_players table');
        }
        
        // Check remaining queue
        const [remaining] = await connection.execute(
            'SELECT COUNT(*) as count FROM matching_names.unprocessed_score365_players WHERE status = "pending"'
        );
        console.log(`\n   📋 Remaining in queue: ${remaining[0].count}`);
        
        // Update sync log
        await connection.execute(
            `UPDATE matching_names.matching_log 
             SET status = ?, players_processed = ?, players_matched = ?, 
                 players_manual_needed = ?, players_no_stats = ?, errors = ?, 
                 completed_at = NOW()
             WHERE run_id = ?`,
            [
                errors === 0 ? 'completed' : 'completed_with_errors',
                unprocessed.length, matched, manualNeeded, noStats, errors,
                runId
            ]
        );
        
        console.log('\n✅ PLAYER NAME MATCHING COMPLETED');
        console.log('='.repeat(60));
        
        return {
            success: errors === 0,
            processed: unprocessed.length,
            matched,
            manualNeeded,
            noStats,
            errors
        };
        
    } catch (error) {
        logger.error('Fatal error:', error);
        
        if (connection) {
            await connection.execute(
                `UPDATE matching_names.matching_log 
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
    const runId = `match-players-${Date.now()}`;
    
    // Parse command line argument for test mode
    // Usage: node match-players.js [test|full]
    const mode = process.argv[2] || 'test';
    const testMode = mode === 'test';
    
    runPlayerMatching(runId, testMode).catch(error => {
        console.error('❌ Fatal error:', error);
        process.exit(1);
    });
}