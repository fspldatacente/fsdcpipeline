// src/fantasy/process-round-fantasy-stats.js
// Processes fantasy stats for players
// Normal mode: processes ALL rounds for ALL players (like original get-players-stats)
// Round override mode: processes ONLY specified round for ALL players

import dbClient from '../database/tidb-client.js';
import createLogger from '../utils/logger.js';

const BOOTSTRAP_URL = 'https://en.fantasy.spl.com.sa/api/bootstrap-static/';
const PLAYER_SUMMARY_URL = 'https://en.fantasy.spl.com.sa/api/element-summary';
const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
};

// Delays to respect rate limits
const DELAY_BETWEEN_PLAYERS = 500; // 500ms between players
const DELAY_BETWEEN_ROUNDS = 100; // 100ms between round inserts

// Test mode limits
const TEST_LIMIT = 100;

async function fetchBootstrap(logger) {
    logger.info('📥 Fetching bootstrap data...');
    
    try {
        const response = await fetch(BOOTSTRAP_URL, { headers: HEADERS });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        logger.debug(`✅ Bootstrap fetched: ${data.events?.length || 0} events found`);
        return data;
        
    } catch (error) {
        logger.error('❌ Failed to fetch bootstrap:', error.message);
        throw error;
    }
}

async function fetchPlayerStats(playerId, logger) {
    const url = `${PLAYER_SUMMARY_URL}/${playerId}/`;
    logger.debug(`   📥 Fetching stats for player ${playerId}`);
    
    try {
        const response = await fetch(url, { headers: HEADERS });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (!data.history || !Array.isArray(data.history)) {
            logger.debug(`   ⚠️ No history data for player ${playerId}`);
            return { history: [] };
        }
        
        logger.debug(`   ✅ Found ${data.history.length} rounds for player ${playerId}`);
        return data;
        
    } catch (error) {
        logger.error(`   ❌ Failed to fetch player ${playerId}:`, error.message);
        throw error;
    }
}

async function savePlayerRoundStats(playerId, roundData, connection, logger) {
    try {
        // Get player directory info
        const [playerInfo] = await connection.execute(
            `SELECT * FROM fantasy_stats.players_directory WHERE id = ?`,
            [playerId]
        );
        
        if (playerInfo.length === 0) {
            throw new Error(`Player ${playerId} not found in directory`);
        }
        
        // Insert stats for this round
        await connection.execute(
            `INSERT INTO fantasy_stats.players_fantasy_stats 
             (id, round, total_points, was_home, opponent_team, team_h_score, team_a_score,
              minutes, goals_scored, assists, clean_sheets, goals_conceded, penalties_saved,
              penalties_missed, yellow_cards, red_cards, saves, own_goals, bonus, fmmp,
              winning_goals, won_tackle, accurate_pass, clearances_blocks_interceptions,
              shot_target, big_chance_created, big_chance_missed, performance_index, value,
              transfers_balance, selected, transfers_in, transfers_out)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                total_points = VALUES(total_points),
                was_home = VALUES(was_home),
                opponent_team = VALUES(opponent_team),
                team_h_score = VALUES(team_h_score),
                team_a_score = VALUES(team_a_score),
                minutes = VALUES(minutes),
                goals_scored = VALUES(goals_scored),
                assists = VALUES(assists),
                clean_sheets = VALUES(clean_sheets),
                goals_conceded = VALUES(goals_conceded),
                penalties_saved = VALUES(penalties_saved),
                penalties_missed = VALUES(penalties_missed),
                yellow_cards = VALUES(yellow_cards),
                red_cards = VALUES(red_cards),
                saves = VALUES(saves),
                own_goals = VALUES(own_goals),
                bonus = VALUES(bonus),
                fmmp = VALUES(fmmp),
                winning_goals = VALUES(winning_goals),
                won_tackle = VALUES(won_tackle),
                accurate_pass = VALUES(accurate_pass),
                clearances_blocks_interceptions = VALUES(clearances_blocks_interceptions),
                shot_target = VALUES(shot_target),
                big_chance_created = VALUES(big_chance_created),
                big_chance_missed = VALUES(big_chance_missed),
                performance_index = VALUES(performance_index),
                value = VALUES(value),
                transfers_balance = VALUES(transfers_balance),
                selected = VALUES(selected),
                transfers_in = VALUES(transfers_in),
                transfers_out = VALUES(transfers_out)`,
            [
                playerId,
                roundData.round,
                roundData.total_points || 0,
                roundData.was_home ? 1 : 0,
                roundData.opponent_team || 0,
                roundData.team_h_score || 0,
                roundData.team_a_score || 0,
                roundData.minutes || 0,
                roundData.goals_scored || 0,
                roundData.assists || 0,
                roundData.clean_sheets || 0,
                roundData.goals_conceded || 0,
                roundData.penalties_saved || 0,
                roundData.penalties_missed || 0,
                roundData.yellow_cards || 0,
                roundData.red_cards || 0,
                roundData.saves || 0,
                roundData.own_goals || 0,
                roundData.bonus || 0,
                roundData.fmmp || 0,
                roundData.winning_goals || 0,
                roundData.won_tackle || 0,
                roundData.accurate_pass || 0,
                roundData.clearances_blocks_interceptions || 0,
                roundData.shot_target || 0,
                roundData.big_chance_created || 0,
                roundData.big_chance_missed || 0,
                roundData.performance_index || 0,
                roundData.value || 0,
                roundData.transfers_balance || 0,
                roundData.selected || 0,
                roundData.transfers_in || 0,
                roundData.transfers_out || 0
            ]
        );
        
        return true;
        
    } catch (error) {
        logger.error(`   ❌ Failed to save round ${roundData.round} for player ${playerId}:`, error.message);
        throw error;
    }
}

async function markPlayerAsFailed(playerId, errorMessage, connection, logger) {
    // Check if already in errors table
    const [existing] = await connection.execute(
        `SELECT retry_count FROM fantasy_stats.directory_errors_ids WHERE id = ?`,
        [playerId]
    );
    
    if (existing.length > 0) {
        // Update existing record
        await connection.execute(
            `UPDATE fantasy_stats.directory_errors_ids 
             SET error_message = ?, retry_count = retry_count + 1, failed_at = NOW()
             WHERE id = ?`,
            [errorMessage.substring(0, 500), playerId]
        );
    } else {
        // Insert new record
        await connection.execute(
            `INSERT INTO fantasy_stats.directory_errors_ids (id, error_message, failed_at, retry_count)
             VALUES (?, ?, NOW(), 1)`,
            [playerId, errorMessage.substring(0, 500)]
        );
    }
}

async function markPlayerAsProcessed(playerId, connection, logger) {
    // Remove from errors table if present
    await connection.execute(
        `DELETE FROM fantasy_stats.directory_errors_ids WHERE id = ?`,
        [playerId]
    );
}

export default async function processRoundFantasyStats(runId, testMode = true, targetRoundOverride = null) {
    const logger = createLogger('process-round-fantasy-stats');
    const mode = testMode ? 'TEST MODE' : 'FULL RUN';
    const roundInfo = targetRoundOverride ? `(OVERRIDE: Round ${targetRoundOverride} only)` : '(ALL rounds)';
    
    console.log('\n' + '='.repeat(60));
    console.log(`🏁 STARTING PROCESS FANTASY STATS ${roundInfo}`);
    console.log('='.repeat(60));
    console.log(`   Run ID: ${runId}`);
    console.log(`   Mode: ${mode}`);
    console.log(`   Target: ${targetRoundOverride ? `Round ${targetRoundOverride} only` : 'All rounds'}`);
    console.log(`   Time: ${new Date().toISOString()}`);
    console.log('-'.repeat(60));
    
    let connection;
    try {
        await dbClient.initialize();
        connection = await dbClient.getConnection();
        logger.success('✅ Database connection established');
        
        // Create sync log entry
        await connection.execute(
            `INSERT INTO fantasy_stats.fantasy_sync_log (run_id, function_name, status, target_round)
             VALUES (?, 'process-round-fantasy-stats', 'running', ?)`,
            [runId, targetRoundOverride || 0]
        );
        
        // Get list of players to process (all players from directory)
        const [players] = await connection.execute(
            `SELECT id FROM fantasy_stats.players_directory ORDER BY id`
        );
        
        logger.info(`📋 Total players in directory: ${players.length}`);
        
        if (players.length === 0) {
            logger.warn('⚠️ No players found in directory!');
            
            await connection.execute(
                `UPDATE fantasy_stats.fantasy_sync_log 
                 SET status = 'failed', completed_at = NOW()
                 WHERE run_id = ?`,
                [runId]
            );
            
            return { success: false, processed: 0, errors: 0 };
        }
        
        // Apply test mode limit
        const playersToProcess = testMode ? players.slice(0, TEST_LIMIT) : players;
        logger.info(`📋 Players to process in this run: ${playersToProcess.length}`);
        
        // Process each player
        let processed = 0;
        let errors = 0;
        let totalRoundsSaved = 0;
        const failedPlayers = [];
        
        for (let i = 0; i < playersToProcess.length; i++) {
            const playerId = playersToProcess[i].id;
            
            console.log(`\n   [${i + 1}/${playersToProcess.length}] Player ID: ${playerId}`);
            
            try {
                // Fetch player stats
                const playerData = await fetchPlayerStats(playerId, logger);
                
                if (playerData.history && playerData.history.length > 0) {
                    let roundsForThisPlayer = 0;
                    
                    // Determine which rounds to process
                    const roundsToProcess = targetRoundOverride 
                        ? playerData.history.filter(r => r.round === targetRoundOverride)  // Only specified round
                        : playerData.history;  // All rounds
                    
                    if (targetRoundOverride && roundsToProcess.length === 0) {
                        logger.debug(`   ℹ️ Player ${playerId} has no data for round ${targetRoundOverride}`);
                    }
                    
                    // Save each round
                    for (const round of roundsToProcess) {
                        await savePlayerRoundStats(playerId, round, connection, logger);
                        roundsForThisPlayer++;
                        totalRoundsSaved++;
                        await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_ROUNDS));
                    }
                    
                    if (roundsForThisPlayer > 0) {
                        logger.debug(`   ✅ Saved ${roundsForThisPlayer} rounds for player ${playerId}`);
                    }
                    
                    // Remove from errors table if it was there
                    await markPlayerAsProcessed(playerId, connection, logger);
                    
                } else {
                    logger.debug(`   ℹ️ No history data for player ${playerId}`);
                    // Still remove from errors table (nothing to process)
                    await markPlayerAsProcessed(playerId, connection, logger);
                }
                
                processed++;
                
                // Delay between players
                await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_PLAYERS));
                
            } catch (error) {
                logger.error(`   ❌ Failed to process player ${playerId}`);
                errors++;
                failedPlayers.push({ id: playerId, error: error.message });
                
                // Add to errors table for retry
                await markPlayerAsFailed(playerId, error.message, connection, logger);
                
                // Continue to next player
            }
        }
        
        // Summary
        console.log('\n' + '='.repeat(60));
        console.log('📊 PROCESSING COMPLETE');
        console.log('='.repeat(60));
        console.log(`   Target: ${targetRoundOverride ? `Round ${targetRoundOverride}` : 'All rounds'}`);
        console.log(`   Players processed successfully: ${processed}`);
        console.log(`   Total rounds saved: ${totalRoundsSaved}`);
        console.log(`   Errors: ${errors}`);
        
        if (failedPlayers.length > 0) {
            console.log('\n❌ Failed players (added to errors table for retry):');
            failedPlayers.slice(0, 10).forEach((p, idx) => {
                console.log(`   ${idx + 1}. Player ${p.id}: ${p.error.substring(0, 100)}`);
            });
            if (failedPlayers.length > 10) {
                console.log(`   ... and ${failedPlayers.length - 10} more`);
            }
        }
        
        // Update sync log
        await connection.execute(
            `UPDATE fantasy_stats.fantasy_sync_log 
             SET status = ?, players_processed = ?, rounds_processed = ?, errors = ?, completed_at = NOW()
             WHERE run_id = ?`,
            [errors === 0 ? 'success' : 'partial', processed, totalRoundsSaved, errors, runId]
        );
        
        console.log('\n✅ PROCESS FANTASY STATS COMPLETED');
        console.log('='.repeat(60));
        
        return {
            success: errors === 0,
            targetRound: targetRoundOverride || 'all',
            processed,
            roundsSaved: totalRoundsSaved,
            errors,
            failedPlayers
        };
        
    } catch (error) {
        logger.error('❌ Fatal error:', error);
        
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
        logger.success('✅ Database connection closed');
    }
}

// If running directly
if (import.meta.url === `file://${process.argv[1]}`) {
    const runId = `process-round-${Date.now()}`;
    
    // Parse command line arguments
    // Usage: node process-round-fantasy-stats.js [test|full] [round]
    // Examples:
    //   node process-round-fantasy-stats.js test        (test mode, all rounds)
    //   node process-round-fantasy-stats.js full        (full mode, all rounds)
    //   node process-round-fantasy-stats.js test 25     (test mode, round 25 only)
    //   node process-round-fantasy-stats.js full 26     (full mode, round 26 only)
    
    const mode = process.argv[2] || 'test';
    const testMode = mode === 'test';
    
    let targetRoundOverride = null;
    if (process.argv[3]) {
        targetRoundOverride = parseInt(process.argv[3], 10);
        if (isNaN(targetRoundOverride)) {
            console.error('❌ Invalid round number. Please provide a valid number.');
            process.exit(1);
        }
    }
    
    processRoundFantasyStats(runId, testMode, targetRoundOverride).catch(error => {
        console.error('❌ Fatal error:', error);
        process.exit(1);
    });
}