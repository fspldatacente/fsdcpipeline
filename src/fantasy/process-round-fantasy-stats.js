// src/fantasy/process-round-fantasy-stats.js
// Processes fantasy stats for a SPECIFIC target round only
// 1. Reads target_event_id from round_processing_control
// 2. Checks bootstrap to verify round is finished
// 3. Processes ALL players for that round (or retries failed ones)
// 4. Tracks errors in directory_errors_ids
// 5. Updates control table when round complete

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
const DELAY_BETWEEN_RETRIES = 100; // 100ms between operations

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

async function fetchPlayerRoundStats(playerId, targetRound, logger) {
    const url = `${PLAYER_SUMMARY_URL}/${playerId}/`;
    logger.debug(`   📥 Fetching stats for player ${playerId}, round ${targetRound}`);
    
    try {
        const response = await fetch(url, { headers: HEADERS });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (!data.history || !Array.isArray(data.history)) {
            logger.debug(`   ⚠️ No history data for player ${playerId}`);
            return null;
        }
        
        // Find the specific round we want
        const roundData = data.history.find(r => r.round === targetRound);
        
        if (!roundData) {
            logger.debug(`   ℹ️ Player ${playerId} has no data for round ${targetRound}`);
            return null;
        }
        
        logger.debug(`   ✅ Found round ${targetRound} data for player ${playerId}`);
        return roundData;
        
    } catch (error) {
        logger.error(`   ❌ Failed to fetch player ${playerId}:`, error.message);
        throw error;
    }
}

async function savePlayerRoundStats(playerId, roundData, connection, logger) {
    try {
        // Get player directory info (for team, name, etc.)
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
        
        logger.debug(`   ✅ Saved round ${roundData.round} for player ${playerId}`);
        return true;
        
    } catch (error) {
        logger.error(`   ❌ Failed to save player ${playerId}:`, error.message);
        throw error;
    }
}

async function markPlayerAsProcessed(playerId, connection, logger) {
    // Remove from errors table if present
    await connection.execute(
        `DELETE FROM fantasy_stats.directory_errors_ids WHERE id = ?`,
        [playerId]
    );
    logger.debug(`   ✅ Player ${playerId} removed from errors table`);
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
        logger.debug(`   ⚠️ Updated error record for player ${playerId} (retry #${existing[0].retry_count + 1})`);
    } else {
        // Insert new record
        await connection.execute(
            `INSERT INTO fantasy_stats.directory_errors_ids (id, error_message, failed_at, retry_count)
             VALUES (?, ?, NOW(), 1)`,
            [playerId, errorMessage.substring(0, 500)]
        );
        logger.debug(`   ⚠️ Added player ${playerId} to errors table`);
    }
}

async function getControlTable(connection, logger) {
    const [rows] = await connection.execute(
        `SELECT target_event_id, round_completed, processed_fixtures 
         FROM fantasy_stats.round_processing_control 
         WHERE id = 1`
    );
    
    if (rows.length === 0) {
        throw new Error('round_processing_control table not initialized. Run setup first.');
    }
    
    logger.info(`📋 Control table: target_event_id=${rows[0].target_event_id}, completed=${rows[0].round_completed}`);
    return rows[0];
}

async function updateControlTable(targetEventId, roundCompleted, connection, logger) {
    await connection.execute(
        `UPDATE fantasy_stats.round_processing_control 
         SET target_event_id = ?, round_completed = ?, last_processed_at = NOW()
         WHERE id = 1`,
        [targetEventId, roundCompleted]
    );
    logger.info(`📋 Control table updated: target_event_id=${targetEventId}, completed=${roundCompleted}`);
}

async function getPlayersToProcess(connection, testMode, includeErrors, logger) {
    let players = [];
    
    // First, get players from errors table if we're including them
    if (includeErrors) {
        const [errorPlayers] = await connection.execute(
            `SELECT id FROM fantasy_stats.directory_errors_ids ORDER BY failed_at ASC`
        );
        logger.info(`📋 Found ${errorPlayers.length} players in errors table to retry`);
        players = errorPlayers.map(p => ({ id: p.id, fromErrors: true }));
    }
    
    // Then get unprocessed players (excluding those already in errors)
    const placeholders = players.length > 0 ? players.map(() => '?').join(',') : 'NULL';
    const [unprocessed] = await connection.execute(
        `SELECT id FROM fantasy_stats.players_directory 
         WHERE id NOT IN (SELECT id FROM fantasy_stats.players_fantasy_stats WHERE round = ?)
         AND id NOT IN (${placeholders})
         ORDER BY id
         ${testMode ? `LIMIT ${TEST_LIMIT - players.length}` : ''}`,
        testMode ? [players[0]?.target_event_id || 25, ...players.map(p => p.id)] : []
    );
    
    players.push(...unprocessed.map(p => ({ id: p.id, fromErrors: false })));
    
    logger.info(`📋 Total players to process: ${players.length} (${players.filter(p => p.fromErrors).length} retries, ${players.filter(p => !p.fromErrors).length} new)`);
    return players;
}

export default async function processRoundFantasyStats(runId, testMode = true) {
    const logger = createLogger('process-round-fantasy-stats');
    const mode = testMode ? 'TEST MODE' : 'FULL RUN';
    
    console.log('\n' + '='.repeat(60));
    console.log(`🏁 STARTING PROCESS ROUND FANTASY STATS (${mode})`);
    console.log('='.repeat(60));
    console.log(`   Run ID: ${runId}`);
    console.log(`   Time: ${new Date().toISOString()}`);
    console.log('-'.repeat(60));
    
    let connection;
    try {
        await dbClient.initialize();
        connection = await dbClient.getConnection();
        logger.success('✅ Database connection established');
        
        // Create sync log entry
        await connection.execute(
            `INSERT INTO fantasy_stats.fantasy_sync_log (run_id, function_name, status)
             VALUES (?, 'process-round-fantasy-stats', 'running')`,
            [runId]
        );
        
        // STEP 1: Get control table
        logger.info('\n📋 STEP 1: Reading control table...');
        const control = await getControlTable(connection, logger);
        const targetEventId = control.target_event_id;
        
        // STEP 2: Check bootstrap for round status
        logger.info('\n📋 STEP 2: Checking round status via bootstrap...');
        const bootstrap = await fetchBootstrap(logger);
        
        const targetEvent = bootstrap.events?.find(e => e.id === targetEventId);
        
        if (!targetEvent) {
            throw new Error(`Event ID ${targetEventId} not found in bootstrap`);
        }
        
        logger.info(`   Round ${targetEventId}: finished=${targetEvent.finished}, deadline=${targetEvent.deadline_time}`);
        
        // If round is not finished, exit early
        if (!targetEvent.finished) {
            logger.warn(`⚠️ Round ${targetEventId} is not finished yet. Exiting.`);
            
            await connection.execute(
                `UPDATE fantasy_stats.fantasy_sync_log 
                 SET status = 'skipped', completed_at = NOW()
                 WHERE run_id = ?`,
                [runId]
            );
            
            console.log('\n✅ EARLY EXIT - Round not finished');
            return { success: true, processed: 0, errors: 0, skipped: true };
        }
        
        // STEP 3: Get players to process
        logger.info('\n📋 STEP 3: Building player list...');
        const players = await getPlayersToProcess(connection, testMode, true, logger);
        
        if (players.length === 0) {
            logger.info('📭 No players to process - all done for this round');
            
            // If this round is complete, update control table to next round
            if (control.round_completed === false) {
                await updateControlTable(targetEventId, true, connection, logger);
                logger.info(`✅ Round ${targetEventId} marked as completed`);
                
                // Optionally increment to next round
                // await updateControlTable(targetEventId + 1, false, connection, logger);
            }
            
            await connection.execute(
                `UPDATE fantasy_stats.fantasy_sync_log 
                 SET status = 'success', completed_at = NOW()
                 WHERE run_id = ?`,
                [runId]
            );
            
            return { success: true, processed: 0, errors: 0 };
        }
        
        // STEP 4: Process each player
        logger.info(`\n📋 STEP 4: Processing ${players.length} players for round ${targetEventId}...`);
        
        let processed = 0;
        let errors = 0;
        let failedPlayers = [];
        
        for (let i = 0; i < players.length; i++) {
            const player = players[i];
            
            console.log(`\n   [${i + 1}/${players.length}] Player ID: ${player.id} ${player.fromErrors ? '(retry)' : ''}`);
            
            try {
                // Fetch round data for this player
                const roundData = await fetchPlayerRoundStats(player.id, targetEventId, logger);
                
                if (roundData) {
                    // Save to database
                    await savePlayerRoundStats(player.id, roundData, connection, logger);
                    await markPlayerAsProcessed(player.id, connection, logger);
                    processed++;
                } else {
                    // No data for this round - still mark as processed (no stats to save)
                    await markPlayerAsProcessed(player.id, connection, logger);
                    processed++;
                    logger.debug(`   ℹ️ No data for player ${player.id} in round ${targetEventId} - marked as processed`);
                }
                
                // Delay to respect rate limits
                await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_PLAYERS));
                
            } catch (error) {
                logger.error(`   ❌ Failed to process player ${player.id}`);
                errors++;
                failedPlayers.push({ id: player.id, error: error.message });
                
                // Add to errors table for retry
                await markPlayerAsFailed(player.id, error.message, connection, logger);
                
                // Continue to next player
            }
        }
        
        // STEP 5: Summary
        console.log('\n' + '='.repeat(60));
        console.log('📊 ROUND PROCESSING COMPLETE');
        console.log('='.repeat(60));
        console.log(`   Target round: ${targetEventId}`);
        console.log(`   Players processed successfully: ${processed}`);
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
        
        // Check if all players are processed
        const [remainingForRound] = await connection.execute(
            `SELECT COUNT(*) as count FROM fantasy_stats.players_directory 
             WHERE id NOT IN (SELECT id FROM fantasy_stats.players_fantasy_stats WHERE round = ?)`,
            [targetEventId]
        );
        
        const roundComplete = remainingForRound[0].count === 0;
        logger.info(`\n📋 Round ${targetEventId} completion status: ${roundComplete ? 'COMPLETE' : 'INCOMPLETE'}`);
        logger.info(`   Players remaining for this round: ${remainingForRound[0].count}`);
        
        // Update control table if round is complete
        if (roundComplete && !control.round_completed) {
            await updateControlTable(targetEventId, true, connection, logger);
            logger.info(`✅ Round ${targetEventId} marked as completed`);
            
            // Check if we should auto-increment to next round
            const [nextRoundCheck] = await connection.execute(
                `SELECT COUNT(*) as count FROM fantasy_stats.players_fantasy_stats WHERE round = ?`,
                [targetEventId + 1]
            );
            
            if (nextRoundCheck[0].count === 0) {
                logger.info(`🔄 Auto-incrementing target to round ${targetEventId + 1}`);
                await updateControlTable(targetEventId + 1, false, connection, logger);
            }
        }
        
        // Update sync log
        await connection.execute(
            `UPDATE fantasy_stats.fantasy_sync_log 
             SET status = ?, players_processed = ?, rounds_processed = ?, errors = ?, completed_at = NOW()
             WHERE run_id = ?`,
            [errors === 0 ? 'success' : 'partial', processed, 1, errors, runId]
        );
        
        console.log('\n✅ PROCESS ROUND FANTASY STATS COMPLETED');
        console.log('='.repeat(60));
        
        return {
            success: errors === 0,
            targetRound: targetEventId,
            processed,
            errors,
            roundComplete,
            remainingForRound: remainingForRound[0].count
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
    
    // Parse command line argument for test mode
    // Usage: node process-round-fantasy-stats.js [test|full]
    const mode = process.argv[2] || 'test';
    const testMode = mode === 'test';
    
    processRoundFantasyStats(runId, testMode).catch(error => {
        console.error('❌ Fatal error:', error);
        process.exit(1);
    });
}