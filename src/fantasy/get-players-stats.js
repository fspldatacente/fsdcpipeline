// src/fantasy/get-players-stats.js
// Fetches historical stats for each player from element-summary endpoint
// Test mode: first 10 players only
// Full mode: all players in unprocessed_players

import dbClient from '../database/tidb-client.js';
import createLogger from '../utils/logger.js';

const BASE_URL = 'https://en.fantasy.spl.com.sa/api';
const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
};

// Test mode: process only first 10 players
const TEST_MODE = true; // Set to false for full run
const TEST_LIMIT = 10;
const DELAY_BETWEEN_PLAYERS = 1000; // 1 second delay to respect rate limits
const DELAY_BETWEEN_ROUNDS = 100; // 100ms between round inserts

async function fetchPlayerStats(playerId, logger) {
    const url = `${BASE_URL}/element-summary/${playerId}/`;
    logger.debug(`   📥 Fetching stats for player ${playerId}`);
    
    try {
        const response = await fetch(url, { headers: HEADERS });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (!data.history || !Array.isArray(data.history)) {
            logger.warn(`   ⚠️ No history data for player ${playerId}`);
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
        // Get player directory info to join with stats
        const [playerInfo] = await connection.execute(
            `SELECT * FROM fantasy_stats.players_directory WHERE id = ?`,
            [playerId]
        );
        
        if (playerInfo.length === 0) {
            throw new Error(`Player ${playerId} not found in directory`);
        }
        
        const player = playerInfo[0];
        
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

export default async function runGetPlayersStats(runId, testMode = true) {
    const logger = createLogger('get-players-stats');
    const mode = testMode ? 'TEST MODE' : 'FULL RUN';
    
    console.log('\n' + '='.repeat(60));
    console.log(`🏁 STARTING GET PLAYERS STATS (${mode})`);
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
             VALUES (?, 'get-players-stats', 'running')`,
            [runId]
        );
        
        // Get unprocessed players
        const query = testMode 
            ? 'SELECT id FROM fantasy_stats.unprocessed_players ORDER BY id LIMIT ?'
            : 'SELECT id FROM fantasy_stats.unprocessed_players ORDER BY id';
        
        const [unprocessed] = await connection.execute(query, testMode ? [TEST_LIMIT] : []);
        
        console.log(`\n📊 Unprocessed players: ${unprocessed.length}`);
        
        if (unprocessed.length === 0) {
            console.log('\n📭 No unprocessed players - exiting');
            
            await connection.execute(
                `UPDATE fantasy_stats.fantasy_sync_log 
                 SET status = 'success', completed_at = NOW()
                 WHERE run_id = ?`,
                [runId]
            );
            
            return { success: true, processed: 0, errors: 0 };
        }
        
        // Process each player
        let processed = 0;
        let errors = 0;
        let totalRounds = 0;
        const failedPlayers = [];
        
        for (let i = 0; i < unprocessed.length; i++) {
            const playerId = unprocessed[i].id;
            
            console.log(`\n📋 [${i + 1}/${unprocessed.length}] Processing player ${playerId}...`);
            
            try {
                // Fetch player stats
                const playerData = await fetchPlayerStats(playerId, logger);
                
                if (playerData.history && playerData.history.length > 0) {
                    // Save each round
                    for (const round of playerData.history) {
                        await savePlayerRoundStats(playerId, round, connection, logger);
                        totalRounds++;
                        await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_ROUNDS));
                    }
                    
                    // Move from unprocessed to processed
                    await connection.execute(
                        `DELETE FROM fantasy_stats.unprocessed_players WHERE id = ?`,
                        [playerId]
                    );
                    
                    await connection.execute(
                        `INSERT INTO fantasy_stats.processed_players (id) VALUES (?)`,
                        [playerId]
                    );
                    
                    processed++;
                    logger.success(`   ✅ Player ${playerId} processed (${playerData.history.length} rounds)`);
                    
                } else {
                    // No history data - still mark as processed (no stats to fetch)
                    await connection.execute(
                        `DELETE FROM fantasy_stats.unprocessed_players WHERE id = ?`,
                        [playerId]
                    );
                    
                    await connection.execute(
                        `INSERT INTO fantasy_stats.processed_players (id) VALUES (?)`,
                        [playerId]
                    );
                    
                    processed++;
                    logger.info(`   ℹ️ Player ${playerId} has no history data - marked as processed`);
                }
                
                // Delay between players to respect rate limits
                await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_PLAYERS));
                
            } catch (error) {
                logger.error(`   ❌ Failed to process player ${playerId}`);
                errors++;
                failedPlayers.push({ id: playerId, error: error.message });
                // Keep in unprocessed_players for retry next time
            }
        }
        
        // Summary
        console.log('\n' + '='.repeat(60));
        console.log('📊 PROCESSING COMPLETE');
        console.log('='.repeat(60));
        console.log(`   Players processed: ${processed}`);
        console.log(`   Total rounds saved: ${totalRounds}`);
        console.log(`   Errors: ${errors}`);
        
        if (failedPlayers.length > 0) {
            console.log('\n❌ Failed players (will be retried next run):');
            failedPlayers.slice(0, 10).forEach(p => {
                console.log(`   - Player ${p.id}: ${p.error}`);
            });
            if (failedPlayers.length > 10) {
                console.log(`   ... and ${failedPlayers.length - 10} more`);
            }
        }
        
        // Check remaining unprocessed
        const [remaining] = await connection.execute(
            'SELECT COUNT(*) as count FROM fantasy_stats.unprocessed_players'
        );
        console.log(`\n   📋 Remaining in unprocessed queue: ${remaining[0].count}`);
        
        // Update sync log
        await connection.execute(
            `UPDATE fantasy_stats.fantasy_sync_log 
             SET status = ?, players_processed = ?, rounds_processed = ?, errors = ?, completed_at = NOW()
             WHERE run_id = ?`,
            [errors === 0 ? 'success' : 'partial', processed, totalRounds, errors, runId]
        );
        
        console.log('\n✅ GET PLAYERS STATS COMPLETED');
        console.log('='.repeat(60));
        
        return {
            success: errors === 0,
            processed,
            rounds: totalRounds,
            errors,
            remaining: remaining[0].count
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
    const runId = `get-players-stats-${Date.now()}`;
    
    // Parse command line argument for test mode
    // Usage: node get-players-stats.js [test|full]
    const mode = process.argv[2] || 'test';
    const testMode = mode === 'test';
    
    runGetPlayersStats(runId, testMode).catch(error => {
        console.error('❌ Fatal error:', error);
        process.exit(1);
    });
}