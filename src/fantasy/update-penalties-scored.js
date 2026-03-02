// src/fantasy/update-penalties-scored.js
// One-time function to fetch and update only penalties_scored for all players
// Reads player IDs directly from players_directory table
// Uses directory_errors_ids for error handling

import dbClient from '../database/tidb-client.js';
import createLogger from '../utils/logger.js';

const BASE_URL = 'https://en.fantasy.spl.com.sa/api';
const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
};

const DELAY_BETWEEN_PLAYERS = 1000; // 1 second delay to respect rate limits
const BATCH_SIZE = 50; // Log progress every 50 players

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

async function updatePlayerPenalties(playerId, roundData, connection, logger) {
    try {
        // Extract penalties_scored from round data
        let penaltiesScored = 0;
        
        // Method 1: Direct field if it exists in the API response
        if (roundData.penalties_scored !== undefined) {
            penaltiesScored = roundData.penalties_scored || 0;
        } 
        // Method 2: Check goals field for "Pk" indicator (like in process-stats.js)
        else if (roundData.goals_scored !== undefined && 
                 roundData.goals_scored !== null && 
                 String(roundData.goals_scored).includes('Pk')) {
            const match = String(roundData.goals_scored).match(/(\d+)Pk/);
            penaltiesScored = match ? parseInt(match[1], 10) : 0;
        }
        // Method 3: Check if there's a separate penalties field
        else if (roundData.penalties !== undefined) {
            penaltiesScored = roundData.penalties || 0;
        }
        
        // Update only the penalties_scored column for this round
        await connection.execute(
            `UPDATE fantasy_stats.players_fantasy_stats 
             SET penalties_scored = ?
             WHERE id = ? AND round = ?`,
            [penaltiesScored, playerId, roundData.round]
        );
        
        return penaltiesScored;
        
    } catch (error) {
        logger.error(`   ❌ Failed to update penalties_scored for player ${playerId} round ${roundData.round}:`, error.message);
        throw error;
    }
}

async function processPlayer(playerId, connection, logger) {
    try {
        logger.debug(`   🔄 Processing player ${playerId}...`);
        
        // Fetch player stats from API
        const playerData = await fetchPlayerStats(playerId, logger);
        
        if (playerData.history && playerData.history.length > 0) {
            let roundsWithPenalties = 0;
            
            // Update penalties_scored for each round
            for (const round of playerData.history) {
                const penalties = await updatePlayerPenalties(playerId, round, connection, logger);
                if (penalties > 0) {
                    roundsWithPenalties++;
                }
            }
            
            logger.success(`   ✅ Player ${playerId} updated (${roundsWithPenalties} rounds with penalties)`);
            return { success: true, roundsWithPenalties };
        } else {
            logger.info(`   ℹ️ Player ${playerId} has no history data - no updates needed`);
            return { success: true, roundsWithPenalties: 0 };
        }
        
    } catch (error) {
        logger.error(`   ❌ Failed to process player ${playerId}`);
        throw error;
    }
}

export default async function runUpdatePenaltiesScored(runId) {
    const logger = createLogger('update-penalties-scored');
    
    console.log('\n' + '='.repeat(60));
    console.log('🏁 STARTING UPDATE PENALTIES SCORED');
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
             VALUES (?, 'update-penalties-scored', 'running')`,
            [runId]
        );
        
        // =========================================================
        // STEP 1: Get all player IDs from players_directory
        // =========================================================
        console.log('\n🔍 Step 1: Getting all player IDs from directory...');
        const [players] = await connection.execute(
            'SELECT id FROM fantasy_stats.players_directory ORDER BY id'
        );
        console.log(`   ✅ Found ${players.length} players in directory`);
        
        // =========================================================
        // STEP 2: Check for any previously failed players in error queue
        // =========================================================
        console.log('\n🔍 Step 2: Checking error queue...');
        const [errorPlayers] = await connection.execute(
            'SELECT id FROM fantasy_stats.directory_errors_ids'
        );
        console.log(`   ✅ Found ${errorPlayers.length} players in error queue to retry`);
        
        // =========================================================
        // STEP 3: Combine and deduplicate player IDs
        // =========================================================
        const playerSet = new Set();
        players.forEach(p => playerSet.add(p.id));
        errorPlayers.forEach(p => playerSet.add(p.id));
        
        const allPlayerIds = Array.from(playerSet);
        console.log(`\n📊 Total unique players to process: ${allPlayerIds.length}`);
        
        if (allPlayerIds.length === 0) {
            console.log('\n📭 No players to process - exiting');
            
            await connection.execute(
                `UPDATE fantasy_stats.fantasy_sync_log 
                 SET status = 'success', completed_at = NOW()
                 WHERE run_id = ?`,
                [runId]
            );
            
            return { success: true, processed: 0, errors: 0 };
        }
        
        // =========================================================
        // STEP 4: Process each player
        // =========================================================
        console.log('\n⚙️ Step 4: Processing players...');
        
        let processed = 0;
        let errors = 0;
        let totalRoundsWithPenalties = 0;
        const failedPlayers = [];
        
        for (let i = 0; i < allPlayerIds.length; i++) {
            const playerId = allPlayerIds[i];
            
            console.log(`\n📋 [${i + 1}/${allPlayerIds.length}] Processing player ${playerId}...`);
            
            try {
                const result = await processPlayer(playerId, connection, logger);
                
                processed++;
                totalRoundsWithPenalties += result.roundsWithPenalties;
                
                // If this player was in error queue, remove them
                await connection.execute(
                    `DELETE FROM fantasy_stats.directory_errors_ids WHERE id = ?`,
                    [playerId]
                );
                
            } catch (error) {
                logger.error(`   ❌ Failed to process player ${playerId}`);
                errors++;
                failedPlayers.push({ id: playerId, error: error.message });
                
                // Add to error queue
                await connection.execute(
                    `INSERT INTO fantasy_stats.directory_errors_ids (id, error_message, retry_count)
                     VALUES (?, ?, 1)
                     ON DUPLICATE KEY UPDATE
                        error_message = VALUES(error_message),
                        retry_count = retry_count + 1,
                        failed_at = CURRENT_TIMESTAMP`,
                    [playerId, error.message.substring(0, 500)]
                );
            }
            
            // Log progress every BATCH_SIZE players
            if ((i + 1) % BATCH_SIZE === 0) {
                console.log(`\n📊 Progress: ${i + 1}/${allPlayerIds.length} players processed`);
                console.log(`   Success: ${processed}, Errors: ${errors}`);
            }
            
            // Delay between players to respect rate limits
            await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_PLAYERS));
        }
        
        // =========================================================
        // STEP 5: Summary
        // =========================================================
        console.log('\n' + '='.repeat(60));
        console.log('📊 PROCESSING COMPLETE');
        console.log('='.repeat(60));
        console.log(`   Total players in directory: ${players.length}`);
        console.log(`   Players processed: ${processed}`);
        console.log(`   Players with errors: ${errors}`);
        console.log(`   Total rounds with penalties found: ${totalRoundsWithPenalties}`);
        
        if (failedPlayers.length > 0) {
            console.log('\n❌ Failed players (added to error queue):');
            failedPlayers.slice(0, 10).forEach(p => {
                console.log(`   - Player ${p.id}: ${p.error}`);
            });
            if (failedPlayers.length > 10) {
                console.log(`   ... and ${failedPlayers.length - 10} more`);
            }
        }
        
        // Check final error queue
        const [finalErrors] = await connection.execute(
            'SELECT COUNT(*) as count FROM fantasy_stats.directory_errors_ids'
        );
        console.log(`\n   📋 Final error queue size: ${finalErrors[0].count}`);
        
        // Update sync log
        await connection.execute(
            `UPDATE fantasy_stats.fantasy_sync_log 
             SET status = ?, players_processed = ?, rounds_processed = ?, errors = ?, completed_at = NOW()
             WHERE run_id = ?`,
            [errors === 0 ? 'success' : 'partial', processed, totalRoundsWithPenalties, errors, runId]
        );
        
        console.log('\n✅ UPDATE PENALTIES SCORED COMPLETED');
        console.log('='.repeat(60));
        
        return {
            success: errors === 0,
            processed,
            roundsWithPenalties: totalRoundsWithPenalties,
            errors,
            errorQueue: finalErrors[0].count
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
    const runId = `update-penalties-${Date.now()}`;
    
    runUpdatePenaltiesScored(runId).catch(error => {
        console.error('❌ Fatal error:', error);
        process.exit(1);
    });
}