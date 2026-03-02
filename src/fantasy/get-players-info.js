// src/fantasy/get-players-info.js
// One-time function to fetch all player directory data from bootstrap-static endpoint
// Populates players_directory and unprocessed_players tables

import dbClient from '../database/tidb-client.js';
import createLogger from '../utils/logger.js';

const BOOTSTRAP_URL = 'https://en.fantasy.spl.com.sa/api/bootstrap-static/';
const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
};

const BATCH_SIZE = 50; // Process 50 players before logging progress

async function fetchBootstrapData() {
    const logger = createLogger('fetch-bootstrap');
    logger.info('📥 Fetching bootstrap data from fantasy API...');
    
    try {
        const response = await fetch(BOOTSTRAP_URL, { headers: HEADERS });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        logger.success(`✅ Bootstrap fetched successfully`);
        logger.info(`📊 Total players in elements: ${data.elements?.length || 0}`);
        
        return data;
    } catch (error) {
        logger.error('Failed to fetch bootstrap data:', error.message);
        throw error;
    }
}

async function processPlayer(player, connection, logger) {
    const playerId = player.id;
    
    try {
        // Insert into players_directory
        await connection.execute(
            `INSERT INTO fantasy_stats.players_directory 
             (id, team, first_name, second_name, web_name, element_type, selected_by_percent, now_cost)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                team = VALUES(team),
                first_name = VALUES(first_name),
                second_name = VALUES(second_name),
                web_name = VALUES(web_name),
                element_type = VALUES(element_type),
                selected_by_percent = VALUES(selected_by_percent),
                now_cost = VALUES(now_cost)`,
            [
                playerId,
                player.team,
                player.first_name || '',
                player.second_name || '',
                player.web_name || '',
                player.element_type,
                parseFloat(player.selected_by_percent) || 0,
                player.now_cost || 0
            ]
        );
        
        // Add to unprocessed_players
        await connection.execute(
            `INSERT INTO fantasy_stats.unprocessed_players (id) VALUES (?)
             ON DUPLICATE KEY UPDATE id = id`,
            [playerId]
        );
        
        // If this player was in errors table, remove them
        await connection.execute(
            `DELETE FROM fantasy_stats.directory_errors_ids WHERE id = ?`,
            [playerId]
        );
        
        return { success: true, id: playerId };
        
    } catch (error) {
        logger.error(`❌ Failed to process player ${playerId}: ${error.message}`);
        
        // Add to errors table
        await connection.execute(
            `INSERT INTO fantasy_stats.directory_errors_ids (id, error_message, retry_count)
             VALUES (?, ?, 1)
             ON DUPLICATE KEY UPDATE
                error_message = VALUES(error_message),
                retry_count = retry_count + 1,
                failed_at = CURRENT_TIMESTAMP`,
            [playerId, error.message.substring(0, 500)]
        );
        
        return { success: false, id: playerId, error: error.message };
    }
}

export default async function runGetPlayersInfo(runId) {
    const logger = createLogger('get-players-info');
    
    console.log('\n' + '='.repeat(60));
    console.log('🏁 STARTING GET PLAYERS INFO');
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
             VALUES (?, 'get-players-info', 'running')`,
            [runId]
        );
        
        // STEP 1: Check for any errors from previous runs
        console.log('\n🔍 Step 1: Checking for players in error queue...');
        const [errorPlayers] = await connection.execute(
            'SELECT id FROM fantasy_stats.directory_errors_ids'
        );
        
        let retryResults = [];
        if (errorPlayers.length > 0) {
            console.log(`   Found ${errorPlayers.length} players to retry`);
            
            // Fetch bootstrap data to get full player info
            const bootstrapData = await fetchBootstrapData();
            const playersMap = new Map(
                bootstrapData.elements.map(p => [p.id, p])
            );
            
            for (const errorPlayer of errorPlayers) {
                const playerId = errorPlayer.id;
                const playerData = playersMap.get(playerId);
                
                if (playerData) {
                    logger.info(`   🔄 Retrying player ${playerId}...`);
                    const result = await processPlayer(playerData, connection, logger);
                    retryResults.push(result);
                } else {
                    logger.warn(`   ⚠️ Player ${playerId} not found in bootstrap data`);
                }
            }
            
            const retrySuccess = retryResults.filter(r => r.success).length;
            console.log(`   ✅ Retry complete: ${retrySuccess} successful, ${retryResults.length - retrySuccess} failed again`);
        } else {
            console.log('   ✅ No players in error queue');
        }
        
        // STEP 2: Fetch fresh bootstrap data for new players
        console.log('\n📥 Step 2: Fetching fresh bootstrap data...');
        const bootstrapData = await fetchBootstrapData();
        
        if (!bootstrapData.elements || !Array.isArray(bootstrapData.elements)) {
            throw new Error('Invalid bootstrap data format: missing elements array');
        }
        
        // Get existing players to avoid reprocessing
        const [existingPlayers] = await connection.execute(
            'SELECT id FROM fantasy_stats.players_directory'
        );
        const existingIds = new Set(existingPlayers.map(p => p.id));
        
        // Filter out players we already have
        const newPlayers = bootstrapData.elements.filter(p => !existingIds.has(p.id));
        
        console.log(`\n📊 Found ${bootstrapData.elements.length} total players`);
        console.log(`   Already processed: ${existingIds.size}`);
        console.log(`   New players to process: ${newPlayers.length}`);
        
        if (newPlayers.length === 0) {
            console.log('\n📭 No new players to process - exiting');
            
            // Update sync log
            await connection.execute(
                `UPDATE fantasy_stats.fantasy_sync_log 
                 SET status = 'success', completed_at = NOW()
                 WHERE run_id = ?`,
                [runId]
            );
            
            return { success: true, processed: 0, errors: 0 };
        }
        
        // STEP 3: Process new players
        console.log('\n⚙️ Step 3: Processing new players...');
        
        let processed = 0;
        let errors = 0;
        const results = [];
        
        for (let i = 0; i < newPlayers.length; i++) {
            const player = newPlayers[i];
            
            // Log progress every BATCH_SIZE players
            if (i > 0 && i % BATCH_SIZE === 0) {
                console.log(`   Progress: ${i}/${newPlayers.length} players processed`);
            }
            
            const result = await processPlayer(player, connection, logger);
            results.push(result);
            
            if (result.success) {
                processed++;
            } else {
                errors++;
            }
            
            // Small delay to avoid overwhelming the API (though we're not calling API per player)
            await new Promise(resolve => setTimeout(resolve, 50));
        }
        
        // STEP 4: Summary
        console.log('\n' + '='.repeat(60));
        console.log('📊 PROCESSING COMPLETE');
        console.log('='.repeat(60));
        console.log(`   Total players in bootstrap: ${bootstrapData.elements.length}`);
        console.log(`   New players processed: ${processed}`);
        console.log(`   Errors: ${errors}`);
        
        if (errors > 0) {
            console.log('\n❌ Players with errors (check directory_errors_ids table):');
            const failedPlayers = results.filter(r => !r.success);
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
             SET status = ?, players_processed = ?, errors = ?, completed_at = NOW()
             WHERE run_id = ?`,
            [errors === 0 ? 'success' : 'failed', processed, errors, runId]
        );
        
        console.log('\n✅ GET PLAYERS INFO COMPLETED');
        console.log('='.repeat(60));
        
        return {
            success: errors === 0,
            processed,
            errors,
            totalPlayers: bootstrapData.elements.length
        };
        
    } catch (error) {
        logger.error('Fatal error:', error);
        
        // Update sync log with failure
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
    const runId = `get-players-info-${Date.now()}`;
    
    runGetPlayersInfo(runId).catch(error => {
        console.error('❌ Fatal error:', error);
        process.exit(1);
    });
}