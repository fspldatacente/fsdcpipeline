// src/fixtures/index.js
// Orchestrator that runs:
// 1. Fetch fixtures (intelligently - first run vs subsequent)
// 2. Process one unprocessed fixture for stats

import dbClient from '../database/tidb-client.js';
import runFetchFixtures from './fetch-fixtures.js';
import runStatsProcessing from './process-stats.js';

async function runAllFixturesFetchers() {
    const timestamp = Date.now();
    const fetchRunId = `fetch-${timestamp}`;
    const processRunId = `process-${timestamp}`;
    
    console.log('='.repeat(60));
    console.log('🚀 FSDC FIXTURES PIPELINE (Season 53)');
    console.log('='.repeat(60));
    console.log(`Start time: ${new Date().toISOString()}`);
    console.log(`Fetch Run ID: ${fetchRunId}`);
    console.log(`Process Run ID: ${processRunId}`);
    console.log('-'.repeat(60));
    
    const startTime = Date.now();
    let connection = null;
    
    try {
        // Initialize ONE connection for the entire pipeline
        await dbClient.initialize();
        connection = await dbClient.getConnection();
        console.log('✅ Single database connection established for entire pipeline');
        
        // STEP 1: Fetch fixtures (smart logic - first run vs subsequent)
        console.log('\n📥 STEP 1: Fetching fixtures...');
        const fetchResult = await runFetchFixtures(fetchRunId, connection);
        
        console.log('-'.repeat(60));
        console.log('📊 FETCH RESULTS:');
        console.log(`✅ Fetch completed: ${fetchResult.success ? 'Success' : 'Failed'}`);
        
        // STEP 2: Process one unprocessed fixture
        console.log('\n' + '-'.repeat(60));
        console.log('📊 STEP 2: Processing stats for one fixture (oldest first)...');
        
        const processResult = await runStatsProcessing(processRunId, connection);
        
        console.log('-'.repeat(60));
        console.log('📊 PROCESS RESULTS:');
        if (processResult.success) {
            console.log(`✅ Processing completed:`);
            console.log(`   Processed: ${processResult.processed}`);
            console.log(`   Failed: ${processResult.failed}`);
        } else {
            console.log(`⚠️ Processing had failures:`);
            console.log(`   Processed: ${processResult.processed}`);
            console.log(`   Failed: ${processResult.failed}`);
        }
        
        const endTime = Date.now();
        const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
        
        console.log('-'.repeat(60));
        console.log(`⏱️  Total duration: ${durationSeconds} seconds`);
        console.log('='.repeat(60));
        
        return {
            success: fetchResult.success && processResult.success,
            fetch: fetchResult,
            process: processResult,
            duration: durationSeconds
        };
        
    } catch (error) {
        console.error('💥 Fatal error in fixtures pipeline:', error);
        throw error;
    } finally {
        // Close the single connection at the very end
        if (connection) {
            await connection.release();
            console.log('✅ Database connection released');
        }
        await dbClient.close();
    }
}

// If running directly
if (import.meta.url === `file://${process.argv[1]}`) {
    runAllFixturesFetchers()
        .then(result => {
            process.exit(result.success ? 0 : 1);
        })
        .catch(error => {
            console.error(error);
            process.exit(1);
        });
}

export default runAllFixturesFetchers;