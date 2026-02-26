// src/fixtures/index.js
// Orchestrator that runs:
// 1. Fetch finished matches
// 2. Fetch unfinished fixtures
// 3. Process one unprocessed fixture for stats

import runFinishedMatchesFetch from './fetch-finished.js';
import runUnfinishedFixturesFetch from './fetch-unfinished.js';
import runStatsProcessing from './process-stats.js';

async function runAllFixturesFetchers() {
    const timestamp = Date.now();
    const finishedRunId = `finished-${timestamp}`;
    const unfinishedRunId = `unfinished-${timestamp}`;
    const processRunId = `process-${timestamp}`;
    
    console.log('='.repeat(60));
    console.log('🚀 FSDC FIXTURES PIPELINE (Season 53)');
    console.log('='.repeat(60));
    console.log(`Start time: ${new Date().toISOString()}`);
    console.log(`Finished Run ID: ${finishedRunId}`);
    console.log(`Unfinished Run ID: ${unfinishedRunId}`);
    console.log(`Process Run ID: ${processRunId}`);
    console.log('-'.repeat(60));
    
    const startTime = Date.now();
    
    try {
        // Step 1 & 2: Run both fetchers in parallel
        console.log('\n📥 STEP 1 & 2: Fetching fixtures...');
        const [finishedResult, unfinishedResult] = await Promise.allSettled([
            runFinishedMatchesFetch(finishedRunId),
            runUnfinishedFixturesFetch(unfinishedRunId)
        ]);
        
        console.log('-'.repeat(60));
        console.log('📊 FETCH RESULTS:');
        
        // Process finished matches result
        if (finishedResult.status === 'fulfilled') {
            console.log('\n✅ FINISHED MATCHES:');
            console.log(`   Total fetched: ${finishedResult.value.count}`);
            if (finishedResult.value.addedToUnprocessed !== undefined) {
                console.log(`   Added to unprocessed queue: ${finishedResult.value.addedToUnprocessed}`);
                console.log(`   Status records created: ${finishedResult.value.addedToUnprocessed}`);
            }
        } else {
            console.log('\n❌ FINISHED MATCHES FAILED:');
            console.log(`   Error: ${finishedResult.reason.message}`);
        }
        
        // Process unfinished fixtures result
        if (unfinishedResult.status === 'fulfilled') {
            console.log('\n✅ UNFINISHED FIXTURES:');
            console.log(`   Total fetched: ${unfinishedResult.value.count}`);
        } else {
            console.log('\n❌ UNFINISHED FIXTURES FAILED:');
            console.log(`   Error: ${unfinishedResult.reason.message}`);
        }
        
        // Step 3: Process one unprocessed fixture
        console.log('\n' + '-'.repeat(60));
        console.log('📊 STEP 3: Processing stats for one fixture (oldest first)...');
        
        const processResult = await runStatsProcessing(processRunId);
        
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
        
        const overallSuccess = 
            finishedResult.status === 'fulfilled' && 
            unfinishedResult.status === 'fulfilled';
        
        return {
            success: overallSuccess,
            finished: finishedResult.status === 'fulfilled' ? finishedResult.value : { error: finishedResult.reason.message },
            unfinished: unfinishedResult.status === 'fulfilled' ? unfinishedResult.value : { error: unfinishedResult.reason.message },
            process: processResult,
            duration: durationSeconds
        };
        
    } catch (error) {
        console.error('💥 Fatal error in fixtures pipeline:', error);
        throw error;
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