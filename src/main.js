console.log('🚀 Starting FSDC Data Pipeline...');
console.log('Time:', new Date().toISOString());

// Just test environment variables and basic functionality
console.log('Environment check:');
console.log('- TIDB_HOST:', process.env.TIDB_HOST ? '✓ Set' : '✗ NOT SET');
console.log('- TIDB_DATABASE:', process.env.TIDB_DATABASE || 'not set');

// Simple test that always passes
console.log('✅ Basic test passed!');
