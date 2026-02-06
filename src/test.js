console.log('🚀 Simple test starting...');

// Test 1: Environment variables
console.log('TIDB_HOST:', process.env.TIDB_HOST ? 'Set' : 'NOT SET');
console.log('TIDB_DATABASE:', process.env.TIDB_DATABASE);

// Test 2: Try to fetch from RSL API
try {
  console.log('Testing RSL API fetch...');
  const response = await fetch('https://en.fantasy.spl.com.sa/api/bootstrap-static/');
  console.log('API Response status:', response.status);
} catch (error) {
  console.log('API Fetch error:', error.message);
}

// Test 3: Try MySQL connection
import mysql from 'mysql2/promise';

try {
  console.log('Testing TiDB connection...');
  const connection = await mysql.createConnection({
    host: process.env.TIDB_HOST,
    port: process.env.TIDB_PORT || 4000,
    user: process.env.TIDB_USER,
    password: process.env.TIDB_PASSWORD,
    database: process.env.TIDB_DATABASE || 'fsdc',
    ssl: { rejectUnauthorized: true },
    connectTimeout: 10000
  });
  console.log('✅ TiDB connection successful!');
  await connection.end();
} catch (error) {
  console.log('❌ TiDB connection error:', error.message);
}

console.log('Test completed');
