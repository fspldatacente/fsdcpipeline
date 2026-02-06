import mysql from 'mysql2/promise';

console.log('🚀 Starting FSDC Data Pipeline Test...');
console.log('Time:', new Date().toISOString());

async function main() {
  let connection;
  
  try {
    console.log('1. Testing TiDB connection...');
    
    // Create database connection
    connection = await mysql.createConnection({
      host: process.env.TIDB_HOST,
      port: process.env.TIDB_PORT || 4000,
      user: process.env.TIDB_USER,
      password: process.env.TIDB_PASSWORD,
      database: process.env.TIDB_DATABASE || 'fsdc',
      ssl: {
        minVersion: 'TLSv1.2',
        rejectUnauthorized: true
      }
    });
    
    console.log('✅ Connected to TiDB successfully!');
    
    // 2. Create test table
    console.log('2. Creating test table...');
    
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS test_chips (
        id INT AUTO_INCREMENT PRIMARY KEY,
        chip_id INT,
        name VARCHAR(100),
        chip_name VARCHAR(100),
        event INT,
        avg_points DECIMAL(5,1),
        status VARCHAR(50),
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // 3. Fetch data from RSL API
    console.log('3. Fetching data from RSL API...');
    
    const response = await fetch('https://en.fantasy.spl.com.sa/api/bootstrap-static/', {
      headers: {
        'User-Agent': 'FSDC-Pipeline/1.0',
        'Accept': 'application/json'
      }
    });
    
    if (!response.ok) {
      throw new Error(`API request failed: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    const chips = data.chips || [];
    
    console.log(`📊 Fetched ${chips.length} chips from API`);
    
    // 4. Store chips in database
    console.log('4. Storing chips in TiDB...');
    
    for (const chip of chips) {
      await connection.execute(
        `INSERT INTO test_chips (chip_id, name, chip_name, event, avg_points, status) 
         VALUES (?, ?, ?, ?, ?, ?)`,
        [
          chip.id || 0,
          chip.name || '',
          chip.chip_name || '',
          chip.event || 0,
          chip.avg_points || 0,
          chip.status || ''
        ]
      );
    }
    
    // 5. Log summary
    const [result] = await connection.execute('SELECT COUNT(*) as count FROM test_chips');
    console.log(`✅ Successfully stored ${chips.length} chips`);
    console.log(`📈 Total chips in database: ${result[0].count}`);
    
    console.log('✅ Test completed successfully!');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.error('Stack:', error.stack);
    process.exit(1);
    
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}

// Run the test
main();
