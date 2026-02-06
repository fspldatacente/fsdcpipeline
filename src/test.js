console.log('🚀 Testing RSL API data fetch...');

async function main() {
  try {
    console.log('1. Fetching from RSL API...');
    const response = await fetch('https://en.fantasy.spl.com.sa/api/bootstrap-static/');
    console.log('API Status:', response.status, response.statusText);
    
    const data = await response.json();
    const chips = data.chips || [];
    console.log(`2. Found ${chips.length} chips in response`);
    
    // Show first 3 chips
    if (chips.length > 0) {
      console.log('3. Sample chips (first 3):');
      chips.slice(0, 3).forEach((chip, i) => {
        console.log(`   Chip ${i + 1}:`, {
          id: chip.id,
          name: chip.name,
          chip_name: chip.chip_name,
          event: chip.event
        });
      });
    }
    
    // DYNAMIC IMPORT - fixes the issue
    console.log('4. Testing TiDB connection...');
    const mysql = await import('mysql2/promise');
    
    try {
      const connection = await mysql.default.createConnection({
        host: process.env.TIDB_HOST,
        port: process.env.TIDB_PORT || 4000,
        user: process.env.TIDB_USER,
        password: process.env.TIDB_PASSWORD,
        database: process.env.TIDB_DATABASE || 'fsdc',
        ssl: { rejectUnauthorized: true },
        connectTimeout: 10000
      });
      
      console.log('✅ TiDB connection successful!');
      
      // Create table
      await connection.execute(`
        CREATE TABLE IF NOT EXISTS chips_test (
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
      
      // Insert chips
      if (chips.length > 0) {
        const stmt = await connection.prepare(
          'INSERT INTO chips_test (chip_id, name, chip_name, event, avg_points, status) VALUES (?, ?, ?, ?, ?, ?)'
        );
        
        for (const chip of chips.slice(0, 10)) { // Only insert first 10 for test
          await stmt.execute([
            chip.id || 0,
            chip.name || '',
            chip.chip_name || '',
            chip.event || 0,
            chip.avg_points || 0,
            chip.status || ''
          ]);
        }
        
        await stmt.close();
        console.log(`✅ Inserted ${Math.min(chips.length, 10)} chips into TiDB`);
      }
      
      await connection.end();
      
    } catch (dbError) {
      console.log('❌ TiDB error:', dbError.message);
    }
    
  } catch (error) {
    console.log('❌ Test failed:', error.message);
  }
}

// Run the async function
main();
