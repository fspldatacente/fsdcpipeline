console.log('🚀 Testing RSL API data fetch...');

try {
  console.log('1. Fetching from RSL API...');
  const response = await fetch('https://en.fantasy.spl.com.sa/api/bootstrap-static/');
  console.log('API Status:', response.status, response.statusText);
  
  const data = await response.json();
  
  // Check if chips array exists
  const chips = data.chips || [];
  console.log(`2. Found ${chips.length} chips in response`);
  
  // Show first 3 chips as sample
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
  
  // Test TiDB connection
  console.log('4. Testing TiDB connection...');
  import mysql from 'mysql2/promise';
  
  try {
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
    
    // Create chips table if not exists
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS chips (
        id INT AUTO_INCREMENT PRIMARY KEY,
        chip_id INT,
        name VARCHAR(100),
        chip_name VARCHAR(100),
        event INT,
        avg_points DECIMAL(5,1),
        status VARCHAR(50),
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_chip_event (chip_id, event)
      )
    `);
    
    // Insert chips
    let inserted = 0;
    for (const chip of chips) {
      try {
        await connection.execute(
          `INSERT INTO chips (chip_id, name, chip_name, event, avg_points, status) 
           VALUES (?, ?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE 
           name = VALUES(name),
           chip_name = VALUES(chip_name),
           avg_points = VALUES(avg_points),
           status = VALUES(status)`,
          [
            chip.id || 0,
            chip.name || '',
            chip.chip_name || '',
            chip.event || 0,
            chip.avg_points || 0,
            chip.status || ''
          ]
        );
        inserted++;
      } catch (insertError) {
        console.log(`   Skipped chip ${chip.id}:`, insertError.message);
      }
    }
    
    // Get count from database
    const [result] = await connection.execute('SELECT COUNT(*) as count FROM chips');
    console.log(`5. Database: Inserted ${inserted} chips, total in DB: ${result[0].count}`);
    
    await connection.end();
    
  } catch (dbError) {
    console.log('❌ TiDB error:', dbError.message);
  }
  
} catch (error) {
  console.log('❌ Test failed:', error.message);
}
