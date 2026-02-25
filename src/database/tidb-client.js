// src/database/tidb-client.js
// Database connection helper for TiDB
// This centralizes all database connections and queries

import mysql from 'mysql2/promise';

class TiDBClient {
    constructor() {
        this.pool = null;
        this.connected = false;
    }

    /**
     * Initialize the connection pool
     * Call this once at startup
     */
    async initialize() {
        if (this.pool) return this.pool;

        // Check for required environment variables
        const required = ['TIDB_HOST', 'TIDB_USER', 'TIDB_PASSWORD'];
        const missing = required.filter(key => !process.env[key]);
        
        if (missing.length > 0) {
            throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
        }

        try {
            this.pool = mysql.createPool({
                host: process.env.TIDB_HOST,
                port: parseInt(process.env.TIDB_PORT || '4000'),
                user: process.env.TIDB_USER,
                password: process.env.TIDB_PASSWORD,
                database: 'fixtures',  // Using fixtures database for all fixture-related tables
                waitForConnections: true,
                connectionLimit: 5,
                queueLimit: 0,
                enableKeepAlive: true,
                keepAliveInitialDelay: 10000,
                ssl: {
                    minVersion: 'TLSv1.2',
                    rejectUnauthorized: true
                }
            });

            // Test the connection
            const connection = await this.pool.getConnection();
            console.log('✅ TiDB connection pool established to fixtures database');
            connection.release();
            this.connected = true;
            
            return this.pool;
        } catch (error) {
            console.error('❌ Failed to initialize TiDB connection:', error.message);
            throw error;
        }
    }

    /**
     * Get a connection from the pool
     */
    async getConnection() {
        if (!this.pool) {
            await this.initialize();
        }
        return await this.pool.getConnection();
    }

    /**
     * Execute a query with automatic connection management
     */
    async query(sql, params = []) {
        let connection;
        try {
            connection = await this.getConnection();
            const [results] = await connection.execute(sql, params);
            return results;
        } catch (error) {
            console.error('Query error:', error.message);
            throw error;
        } finally {
            if (connection) connection.release();
        }
    }

    /**
     * Execute multiple queries in a transaction
     */
    async transaction(callback) {
        let connection;
        try {
            connection = await this.getConnection();
            await connection.beginTransaction();
            
            const result = await callback(connection);
            
            await connection.commit();
            return result;
        } catch (error) {
            if (connection) await connection.rollback();
            throw error;
        } finally {
            if (connection) connection.release();
        }
    }

    /**
     * Create tables if they don't exist
     */
    async initializeSchema() {
        console.log('Initializing fixtures database schema...');
        
        // Create finished_matches table for completed games with scores
        await this.query(`
            CREATE TABLE IF NOT EXISTS finished_matches (
                id INT AUTO_INCREMENT PRIMARY KEY,
                fixture_id INT UNIQUE,
                round_num INT,
                home_team VARCHAR(100),
                away_team VARCHAR(100),
                home_score INT,
                away_score INT,
                match_date DATETIME,
                status VARCHAR(50),
                full_data JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_round (round_num),
                INDEX idx_date (match_date)
            )
        `);

        // Create upcoming_fixtures table for scheduled/unfinished games
        await this.query(`
            CREATE TABLE IF NOT EXISTS upcoming_fixtures (
                id INT AUTO_INCREMENT PRIMARY KEY,
                fixture_id INT UNIQUE,
                round_num INT,
                home_team VARCHAR(100),
                away_team VARCHAR(100),
                kickoff_time DATETIME,
                status VARCHAR(50),
                full_data JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_round (round_num),
                INDEX idx_kickoff (kickoff_time)
            )
        `);

        // Create sync_log table to track pipeline runs
        await this.query(`
            CREATE TABLE IF NOT EXISTS sync_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(50) UNIQUE,
                source VARCHAR(50),
                finished_fetched INT DEFAULT 0,
                unfinished_fetched INT DEFAULT 0,
                status ENUM('running', 'success', 'failed') DEFAULT 'running',
                error_message TEXT,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP NULL,
                INDEX idx_status (status)
            )
        `);

        console.log('✅ Fixtures database schema initialized with tables:');
        console.log('   - finished_matches');
        console.log('   - upcoming_fixtures');
        console.log('   - sync_log');
    }

    /**
     * Close all connections
     */
    async close() {
        if (this.pool) {
            await this.pool.end();
            this.pool = null;
            this.connected = false;
            console.log('Database connection pool closed');
        }
    }
}

// Export a singleton instance
const dbClient = new TiDBClient();
export default dbClient;