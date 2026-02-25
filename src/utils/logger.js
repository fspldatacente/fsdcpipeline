// src/utils/logger.js
// Simple logging utility

const LOG_LEVELS = {
    DEBUG: 0,
    INFO: 1,
    WARN: 2,
    ERROR: 3
};

const currentLogLevel = process.env.LOG_LEVEL || 'INFO';

class Logger {
    constructor(context) {
        this.context = context;
    }

    debug(...args) {
        if (LOG_LEVELS[currentLogLevel] <= LOG_LEVELS.DEBUG) {
            console.log(`🔍 [${this.context}]`, ...args);
        }
    }

    info(...args) {
        if (LOG_LEVELS[currentLogLevel] <= LOG_LEVELS.INFO) {
            console.log(`ℹ️ [${this.context}]`, ...args);
        }
    }

    success(...args) {
        if (LOG_LEVELS[currentLogLevel] <= LOG_LEVELS.INFO) {
            console.log(`✅ [${this.context}]`, ...args);
        }
    }

    warn(...args) {
        if (LOG_LEVELS[currentLogLevel] <= LOG_LEVELS.WARN) {
            console.warn(`⚠️ [${this.context}]`, ...args);
        }
    }

    error(...args) {
        if (LOG_LEVELS[currentLogLevel] <= LOG_LEVELS.ERROR) {
            console.error(`❌ [${this.context}]`, ...args);
        }
    }

    table(data) {
        console.table(data);
    }
}

export default function createLogger(context) {
    return new Logger(context);
}