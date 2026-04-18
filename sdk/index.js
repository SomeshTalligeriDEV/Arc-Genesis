/**
 * Arc Genesis SDK v3 — Node.js Client
 * Capture and send SQL queries for real-time analysis.
 * Supports auto-instrumentation for Prisma, Sequelize, and raw pools.
 *
 * Usage:
 *   const arc = require('./arc-genesis-sdk');
 *   const client = arc.init({ endpoint: 'http://localhost:8000', appName: 'my-api' });
 *
 *   // Manual capture
 *   await client.captureQuery('SELECT * FROM users', { user: 'admin' });
 *
 *   // Auto-instrument Prisma
 *   arc.instrumentPrisma(prismaClient, client);
 *
 *   // Auto-instrument Sequelize
 *   arc.instrumentSequelize(sequelizeInstance, client);
 *
 *   // Wrap a pg/mysql pool
 *   arc.wrapPool(pool, client);
 */

class ArcGenesisClient {
  constructor(options = {}) {
    this.endpoint = (options.endpoint || 'http://localhost:8000').replace(/\/$/, '');
    this.appName = options.appName || 'unknown';
    this.autoCapture = options.autoCapture !== false;
    this.queue = [];
    this.batchSize = options.batchSize || 5;
    this.flushInterval = options.flushInterval || 3000;
    this._timer = null;
    this._stats = { captured: 0, sent: 0, errors: 0 };

    if (this.autoCapture) {
      this._startAutoFlush();
    }
  }

  /**
   * Capture a SQL query for analysis.
   */
  async captureQuery(sql, metadata = {}) {
    const startTime = Date.now();
    const payload = {
      sql,
      source: 'sdk',
      metadata: {
        app: this.appName,
        captured_at: new Date().toISOString(),
        ...metadata,
      },
      execution_time_ms: metadata.execution_time_ms || 0,
      rows_scanned: metadata.rows_scanned || 0,
      user_name: metadata.user || '',
      database_name: metadata.database || '',
      app_name: this.appName,
    };

    this._stats.captured++;

    if (this.autoCapture) {
      this.queue.push(payload);
      if (this.queue.length >= this.batchSize) {
        return this.flush();
      }
      return { status: 'queued', queue_size: this.queue.length };
    }

    return this._send('/ingest', payload);
  }

  /**
   * Capture an error with SQL context.
   */
  async captureError(error, context = {}) {
    const sql = context.sql || 'UNKNOWN';
    return this.captureQuery(sql, {
      error: error.message,
      error_type: error.constructor.name,
      stack: error.stack?.split('\n').slice(0, 3).join('\n'),
      ...context,
    });
  }

  /**
   * Review a query synchronously.
   */
  async review(sql) {
    return this._send('/review', { sql });
  }

  /**
   * Ask a natural language question.
   */
  async ask(question, context = '') {
    return this._send('/ask', { question, context });
  }

  /**
   * Flush queued queries.
   */
  async flush() {
    if (this.queue.length === 0) return { status: 'empty' };
    const batch = this.queue.splice(0, 20);
    try {
      const result = await this._send('/ingest/batch', { queries: batch });
      this._stats.sent += batch.length;
      return result;
    } catch (e) {
      this._stats.errors++;
      // Re-queue on failure
      this.queue.unshift(...batch.slice(0, 10));
      throw e;
    }
  }

  async stats() { return this._get('/stats'); }
  async events(limit = 20) { return this._get(`/events?limit=${limit}`); }
  async threats(limit = 10) { return this._get(`/threats?limit=${limit}`); }
  async warehouseStatus() { return this._get('/warehouse/status'); }

  /**
   * Express middleware — auto-capture SQL from request bodies.
   */
  expressMiddleware() {
    const client = this;
    return (req, res, next) => {
      if (req.body?.sql) {
        client.captureQuery(req.body.sql, {
          method: req.method,
          path: req.path,
          ip: req.ip,
        }).catch(() => {});
      }
      next();
    };
  }

  get clientStats() {
    return { ...this._stats, queue_size: this.queue.length };
  }

  // ─── Internal ────────────────────────────────

  _startAutoFlush() {
    this._timer = setInterval(() => {
      if (this.queue.length > 0) {
        this.flush().catch(err => {
          console.warn('[arc-genesis] flush failed:', err.message);
        });
      }
    }, this.flushInterval);
    if (this._timer.unref) this._timer.unref();
  }

  async _send(path, body) {
    const res = await fetch(`${this.endpoint}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`Arc Genesis API error: ${res.status}`);
    return res.json();
  }

  async _get(path) {
    const res = await fetch(`${this.endpoint}${path}`);
    if (!res.ok) throw new Error(`Arc Genesis API error: ${res.status}`);
    return res.json();
  }

  destroy() {
    if (this._timer) clearInterval(this._timer);
  }
}


// ═══════════════════════════════════════════════════════════
// AUTO-INSTRUMENTATION
// ═══════════════════════════════════════════════════════════

/**
 * Instrument a Prisma client to auto-capture all queries.
 *
 * Usage:
 *   const { PrismaClient } = require('@prisma/client');
 *   const prisma = new PrismaClient();
 *   instrumentPrisma(prisma, arcClient);
 */
function instrumentPrisma(prisma, arcClient) {
  if (!prisma || !prisma.$use) {
    console.warn('[arc-genesis] Invalid Prisma client — $use not available. Ensure Prisma >= 4.0');
    return;
  }

  prisma.$use(async (params, next) => {
    const start = Date.now();
    let error = null;

    try {
      const result = await next(params);
      const elapsed = Date.now() - start;

      // Reconstruct approximate SQL from Prisma operation
      const sql = `/* Prisma */ ${params.action.toUpperCase()} ${params.model || 'unknown'} ${
        params.args?.where ? 'WHERE ' + JSON.stringify(params.args.where) : ''
      }`.trim();

      arcClient.captureQuery(sql, {
        orm: 'prisma',
        model: params.model,
        action: params.action,
        execution_time_ms: elapsed,
        args_preview: JSON.stringify(params.args || {}).slice(0, 200),
      }).catch(() => {});

      return result;
    } catch (e) {
      error = e;
      arcClient.captureError(e, {
        sql: `/* Prisma Error */ ${params.action} ${params.model}`,
        orm: 'prisma',
        model: params.model,
        action: params.action,
      }).catch(() => {});
      throw e;
    }
  });

  console.log('[arc-genesis] ✓ Prisma instrumented');
}


/**
 * Instrument a Sequelize instance to auto-capture queries.
 *
 * Usage:
 *   const { Sequelize } = require('sequelize');
 *   const sequelize = new Sequelize(/* ... *\/);
 *   instrumentSequelize(sequelize, arcClient);
 */
function instrumentSequelize(sequelize, arcClient) {
  if (!sequelize || !sequelize.addHook) {
    console.warn('[arc-genesis] Invalid Sequelize instance');
    return;
  }

  sequelize.addHook('afterQuery', (options, query) => {
    const sql = typeof query === 'string' ? query : (options?.sql || options?.query || '');
    if (sql) {
      arcClient.captureQuery(sql, {
        orm: 'sequelize',
        type: options?.type,
        execution_time_ms: options?.benchmark ? options._benchmarkStart
          ? Date.now() - options._benchmarkStart : 0 : 0,
      }).catch(() => {});
    }
  });

  // Also capture errors
  sequelize.addHook('afterQueryError', (error, options) => {
    const sql = options?.sql || options?.query || 'UNKNOWN';
    arcClient.captureError(error, {
      sql,
      orm: 'sequelize',
    }).catch(() => {});
  });

  console.log('[arc-genesis] ✓ Sequelize instrumented');
}


/**
 * Wrap a database pool (pg, mysql2, etc.) to auto-capture queries.
 *
 * Usage:
 *   const { Pool } = require('pg');
 *   const pool = new Pool({ /* ... *\/ });
 *   wrapPool(pool, arcClient);
 */
function wrapPool(pool, arcClient) {
  if (!pool || !pool.query) {
    console.warn('[arc-genesis] Invalid pool — .query() not found');
    return;
  }

  const originalQuery = pool.query.bind(pool);

  pool.query = function arcWrappedQuery(...args) {
    const sql = typeof args[0] === 'string' ? args[0] : (args[0]?.text || '');
    const start = Date.now();

    // Handle callback style
    const lastArg = args[args.length - 1];
    if (typeof lastArg === 'function') {
      const originalCallback = lastArg;
      args[args.length - 1] = function(err, result) {
        const elapsed = Date.now() - start;
        if (sql) {
          arcClient.captureQuery(sql, {
            pool: 'raw',
            execution_time_ms: elapsed,
            rows_scanned: result?.rowCount || 0,
            error: err ? err.message : undefined,
          }).catch(() => {});
        }
        originalCallback(err, result);
      };
      return originalQuery(...args);
    }

    // Handle promise style
    const promise = originalQuery(...args);
    if (promise && promise.then) {
      return promise.then(result => {
        const elapsed = Date.now() - start;
        if (sql) {
          arcClient.captureQuery(sql, {
            pool: 'raw',
            execution_time_ms: elapsed,
            rows_scanned: result?.rowCount || 0,
          }).catch(() => {});
        }
        return result;
      }).catch(err => {
        if (sql) {
          arcClient.captureError(err, { sql, pool: 'raw' }).catch(() => {});
        }
        throw err;
      });
    }

    return promise;
  };

  console.log('[arc-genesis] ✓ Pool wrapped');
}


// ─── Factory ─────────────────────────────────────────────

function init(options = {}) {
  return new ArcGenesisClient(options);
}

module.exports = {
  init,
  ArcGenesisClient,
  instrumentPrisma,
  instrumentSequelize,
  wrapPool,
};
