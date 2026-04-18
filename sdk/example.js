/**
 * Arc Genesis SDK — Real Usage Example
 * 
 * This shows how to integrate Arc Genesis into an actual Node.js/Express app
 * to capture real queries from your application.
 * 
 * Run: node example.js
 */

const { init } = require('./index');

// ─── Initialize ─────────────────────────────────────────
const arc = init({
  endpoint: 'http://localhost:8000',
  appName: 'my-express-api',
  batchSize: 3,           // Send batch every 3 queries
  flushInterval: 5000,    // Or every 5 seconds
});

// ─── Example 1: Capture a query from your ORM/DB layer ──
async function captureFromORM() {
  // Imagine this comes from your Knex/Sequelize/Prisma query logger
  const sql = `
    SELECT o.id, c.name, SUM(oi.price) as total
    FROM orders o
    JOIN customers c ON c.id = o.customer_id
    JOIN order_items oi ON oi.order_id = o.id
    WHERE o.status = 'completed'
    GROUP BY o.id, c.name
    ORDER BY total DESC
    LIMIT 100
  `;

  await arc.captureQuery(sql, {
    endpoint: '/api/orders/summary',
    method: 'GET',
    userId: 'user-123',
  });

  console.log('✅ Good query captured');
}

// ─── Example 2: Capture a bad query ─────────────────────
async function captureBadQuery() {
  const sql = 'SELECT * FROM orders JOIN customers';

  await arc.captureQuery(sql, {
    endpoint: '/api/legacy/report',
    method: 'GET',
    warning: 'This query is from legacy code',
  });

  console.log('⚠️  Bad query captured');
}

// ─── Example 3: Capture an error with SQL context ───────
async function captureQueryError() {
  try {
    // Simulating a failed database query
    throw new Error('Query execution exceeded timeout (30s)');
  } catch (err) {
    await arc.captureError(err, {
      sql: 'SELECT * FROM user_transactions WHERE amount > 10000',
      endpoint: '/api/transactions/large',
      database: 'analytics-prod',
    });

    console.log('🔴 Error captured');
  }
}

// ─── Example 4: Synchronous review (get instant decision) ──
async function reviewBeforeExecute() {
  const sql = 'DELETE FROM logs';

  const result = await arc.review(sql);
  console.log('\n📋 Review result:');
  console.log(`   Decision: ${result.decision}`);
  console.log(`   Risk: ${result.risk_level}`);
  console.log(`   Issues: ${result.issues?.join(', ')}`);

  if (result.decision === 'REJECT' || result.status === 'BLOCKED') {
    console.log('   ❌ Query BLOCKED — not executing');
  } else {
    console.log('   ✅ Query safe to execute');
  }
}

// ─── Example 5: Ask a question ──────────────────────────
async function askQuestion() {
  const answer = await arc.ask('Why is SELECT * bad for production?');
  console.log('\n💬 AI Answer:', answer.answer);
}

// ─── Example 6: Express middleware ──────────────────────
/*
const express = require('express');
const app = express();

// Add Arc Genesis middleware — auto-captures any SQL in request body
app.use(express.json());
app.use(arc.expressMiddleware());

app.post('/api/query', (req, res) => {
  // The middleware already captured req.body.sql
  res.json({ status: 'ok' });
});
*/

// ─── Run all examples ───────────────────────────────────
async function main() {
  console.log('⚡ Arc Genesis SDK — Example Usage\n');

  await captureFromORM();
  await captureBadQuery();
  await captureQueryError();

  // Flush remaining queued queries
  const batchResult = await arc.flush();
  console.log('\n📦 Batch sent:', batchResult);

  await reviewBeforeExecute();
  await askQuestion();

  // Get system stats
  const stats = await arc.stats();
  console.log('\n📊 System stats:', stats);

  arc.destroy();
  console.log('\n✅ Done');
}

main().catch(console.error);
