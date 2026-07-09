const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const config = require('./config');

const pool = new Pool({ connectionString: config.databaseUrl });

async function migrate() {
  const dir = path.join(__dirname, '..', 'migrations');
  const files = fs.readdirSync(dir).filter((f) => f.endsWith('.sql')).sort();
  for (const file of files) {
    const sql = fs.readFileSync(path.join(dir, file), 'utf8');
    console.log(`Applying migration: ${file}`);
    await pool.query(sql);
  }
  console.log('Migrations complete.');
}

if (require.main === module && process.argv[2] === 'migrate') {
  migrate()
    .then(() => process.exit(0))
    .catch((err) => {
      console.error('Migration failed:', err);
      process.exit(1);
    });
}

module.exports = { pool, migrate };
