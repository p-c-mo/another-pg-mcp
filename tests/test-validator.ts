import { QueryValidator } from '../src/queryValidator.js';

// Test the validator
const validator = new QueryValidator(true); // read-only mode

const testQueries = [
  // Should be blocked
  { query: 'SET search_path = public', expected: false },
  { query: 'SELECT * FROM users; SET work_mem = "256MB"', expected: false },
  { query: 'UPDATE users SET name = "test"', expected: false },
  { query: 'INSERT INTO users (name) VALUES ("test")', expected: false },
  { query: 'DELETE FROM users WHERE id = 1', expected: false },
  { query: 'CREATE TABLE test (id INT)', expected: false },
  { query: 'DROP TABLE users', expected: false },
  { query: 'ALTER TABLE users ADD COLUMN test VARCHAR(255)', expected: false },
  { query: 'GRANT SELECT ON users TO public', expected: false },
  { query: 'VACUUM ANALYZE users', expected: false },
  
  // Should be allowed
  { query: 'SELECT * FROM users', expected: true },
  { query: 'SELECT COUNT(*) FROM users WHERE active = true', expected: true },
  { query: 'BEGIN; SELECT * FROM users; COMMIT;', expected: true },
  { query: 'BEGIN; SELECT * FROM users FOR UPDATE; ROLLBACK;', expected: true },
  { query: 'WITH cte AS (SELECT * FROM users) SELECT * FROM cte', expected: true },
  { query: 'EXPLAIN SELECT * FROM users', expected: true },
];

console.log('Testing QueryValidator...\n');

testQueries.forEach(({ query, expected }) => {
  const result = validator.validate(query);
  const passed = result.isValid === expected;
  console.log(`${passed ? '✓' : '✗'} Query: ${query.substring(0, 50)}...`);
  if (!passed) {
    console.log(`  Expected: ${expected}, Got: ${result.isValid}`);
    if (result.error) {
      console.log(`  Error: ${result.error}`);
    }
  }
});

console.log('\nTesting with read-only mode disabled...\n');
const validatorWriteMode = new QueryValidator(false);

const writeQueries = [
  // Should still be blocked
  { query: 'SET search_path = public', expected: false },
  { query: 'RESET ALL', expected: false },
  
  // Should now be allowed
  { query: 'UPDATE users SET name = "test"', expected: true },
  { query: 'INSERT INTO users (name) VALUES ("test")', expected: true },
  { query: 'DELETE FROM users WHERE id = 1', expected: true },
  { query: 'CREATE TABLE test (id INT)', expected: true },
];

writeQueries.forEach(({ query, expected }) => {
  const result = validatorWriteMode.validate(query);
  const passed = result.isValid === expected;
  console.log(`${passed ? '✓' : '✗'} Query: ${query.substring(0, 50)}...`);
  if (!passed) {
    console.log(`  Expected: ${expected}, Got: ${result.isValid}`);
    if (result.error) {
      console.log(`  Error: ${result.error}`);
    }
  }
});