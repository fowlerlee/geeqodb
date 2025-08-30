1.1 CREATE TABLE Command
Test First: Create tests for CREATE TABLE parsing and execution
Implementation:
Extend SQL parser in src/query/planner.zig to parse CREATE TABLE statements
Add table schema storage in src/core/database.zig
Implement table creation in RocksDB storage layer
Add table metadata management
1.2 INSERT INTO Command
Test First: Create tests for INSERT INTO parsing and execution
Implementation:
Extend SQL parser to parse INSERT INTO statements
Implement row insertion in RocksDB storage
Add data validation against table schemas
Implement auto-incrementing primary keys
1.3 SELECT Command Enhancement
Test First: Create tests for SELECT with actual table data
Implementation:
Replace mock data with actual table data retrieval
Implement proper column projection
Add WHERE clause filtering
Support for multiple table joins
Phase 2: Data Definition Language (DDL) Support (Priority 2)
2.1 ALTER TABLE Command
Test First: Create tests for ALTER TABLE operations
Implementation:
Add column operations (ADD, DROP, MODIFY)
Implement constraint modifications
Add table renaming support
2.2 DROP TABLE Command
Test First: Create tests for DROP TABLE operations
Implementation:
Implement table deletion from storage
Clean up associated indexes and constraints
Add cascade delete support
2.3 CREATE INDEX Command
Test First: Create tests for index creation
Implementation:
Extend existing index infrastructure
Add index metadata management
Implement index creation on existing tables
Phase 3: Data Manipulation Language (DML) Support (Priority 3)
3.1 UPDATE Command
Test First: Create tests for UPDATE operations
Implementation:
Parse UPDATE statements
Implement row modification in storage
Add WHERE clause support for updates
Implement transaction safety
3.2 DELETE Command
Test First: Create tests for DELETE operations
Implementation:
Parse DELETE statements
Implement row deletion from storage
Add WHERE clause support
Implement cascade deletes
Phase 4: Advanced Query Features (Priority 4)
4.1 JOIN Support
Test First: Create tests for various JOIN types
Implementation:
INNER JOIN, LEFT JOIN, RIGHT JOIN
Implement join algorithms (nested loop, hash join)
Add join condition parsing
4.2 Aggregate Functions
Test First: Create tests for aggregate functions
Implementation:
COUNT, SUM, AVG, MIN, MAX
GROUP BY clause support
HAVING clause support
4.3 Subqueries and CTEs
Test First: Create tests for subqueries
Implementation:
Correlated and non-correlated subqueries
Common Table Expressions (CTEs)
Recursive CTEs
Phase 5: Transaction and Concurrency (Priority 5)
5.1 Transaction Management
Test First: Create tests for transaction operations
Implementation:
Proper transaction isolation
ACID compliance
Deadlock detection and resolution
5.2 Concurrency Control
Test First: Create tests for concurrent operations
Implementation:
Multi-version concurrency control (MVCC)
Lock management
Read/write conflict resolution
Implementation Strategy
Step 1: Start with CREATE TABLE (Test-Driven Development)
Write comprehensive tests for CREATE TABLE parsing
Implement basic table schema parsing
Add table storage in RocksDB
Verify table creation works
Step 2: Implement INSERT INTO
Write tests for INSERT INTO parsing and execution
Implement row insertion logic
Add data validation
Test with actual data storage
Step 3: Enhance SELECT
Write tests for SELECT with real table data
Replace mock data with actual table queries
Implement proper column projection
Add WHERE clause support
Step 4: Add DDL Commands
Implement DROP TABLE
Add ALTER TABLE support
Implement CREATE INDEX
Step 5: Add DML Commands
Implement UPDATE and DELETE
Add transaction support
Implement concurrency control
Key Files to Modify
src/query/planner.zig - SQL parsing and AST generation
src/query/executor.zig - Query execution engine
src/core/database.zig - Database interface and table management
src/storage/rocksdb.zig - Storage layer for tables and data
src/tests/ - Comprehensive test suite for each command
Success Criteria
All existing tests pass without memory leaks
New tests for each SQL command pass
Database can create, populate, and query tables
Proper error handling for invalid SQL
Performance benchmarks show reasonable query times
Would you like me to start implementing this plan? I suggest we begin with Phase 1.1: CREATE TABLE Command using test-driven development as you requested.