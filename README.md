# LiteDB — A Simple Database Engine

A lightweight database engine built from scratch in Java, supporting key-value storage, relational-style tables, transactions, and a client/server architecture.

---

## Architecture

```
Client (REPL)
    ↓  socket
Server
    ↓
Parser → Command
    ↓
Router
    ↓                        ↓
TransactionManager       SchemaManager (Singleton)
    ↓                        
KeyValueStore (Singleton)
    ↓              ↓
   WAL          Snapshot (flushed every 5 mins)
```

### Core Components

**WAL (Write-Ahead Log)**
Every write is appended to `wal.log` before touching in-memory state. On startup, WAL is replayed to rebuild state. Provides crash safety via `fsync`.

**Snapshot**
Periodically flushes the entire in-memory cache to disk every 5 minutes. On startup, snapshot is loaded first, then WAL entries are replayed on top. Atomic rename ensures snapshot is never half-written.

**Cache**
Nested `HashMap` storing all table data in memory:
```
HashMap<tableName, HashMap<rowId, HashMap<columnName, value>>>
```

**Index**
Secondary index using `TreeMap` per column — supports exact match and range queries:
```
HashMap<tableName, HashMap<columnName, TreeMap<columnValue, HashSet<rowId>>>>
```

**TransactionManager**
Manages transaction lifecycle using intermediate HashMaps per session. Writes to WAL and updates cache/index only on COMMIT. ROLLBACK simply discards intermediate state.

**SchemaManager**
Singleton managing strict table schemas. All tables and columns must be defined before use.

**Client/Server**
Server listens on port `8080`, handles each client connection in a separate thread via a fixed thread pool. Each session maintains its own transaction state.

---

## Getting Started

**Start the server:**
```
java Server
```

**Connect a client:**
```
java Client
```

---

## Commands

### Table Management

**Create a table:**
```
create table tableName col1 col2 col3
```
Example:
```
create table users name age city
```

**Create an index on a column:**
```
create index on tableName columnName
```
Example:
```
create index on users name
```

---

### Data Manipulation

**Insert a row:**
```
insert col1:val1 col2:val2 col3:val3 tableName
```
Example:
```
insert name:Alice age:30 city:Boston users
```

**Update a row:**
```
update from tableName where id = ?
```
Example:
```
update from users where id = 1 name:Bob age:31
```

**Delete a row:**
```
delete from tableName where id = ?
```
Example:
```
delete from users where id = 1
```

---

### Querying

**Select all rows, all columns:**
```
select * from tableName
```

**Select all rows, specific columns:**
```
select col1,col2 from tableName
```

**Select by row ID, all columns:**
```
select * from tableName where id = ?
```

**Select by row ID, specific columns:**
```
select col1,col2 from tableName where id = ?
```

**Select by column value, all columns:**
```
select * from tableName where columnName = ?
```
Requires index on that column.

**Select by column value, specific columns:**
```
select col1,col2 from tableName where columnName = ?
```
Requires index on that column.

---

### Transactions

**Begin a transaction:**
```
begin
```

**Commit a transaction:**
```
commit
```

**Rollback a transaction:**
```
rollback
```

Example:
```
begin
insert name:Alice age:30 city:Boston users
insert name:Bob age:25 city:NewYork users
commit
```

If no `begin` is issued, every command is auto-committed immediately.

---

## Design Decisions

### Append-Only WAL
Every write appends to `wal.log` — no in-place updates. Latest write wins on replay. WAL is truncated after every successful snapshot flush.

### Strict Schema
Tables and columns must be defined upfront via `create table`. Inserting unknown columns throws an error.

### Transactions
- DML commands (`insert`, `update`, `delete`, `select`) are transaction-aware
- DDL commands (`create table`, `create index`) are auto-committed immediately
- On client disconnect, any open transaction is automatically rolled back

### Concurrency
- `ReadWriteLock` protects cache and index — multiple readers allowed, exclusive writer
- `ConcurrentHashMap` + `AtomicInteger` for row ID generation — thread safe, no duplicates
- WAL append is `synchronized` — no interleaving between sessions

### Index
- Only columns explicitly indexed via `create index` are indexed
- Index is never persisted — rebuilt from cache on startup
- Index uses `TreeMap` for future range query support

---

## Roadmap

```
✅ Key-Value Store
✅ WAL + Snapshot + Compaction
✅ Schema Management
✅ Secondary Index (TreeMap)
✅ Transactions (ACID)
✅ Client/Server Architecture
⬜ Error Handling
⬜ Docker Support
⬜ JOIN, GROUP BY
⬜ Page Based Storage
⬜ SQL Parser (ANTLR)
⬜ Replication
```
