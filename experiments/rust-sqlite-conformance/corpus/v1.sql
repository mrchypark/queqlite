-- rust-sqlite-conformance corpus v1
PRAGMA foreign_keys = ON;
CREATE TABLE items(
  id INTEGER PRIMARY KEY,
  k TEXT NOT NULL UNIQUE,
  n NUMERIC,
  payload BLOB,
  note TEXT NOT NULL DEFAULT 'default-note'
);
CREATE TABLE audit(
  seq INTEGER PRIMARY KEY,
  item_id INTEGER NOT NULL,
  op TEXT NOT NULL
);
CREATE TRIGGER items_ai AFTER INSERT ON items BEGIN
  INSERT INTO audit(item_id, op) VALUES (new.id, 'insert');
END;
CREATE INDEX items_n ON items(n);
INSERT INTO items(id, k, n, payload) VALUES
  (1, 'alpha', 10, x'00ff'),
  (2, 'beta', 2.5, NULL),
  (3, 'gamma', NULL, x'');
INSERT INTO items(id, k, n, payload) VALUES (20, 'beta', 22, x'beef')
  ON CONFLICT(k) DO UPDATE SET n = excluded.n, payload = excluded.payload;
INSERT OR REPLACE INTO items(id, k, n, payload, note)
  VALUES (3, 'gamma', 33, x'33', 'replaced');
UPDATE items SET note = upper(k) WHERE id IN (1, 2);
BEGIN;
INSERT INTO items(id, k, n) VALUES (90, 'rolled-back', 90);
ROLLBACK;
SAVEPOINT corpus_savepoint;
INSERT INTO items(id, k, n) VALUES (91, 'savepoint-rolled-back', 91);
ROLLBACK TO corpus_savepoint;
RELEASE corpus_savepoint;
