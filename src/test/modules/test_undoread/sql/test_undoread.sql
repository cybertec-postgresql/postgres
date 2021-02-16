CREATE EXTENSION test_undoread;

CREATE TABLE input (
	id	serial	PRIMARY KEY,
	value	text	NOT NULL,
	ptr	text
);

INSERT INTO input(value) VALUES ('one'), ('two'), ('three');

-- Write the data into the UNDO log and update the pointers in the table.
BEGIN;
SELECT test_undoread_create();
UPDATE	input
SET ptr = test_undoread_insert(value);
SELECT test_undoread_close();
COMMIT;

CREATE TABLE output (
	id	serial	PRIMARY KEY,
	value	text	NOT NULL
);

-- Read the data. Note that the last pointer should not be included in the
-- result.
INSERT INTO output(value)
SELECT v
FROM test_undoread_read(
	(SELECT ptr FROM input WHERE id = (SELECT min(id) FROM input)),
	(SELECT ptr FROM input WHERE id = (SELECT max(id) FROM input)),
	false) r(v);

-- Check that output data match the input.
SELECT i.id, i.value, o.id, o.value
FROM input i
FULL JOIN output o USING(value)
WHERE i.value ISNULL or o.value ISNULL;

-- Test reading in the backward direction.
--
-- Since the record lengths are stored in "varbyte" format, add one value that
-- requires more than one byte (7 bits are used of each byte for the actual
-- length information).
INSERT INTO input(value) VALUES (repeat('x', '128'));

-- Dummy value that should not appear in the output. It's the easiest way to
-- generate the end pointer that spans all the values inserted above.
INSERT INTO input(value) VALUES ('end');

BEGIN;
SELECT test_undoread_create();
UPDATE input
SET ptr = test_undoread_insert(value);
SELECT test_undoread_close();
COMMIT;

SELECT *
FROM test_undoread_read(
	(SELECT ptr FROM input WHERE id = (SELECT min(id) FROM input)),
	(SELECT ptr FROM input WHERE id = (SELECT max(id) FROM input)),
	true) r(v);
