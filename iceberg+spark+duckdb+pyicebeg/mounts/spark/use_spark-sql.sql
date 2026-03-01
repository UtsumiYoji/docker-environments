-- create
CREATE NAMESPACE IF NOT EXISTS test_catalog.public;

CREATE TABLE IF NOT EXISTS test_catalog.public.raw (
    id   BIGINT,
    name STRING,
    age  INT
)
USING iceberg;

-- write
INSERT INTO test_catalog.public.raw VALUES
    (1, 'Alice',   30),
    (2, 'Bob',     25),
    (3, 'Charlie', 35);

-- read (age >= 30)
SELECT * FROM test_catalog.public.raw
WHERE age >= 30;

-- delete
DROP TABLE IF EXISTS test_catalog.public.raw;
DROP NAMESPACE IF EXISTS test_catalog.public;
