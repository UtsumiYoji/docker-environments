INSTALL aws;
INSTALL httpfs;
INSTALL iceberg;
LOAD iceberg;
LOAD httpfs;

-- oauth2
CREATE SECRET token (
    TYPE iceberg,
    CLIENT_ID 'root',
    CLIENT_SECRET 's3cr3t',
    OAUTH2_SERVER_URI 'http://polaris:8181/api/catalog/v1/oauth/tokens'
);

-- s3
CREATE SECRET s3_secret (
    TYPE s3,
    KEY_ID 'POLARIS123ACCESS',
    SECRET 'POLARIS456SECRET',
    ENDPOINT 'rgw1:7480',
    URL_STYLE 'path',
    USE_SSL false
);

-- catalog
ATTACH 'test_catalog' AS test_catalog (
   TYPE iceberg,
   SECRET token,
   ENDPOINT 'http://polaris:8181/api/catalog',
   ACCESS_DELEGATION_MODE 'none'
);

-- create namespace
CREATE SCHEMA IF NOT EXISTS test_catalog.public;

-- create table
CREATE TABLE IF NOT EXISTS test_catalog.public.raw (
    id    BIGINT,
    name  VARCHAR,
    age   INTEGER
);

-- write
INSERT INTO test_catalog.public.raw VALUES
    (1, 'Alice',   30),
    (2, 'Bob',     25),
    (3, 'Charlie', 35);

-- read
SELECT * FROM test_catalog.public.raw 
WHERE age >= 30;

-- delete (table first, then namespace)
DROP TABLE  IF EXISTS test_catalog.public.raw;
DROP SCHEMA IF EXISTS test_catalog.public;
