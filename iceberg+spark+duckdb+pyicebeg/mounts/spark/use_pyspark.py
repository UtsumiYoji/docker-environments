from pyspark.sql import SparkSession

spark = (SparkSession.builder
	.config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1")
	.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
	.config("spark.sql.catalog.test_catalog", "org.apache.iceberg.spark.SparkCatalog")
	# S3
	.config("spark.sql.catalog.test_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
	.config("spark.sql.catalog.test_catalog.client.region", "irrelevant")
	.config("spark.sql.catalog.test_catalog.s3.access-key-id", "POLARIS123ACCESS")
	.config("spark.sql.catalog.test_catalog.s3.secret-access-key", "POLARIS456SECRET")
	.config("spark.sql.catalog.test_catalog.s3.endpoint", "http://rgw1:7480")
	# oauth2
	.config("spark.sql.catalog.test_catalog.rest.auth.type", "oauth2")
	.config("spark.sql.catalog.test_catalog.credential", "root:s3cr3t")
	.config("spark.sql.catalog.test_catalog.oauth2-server-uri", "http://polaris:8181/api/catalog/v1/oauth/tokens")
	.config("spark.sql.catalog.test_catalog.scope", 'PRINCIPAL_ROLE:ALL')
	.config("spark.sql.catalog.test_catalog.token-refresh-enabled", "true")
	# catalog
	.config("spark.sql.catalog.test_catalog.type", "rest")
	.config("spark.sql.catalog.test_catalog.uri", "http://polaris:8181/api/catalog")
	.config("spark.sql.catalog.test_catalog.warehouse", "test_catalog")
).getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS test_catalog.public;")

spark.sql(
	"CREATE TABLE IF NOT EXISTS test_catalog.public.raw ("
		"id   BIGINT,"
		"name STRING,"
		"age  INT"
	") USING iceberg;"
)

spark.sql(
    "INSERT INTO test_catalog.public.raw VALUES "
    "(1, 'Alice',   30),"
    "(2, 'Bob',     25),"
    "(3, 'Charlie', 35);"
)

spark.sql(
    "SELECT * FROM test_catalog.public.raw "
	"WHERE age >= 30;"
).show()

spark.sql("DROP TABLE IF EXISTS test_catalog.public.raw;")
spark.sql("DROP NAMESPACE IF EXISTS test_catalog.public;")
