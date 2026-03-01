import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, LongType, NestedField, StringType


def main():
    catalog = load_catalog(
        "test_catalog",
        **{
            # Storage
            "s3.endpoint": "http://rgw1:7480",
            "s3.access-key-id": "POLARIS123ACCESS",
            "s3.secret-access-key": "POLARIS456SECRET",

            # Oauth2
            "auth.type": "oauth2",
            "auth.oauth2.client_id": "root",
            "auth.oauth2.client_secret": "s3cr3t",
            "auth.oauth2.scope": "PRINCIPAL_ROLE:ALL",

            # Catalog
            "type": "rest",
            "uri": "http://polaris:8181/api/catalog",
            "warehouse": "test_catalog",
            # "header.X-Iceberg-Access-Delegation": "",
        }
    )

    # write
    catalog.create_namespace_if_not_exists("public")
    table = catalog.create_table_if_not_exists(
        identifier="public.raw",
        schema=Schema(
            NestedField(field_id=1, name="id",   field_type=LongType(),    required=False),
            NestedField(field_id=2, name="name", field_type=StringType(),  required=False),
            NestedField(field_id=3, name="age",  field_type=IntegerType(), required=False),
        ),
    )

    records = pa.table({
        "id":   pa.array([1, 2, 3], type=pa.int64()),
        "name": pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
        "age":  pa.array([30, 25, 35], type=pa.int32()),
    })
    table.append(records)

    # read
    table = catalog.load_table("public.raw")
    print(table.scan(row_filter=GreaterThanOrEqual("age", 30)).to_pandas())

    # delete
    catalog.drop_table("public.raw")
    catalog.drop_namespace("public")

if __name__ == "__main__":
    main()
