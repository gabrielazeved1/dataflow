from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

_SPARK_TYPES = {
    "date": DateType(),
    "string": StringType(),
    "integer": LongType(),
    "float": DoubleType(),
}


def build_struct_type(schema_jsonb: dict) -> StructType:
    fields = schema_jsonb["fields"]
    return StructType(
        [
            StructField(
                name=field["name"],
                dataType=_SPARK_TYPES[field["type"]],
                nullable=not field["required"],
            )
            for field in fields
        ]
    )
