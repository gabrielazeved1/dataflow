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
    fields = schema_jsonb["campos"]
    return StructType(
        [
            StructField(
                name=field["nome"],
                dataType=_SPARK_TYPES[field["tipo"]],
                nullable=not field["obrigatorio"],
            )
            for field in fields
        ]
    )
