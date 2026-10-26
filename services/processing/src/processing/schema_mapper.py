from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

_TIPO_PARA_SPARK = {
    "date": DateType(),
    "string": StringType(),
    "integer": LongType(),
    "float": DoubleType(),
}


def build_struct_type(schema_jsonb: dict) -> StructType:
    campos = schema_jsonb["campos"]
    return StructType(
        [
            StructField(
                name=campo["nome"],
                dataType=_TIPO_PARA_SPARK[campo["tipo"]],
                nullable=not campo["obrigatorio"],
            )
            for campo in campos
        ]
    )
