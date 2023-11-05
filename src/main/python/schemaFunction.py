from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType, TimestampType


# defining function to read schema from config.ini file

def read_schema(schema_arg):
    d_types = {
        "StringType()":StringType(),
        "IntegerType()":IntegerType(),
        "TimestampType()":TimestampType(),
        "DoubleType()":DoubleType(),
        "FloatType()": FloatType()
    }

    split_values = schema_arg.split(",")
    sch = StructType()
    for i in split_values:
        x = i.split(" ")
        sch.add(x[0], d_types[x[1]], True)
    return sch

