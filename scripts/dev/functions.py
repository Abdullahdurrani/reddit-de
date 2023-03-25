from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType
from vars import *

def flatten_json(df, json_column_name):
    # gets all fields of StructType or ArrayType in the nested_fields dictionary
    nested_fields = dict([
        (field.name, field.dataType)
        for field in df.schema.fields
        if isinstance(field.dataType, ArrayType) or isinstance(field.dataType, StructType)
    ])

    # repeat until all nested_fields i.e. belonging to StructType or ArrayType are covered
    while nested_fields:
        # if there are any elements in the nested_fields dictionary
        if nested_fields:
            # get a column
            column_name = list(nested_fields.keys())[0]
            # if field belongs to a StructType, all child fields inside it are accessed
            # and are aliased with complete path to every child field
            if isinstance(nested_fields[column_name], StructType):
                unnested = [col(column_name + '.' + child).alias(column_name + '>' + child) for child in [ n.name for n in  nested_fields[column_name]]]
                df = df.select("*", *unnested).drop(column_name)
            # else, if the field belongs to an ArrayType, an explode_outer is done
            elif isinstance(nested_fields[column_name], ArrayType):
                df = df.withColumn(column_name, explode_outer(column_name))

        # Now that df is updated, gets all fields of StructType and ArrayType in a fresh nested_fields dictionary
        nested_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, ArrayType) or isinstance(field.dataType, StructType)
        ])

    # renaming all fields extracted with json> to retain complete path to the field
    for df_col_name in df.columns:
        df = df.withColumnRenamed(df_col_name, df_col_name.replace("transformedJSON", json_column_name))
    return df

def loadConfigs(sparkContext):
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio_access_key)
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio_secret_key)
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_endpoint)
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")