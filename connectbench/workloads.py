"""Deterministic plan shapes; no actions or schema inference RPCs."""

FAMILIES = ("deep", "wide", "expression", "joins", "unions", "mixed", "local")


def build(spark, family, size):
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import LongType, StructField, StructType

    base = spark.range(1000)
    if family == "deep":
        for i in range(size):
            base = base.filter(F.col("id") >= i)
    elif family == "wide":
        base = base.select(*[(F.col("id") + i).alias(f"c{i}") for i in range(size)])
    elif family == "expression":
        expr = F.col("id")
        for i in range(size):
            expr = F.when(expr > i, expr + 1).otherwise(F.lit(i))
        base = base.select(expr.alias("value"))
    elif family == "joins":
        base = base.alias("left").select(F.col("id").alias("key"))
        for i in range(size):
            right = spark.range(1000).select(F.col("id").alias(f"k{i}"))
            base = base.join(right, F.col("key") == F.col(f"k{i}"), "left")
    elif family == "unions":
        branches = [spark.range(i, i + 1000) for i in range(size)]
        while len(branches) > 1:
            branches = [branches[i].union(branches[i + 1]) if i + 1 < len(branches)
                        else branches[i] for i in range(0, len(branches), 2)]
        base = branches[0]
    elif family == "mixed":
        for i in range(size):
            base = base.withColumn(f"c{i}", (F.col("id") + i) % 7)
        base = base.groupBy(F.col("id")).agg(F.count("*").alias("n"))
        base = base.withColumn("rank", F.row_number().over(Window.orderBy("n")))
        base = base.orderBy("rank")
    elif family == "local":
        schema = StructType([StructField("id", LongType(), False)])
        base = spark.createDataFrame([(i,) for i in range(size)], schema)
    else:
        raise ValueError(family)
    return base
