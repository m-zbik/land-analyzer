"""
Core module, intended to hold single point-of-entry to pseudo-singleton 
Spark Session and other core functions
"""

# Expose only the instantiated variables, not the protected functions and classes
__all__ = ["spark"]

from pyspark.sql import SparkSession


def __get_spark_session():
    return SparkSession\
        .builder\
        .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.1")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()


#################################
# Pseudo-singleton declarations #
#################################

spark = __get_spark_session()