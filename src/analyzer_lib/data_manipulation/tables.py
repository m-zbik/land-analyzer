"""
Main logic for table manipulation
"""

import analyzer_lib.utils.utils as u

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType, DoubleType, FloatType 
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

import ast

def create_df_with_parent_child_lists(df_cr: DataFrame) -> DataFrame:
    """
    creates a column with list of children asigned to a parent company_id
    
    :param c_c: Configuration class
    :type c_c: Config    
    :return:
    :type: (Dict) 
    """
    parent_df = df_cr\
      .groupBy("parent")\
      .agg(F.collect_list("company_id_cr").alias("company_list"))
    return parent_df


def create_df_with_land_parcels_ownership_count(df_lo: DataFrame) -> DataFrame:
    """
    Create Data Frame with count on land parcels ownership grouped by company id

    :param : 
    :type :     
    :return:
    :type:  
    """
    company_land_count = df_lo\
      .groupBy("company_id_lo")\
      .count()\
      .withColumn("count_result", \
          F.concat(F.lit("owner of "), F.col("count"), F.lit(" land parcels")))\
      .drop(F.col("count"))
    return company_land_count


def create_df_with_full_description(df_cr: DataFrame, company_land_count: DataFrame) -> DataFrame:
    """
    Create Data Frame with all the informatiomaabout the companys

    :param : 
    :type :     
    :return:
    :type:  
    """ 
    company_df = df_cr\
    .join(company_land_count,  df_cr.company_id_cr==company_land_count.company_id_lo, how="left")\
    .select(F.col("company_id_cr").alias("company_id"), F.col("name"), F.col("count_result"))\
    .withColumn("description", \
                F.concat( F.col("company_id"), 
                F.lit(" ;"), F.col("name"), 
                F.lit(" ;"), F.col("count_result")))\
    .drop(F.col("count_result"))\
    .withColumn("description", F.when(F.col("description").isNull(), 
          F.concat( F.col("company_id"), 
          F.lit(" ;"), F.col("name"), 
          F.lit(" ; No information about land parcels ownership number"))
         ).otherwise(F.col("description")))
    return company_df