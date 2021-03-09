"""
Man Run Land Analyzer.
"""


import analyzer_lib.utils.utils as u
import analyzer_lib.data_manipulation.tables as t 

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType, DoubleType, FloatType 
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
import configparser
import sys

# read information from configs
config = configparser.ConfigParser()
config.read('conf.ini')
file_dir = config['local']['file_dir']
company_relations_file = config['local']['company_relations_file']
land_ownership_file = config['local']['land_ownership_file']
hierarchical_structure_file_name = config['local']['hierarchical_structure_file_name']

# define schema for input files
cr_schema = StructType([
   StructField("company_id", StringType(), True),
   StructField("name", StringType(), True),
   StructField("parent", StringType(), True)
])

lo_schema = StructType([
   StructField("land_id", StringType(), True),
   StructField("company_id", StringType(), True)
])

hierarchical_structure_schema = StructType([
    StructField("company_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("hierarchical_structure", StringType(), True)
])

def run_with_rebuild(arg_file_dir: str, arg_company_relations_file: str, arg_cr_schema: StructType,
                        arg_land_ownership_file:str, arg_lo_schema: StructType, 
                        arg_hierarchical_structure_file_name) -> DataFrame:

    # READ DFs
    pathcr = f"{arg_file_dir}/{arg_company_relations_file}"
    pathlo = f"{arg_file_dir}/{arg_land_ownership_file}"
    df_cr = u.load_df(arg_cr_schema, pathcr)\
        .select(F.col("company_id").alias("company_id_cr"), F.col("name"), F.col("parent"))
    df_lo = u.load_df(arg_lo_schema, pathlo)\
        .select(F.col("land_id"), F.col("company_id").alias("company_id_lo"))

    # ENRICH DFs WITH MORE INFORMATION
    parent_df = t.create_df_with_parent_child_lists(df_cr)
    company_land_count = t.create_df_with_land_parcels_ownership_count(df_lo)
    company_df = t.create_df_with_full_description(df_cr, company_land_count)

    data_frame = parent_df
    list_of_roots = None

   # GET THE LIST OF ROOTS 
    try:
      list_of_roots = data_frame\
        .where( (F.col("parent")=="") | (F.col("parent").isNull()) )\
        .select(F.col("company_list"))\
        .first()[0]
    except Exception as e:
      print(e)

    # BUILD HIERARICHCAL STRUCTURE FOR EACH ROOT - TOP DOWN 
    if list_of_roots is not None:
      company_hierarchical_structure_dict = u.build_list_of_dict_with_hierarchical_structure_for_each_root(data_frame, list_of_roots)
      hierarchical_structure_df = u.create_company_hierarchical_structure_df(company_hierarchical_structure_dict)
    else:
      print(f"no roots in the list of roots in the df: {data_frame}")  

   # BUILD FINAL ENRICHED TABLE 
    df_total = company_df.join(hierarchical_structure_df,  company_df.company_id==hierarchical_structure_df.id, how="left")\
      .drop(F.col("parent_2"))\
      .drop(F.col("id"))

    u.write_df(df_total, arg_file_dir, arg_hierarchical_structure_file_name)

    return df_total


if __name__ == '__main__':

    # example python main.py CR995643170992 rebuild
    if len(sys.argv) > 2 and sys.argv[2] == "rebuild":
        print("building of hierarchical_structure_file_name has started and may take few minutes ... ")
        df = run_with_rebuild(file_dir, company_relations_file, cr_schema, 
                      land_ownership_file, lo_schema, hierarchical_structure_file_name)
        print("building of hierarchical_structure_file_name has finished")
        u.print_dict_for_company_id(df, sys.argv[1])
    elif len(sys.argv) == 2:
        df = u.load_df(hierarchical_structure_schema, f"{file_dir}/{hierarchical_structure_file_name}")
        u.print_dict_for_company_id(df, sys.argv[1])