"""
Utils functions
"""

from analyzer_lib.utils.core import spark

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType, DoubleType, FloatType 
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

import ast
import shutil
import os


def load_df(df_schema: StructType, path_to_csv: str) -> DataFrame:
  """
  reading and loading pypak 

  """
  df = spark.read.format('csv')\
    .option('path', path_to_csv)\
    .option('header', True)\
    .schema(df_schema)\
    .option("mode", "FAILFAST")\
    .load()
  return df


def build_hierarchical_structure_for_child_from_root(df: DataFrame, list_of_children: list) -> list:
  """
  Having on the input DataFrame with child parent relation this function build a hierarchical 
  dictionary of relations from root till the end node.

  :param : 
  :type :     
  :return:
  :type:  
  """ 
  childrens = {}
  for child in list_of_children:
    rabbit_hole = None
    try:
      rabbit_hole = list(df\
        .filter(F.col("parent") == child)\
        .select(F.col("company_list"))\
        .first()[0])
    except Exception as e:
      e
    if rabbit_hole is None:
      childrens.update({child:rabbit_hole})
    else:
      childrens.update({child: build_hierarchical_structure_for_child_from_root(df, rabbit_hole)}) 
  return childrens


def create_id_list_from_dict_keys(nested_dict:dict) -> list:
  """
  Create a list of company id from the hierarchical dictionary which is eaqula 
  to get all the keys from the

  :param : 
  :type :     
  :return:
  :type:  
  """ 
  list_of_keys = []
  for key, value in nested_dict.items():
      list_of_keys.extend([key])
      if type(value) is dict:
          list_of_keys.extend(create_id_list_from_dict_keys(value)) 
  return list_of_keys
  
  
def create_df_row(company_id: str, hierarchical_structure: dict) -> dict:
  """
  Create a dictionary structure reflecting row in dataframe strucuture/schema 

  :param : 
  :type :     
  :return:
  :type:  
  """ 
  return {"id": company_id, "hierarchical_structure": str(hierarchical_structure)}


def create_df_rows(id_list, value) -> list:
  """
  Create a dictionary structure reflecting row in dataframe strucuture/schema 

  :param : 
  :type :     
  :return:
  :type:  
  """ 
  list_of_rows = []
  for id in id_list:
    row = create_df_row(id, value)
    list_of_rows.extend([row])
  return list_of_rows


def build_list_of_dict_with_hierarchical_structure_for_each_root(df: DataFrame, list_of_roots: list) -> dict:
  """
  Create a list of dictionaries ready to be transformed into Data Frame 

  :param : 
  :type :     
  :return:
  :type:  
  """ 
  list_of_dicts = []
  for root in list_of_roots:
    hierarchical_structure = build_hierarchical_structure_for_child_from_root(df, [root])
    id_list = create_id_list_from_dict_keys(hierarchical_structure)
    list_of_dicts.extend(create_df_rows(id_list, hierarchical_structure))
  return list_of_dicts


def create_company_hierarchical_structure_df(company_hierarchical_structure_dict: list) -> DataFrame:
  """
  Create a Data Frame with hierarchical structure

  :param : 
  :type :     
  :return:
  :type:  
  """ 
  return spark.createDataFrame(company_hierarchical_structure_dict)
  
def build_dict_from_starting_node(dictionary: dict, company_id: str)-> dict:
  """
  #To Be developed
  # placeholder for function returning sub dictionary from hierarchical structure relevand for comapny_id not from root
  :param : 
  :type :     
  :return:
  :type:  
  """ 
  #ToDo Be developed
  # placeholder for function returning sub dictionary from hierarchical structure relevand for comapny_id not from root
  return dictionary
  
  
def print_dict(dictionary: dict, df: DataFrame, indent):
  """
  Print out dictionary keys using user friendly structure. Keys will be used to query data frame 
  to select full description. 
  :param : 
  :type :     
  :return:
  :type:  
  """ 
  for k,v in sorted(dictionary.items(), key=lambda x: x[0]):
    if isinstance(v, dict):
      print (f"{'├──'*indent} {(df.filter(F.col('company_id') == k).select(F.col('description')).first()[0])}")
      print_dict(v, df, indent+1)
    else:
      print (f"{'└──'*indent} {(df.filter(F.col('company_id') == k).select(F.col('description')).first()[0])}")
         
        
def print_dict_for_company_id(df, company_id, from_root=True, indent=0):
  """
  Print out dictionary useriendly structure for specific company id. 
  Keys will be used to query data frame for company id to select full description. 

  :param : 
  :type :     
  :return:
  :type:  
  """ 
  dictionary = {}
  try:
    dictionary = ast.literal_eval( 
      df\
        .filter(F.col('company_id') == company_id)\
        .select(F.col('hierarchical_structure'))\
        .first()[0])
  except Exception as e:
      print(f"There is a problem with selecting hierarchical_structure for company_id: {company_id}. \n error: {e}")
  
  if not from_root:
    # To Be Developed
    dictionary = build_dict_from_starting_node(dictionary, company_id)
  print_dict(dictionary, df, indent)


def write_df(df: DataFrame, dir: str, file_name: str, headers=True):
  """
  write data frame to single csv file

  :param : 
  :type :     
  :return:
  :type:  
  """ 
  tmp_dir = f"{dir}/tmp"

  df.coalesce(1).write\
    .format("com.databricks.spark.csv")\
    .mode("overwrite")\
    .option("header", headers)\
    .save(tmp_dir)

  files_in_folder = []
  for (dirpath, dirnames, filenames) in os.walk(tmp_dir):
    files_in_folder.extend(filenames)

  file = [item for item in files_in_folder if item.startswith('part-00000')]
  print(f"---> file {file} ")
  shutil.move(os.path.join(tmp_dir, file[0]), os.path.join(dir, file_name))
  try:
    shutil.rmtree(tmp_dir)
  except OSError as e:
    print("Error: %s : %s" % (tmp_dir, e.strerror))
  print(f"folowing file: {dir}/{file_name} has been written")