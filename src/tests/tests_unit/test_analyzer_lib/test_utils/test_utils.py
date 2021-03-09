# """
# UTILS/UTILS Unit Tests
# """
# from time import gmtime, strftime
# from typing import Dict, Tuple, List
# from functools import reduce
# from datetime import datetime

import pytest
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import StructType, DataType, StringType, IntegerType, StructField
import pyspark.sql.functions as spark_function

import analyzer_lib.utils.utils as u
from analyzer_lib.utils.core import spark


def create_id_list_from_dict_keys():
    pass
