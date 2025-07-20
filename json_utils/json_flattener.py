from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, count, explode_outer
from pyspark.sql.types import (StructType, StructField, StringType, LongType, BooleanType, ArrayType)
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(module)s:%(funcName)s:%(lineno)s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

class JsonFlattener():
    def __init__(self, debug: bool = False):
        self.debug = debug

    # Recursively flattens nested fields in a DataFrame schema
    def _flattenColumns(self, df: DataFrame, prefix: str = '') -> list[str]:
        """Args:
            df (DataFrame): The Spark DataFrame to flatten.
            prefix (str): A prefix for column names, used during recursion.
        Returns:
            List[str]: A list of flattened column names.
        """
        expanded_cols = []
        # Iterate through each field in dataframe and flatten all nested fields with StructType
        for field in df.schema.fields:
            if self.debug == True:
                logger.info(f"Flattening {field.name}")
            field_name = f"{prefix}.{field.name}" if prefix else field.name
            if isinstance(field.dataType, StructType):
                sub_field_name = field_name.split(',')[-1]
                flat_df = df.select(col(f"{sub_field_name}.*"))
                expanded_cols.extend(self._flattenColumns(flat_df, field_name))
            else:
                expanded_cols.append(field_name)

        return expanded_cols
    
    def __flatten(self, df: DataFrame) -> DataFrame:
        """Flattens nested fields and renames columns to column standards
        Args:
            df (DataFrame): The Spark DataFrame to flatten.
        Returns:
            DataFrame: A DataFrame with flattened columns.
        """
        expanded_cols = self._flattenColumns(df)
        return df.select(*[col(x).alias(x.replace('.', '_')) for x in expanded_cols])

def _explode(self, df: DataFrame, prefix: str = "") -> DataFrame:
    """Explodes nested array fields in a DataFrame.

    Args:
        df (DataFrame): The Spark DataFrame to flatten.
        prefix (str): A prefix for column names, used during recursion.

    Returns:
        DataFrame: DataFrame with flattened rows.
    """

    for field in df.schema.fields:
        if self.debug:
            logger.info(f"Exploding field: {field.name}")

        if isinstance(field.dataType, ArrayType):
            explode_df = df.withColumn(field.name, explode_outer(col(field.name)))
            explode_df = self._flatten(explode_df)
            array_type = df.limit(5).select(col(field.name)).schema["col"].dataType

            if isinstance(array_type, ArrayType):
                field_names = array_type.elementType.names
                if all(field in explode_df.columns for field in field_names):
                    explode_df = explode_df.drop(field.name)
                    if self.debug:
                        logger.info(f"Field {field.name} Dropped")

            elif isinstance(array_type, StructType):
                if field.name in explode_df.columns:
                    explode_df = explode_df.drop(field.name)
                    if self.debug:
                        logger.info(f"Field {field.name} Dropped")

            elif isinstance(array_type, StringType):
                explode_df = explode_df.drop(field.name)
                if self.debug:
                    logger.info(f"Field {field.name} Dropped")
            else:
                 logger.info("'{}' object has no attribute 'names'".format(type(array_type)._name__))

    return explode_df

def renameColumns(self, df: DataFrame, schema_mapping: dict) -> dict:
    """
    Renames columns in a DataFrame based on a schema mapping.

    Args:
        df (DataFrame): Flattened Spark DataFrame.
        schema_mapping (str): Field name with respective json field names.
    Returns:
        DataFrame: Dataframe with renamed column names as mapped in schema string.
    """
    rename_dict = {}
    mapping_dict = {row.split(":")[0].strip(): row.split(":")[1].strip()
                      for row in schema_mapping.split(",")}
    for column in df.columns:
        if column in mapping_dict.keys():
            rename_dict[column] = mapping_dict[column]
            if self.debug == True:
                logger.info(f"Renamed Field (column) to {mapping_dict[column]}")
        else:
            if self.debug == True:
                logger.info(f"No mapping found for Field: {column} to rename")
            rename_dict[column] = column
    return rename_dict
def readJson(self, json_df: DataFrame, rename_columns: str = None) -> DataFrame:
        """
        Reads JSON data into a DataFrame, flattens nested fields, explodes array fields, and renames columns based 
        on schema mapping.

        Args:
            json_df (DataFrame): The Spark DataFrame to flatten.
            rename_columns (str, optional): Mapped schema with JSON field names. Defaults to None.

        Returns:
            DataFrame: DataFrame with renamed column names as mapped in the schema string.
        """

        jsonNotFlat = True
        # Iterate through the schema of the JSON data until all nested fields are flattened
        while jsonNotFlat:
            jsonNotFlat = False # Assume no flattening needed initially
            for field in json_df.schema.fields:
                if self.debugTrue:
                    self.logger.debug(f"Field ({field.name}) is type of ({field.dataType})")
                if isinstance(field.dataType, StructType):
                    json_df = self._flatten(json_df)
                    jsonNotFlat = True  # Flattening happened, so loop again
                    break # Re-iterate from the beginning once a flatten op happened
                elif isinstance(field.dataType, ArrayType):
                    json_df = self._explode(json_df)
                    jsonNotFlat = True # Flattening happened, so loop again
                    break # Re-iterate from the beginning once an explode op happened
                elif isinstance(field.dataType, (StringType, LongType)):
                    json_df = json_df.withColumn(field.name, col(field.name))
                else:
                    continue

        # Rename columns in the DataFrame based on the schema mapping
        if rename_columns is not None:
            if self.debugTrue:
                self.logger.debug("Field Rename is enabled")
            rename_dict = self.renameColumns(json_df, rename_columns)
            json_df = json_df.withColumnsRenamed(rename_dict)
        else:
            if self.debugTrue:
                self.logger.debug("Field Rename is not enabled")

        return json_df
