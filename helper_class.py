import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import StringType, BinaryType, TimestampType
import datetime
from functools import reduce
from snowflake.snowpark import DataFrame


class HelperMethods():
    
    def __init__(self,
                 session,
                 process_start_datetime: datetime.datetime = None,
                 process_end_datetime: datetime.datetime = None,
                 business_key_columns: list = None,
                 type_i_columns_list: list = None,
                 additional_column_list: list = None,
                 creation_date_time_column: str = None,
                 modification_date_time_column: str = None
                 ):
        self.process_start_datetime = process_start_datetime
        self.process_end_datetime = process_end_datetime
        self.session = session
        # Values to be set at the top of the methods that are generating those tables.
        if (business_key_columns is not None):
            self._business_key_columns = business_key_columns
        else:
            self._business_key_columns = []

        if (type_i_columns_list is not None):
            self._type_i_columns_list = type_i_columns_list
            print(self._type_i_columns_list)
        else:
            self._type_i_columns_list = []
            print(self._type_i_columns_list)

        # additional column if needed. usually only audit columns is needed extra
        if (additional_column_list is not None):
            self.additional_column_list = additional_column_list
        else:
            self.additional_column_list = []

        # Columns that will be used to set the Effective Start Date audit column in the target the Dim table.
        # Use the creation/modification datetime column from the source if available.
        if (creation_date_time_column is not None):
            self.creation_date_time_column = creation_date_time_column
        else:
            self.creation_date_time_column = ''

        if (modification_date_time_column is not None):
            self.modification_date_time_column = modification_date_time_column
        else:
            self.modification_date_time_column = ''

        # If creation date time is an audit field, add to the additional column list.
        if (self.creation_date_time_column.startswith('__')
           and self.creation_date_time_column not in self.additional_column_list):
            self.additional_column_list.append(self.creation_date_time_column)

        # If modification date time is an audit field, add to the additional column list.
        if (self.modification_date_time_column.startswith('__')
           and self.modification_date_time_column not in self.additional_column_list):
            self.additional_column_list.append(self.modification_date_time_column)

    def _get_data_column_list(self, dataframe_column_list: list):
        """
        Returns a list of colums whose names don't start with __ thus are data columns.
        If Additional_column_list is defined in the __init__ func, those columns will be append to the column list.
        additional column list should only include audit columns, since all data columns are selected automatically
        """
        return [column_name for column_name in dataframe_column_list if not column_name.startswith('__')] \
            + self.additional_column_list

    #def _column_name_list_sort(self, column_name_list):
    #    """
    #    Retruns a sorted list of column names based on column names in lower case
    #    """
    #    return sorted(column_name_list, key=lambda element: element.lower())
       
    def _unioning_dataframe_list(self, df_list: list):
        """
        Unions a list of datarames. Used for reading the same table of data from multiple mines at once.
        """
        return reduce(DataFrame.unionByName, df_list)

    def _select_data_columns(self, df):
        """
        Selecting data column i.e. columns whose names don't start with __.
        """
        return df.select(*self._get_data_column_list(df.columns))

    def _data_read(self,
                   source_database: str,
                   source_schema: str,
                   table_name: str,
                   data_read_type: str,
                   list_of_partition_values: list = None,
      
                   ):
        """
        Reading data from silver source table.
        If the read mode is regular_incremental, the table is filtered on __process_datetime column
        which is the timestamp of the records landing in silver layer.
        type_i_incremental_join_right_side data read mode is used for reading right side of left joins
        to improve the performance.
        Essentially, the partitions with data to contribute to the joins are identified
        and only those partitions are read.
        .trasnform pyspark transformation applies a function to the dataframe.
        """
        df = self.session.table(f'{source_database}.{source_schema}.{table_name}')

        if (data_read_type == 'regular_incremental'):
            process_datetime_columns = ['__process_date_time', '__process_datetime', '__lastmodified']
            process_datetime_column = next((col for col in process_datetime_columns if col in df.columns), None)
            if ('__current' in df.columns):
                df_final = (df
                            .where((col(process_datetime_column) >= self.process_start_datetime) &
                                   (col(process_datetime_column) < self.process_end_datetime) &
                                   (col('__current'))
                                   )
                            .transform(self._select_data_columns))
            else:
                df_final = (df
                            .where((col(process_datetime_column) >= self.process_start_datetime) &
                                   (col(process_datetime_column) < self.process_end_datetime)
                                   )
                            .transform(self._select_data_columns))

        elif (data_read_type == 'type_i_incremental_join_right_side'):
            df_final = (df
                        .where(col('__transaction_partition_value').isin(list_of_partition_values))
                        .transform(self._select_data_columns))

        elif (data_read_type == 'type_ii_incremental_join_right_side'):
            df_final = (df
                        .where(col('__current'))
                        .transform(self._select_data_columns))

        elif (data_read_type == 'type_i_full_load'):
            df_final = df

        else:
            raise ValueError('Incorrect Data Read Type!')

        return df_final

    def _cache(self, df):
        """
        caching dataframes and running an action on them to solidify the cache.
        """
        df.cache()
        print(f'caching {df.count()} records.')

        return True

    def _list_of_right_side_partitions_finder(self, df, list_of_columns: list):
        """
        Needs to be discussed!
        Finding the list of the partition values that are applicable to the right side of the join
        in which df is the left side.
        Multiple columns on the left side of the join can be targetted in the join,
        so the output is a dictionary whose keys are column names from the left side of the join
        and values are lists of applicable partition values on the silver tables of right side of the join.
        Joins in the cases that we have right now are happening on ids.
        the silver tables are partitioned on the first 4 digits of ids, and that explains the substring function.
        """
        distinct_values_dictionary = {}
        for column_name in list_of_columns:
            distinct_values_dictionary[column_name] = \
                [element['value']
                 for element in df.select(substring(col(column_name).cast(StringType()), 1, 4).alias('value'))
                 .where(col('value').isNotNull()).distinct().collect()]

        return distinct_values_dictionary
        
    def _concat_columns_with_separator(self, columns, separator):
        if not columns:  # Check if the list is empty
            return lit('0')  # Return an empty string literal if there are no columns to concatenate

        # Ensure that columns are stripped of whitespace and cast to strings
        column_expressions = [col(column.strip()).cast(StringType()) for column in columns]

        # Construct a compact array from the column expressions to remove NULL values
        array_expr = array_construct_compact(*column_expressions)
        
        # Convert the compacted array to a string with the separator
        concat_expr = array_to_string(array_expr, separator)
   
        return concat_expr

    def _column_name_list_sort(self, column_name_list):
        # This function should sort the column names in uppercase to match the SQL function
        return sorted(column_name_list, key=lambda element: element.upper())

    def add_dimension_audit_columns(self, df):
        # Sort the column names in upper case to match the SQL function implementation
        deleted_flag = '__deleted' in df.columns
        
        output_df = (df
                     .with_column('__BusinessKeyHash', 
                                  to_binary(sha2(lower(self._concat_columns_with_separator(self._column_name_list_sort(self._business_key_columns), lit('|'))), 256)))
                     .with_column('__Type1Hash', 
                                  to_binary(sha2(lower(self._concat_columns_with_separator(self._column_name_list_sort(self._type_i_columns_list), lit('|'))), 256)))
                     .with_column('__Type2Hash', 
                                  to_binary(sha2(lower(self._concat_columns_with_separator(self._column_name_list_sort(set(df.columns) - set(self._type_i_columns_list) - {'__deleted'}
                                                                                                   - {self.creation_date_time_column, self.modification_date_time_column}), lit('|'))), 256)))
                     .with_column('__DeletedFlag', col('__deleted') if deleted_flag else lit(False))
                     .with_column('__CreateDateTime', lit(datetime.datetime.now())))
        
        
        # If creation date time column does not exist, set to null.
        if (self.creation_date_time_column in df.columns):
            output_df = (output_df .with_column('__DataCreationDateTime', when(col(self.creation_date_time_column).isNotNull(), col(
                self.creation_date_time_column).cast(TimestampType())) .otherwise(lit(None).cast(TimestampType()))))
        else:
            output_df = (output_df
                         .with_column('__DataCreationDateTime', lit(None).cast(TimestampType())))

        # If modification date time column does not exist or null, set to null.
        if (self.modification_date_time_column in df.columns):
            output_df = (
                output_df .with_column(
                    '__DataModificationDateTime', when(
                        col(
                            self.modification_date_time_column).isNotNull(), col(
                            self.modification_date_time_column).cast(
                            TimestampType())) .otherwise(
                        lit(None).cast(
                            TimestampType()))))
        else:
            output_df = (output_df
                         .with_column('__DataModificationDateTime', lit(None).cast(TimestampType())))
        return (output_df)

    def add_fact_audit_columns(self, df):
        """
        Adding audit columns that are used in fact tables
        """
        # This Comment Is Added To Trigger Deployment.
        deleted_flag = '__deleted' in df.columns
        #return (
        #    df .withColumn(
        #        '__FactKeyHash', sha2(
        #            concat_ws(
        #                lit(lit(lit('|'))), *[
        #                    col(column).cast(
        #                        StringType()) for column in self._column_name_list_sort(
        #                        self._business_key_columns)]), 256) .cast(
        #            BinaryType())) .withColumn(
        #        '__DeletedFlag', col('__deleted') if deleted_flag else lit(False)) .withColumn(
        #        '__CreateDateTime', lit(
        #            datetime.datetime.now())))
        deleted_flag = '__deleted' in df.columns
        return (
            df.with_column('__FactKeyHash',to_binary(sha2(lower(self._concat_columns_with_separator(self._column_name_list_sort(self._business_key_columns), lit('|'))), 256)))
            .with_column(
                '__DeletedFlag', col('__deleted') if deleted_flag else lit(False))
            .with_column(
                '__CreateDateTime', lit(datetime.datetime.now())
            ))