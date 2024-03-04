import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, LongType, StructType
from chispa.dataframe_comparer import assert_df_equality
from main import *

# Setting up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define sample datasets for testing
client_table_values = [
    (1, 'first_name1', 'last_name1', 'first_name1@test.test', 'Netherlands'),
    (2, 'first_name2', 'last_name2', 'first_name2@test.test', 'Netherlands'),
    (3, 'first_name3', 'last_name3', 'first_name3@test.test', 'United Kingdom'),
    (4, 'first_name4', 'last_name4', 'first_name4@test.test', 'France')
]
client_schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('first_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('email', StringType(), True),
    StructField('country', StringType(), True),
])

financial_table_values = [
    (1, 'btc_a1', 'visa-electron', 12345678901),
    (2, 'btc_a2', 'jcb', 12345678902),
    (3, 'btc_a3', 'diners-club-enroute', 12345678903),
    (4, 'btc_a4', 'switch', 12345678904)
]
financial_schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('btc_a', StringType(), True),
    StructField('cc_t', StringType(), True),
    StructField('cc_n', LongType(), True),
])

output_schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('bitcoin_address', StringType(), True),
    StructField('credit_card_type', StringType(), True),
    StructField('email', StringType(), True),
    StructField('country', StringType(), True),
])

def test_drop_column():
    """
    Test for drop_column function.

    This function tests whether drop_columns function correctly drops a specified column from the DataFrame.
    """
    input_data = spark.createDataFrame(client_table_values, schema=client_schema)
    actual_data = drop_columns(input_data, ['last_name'])

    expected_data = spark.createDataFrame([(1, 'first_name1', 'first_name1@test.test', 'Netherlands'),
                                           (2, 'first_name2', 'first_name2@test.test', 'Netherlands'),
                                           (3, 'first_name3', 'first_name3@test.test', 'United Kingdom'),
                                           (4, 'first_name4', 'first_name4@test.test', 'France')],
                                          schema=StructType([StructField('id', IntegerType(), True),
                                                             StructField('first_name', StringType(), True),
                                                             StructField('email', StringType(), True),
                                                             StructField('country', StringType(), True)]))

    assert_df_equality(actual_data, expected_data)
    logger.info("Test for drop_columns function passed.")

def test_drop_columns():
    """
    Test for drop_columns function.

    This function tests whether drop_columns function correctly drops specified columns from the DataFrame.
    """
    input_data = spark.createDataFrame(client_table_values, schema=client_schema)
    actual_data = drop_columns(input_data, ['first_name', 'last_name'])

    expected_data = spark.createDataFrame([(1, 'first_name1@test.test', 'Netherlands'),
                                           (2, 'first_name2@test.test', 'Netherlands'),
                                           (3, 'first_name3@test.test', 'United Kingdom'),
                                           (4, 'first_name4@test.test', 'France')],
                                          schema=StructType([StructField('id', IntegerType(), True),
                                                             StructField('email', StringType(), True),
                                                             StructField('country', StringType(), True)]))

    assert_df_equality(actual_data, expected_data)
    logger.info("Test for drop_columns function passed.")

def test_filter_by_field():
    """
    Test for filter_by_field function.

    This function tests whether filter_by_field function correctly filters DataFrame based on specified field and value.
    """
    input_data = spark.createDataFrame(client_table_values, schema=client_schema)
    actual_data = filter_by_field(input_data, 'country', 'United Kingdom')

    expected_data = spark.createDataFrame([(3, 'first_name3', 'last_name3', 'first_name3@test.test', 'United Kingdom')],
                                          schema=client_schema)

    assert_df_equality(actual_data, expected_data)
    logger.info("Test for filter_by_field function passed.")

def test_filter_by_fields():
    """
    Test for filter_by_fields function.

    This function tests whether filter_by_field function correctly filters DataFrame based on multiple values.
    """
    input_data = spark.createDataFrame(client_table_values, schema=client_schema)
    actual_data = filter_by_field(input_data, 'country', ['Netherlands', 'France'])

    expected_data = spark.createDataFrame([(1, 'first_name1', 'last_name1', 'first_name1@test.test', 'Netherlands'),
                                           (2, 'first_name2', 'last_name2', 'first_name2@test.test', 'Netherlands'),
                                           (4, 'first_name4', 'last_name4', 'first_name4@test.test', 'France')],
                                          schema=client_schema)

    assert_df_equality(actual_data, expected_data)
    logger.info("Test for filter_by_fields function passed.")

def test_join_datasets():
    """
    Test for join_datasets function.

    This function tests whether join_datasets function correctly joins two DataFrames based on a common key.
    """
    client_data = spark.createDataFrame(client_table_values, schema=client_schema)
    financial_data = spark.createDataFrame(financial_table_values, schema=financial_schema)
    actual_data = join_datasets(financial_data, client_data, "id")
    
    expected_data = spark.createDataFrame([(1, 'btc_a1', 'visa-electron', 12345678901, 'first_name1', 'last_name1', 'first_name1@test.test', 'Netherlands'),
                                           (2, 'btc_a2', 'jcb', 12345678902, 'first_name2', 'last_name2', 'first_name2@test.test', 'Netherlands'),
                                           (3, 'btc_a3', 'diners-club-enroute', 12345678903, 'first_name3', 'last_name3', 'first_name3@test.test', 'United Kingdom'),
                                           (4, 'btc_a4', 'switch', 12345678904, 'first_name4', 'last_name4', 'first_name4@test.test', 'France')],
                                           schema=StructType([StructField('id', IntegerType(), True),
                                                              StructField('btc_a', StringType(), True),
                                                              StructField('cc_t', StringType(), True),
                                                              StructField('cc_n', LongType(), True),
                                                              StructField('first_name', StringType(), True),
                                                              StructField('last_name', StringType(), True),
                                                              StructField('email', StringType(), True),
                                                              StructField('country', StringType(), True)]))

    assert_df_equality(actual_data, expected_data)
    logger.info("Test for join_datasets function passed.")

def test_rename_columns():
    """
    Test for rename_columns function.

    This function tests whether rename_columns function correctly renames columns of the DataFrame.
    """
    input_data = spark.createDataFrame(financial_table_values, schema=financial_schema)
    actual_data = rename_columns(input_data, {'id': 'id', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type', 'cc_n': 'credit_card_number'})
    
    expected_data = spark.createDataFrame(financial_table_values ,
                                          schema=StructType([
                                                 StructField('id', IntegerType(), True),
                                                 StructField('bitcoin_address', StringType(), True),
                                                 StructField('credit_card_type', StringType(), True),
                                                 StructField('credit_card_number', LongType(), True),
                                          ]))

    assert_df_equality(actual_data, expected_data)
    logger.info("Test for rename_columns function passed.")

def test_clean_data():
    """
    Test for clean_data function.

    This function tests whether the general clean_data function correctly cleans and processes the input data by combining all of the steps.
    """
    client_data = spark.createDataFrame(client_table_values, schema=client_schema)
    financial_data = spark.createDataFrame(financial_table_values, schema=financial_schema)
    actual_data = clean_data(client_data, financial_data, "Netherlands,France")

    expected_data = spark.createDataFrame([(1, 'btc_a1', 'visa-electron', 'first_name1@test.test', 'Netherlands'),
                                           (2, 'btc_a2', 'jcb', 'first_name2@test.test', 'Netherlands'),
                                           (4, 'btc_a4', 'switch', 'first_name4@test.test', 'France')],
                                          schema=output_schema)
    
    assert_df_equality(actual_data, expected_data)
    logger.info("Test for clean_data function passed.")

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("Data Processor Test").getOrCreate()
    
    try:
        logger.info("Starting tests...")
        test_functions = [
            test_drop_column,
            test_drop_columns,
            test_filter_by_field,
            test_filter_by_fields,
            test_join_datasets,
            test_rename_columns,
            test_clean_data
        ]

        # Keep track of how many tests are completed succesfully
        total_tests = len(test_functions)
        successful_tests = 0

        for test_function in test_functions:
            try:
                test_function()
                successful_tests += 1
                logger.info(f"Test {test_function.__name__} passed successfully.")
            except Exception as e:
                logger.error(f"Test {test_function.__name__} failed: {str(e)}")
        
        logger.info(f"All tests executed. {successful_tests}/{total_tests} tests passed successfully.")
    finally:
        # Stop Spark session
        spark.stop()

    logger.info("Finished testing")