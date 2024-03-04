import argparse
import logging
import os
from pyspark.sql import SparkSession

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data(spark, client_data_path, financial_data_path):
    """
    Load client and financial data from CSV files.
    """
    logger.info("Loading data")
    client_data = spark.read.csv(client_data_path, header=True)
    financial_data = spark.read.csv(financial_data_path, header=True)
    return client_data, financial_data

def drop_columns(df, columns):
    """
    Drop specified columns from the DataFrame.
    """
    logger.info(f"Dropping columns: {columns}")
    return df.drop(*columns)

def filter_by_field(df, field_name, value):
    """
    Filter DataFrame to include only rows where the specified field matches the given value.
    """
    logger.info(f"Filtering data where {field_name} is {value}")
    return df.filter(df[field_name] == value)

def join_datasets(df1, df2, on_column):
    """
    Join two DataFrames based on the specified column.
    """
    logger.info(f"Joining datasets on column: {on_column}")
    return df1.join(df2, on_column)

def rename_columns(df, column_mapping):
    """
    Rename columns in the DataFrame based on the specified mapping.
    """
    logger.info(f"Start renaming columns")
    for old_name, new_name in column_mapping.items():
        logger.info(f"Renaming column {old_name} with {new_name}")
        df = df.withColumnRenamed(old_name, new_name)
    return df

def clean_data(client_data, financial_data, countries):
    """
    Clean and filter the data on the requested constraints.
    """

    # Clean the client data
    logger.info("Cleaning client data")
    client_data_cleaned = client_data.drop('first_name', 'last_name')

    # Cleaning financial data
    logger.info("Cleaning financial data")
    financial_data_cleaned = drop_columns(financial_data, ['cc_n'])
    
    # Filter client data by countries
    filtered_client_data = client_data_cleaned.filter(client_data_cleaned['country'].isin(countries.split(',')))

    # Join the data sets
    joined_data = join_datasets(financial_data_cleaned, filtered_client_data, "id")

    # Remap the columns
    column_mapping = {'client_identifier': 'id', 'bitcoin_address': 'btc_a', 'cc_t': 'credit_card_type'}
    cleaned_data = rename_columns(joined_data, column_mapping)

    return cleaned_data

def save_data(df, output_path):
    """
    Save the processed data to a directory.
    """

    logger.info("Saving data...")
    df.write.mode("overwrite").csv(output_path)

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Process client data")
    parser.add_argument("client_data_path", type=str, help="(Relative) path to client data CSV file")
    parser.add_argument("financial_data_path", type=str, help="(Relative) path to financial data CSV file")
    parser.add_argument("countries", type=str, help="Comma-separated list of countries to filter")
    args = parser.parse_args()

    # Set the relative paths with the current working directory
    current_dir = os.getcwd()
    client_data_path = os.path.join(current_dir, args.client_data_path)
    financial_data_path = os.path.join(current_dir, args.financial_data_path)

    # Initialize Spark session
    spark = SparkSession.builder \
                        .appName("PySpark Client Data Processor") \
                        .getOrCreate()

    # Load in the data
    client_data, financial_data = load_data(spark, args.client_data_path, args.financial_data_path)

    # Clean and filter the data
    processed_data = clean_data(client_data, financial_data, args.countries)

    # Save data
    save_data(processed_data, "output/")

    # Stop Spark session
    spark.stop()

    logger.info("Processing completed.")
