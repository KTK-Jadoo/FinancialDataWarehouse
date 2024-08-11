# etl/extract.py

from pyspark.sql import SparkSession
import os

def load_nifty50_data(base_dir, limit=5):
    """
    Load a limited number of NIFTY 50 stock data CSV files into a Spark DataFrame.

    :param base_dir: str - The base directory containing CSV files.
    :param limit: int - The maximum number of files to load.
    :return: pyspark.sql.DataFrame - A DataFrame containing the loaded stock data.
    """
    spark = SparkSession.builder \
        .appName("Nifty50StockAnalysis") \
        .getOrCreate()

    # Load a limited number of CSV files
    files = os.listdir(base_dir)[:limit]  # Limit the number of files
    paths = [os.path.join(base_dir, file) for file in files]

    all_files_df = spark.read.csv(paths, header=True, inferSchema=True)
    return all_files_df