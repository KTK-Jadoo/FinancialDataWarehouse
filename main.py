# main.py

from etl.extract import load_nifty50_data
import os

# Set the base directory path to the 'data' folder
base_dir = r"data\Nifty Fifty Master Data"

# Print the base directory to ensure it's correctly set
print(f"Base Directory: {os.path.abspath(base_dir)}")
print("Checking directory contents...")

# Walk through the directory structure and print all files found
for root, dirs, files in os.walk(base_dir):
    print(f"Current Directory: {root}")
    if files:
        print(f"Files: {files}")
    else:
        print("No files found in this directory.")

# Load the data using the extract function
nifty50_df = load_nifty50_data(base_dir)

# Check the schema and show the first few rows
nifty50_df.printSchema()
nifty50_df.show(5)
