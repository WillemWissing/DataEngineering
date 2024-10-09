"""
CSV Subset Creator

This script reads a CSV file, creates a random subset of the specified size using pandas,
and writes the subset to a new CSV file. It ensures reproducibility by using a fixed random seed.
"""

import pandas as pd

# File paths
input_file = 'dataset.csv'          # Path to the input CSV file
output_file = 'data_subset_2m.csv'  # Path to the output CSV file

def create_subset(input_file, output_file, subset_size):
    """
    Create a random subset of a CSV file and save it to a new CSV file.

    Args:
        input_file (str): The path to the input CSV file.
        output_file (str): The path to the output CSV file.
        subset_size (int): The number of records in the subset.
    """
    try:
        # Read the entire CSV file into a pandas DataFrame
        df = pd.read_csv(input_file)
        
        # Create a random subset of the DataFrame with the specified size
        subset = df.sample(n=subset_size, random_state=1)  # Random sampling with a fixed seed for reproducibility
        
        # Write the subset DataFrame to a new CSV file
        subset.to_csv(output_file, index=False)
        
        print(f'Successfully created subset CSV file: {output_file}')

    except Exception as e:
        print(f'Error creating subset CSV file: {e}')

if __name__ == '__main__':
    subset_size = 2000000  # Number of records in the subset

    # Create the subset
    create_subset(input_file, output_file, subset_size)
