import json
import pandas as pd

def parse_spark_log(log_data):
    """
    Parses the Spark job log to extract relevant partition data from either 
    a file or a dictionary passed in the `log_data`.
    
    Args:
        log_data (dict or pd.DataFrame): The Spark job log data, either 
                                         as a dictionary (from JSON) 
                                         or a pandas DataFrame (from CSV).
    
    Returns:
        list: A list of dictionaries, each representing a partition's details.
    """
    
    partitions = []  # List to store partition data

    # If the input is a pandas DataFrame (CSV file)
    if isinstance(log_data, pd.DataFrame):
        for _, row in log_data.iterrows():
            partitions.append({
                'stage_id': row['stageId'],  
                'num_partitions': row['numPartitions'],  
                'partition_data': row['partitionData']  # Assumes partition data is stored as a list in CSV
            })

    # If the input is a dictionary (JSON or text file parsed into a dictionary)
    elif isinstance(log_data, dict):
        # Iterate over the stages in the log and extract partition info
        for stage in log_data.get('stages', []):
            partitions.append({
                'stage_id': stage['stageId'],  # Unique ID for the stage
                'num_partitions': stage['numPartitions'],  # Number of partitions in the stage
                'partition_data': stage.get('partitionData', [])  # Partition data (keys and values)
            })
    
    return partitions
