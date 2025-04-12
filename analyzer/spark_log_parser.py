import json
import pandas as pd

def parse_spark_log(log_data):
    """
    Parses the Spark job log to extract relevant partition data from either 
    a file or a dictionary passed in the `log_data`.

    Args:
        log_data (dict or pd.DataFrame): The Spark job log data, either 
                                         as a dictionary (from JSON or text) 
                                         or a pandas DataFrame (from CSV).
    
    Returns:
        list: A list of dictionaries, each representing a partition's details.
              Returns empty list if required fields are missing.
    """
    
    partitions = []

    # If input is a DataFrame (from CSV)
    if isinstance(log_data, pd.DataFrame):
        required_cols = {'stageId', 'numPartitions', 'partitionData'}
        if not required_cols.issubset(log_data.columns):
            print(f"[ERROR] CSV file is missing one or more required columns: {required_cols}")
            return []

        for _, row in log_data.iterrows():
            partitions.append({
                'stage_id': row['stageId'],
                'num_partitions': row['numPartitions'],
                'partition_data': row['partitionData']
            })

    # If input is a dict (from JSON or TXT parsed to dict)
    elif isinstance(log_data, dict):
        stages = log_data.get('stages', [])
        if not stages:
            print("[WARNING] No 'stages' key found in the uploaded JSON/text log.")
            return []

        for stage in stages:
            if 'stageId' in stage and 'numPartitions' in stage:
                partitions.append({
                    'stage_id': stage['stageId'],
                    'num_partitions': stage['numPartitions'],
                    'partition_data': stage.get('partitionData', [])
                })
            else:
                print("[WARNING] One or more stages missing 'stageId' or 'numPartitions'. Skipping stage.")

    else:
        print("[ERROR] Unsupported log format. Expecting CSV (DataFrame) or JSON/TXT (dict).")
        return []

    return partitions
