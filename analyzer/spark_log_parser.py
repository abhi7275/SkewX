import json

def parse_spark_log(log_file_path):
    """
    Parses the Spark job log to extract relevant partition data.
    
    Args:
        log_file_path (str): The path to the Spark job log (in JSON format).
    
    Returns:
        list: A list of dictionaries, each representing a partition's details.
    """
    with open(log_file_path, 'r') as file:
        log_data = json.load(file)
    
    partitions = []  # List to store partition data
    
    # Iterate over the stages in the log and extract partition info
    for stage in log_data.get('stages', []):
        partitions.append({
            'stage_id': stage['stageId'],  # Unique ID for the stage
            'num_partitions': stage['numPartitions'],  # Number of partitions in the stage
            'partition_data': stage['partitionData']  # Partition data (keys and values)
        })
    
    return partitions
