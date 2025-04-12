import random
import json

def generate_large_mock_log(num_stages=10, max_partitions=20, max_keys=6):
    """
    Generates a large mock Spark log with a specified number of stages and partitions.
    
    Args:
        num_stages (int): Number of stages in the mock log.
        max_partitions (int): Maximum number of partitions per stage.
        max_keys (int): Number of distinct keys in the log.
    
    Returns:
        dict: A mock Spark log.
    """
    keys = ['a', 'b', 'c', 'd', 'e', 'f'][:max_keys]  # Can add more keys if needed
    log = {
        "stages": []
    }

    for stage_id in range(1, num_stages + 1):  # Loop through the stages
        num_partitions = random.randint(5, max_partitions)  # Random partitions per stage
        
        stage = {
            "stageId": stage_id,
            "numPartitions": num_partitions,
            "partitionData": []
        }
        
        for _ in range(num_partitions):
            key = random.choice(keys)  # Randomly choose a key
            value = random.randint(50, 150)  # Random value for the key
            stage["partitionData"].append({"key": key, "value": value})
        
        log["stages"].append(stage)
    
    return log

# Generate and save the large mock log to file
log_data = generate_large_mock_log(num_stages=15, max_partitions=30, max_keys=10)
with open("large_mock_spark_log.json", "w") as f:
    json.dump(log_data, f, indent=4)

print("Large mock log data has been generated and saved as 'large_mock_spark_log.json'.")
