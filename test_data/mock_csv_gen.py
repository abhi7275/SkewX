import pandas as pd
import random
import json

def generate_partition_data(num_partitions, skew=False):
    if skew:
        # One partition has very large size, rest are small
        skewed = [random.randint(5000, 10000)] + [random.randint(50, 200) for _ in range(num_partitions - 1)]
        random.shuffle(skewed)
        return json.dumps(skewed)
    else:
        # Uniform partition sizes
        return json.dumps([random.randint(100, 300) for _ in range(num_partitions)])

records = []
for i in range(1000):
    stage_id = i
    num_partitions = random.randint(5, 20)
    skew = random.random() < 0.3  # ~30% records will be skewed
    partition_data = generate_partition_data(num_partitions, skew=skew)
    
    records.append({
        "stageId": stage_id,
        "numPartitions": num_partitions,
        "partitionData": partition_data
    })

df = pd.DataFrame(records)
df.to_csv("mock_spark_log.csv", index=False)
print("✅ mock_spark_log.csv (1000 rows) generated successfully.")
