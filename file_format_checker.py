import json
import pandas as pd
import streamlit as st

# CSV file check
def check_csv(file):
    required_columns = {'numPartitions', 'partitionData', 'stageId'}
    file_columns = set(file.columns)
    
    missing_columns = required_columns - file_columns
    if missing_columns:
        st.error(f"CSV is missing one or more required columns: {missing_columns}")
        st.info("""
            **Suggestions for Correct File Format (CSV):**
            - Ensure the CSV file contains the following columns: 
              - `numPartitions`: Number of partitions in the Spark job.
              - `partitionData`: Data corresponding to each partition.
              - `stageId`: The ID of the Spark stage.
            
            **Example CSV Format:**
            ```
            stageId,numPartitions,partitionData
            1,10,[{"key": "A", "value": 100}, {"key": "B", "value": 200}]
            2,15,[{"key": "A", "value": 300}, {"key": "C", "value": 150}]
            ```
        """)
        return False
    return True

# JSON file check
def check_json(file):
    required_keys = {'numPartitions', 'partitionData', 'stageId'}
    try:
        data = json.load(file)
    except json.JSONDecodeError:
        st.error("Invalid JSON format. Please check the file.")
        return False
    
    # Assuming JSON is a list of records
    missing_keys = required_keys - set(data[0].keys())
    if missing_keys:
        st.error(f"JSON is missing one or more required keys: {missing_keys}")
        st.info("""
            **Suggestions for Correct File Format (JSON):**
            - Ensure each record in the JSON contains the following keys: 
              - `numPartitions`: Number of partitions in the Spark job.
              - `partitionData`: Data corresponding to each partition.
              - `stageId`: The ID of the Spark stage.
            
            **Example JSON Format:**
            ```json
            [
                {"stageId": 1, "numPartitions": 10, "partitionData": [{"key": "A", "value": 100}]},
                {"stageId": 2, "numPartitions": 15, "partitionData": [{"key": "B", "value": 200}]}
            ]
            ```
        """)
        return False
    return True

# Text file check
def check_text(file):
    required_patterns = ['numPartitions', 'partitionData', 'stageId']
    missing_patterns = [pattern for pattern in required_patterns if pattern not in file.read()]
    
    if missing_patterns:
        st.error(f"Text file is missing one or more required patterns: {missing_patterns}")
        st.info("""
            **Suggestions for Correct File Format (Text):**
            - Ensure the text file contains the following patterns:
              - `numPartitions`: Number of partitions in the Spark job.
              - `partitionData`: Data corresponding to each partition.
              - `stageId`: The ID of the Spark stage.
            
            **Example Text Format:**
            ```
            stageId: 1, numPartitions: 10, partitionData: [{"key": "A", "value": 100}]
            stageId: 2, numPartitions: 15, partitionData: [{"key": "B", "value": 200}]
            ```
        """)
        return False
    return True

# Main function to check the format
def check_file_format(file, file_type):
    if file_type == 'csv':
        return check_csv(file)
    elif file_type == 'json':
        return check_json(file)
    elif file_type == 'text':
        return check_text(file)
    else:
        st.error("Unsupported file type. Please upload a CSV, JSON, or Text file.")
        return False
