import json
import pandas as pd
import streamlit as st
import io
# CSV file check
def check_csv(file) -> bool:
    try:
        file.seek(0)
        decoded = file.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(decoded), quoting=csv.QUOTE_MINIMAL)
        required = {"stageId", "numPartitions", "partitionData"}
        return required.issubset(set(df.columns))
    except Exception as e:
        st.error(f"CSV check failed: {e}")
        return False


# JSON file check
def check_json(file):
    required_keys = {'numPartitions', 'partitionData', 'stageId'}
    try:
        file.seek(0)
        data = json.load(file)
        st.json(data if isinstance(data, list) else {"sample": data})
    except json.JSONDecodeError:
        st.error("Invalid JSON format.")
        return False
    except Exception as e:
        st.error(f"JSON read error: {e}")
        return False

    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        missing_keys = required_keys - set(data[0].keys())
        if missing_keys:
            st.error(f"JSON is missing keys: {missing_keys}")
            return False
        return True
    else:
        st.error("Unexpected JSON structure. Expecting a list of objects.")
        return False


# Text file check
def check_text(file):
    try:
        file.seek(0)  # 👈 Reset file pointer
        content = file.read().decode("utf-8") if hasattr(file, "read") else str(file)
    except Exception as e:
        st.error(f"Text read error: {e}")
        return False

    required_patterns = ['numPartitions', 'partitionData', 'stageId']
    missing_patterns = [pattern for pattern in required_patterns if pattern not in content]

    if missing_patterns:
        st.error(f"Text file is missing one or more required patterns: {missing_patterns}")
        st.info("""
            **Suggestions for Correct File Format (Text):**
            - Text must include:
              - `numPartitions`
              - `partitionData`
              - `stageId`
            **Example Format:**
            ```
            stageId: 1, numPartitions: 10, partitionData: [{"key": "A", "value": 100}]
            stageId: 2, numPartitions: 15, partitionData: [{"key": "B", "value": 200}]
            ```
        """)
        return False

    return True

# Main file format checker
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
