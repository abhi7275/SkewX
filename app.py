# In app.py
import streamlit as st
import json
from analyzer.spark_log_parser import parse_spark_log
from analyzer.zipf_detector import zipf_detect
from analyzer.suggestions import generate_suggestions

def main():
    """
    Main function to run the Streamlit web application. It allows the user to upload a 
    Spark job log, analyze skew, and get optimization suggestions.
    """
    st.title('SkewX - Data Skew Optimizer')

    # File uploader for Spark job log file
    uploaded_file = st.file_uploader("Upload your Spark job log (JSON)", type="json")
    
    if uploaded_file is not None:
        # Read the uploaded file
        log_data = json.load(uploaded_file)
        
        # Parse Spark job log to extract partition data
        partitions = parse_spark_log(log_data)
        
        # Extract all keys from partition data for Zipf detection
        all_keys = [item['key'] for part in partitions for item in part['partition_data']]
        
        # Detect Zipf coefficient and frequencies of keys
        zipf_coefficient, sorted_freq = zipf_detect(all_keys)
        
        # Display Zipf coefficient value
        st.write(f"Zipf Coefficient: {zipf_coefficient}")
        
        # Generate optimization suggestions based on Zipf coefficient and skew
        suggestions = generate_suggestions(zipf_coefficient, sorted_freq[:10])
        
        # Display optimization suggestions to the user
        st.write("Optimization Suggestions:")
        for suggestion in suggestions:
            st.write(f"- {suggestion}")
    
if __name__ == '__main__':
    main()
