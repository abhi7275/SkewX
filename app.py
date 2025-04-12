import streamlit as st
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from io import StringIO
from analyzer.spark_log_parser import parse_spark_log
from analyzer.zipf_detector import zipf_detect
from analyzer.suggestions import generate_suggestions

def main():
    """
    Main function to run the Streamlit web application. It allows the user to upload a 
    Spark job log, analyze skew, and get optimization suggestions.
    """
    st.set_page_config(page_title="SkewX - Data Skew Optimizer", layout="wide")
    
    # Sidebar for file upload
    with st.sidebar:
        st.header("File Upload")
        uploaded_file = st.file_uploader("Upload your Spark job log (JSON)", type="json")
        st.write("---")
        st.header("About")
        st.write(
            "SkewX is a Data Skew Optimizer that analyzes Spark job logs to detect data skew, "
            "calculate the Zipf coefficient, and provide optimization suggestions for better "
            "load balancing and partitioning."
        )
        st.write("---")
        st.write(
            "### What is Zipf's Coefficient?"
        )
        st.write(
            "Zipf's Law is a principle that describes the frequency distribution of events in many types of data. "
            "In the context of Spark job logs, Zipf's Coefficient helps quantify how unevenly data is distributed "
            "across partitions. A higher Zipf coefficient suggests a more uneven distribution, meaning some "
            "partitions may have significantly more data than others. "
            "This can lead to inefficiencies in processing, as some partitions may become overloaded while others remain underutilized."
        )
        st.write(
            "The Zipf coefficient ranges between 1 and infinity: "
            "- A coefficient close to 1 means a more uniform distribution of data across partitions. "
            "- A higher coefficient indicates increasing skew, where some partitions carry much more data than others."
        )
    
    if uploaded_file is not None:
        # Read the uploaded file
        log_data = json.load(uploaded_file)
        
        # Parse Spark job log to extract partition data
        partitions = parse_spark_log(log_data)
        
        # Extract all keys from partition data for Zipf detection
        all_keys = [item['key'] for part in partitions for item in part['partition_data']]
        
        # Detect Zipf coefficient and frequencies of keys
        zipf_coefficient, sorted_freq = zipf_detect(all_keys)
        
        # Show summary stats
        total_partitions = sum([part['num_partitions'] for part in partitions])
        total_keys = len(all_keys)
        
        # Display summary stats in columns
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Partitions", total_partitions)
        with col2:
            st.metric("Total Keys", total_keys)
        
        # Display Zipf coefficient value
        st.write(f"Zipf Coefficient: {zipf_coefficient}")
        
        # Generate optimization suggestions based on Zipf coefficient and skew
        suggestions = generate_suggestions(zipf_coefficient, sorted_freq[:10])
        
        # Display optimization suggestions in an expander
        # Display optimization suggestions in an interactive, categorized format
        with st.expander("⚙️ Optimization Suggestions"):
            for suggestion in suggestions:
                if suggestion.startswith("✅"):
                    st.success(suggestion)
                elif suggestion.startswith("⚠️") or suggestion.startswith("🔁"):
                    st.warning(suggestion)
                elif suggestion.startswith("🔥") or suggestion.startswith("🚨"):
                    st.error(suggestion)
                else:
                    st.info(suggestion)

        
        # Show key frequency distribution
        key_frequencies = [item[1] for item in sorted_freq[:10]]
        keys = [item[0] for item in sorted_freq[:10]]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.barplot(x=keys, y=key_frequencies, ax=ax, palette="viridis")
        ax.set_title("Top 10 Key Frequencies", fontsize=16)
        ax.set_xlabel("Keys", fontsize=12)
        ax.set_ylabel("Frequency", fontsize=12)
        st.pyplot(fig)
        
        # Show key distribution as a heatmap
        key_count = {key: freq for key, freq in zip(keys, key_frequencies)}
        key_df = pd.DataFrame(list(key_count.items()), columns=["Key", "Frequency"])
        
        # Create heatmap
        st.write("Key Frequency Distribution Heatmap")
        fig, ax = plt.subplots(figsize=(10, 2))
        sns.heatmap(key_df.set_index("Key").T, annot=True, cmap="YlGnBu", cbar=False, ax=ax)
        st.pyplot(fig)
        
        # Add a download button for results
        result_df = pd.DataFrame(sorted_freq, columns=["Key", "Frequency"])
        result_df["Rank"] = result_df.index + 1
        csv = result_df.to_csv(index=False)
        st.download_button("Download Results as CSV", csv, file_name="skewx_results.csv", mime="text/csv")
    
if __name__ == '__main__':
    main()
