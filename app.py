import streamlit as st
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from io import StringIO
from analyzer.spark_log_parser import parse_spark_log
from analyzer.zipf_detector import zipf_detect
from analyzer.suggestions import generate_suggestions
from file_format_checker import check_file_format

def main():
    st.set_page_config(page_title="SkewX - Data Skew Optimizer", layout="centered")  # Changed layout to centered

    # Sidebar
    with st.sidebar:
        st.header("File Upload")
        uploaded_file = st.file_uploader("Upload Spark log file", type=["json", "csv", "txt"])
        st.write("---")
        st.header("About")
        st.write(
            "SkewX is a Data Skew Optimizer that analyzes Spark job logs to detect data skew, "
            "calculate the Zipf coefficient, and provide optimization suggestions for better "
            "load balancing and partitioning."
        )
        st.write("---")
        st.write("### What is Zipf's Coefficient?")
        st.write(
            "Zipf's Law describes how data values are distributed — a few keys occur very frequently, "
            "while many appear rarely. SkewX calculates the Zipf Coefficient to quantify this unevenness. "
            "Higher Zipf Coefficients indicate greater skew."
        )
        st.write(
            "- Coefficient ≈ 1: Balanced partitioning\n"
            "- Coefficient > 1.2: Increasing skew — may require optimization\n"
            "- Coefficient >> 1.5: Severe skew — high risk of Spark bottlenecks"
        )
    
    if uploaded_file is not None:
        file_type = uploaded_file.name.split('.')[-1].lower()
        if not check_file_format(uploaded_file, file_type):
            st.stop()  
        try:
            if file_type == 'csv':
                df = pd.read_csv(uploaded_file)
                required_cols = {'stageId', 'numPartitions', 'partitionData'}
                if not required_cols.issubset(df.columns):
                    st.error(f"CSV is missing one or more required columns: {required_cols}")
                    return
                log_data = df.to_dict(orient='records')
                partitions = parse_spark_log(log_data)

            elif file_type == 'json':
                log_data = json.load(uploaded_file)
                partitions = parse_spark_log(log_data)

            elif file_type == 'txt':
                content = uploaded_file.read().decode("utf-8")
                try:
                    log_data = json.loads(content)
                except json.JSONDecodeError:
                    st.error("TXT file is not in valid JSON format.")
                    return
                partitions = parse_spark_log(log_data)

            else:
                st.error("Unsupported file type. Please upload a CSV, JSON, or TXT file.")
                return

            if not partitions:
                st.warning("No valid partition data found.")
                return

            # Extract all keys for Zipf analysis
            all_keys = [item['key'] for part in partitions for item in part['partition_data']]
            zipf_coefficient, sorted_freq = zipf_detect(all_keys)

            total_partitions = sum([part['num_partitions'] for part in partitions])
            total_keys = len(all_keys)

            # Use columns for layout and centering content
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Partitions", total_partitions)
            with col2:
                st.metric("Total Keys", total_keys)

            st.subheader("📈 Zipf Coefficient")
            st.write(f"**{zipf_coefficient:.4f}**")

            suggestions = generate_suggestions(zipf_coefficient, sorted_freq[:10])

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

            # Bar plot for the top 10 keys
            key_frequencies = [item[1] for item in sorted_freq[:10]]
            keys = [item[0] for item in sorted_freq[:10]]

            fig, ax = plt.subplots(figsize=(10, 6))
            sns.barplot(x=keys, y=key_frequencies, ax=ax, hue=keys, palette="viridis")
            ax.set_title("Top 10 Key Frequencies", fontsize=16)
            ax.set_xlabel("Keys", fontsize=12)
            ax.set_ylabel("Frequency", fontsize=12)
            st.pyplot(fig)

            # Heatmap for key frequencies
            key_df = pd.DataFrame({'Key': keys, 'Frequency': key_frequencies})
            st.write("Key Frequency Distribution Heatmap")
            fig, ax = plt.subplots(figsize=(10, 2))
            sns.heatmap(key_df.set_index("Key").T, annot=True, cmap="YlGnBu", cbar=False, ax=ax)
            st.pyplot(fig)

            # Download the results as CSV
            result_df = pd.DataFrame(sorted_freq, columns=["Key", "Frequency"])
            result_df["Rank"] = result_df.index + 1
            csv = result_df.to_csv(index=False)
            st.download_button("Download Results as CSV", csv, file_name="skewx_results.csv", mime="text/csv")

        except Exception as e:
            st.error(f"Error processing file: {e}")

if __name__ == '__main__':
    main()
