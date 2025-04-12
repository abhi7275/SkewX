def generate_suggestions(zipf_coefficient, sorted_freq):
    """
    Generates optimization suggestions based on Zipf coefficient and key frequency distribution.

    Args:
        zipf_coefficient (float): The Zipf coefficient indicating skew severity.
        sorted_freq (list): List of tuples [(key, frequency)] sorted in descending order.

    Returns:
        list: List of suggestion strings.
    """
    suggestions = []

    # Calculate dominance of top key (frequency percentage)
    total_frequency = sum(freq for _, freq in sorted_freq)
    top_key_freq = sorted_freq[0][1] if sorted_freq else 0
    top_key_ratio = top_key_freq / total_frequency if total_frequency > 0 else 0

    # Case 1: Low skew
    if zipf_coefficient < 1.2:
        suggestions.append("Data seems well balanced across partitions. No immediate optimization is necessary.")
        suggestions.append("Keep monitoring the workload as the data grows to detect any future skew.")

    # Case 2: Moderate skew
    elif 1.2 <= zipf_coefficient <= 1.6:
        suggestions.append("Moderate skew detected. Consider salting high-frequency keys to spread them across partitions.")
        suggestions.append("Try increasing the number of partitions using `.repartition()` for improved load distribution.")

    # Case 3: High skew
    else:
        suggestions.append("High skew detected. Use custom partitioning to spread workload more evenly.")
        suggestions.append("Apply skew handling techniques like broadcast joins or map-side aggregations.")

    # Additional suggestion based on key dominance
    if top_key_ratio > 0.5:
        suggestions.append("Top key dominates more than 50% of the workload. Use salting or split heavy keys across partitions.")

    # If total number of keys is very small
    if len(sorted_freq) < 5:
        suggestions.append("Very few unique keys. Skew may be natural due to limited diversity. Consider adjusting logic based on domain.")

    return suggestions
